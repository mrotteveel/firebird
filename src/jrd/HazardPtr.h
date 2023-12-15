/*
 *	PROGRAM:	Engine Code
 *	MODULE:		HazardPtr.h
 *	DESCRIPTION:	Use of hazard pointers in metadata cache
 *
 *  The contents of this file are subject to the Initial
 *  Developer's Public License Version 1.0 (the "License");
 *  you may not use this file except in compliance with the
 *  License. You may obtain a copy of the License at
 *  http://www.ibphoenix.com/main.nfs?a=ibphoenix&page=ibp_idpl.
 *
 *  Software distributed under the License is distributed AS IS,
 *  WITHOUT WARRANTY OF ANY KIND, either express or implied.
 *  See the License for the specific language governing rights
 *  and limitations under the License.
 *
 *  The Original Code was created by Alexander Peshkov
 *  for the Firebird Open Source RDBMS project.
 *
 *  Copyright (c) 2021 Alexander Peshkov <peshkoff@mail.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 *
 *
 */

#ifndef JRD_HAZARDPTR_H
#define JRD_HAZARDPTR_H

#define HZ_DEB(A)

#include "../common/classes/alloc.h"
#include "../common/classes/array.h"
#include "../common/gdsassert.h"
#include "fb_blk.h"

#include <cds/gc/dhp.h>
#include <cds/algo/atomic.h>

#include <type_traits>

#include "../jrd/tdbb.h"
#include "../jrd/Database.h"

namespace Jrd {

	class HazardObject
	{
	protected:
		void retire()
		{
			struct Disposer
			{
				void operator()(HazardObject* ho)
				{
					fb_assert(ho);
					delete ho;
				}
			};

			cds::gc::DHP::retire<Disposer>(this);
		}

		virtual ~HazardObject();
	};

	template <typename T>
	class HazardPtr : private cds::gc::DHP::Guard
	{
		typedef cds::gc::DHP::Guard inherited;
		static_assert(std::is_base_of<HazardObject, T>::value, "class derived from HazardObject should be used");

	public:
		HazardPtr() = default;

		HazardPtr(const atomics::atomic<T*>& from)
		{
			protect(from);
		}

		HazardPtr(const HazardPtr& copyFrom)
		{
			copy(copyFrom);
		}

		HazardPtr(HazardPtr&& moveFrom) = default;

		template <class T2>
		HazardPtr(const HazardPtr<T2>& copyFrom)
		{
			checkAssign<T2>();
			copy(copyFrom);
		}

		template <class T2>
		HazardPtr(HazardPtr<T2>&& moveFrom)
			: inherited(std::move(moveFrom))
		{
			checkAssign<T2>();
		}

		~HazardPtr()
		{ }

		T* getPointer() const
		{
			return get<T>();
		}

		T* releasePointer()
		{
			T* rc = get<T>();
			clear();
			return rc;
		}

		void set(const atomics::atomic<T*>& from)
		{
			protect(from);
		}

		/*/ atomically replaces 'where' with 'newVal', using *this as old value for comparison
		// always sets *this to actual data from 'where'
		bool replace(atomics::atomic<T*>& where, T* newVal)
		{
			T* val = get<T>();
			bool rc = where.compare_exchange_strong(val, newVal,
				std::memory_order_release, std::memory_order_acquire);
			assign(rc ? newVal : val);
			return rc;
		}
*/
		// atomically replaces 'where' with 'newVal', using *this as old value for comparison
		// sets *this to actual data from 'where' if replace failed
		bool replace2(atomics::atomic<T*>& where, T* newVal)
		{
			T* val = get<T>();
			bool rc = where.compare_exchange_strong(val, newVal,
				std::memory_order_release, std::memory_order_acquire);
			if(rc)
				assign(newVal);
			return rc;
		}

		void clear()
		{
			inherited::clear();
		}

		T* operator->()
		{
			return get<T>();
		}

		const T* operator->() const
		{
			return get<T>();
		}
/*
		template <typename R>
		R& operator->*(R T::*mem)
		{
			return (this->hazardPointer)->*mem;
		}
 */
		bool operator!() const
		{
			return !hasData();
		}

		bool hasData() const
		{
			return get_native() != nullptr;
		}

		bool operator==(const T* v) const
		{
			return get<T>() == v;
		}

		bool operator!=(const T* v) const
		{
			return get<T>() != v;
		}

		operator bool() const
		{
			return hasData();
		}

		HazardPtr& operator=(const HazardPtr& copyAssign)
		{
			copy(copyAssign);
			return *this;
		}

		HazardPtr& operator=(HazardPtr&& moveAssign)
		{
			inherited::operator=(std::move(moveAssign));
			return *this;
		}

		template <class T2>
		HazardPtr& operator=(const HazardPtr<T2>& copyAssign)
		{
			checkAssign<T2>();
			copy(copyAssign);
			return *this;
		}

		template <class T2>
		HazardPtr& operator=(HazardPtr<T2>&& moveAssign)
		{
			checkAssign<T2>();
			inherited::operator=(std::move(moveAssign));
			return *this;
		}

		void safePointer(T* ptr)
		{
			assign(ptr);
		}

	private:
		template <class T2>
		struct checkAssign
		{
			static_assert(std::is_trivially_assignable<T*&, T2*>::value, "Invalid type of pointer assigned");
		};
	};

	template <typename T>
	bool operator==(const T* v1, const HazardPtr<T> v2)
	{
		return v2 == v1;
	}

	template <typename T, typename T2>
	bool operator==(const T* v1, const HazardPtr<T2> v2)
	{
		return v1 == v2.getPointer();
	}

	template <typename T>
	bool operator!=(const T* v1, const HazardPtr<T> v2)
	{
		return v2 != v1;
	}


	// Shared read here means that any thread can read from vector using HP.
	// It can be modified only in single thread, and it's caller's responsibility
	// that modifying thread is single.

	template <typename T, FB_SIZE_T CAP>
	class SharedReadVector : public Firebird::PermanentStorage
	{
	public:
		class Generation : public HazardObject, public pool_alloc_rpt<T>
		{
		private:
			Generation(FB_SIZE_T size)
				: count(0), capacity(size)
			{ }

			FB_SIZE_T count, capacity;
			T data[1];

		public:
			static Generation* create(MemoryPool& p, FB_SIZE_T cap)
			{
				return FB_NEW_RPT(p, cap) Generation(cap);
			}

			FB_SIZE_T getCount() const
			{
				return count;
			}

			FB_SIZE_T getCapacity() const
			{
				return capacity;
			}

			const T& value(FB_SIZE_T i) const
			{
				fb_assert(i < count);
				return data[i];
			}

			T& value(FB_SIZE_T i)
			{
				fb_assert(i < count);
				return data[i];
			}

			bool hasSpace(FB_SIZE_T needs = 1) const
			{
				return count + needs <= capacity;
			}

			bool add(const Generation* from)
			{
				if (!hasSpace(from->count))
					return false;
				memcpy(&data[count], from->data, from->count * sizeof(T));
				count += from->count;
				return true;
			}

			T* add()
			{
				if (!hasSpace())
					return nullptr;
				return &data[count++];
			}

			void truncate(const T& notValue)
			{
				while (count && data[count - 1] == notValue)
					count--;
			}

			static void destroy(Generation* gen)
			{
				// delay delete - someone else may access it
				gen->retire();
			}
		};

		SharedReadVector(MemoryPool& p)
			: Firebird::PermanentStorage(p),
			  currentData(Generation::create(getPool(), CAP))
		{ }

		Generation* writeAccessor()
		{
			return currentData.load(std::memory_order_acquire);
		}

		HazardPtr<Generation> readAccessor() const
		{
			return HazardPtr<Generation>(currentData);
		}

		void grow(FB_SIZE_T newSize = 0)
		{
			for(;;)
			{
				HazardPtr<Generation> current(currentData);
				if (newSize && (current->getCapacity() >= newSize))
					return;

				FB_SIZE_T doubleSize = current->getCapacity() * 2;
				if (newSize > doubleSize)
					doubleSize = newSize;

				Generation* newGeneration = Generation::create(getPool(), doubleSize);
				Generation* oldGeneration = current.getPointer();
				newGeneration->add(oldGeneration);
				if (current.replace2(currentData, newGeneration))
				{
					Generation::destroy(oldGeneration);
					break;
				}
				else
				{
					// Use plain delete - this instance is not known to anybody else
					delete newGeneration;
				}
			}
		}

		~SharedReadVector()
		{
			Generation::destroy(currentData.load(std::memory_order_acquire));
		}

		void clear() { }	// NO-op, rely on dtor

	private:
		atomics::atomic<Generation*> currentData;
	};

	class thread_db;
	class CacheObject
	{
	public:
		typedef unsigned Flag;
		virtual bool checkObject(thread_db* tdbb, Firebird::Arg::StatusVector&) /*const*/;
		virtual void afterUnlock(thread_db* tdbb, unsigned flags);
		virtual void lockedExcl [[noreturn]] (thread_db* tdbb) /*const*/;
		virtual const char* c_name() const = 0;
	};

class ObjectBase : public HazardObject
{
public:
	enum ResetType {Recompile, Mark, Commit, Rollback};

	typedef SLONG ReturnedId;	// enable '-1' as not found

public:
	virtual void resetDependentObject(thread_db* tdbb, ResetType rt) = 0;
	virtual void eraseObject(thread_db* tdbb) = 0;			// erase object

public:
	void resetDependentObjects(thread_db* tdbb, TraNumber olderThan);
	void addDependentObject(thread_db* tdbb, ObjectBase* dep);
	void removeDependentObject(thread_db* tdbb, ObjectBase* dep);
	[[noreturn]] void busyError(thread_db* tdbb, MetaId id, const char* name);
};

namespace CacheFlag
{
	static const CacheObject::Flag COMMITTED =	0x01;
	static const CacheObject::Flag ERASED =		0x02;
	static const CacheObject::Flag NOSCAN =		0x04;
	static const CacheObject::Flag AUTOCREATE =	0x08;
	static const CacheObject::Flag INIT =		0x10;

	static const CacheObject::Flag IGNORE_MASK = COMMITTED | ERASED;
}


class VersionSupport
{
public:
	static MdcVersion next(thread_db* tdbb);
};

class CachePool
{
public:
	static MemoryPool& get(thread_db* tdbb);
/*
				Database* dbb = tdbb->getDatabase();
				return dbb->dbb_mdc->getPool();
*/
};

template <class OBJ>
class CacheList : public HazardObject
{
public:
	CacheList(OBJ* obj, TraNumber currentTrans, CacheObject::Flag fl)
		: object(obj), next(nullptr), traNumber(currentTrans), cacheFlags(fl)
	{ }

	~CacheList()
	{
		OBJ::destroy(object);
		delete next;
	}

	// find appropriate object in cache
	static OBJ* getObject(HazardPtr<CacheList>& listEntry, TraNumber currentTrans, CacheObject::Flag flags)
	{
		for (; listEntry; listEntry.set(listEntry->next))
		{
			CacheObject::Flag f(listEntry->cacheFlags.load() & ~(flags & CacheFlag::IGNORE_MASK));

			if ((f & CacheFlag::COMMITTED) ||
					// committed (i.e. confirmed) objects are freely available
				(currentTrans && (listEntry->traNumber == currentTrans)))
					// transaction that created an object can always access it
			{
				if (f & CacheFlag::ERASED)
				{
					// object does not exist
					fb_assert(!listEntry->object);
					return nullptr;
				}

				// required entry found in the list
				return listEntry->object;
			}
		}

		return nullptr;	// object created (not by us) and not committed yet
	}

	bool isBusy(TraNumber currentTrans) const
	{
		return traNumber != currentTrans && !(cacheFlags & CacheFlag::COMMITTED);
	}

	// add new entry to the list
	static bool add(atomics::atomic<CacheList*>& list, CacheList* newVal)
	{
		HazardPtr<CacheList> oldVal(list);

		do
		{
			if (oldVal && oldVal->isBusy(newVal->traNumber))	// modified in other transaction
				return false;
			newVal->next.store(oldVal.getPointer(), atomics::memory_order_acquire);
		} while (! oldVal.replace2(list, newVal));

		return true;
	}

	// insert newVal in the beginning of a list provided there is still oldVal at the top of the list
	static bool replace(atomics::atomic<CacheList*>& list, CacheList* newVal, CacheList* oldVal)
	{
		if (oldVal && oldVal->isBusy(newVal->traNumber))	// modified in other transaction
			return false;

		newVal->next.store(oldVal, atomics::memory_order_acquire);
		return list.compare_exchange_strong(oldVal, newVal, std::memory_order_release, std::memory_order_acquire);
	}

	// remove too old objects - they are anyway can't be in use
	static TraNumber cleanup(atomics::atomic<CacheList*>& list, const TraNumber oldest)
	{
		TraNumber rc = 0;
		for (HazardPtr<CacheList> entry(list); entry; entry.set(entry->next))
		{
			if ((entry->cacheFlags & CacheFlag::COMMITTED) && entry->traNumber < oldest)
			{
				if (entry->cacheFlags.fetch_or(CacheFlag::ERASED) & CacheFlag::ERASED)
					break;	// someone else also performs cleanup

				// split remaining list off
				if (entry.replace2(list, nullptr))
				{
					while (entry && !(entry->cacheFlags.fetch_or(CacheFlag::ERASED) & CacheFlag::ERASED))
					{
						entry->retire();
						OBJ::destroy(entry->object);
						entry.set(entry->next);
					}
				}
				break;
			}

			// store traNumber of last not removed list element
			rc = entry->traNumber;
		}

		return rc;		// 0 is returned in a case when list becomes empty
	}

	// created earlier object is OK and should become visible to the world
	void commit(thread_db* tdbb, TraNumber currentTrans, TraNumber nextTrans)
	{
		fb_assert(cacheFlags == 0);
		fb_assert(traNumber == currentTrans);
		traNumber = nextTrans;
		cacheFlags |= CacheFlag::COMMITTED;
		version = VersionSupport::next(tdbb);
	}

	// created earlier object is bad and should be destroyed
	static void rollback(atomics::atomic<CacheList*>& list, const TraNumber currentTran)
	{
		// Take into an account that no other transaction except current (i.e. object creator)
		// can access uncommitted objects, only list entries may be accessed as hazard pointers.
		// Therefore rollback can retire such entries at once, a kind of pop() from stack.

		HazardPtr<CacheList> entry(list);
		while (entry)
		{
			if (entry->cacheFlags & CacheFlag::COMMITTED)
				break;
			fb_assert(entry->traNumber == currentTran);

			if (entry.replace2(list, entry->next))
			{
				entry->retire();
				OBJ::destroy(entry->object);
				entry = list;
			}
		}
	}

	void assertCommitted()
	{
		fb_assert(cacheFlags & CacheFlag::COMMITTED);
	}

private:
	// object (nill/not nill) & ERASED bit in cacheFlags together control state of cache element
	//				|				 ERASED
	//----------------------------------|-----------------------------
	//		object	|		true		|			false
	//----------------------------------|-----------------------------
	//		nill	|	object dropped	|	cache to be loaded
	//	not nill	|	prohibited		|	cache is actual

	OBJ* object;
	atomics::atomic<CacheList*> next;
	TraNumber traNumber;	// when COMMITTED not set - stores transaction that created this list element
							// when COMMITTED is set - stores transaction after which older elements are not needed
							// traNumber to be changed BEFORE setting COMMITTED
	MdcVersion version;		// version of metadata cache when object was added
	atomics::atomic<CacheObject::Flag> cacheFlags;
};


class TransactionNumber
{
public:
	static TraNumber current(thread_db* tdbb);
	static TraNumber oldestActive(thread_db* tdbb);
	static TraNumber next(thread_db* tdbb);
};


class Lock;

template <class E>
MemoryPool& getObjectPool(thread_db* tdbb, E* ext)
{
	return ext->getPool();
}

template <>
MemoryPool& getObjectPool(thread_db* tdbb, NullClass* ext)
{
	return CachePool::get(tdbb);
}

template <class OBJ, class EXT>
class CacheElement : public ObjectBase, public EXT
{
	typedef CacheList<OBJ> CachedObj;

public:
	CacheElement(MemoryPool& p, MetaId id, Lock* lock) :
		EXT(p, id, lock), list(nullptr), resetAt(0), myId(id)
	{ }

	CacheElement() :
		EXT(), list(nullptr), resetAt(0), myId(0)
	{ }

	~CacheElement()
	{
		delete list.load();
		cleanup();
	}

	OBJ* getObject(thread_db* tdbb, CacheObject::Flag flags = 0)
	{
		TraNumber cur = TransactionNumber::current(tdbb);
		HazardPtr<CachedObj> listEntry(list);
		if (!listEntry)
		{
			OBJ* obj = OBJ::create(tdbb, getObjectPool<EXT>(tdbb, this), myId, 0);
			CachedObj* newVal = FB_NEW_POOL(CachePool::get(tdbb)) CachedObj(obj, cur, obj ? 0 : CacheFlag::ERASED);

			if (CachedObj::replace(list, newVal, nullptr))
				return obj;

			delete newVal;
			if (obj)
				OBJ::destroy(obj);

			listEntry.set(list);
			fb_assert(listEntry);
		}
		return CachedObj::getObject(listEntry, cur, flags);
	}

	bool storeObject(thread_db* tdbb, OBJ* obj, CacheObject::Flag fl = 0)
	{
		TraNumber oldest = TransactionNumber::oldestActive(tdbb);
		TraNumber oldResetAt = resetAt.load(atomics::memory_order_acquire);
		if (oldResetAt && oldResetAt < oldest)
			setNewResetAt(oldResetAt, CachedObj::cleanup(list, oldest));

		TraNumber current = TransactionNumber::current(tdbb);
		CachedObj* value = FB_NEW CachedObj(obj, current, fl & CacheFlag::IGNORE_MASK);

		bool stored = fl & CacheFlag::INIT ? CachedObj::replace(list, value, nullptr) : CachedObj::add(list, value);
		if (stored)
			setNewResetAt(oldResetAt, current);
		else
			delete value;

		return stored;
	}

	void storeObjectWithTimeout(thread_db* tdbb, OBJ* obj, std::function<void()> error);

	void commit(thread_db* tdbb)
	{
		HazardPtr<CachedObj> current(list);
		if (current)
			current->commit(tdbb, TransactionNumber::current(tdbb), TransactionNumber::next(tdbb));
	}

	void rollback(thread_db* tdbb)
	{
		CachedObj::rollback(list, TransactionNumber::current(tdbb));
	}

	void cleanup()
	{
		list.load()->assertCommitted();
		CachedObj::cleanup(list, MAX_TRA_NUMBER);
	}

	void resetDependentObject(thread_db* tdbb, ResetType rt) override
	{
		switch (rt)
		{
		case ObjectBase::ResetType::Recompile:
			{
				OBJ* newObj = OBJ::create(tdbb, CachePool::get(tdbb), myId, 0);
				if (!storeObject(tdbb, newObj))
				{
					OBJ::destroy(newObj);
					OBJ* oldObj = getObject(tdbb);
					busyError(tdbb, myId, oldObj ? oldObj->c_name() : nullptr);
				}
			}
			break;

		case ObjectBase::ResetType::Mark:
			// used in AST, therefore ignore error when saving empty object
			if (storeObject(tdbb, nullptr))
				commit(tdbb);
			break;

		case ObjectBase::ResetType::Commit:
			commit(tdbb);
			break;

		case ObjectBase::ResetType::Rollback:
			rollback(tdbb);
			break;
		}
	}

	void eraseObject(thread_db* tdbb) override
	{
		HazardPtr<CachedObj> l(list);
		fb_assert(l);
		if (!l)
			return;

		if (!storeObject(tdbb, nullptr, CacheFlag::ERASED))
		{
			OBJ* oldObj = getObject(tdbb);
			busyError(tdbb, myId, oldObj ? oldObj->c_name() : nullptr);
		}
	}

	// Checking it does not protect from something to be added in this element at next cycle!!!
	bool hasData() const
	{
		return list.load(atomics::memory_order_relaxed);
	}

private:
	void setNewResetAt(TraNumber oldVal, TraNumber newVal)
	{
		resetAt.compare_exchange_strong(oldVal, newVal,
			atomics::memory_order_release, atomics::memory_order_relaxed);
	}

private:
	atomics::atomic<CachedObj*> list;
	atomics::atomic<TraNumber> resetAt;

public:
	atomics::atomic<ULONG> flags;				// control non-versioned features (like foreign keys)
	const MetaId myId;
};


template <class E, class EXT = NullClass, unsigned SUBARRAY_SHIFT = 8>
class CacheVector : public Firebird::PermanentStorage
{
public:
	static const unsigned SUBARRAY_SIZE = 1 << SUBARRAY_SHIFT;
	static const unsigned SUBARRAY_MASK = SUBARRAY_SIZE - 1;

	typedef CacheElement<E, EXT> StoredObject;
	typedef atomics::atomic<StoredObject*> SubArrayData;
	typedef atomics::atomic<SubArrayData*> ArrayData;
	typedef SharedReadVector<ArrayData, 4> Storage;

	explicit CacheVector(MemoryPool& pool)
		: Firebird::PermanentStorage(pool),
		  m_objects(getPool())
	{}

private:
	static FB_SIZE_T getCount(const HazardPtr<typename Storage::Generation>& v)
	{
		return v->getCount() << SUBARRAY_SHIFT;
	}

	SubArrayData* getDataPointer(MetaId id) const
	{
		auto up = m_objects.readAccessor();
		if (id >= getCount(up))
			return nullptr;

		auto sub = up->value(id >> SUBARRAY_SHIFT).load(atomics::memory_order_acquire);
		fb_assert(sub);
		return &sub[id & SUBARRAY_MASK];
	}

	void grow(FB_SIZE_T reqSize)
	{
		fb_assert(reqSize > 0);
		reqSize = ((reqSize - 1) >> SUBARRAY_SHIFT) + 1;

		Firebird::MutexLockGuard g(objectsGrowMutex, FB_FUNCTION);

		m_objects.grow(reqSize);
		auto wa = m_objects.writeAccessor();
		fb_assert(wa->getCapacity() >= reqSize);
		while (wa->getCount() < reqSize)
		{
			SubArrayData* sub = FB_NEW_POOL(getPool()) SubArrayData[SUBARRAY_SIZE];
			memset(sub, 0, sizeof(SubArrayData) * SUBARRAY_SIZE);
			wa->add()->store(sub, atomics::memory_order_release);
		}
	}

public:
	StoredObject* getData(MetaId id)
	{
		auto ptr = getDataPointer(id);
		return ptr ? *ptr : nullptr;
	}

	E* getObject(thread_db* tdbb, MetaId id, CacheObject::Flag flags)
	{
//		In theory that should be endless cycle - object may arrive/disappear again and again.
//		But in order to faster find devel problems we run it very limited number of times.
#ifdef DEV_BUILD
		for (int i = 0; i < 2; ++i)
#else
		for (;;)
#endif
		{
			auto ptr = getDataPointer(id);
			if (ptr)
			{
				HazardPtr<StoredObject> data(*ptr);
				if (data)
				{
					auto rc = data->getObject(tdbb, flags);
					if (rc)
						return rc;
				}
			}

			if (!(flags & CacheFlag::AUTOCREATE))
				return nullptr;

			auto val = E::create(tdbb, getPool(), id, flags);
			if (!val)
				(Firebird::Arg::Gds(isc_random) << "Object create failed").raise();

			if (storeObject(tdbb, id, val))
				return val;

			E::destroy(val);
		}
#ifdef DEV_BUILD
		(Firebird::Arg::Gds(isc_random) << "Object suddenly disappeared").raise();
#endif
	}

	StoredObject* storeObject(thread_db* tdbb, MetaId id, E* const val)
	{
		if (id >= getCount())
			grow(id + 1);

		auto ptr = getDataPointer(id);
		fb_assert(ptr);

		HazardPtr<StoredObject> data(*ptr);
		if (!data)
		{
			StoredObject* newData = FB_NEW_POOL(getPool()) StoredObject(getPool(), id, E::getLock(getPool(), tdbb));
			if (!data.replace2(*ptr, newData))
				delete newData;
			else
				data.set(*ptr);
		}

		if (!data->storeObject(tdbb, val))
			data.clear();
		return data.getPointer();
	}

	StoredObject* lookup(thread_db* tdbb, std::function<bool(E* val)> cmp, MetaId* foundId = nullptr) const
	{
		auto a = m_objects.readAccessor();
		for (FB_SIZE_T i = 0; i < a->getCount(); ++i)
		{
			SubArrayData* const sub = a->value(i).load(atomics::memory_order_relaxed);
			if (!sub)
				continue;

			for (SubArrayData* end = &sub[SUBARRAY_SIZE]; sub < end--;)
			{
				StoredObject* ptr = end->load(atomics::memory_order_relaxed);
				if (!ptr)
					continue;

				E* val = ptr->getObject(tdbb);
				if (val && cmp(val))
				{
					if (foundId)
						*foundId = (i << SUBARRAY_SHIFT) + (end - sub);
					return ptr;
				}
			}
		}

		return nullptr;
	}

	~CacheVector()
	{
		cleanup();
	}

	void cleanup()
	{
		auto a = m_objects.writeAccessor();
		for (FB_SIZE_T i = 0; i < a->getCount(); ++i)
		{
			SubArrayData* const sub = a->value(i).load(atomics::memory_order_relaxed);
			if (!sub)
				continue;

			for (SubArrayData* end = &sub[SUBARRAY_SIZE]; sub < end--;)
				delete *end;		// no need using release here in CacheVector's dtor

			delete[] sub;
		}

		m_objects.clear();
		//delete a;
	}

	FB_SIZE_T getCount() const
	{
		return m_objects.readAccessor()->getCount() << SUBARRAY_SHIFT;
	}

	bool replace2(MetaId id, HazardPtr<E>& oldVal, E* const newVal)
	{
		if (id >= getCount())
			grow(id + 1);

		auto a = m_objects.readAccessor();
		SubArrayData* sub = a->value(id >> SUBARRAY_SHIFT).load(atomics::memory_order_acquire);
		fb_assert(sub);
		sub = &sub[id & SUBARRAY_MASK];

		return oldVal.replace2(sub, newVal);
	}

	bool clear(MetaId id)
	{
		if (id >= getCount())
			return false;

		auto a = m_objects.readAccessor();
		SubArrayData* sub = a->value(id >> SUBARRAY_SHIFT).load(atomics::memory_order_acquire);
		fb_assert(sub);
		sub = &sub[id & SUBARRAY_MASK];

		sub->store(nullptr, atomics::memory_order_release);
		return true;
	}

	bool load(MetaId id, HazardPtr<E>& val) const
	{
		auto a = m_objects.readAccessor();
		if (id < getCount(a))
		{
			SubArrayData* sub = a->value(id >> SUBARRAY_SHIFT).load(atomics::memory_order_acquire);
			if (sub)
			{
				val.set(sub[id & SUBARRAY_MASK]);
				if (val && val->hasData())
					return true;
			}
		}

		return false;
	}

	HazardPtr<E> load(MetaId id) const
	{
		HazardPtr<E> val;
		if (!load(id, val))
			val.clear();
		return val;
	}

	HazardPtr<typename Storage::Generation> readAccessor() const
	{
		return m_objects.readAccessor();
	}

	class Iterator
	{
	public:
		StoredObject* operator*()
		{
			return get();
		}

/*		StoredObject& operator->()
		{
			return get();
		}
 */
		Iterator& operator++()
		{
			index = locateData(index + 1);
			return *this;
		}

		bool operator==(const Iterator& itr) const
		{
			fb_assert(data == itr.data);
			return index == itr.index;
		}

		bool operator!=(const Iterator& itr) const
		{
			fb_assert(data == itr.data);
			return index != itr.index;
		}

	private:
		void* operator new(size_t);
		void* operator new[](size_t);

	public:
		enum class Location {Begin, End};
		Iterator(const CacheVector* v, Location loc)
			: data(v),
			  index(loc == Location::Begin ? locateData(0) : data->getCount())
		{ }

		StoredObject* get()
		{
			StoredObject* ptr = data->getData(index);
			fb_assert(ptr);
			return ptr;
		}

		FB_SIZE_T locateData(FB_SIZE_T i)
		{
			while (!data->getData(i))
				++i;
			return i;
		}

	private:
		const CacheVector* data;
		FB_SIZE_T index;		// should be able to store MAX_METAID + 1 value
	};

	Iterator begin() const
	{
		return Iterator(this, Iterator::Location::Begin);
	}

	Iterator end() const
	{
		return Iterator(this, Iterator::Location::End);
	}

private:
	Storage m_objects;
	Firebird::Mutex objectsGrowMutex;
};

} // namespace Jrd

#endif // JRD_HAZARDPTR_H
