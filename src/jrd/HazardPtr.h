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

#include "../common/classes/alloc.h"
#include "../common/classes/array.h"
#include "../common/gdsassert.h"
#include "fb_blk.h"

#include <cds/gc/dhp.h>
#include <cds/algo/atomic.h>

#include <type_traits>
#include <condition_variable>
#include <utility>

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

	// atomically replaces 'where' with 'newVal', using *this as old value for comparison
	// sets *this to actual data from 'where' if replace failed
	bool replace(atomics::atomic<T*>& where, T* newVal)
	{
		T* val = get<T>();
		bool rc = where.compare_exchange_strong(val, newVal,
			std::memory_order_release, std::memory_order_acquire);
		if (!rc)
			assign(val);
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
class SharedReadVector
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
		static Generation* create(FB_SIZE_T cap)
		{
			return FB_NEW_RPT(*getDefaultMemoryPool(), cap) Generation(cap);
		}

		FB_SIZE_T getCount() const
		{
			return count;
		}

		FB_SIZE_T getCapacity() const
		{
			return capacity;
		}

		T* begin()
		{
			return &data[0];
		}

		T* end()
		{
			return &data[count];
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

		T* addStart()
		{
			if (!hasSpace())
				return nullptr;
			return &data[count];
		}

		void addComplete()
		{
			++count;
		}

		void truncate(const T& notValue)
		{
			while (count && data[count - 1] == notValue)
				count--;
		}

		void clear()
		{
			count = 0;
		}

		void grow(const FB_SIZE_T newCount)
		{
			fb_assert(newCount >= count);
			fb_assert(newCount <= capacity);
			memset(data + count, 0, sizeof(T) * (newCount - count));
			count = newCount;
		}

		static void destroy(Generation* gen)
		{
			// delay delete - someone else may access it
			gen->retire();
		}
	};

	typedef HazardPtr<Generation> ReadAccessor;
	typedef Generation* WriteAccessor;

	SharedReadVector()
		: currentData(Generation::create(CAP))
	{ }

	WriteAccessor writeAccessor()
	{
		return currentData.load(std::memory_order_acquire);
	}

	ReadAccessor readAccessor() const
	{
		return HazardPtr<Generation>(currentData);
	}

	void grow(FB_SIZE_T const newSize, bool arrGrow)
	{
		// decide how much vector grows
		Generation* const oldGeneration = writeAccessor();
		FB_SIZE_T cap = oldGeneration->getCapacity();
		FB_SIZE_T doubleSize = oldGeneration->getCapacity() * 2;
		if (newSize > doubleSize)
			doubleSize = newSize;
		FB_SIZE_T singleSize = newSize ? newSize : doubleSize;

		if (cap >= singleSize)
		{
			// grow old generation inplace
			if (arrGrow)
				oldGeneration->grow(singleSize);
			return;
		}

		// create new generation and store it in the vector
		Generation* const newGeneration = Generation::create(doubleSize);
		newGeneration->add(oldGeneration);
		if (arrGrow)
			newGeneration->grow(singleSize);
		currentData.store(newGeneration, std::memory_order_release);

		// cleanup
		Generation::destroy(oldGeneration);
	}

	~SharedReadVector()
	{
		Generation::destroy(currentData.load(std::memory_order_acquire));
	}

	void clear()
	{
		// expected to be called when going to destroy an object
		writeAccessor()->clear();
	}

	bool hasData()
	{
		return readAccessor()->getCount() != 0;
	}

private:
	atomics::atomic<Generation*> currentData;
};

class thread_db;
class ObjectBase
{
public:
	typedef unsigned Flag;
	virtual void lockedExcl [[noreturn]] (thread_db* tdbb) /*const*/;
	virtual const char* c_name() const = 0;
};


class ElementBase
{
public:
	enum ResetType {Recompile, Mark, Commit, Rollback};

	typedef SLONG ReturnedId;	// enable '-1' as not found

public:
	virtual ~ElementBase();
	virtual void resetDependentObject(thread_db* tdbb, ResetType rt) = 0;
	virtual void cleanup(thread_db* tdbb) = 0;

public:
	void resetDependentObjects(thread_db* tdbb, TraNumber olderThan);
	void addDependentObject(thread_db* tdbb, ElementBase* dep);
	void removeDependentObject(thread_db* tdbb, ElementBase* dep);
	[[noreturn]] void busyError(thread_db* tdbb, MetaId id, const char* name, const char* family);
	void commitErase(thread_db* tdbb);
};

namespace CacheFlag
{
	static const ObjectBase::Flag COMMITTED =	0x01;		// version already committed
	static const ObjectBase::Flag ERASED =		0x02;		// object erased
	static const ObjectBase::Flag NOSCAN =		0x04;		// do not call Versioned::scan()
	static const ObjectBase::Flag AUTOCREATE =	0x08;		// create initial version automatically
	static const ObjectBase::Flag NOCOMMIT =	0x10;		// do not commit created version
	static const ObjectBase::Flag RET_ERASED =	0x20;		// return erased objects
	static const ObjectBase::Flag RETIRED = 	0x40;		// object is in a process of GC
	static const ObjectBase::Flag UPGRADE = 	0x80;		// create new versions for already existing in a cache objects
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
};

class TransactionNumber
{
public:
	static TraNumber current(thread_db* tdbb);
	static TraNumber oldestActive(thread_db* tdbb);
	static TraNumber next(thread_db* tdbb);
	static bool isNotActive(thread_db* tdbb, TraNumber traNumber);
};


class StartupBarrier
{
public:
	StartupBarrier()
		: thd(0), flg(INITIAL)
	{ }

	bool scanInProgress() const
	{
		if (flg == READY)
			return false;

		if ((thd == Thread::getId()) && (flg == SCANNING))
			return true;

		return false;
	}

	template <typename F>
	void scanBar(F&& objScan)
	{
		// no need opening barrier twice
		if (flg == READY)
			return;

		// enable recursive no-action pass by scanning thread
		// if thd is current thread flg is not going to be changed - current thread holds mutex
		if ((thd == Thread::getId()) && (flg == SCANNING))
			return;

		std::unique_lock<std::mutex> g(mtx);
		for(;;)
		{
			bool reason = true;
			switch (flg)
			{
			case INITIAL: 		// Our thread becomes scanning thread
				reason = false;
				// fall through...
			case RELOAD: 		// may be because object body reload required.
				thd = Thread::getId();
				flg = SCANNING;

				try
				{
					if (!objScan(reason))
					{
						// scan complete but reload was requested
						flg = RELOAD;
						thd = 0;
						cond.notify_all();		// avoid deadlock in other threads

						return;
					}
				}
				catch(...)		// scan failed - give up
				{
					flg = INITIAL;
					thd = 0;
					cond.notify_all();		// avoid deadlock in other threads

					throw;
				}

				flg = READY;
				cond.notify_all();			// other threads may proceed successfully
				return;

			case SCANNING:		// somebody is already scanning object
				cond.wait(g, [this]{ return flg != SCANNING; });
				continue;		// repeat check of FLG value

			case READY:
				return;
			}
		}
	}

private:
	std::condition_variable cond;
	std::mutex mtx;
	ThreadId thd;

public:
	enum { INITIAL, RELOAD, SCANNING, READY } flg;
};

template <class OBJ>
class ListEntry : public HazardObject
{
public:
	ListEntry(OBJ* obj, TraNumber currentTrans, ObjectBase::Flag fl)
		: object(obj), traNumber(currentTrans), cacheFlags(fl)
	{ }

	~ListEntry()
	{
		fb_assert(!object);
	}

	void cleanup(thread_db* tdbb)
	{
		if (object)		// be careful with ERASED entries
		{
			OBJ::destroy(tdbb, object);
			object = nullptr;
		}

		auto* ptr = next.load(atomics::memory_order_relaxed);
		if (ptr)
		{
			ptr->cleanup(tdbb);
			delete ptr;
			next.store(nullptr, atomics::memory_order_relaxed);
		}
	}

	// find appropriate object in cache
	static OBJ* getObject(thread_db* tdbb, HazardPtr<ListEntry>& listEntry, TraNumber currentTrans, ObjectBase::Flag fl)
	{
		for (; listEntry; listEntry.set(listEntry->next))
		{
			ObjectBase::Flag f(listEntry->getFlags());

			if ((f & CacheFlag::COMMITTED) ||
					// committed (i.e. confirmed) objects are freely available
				(listEntry->traNumber == currentTrans))
					// transaction that created an object can always access it
			{
				if (f & CacheFlag::ERASED)
				{
					// object does not exist
					fb_assert(!listEntry->object);

					if (fl & CacheFlag::ERASED)
						continue;

					return nullptr;		// object dropped
				}

				// required entry found in the list
				auto* obj = listEntry->object;
				if (obj)
				{
					listEntry->scanObject(
						[&](bool rld) { return scanCallback(tdbb, obj, rld, fl); },
					fl);
				}
				return obj;
			}
		}

		return nullptr;	// object created (not by us) and not committed yet
	}

	bool isBusy(TraNumber currentTrans) const noexcept
	{
		return !((getFlags() & CacheFlag::COMMITTED) || (traNumber == currentTrans));
	}

	ObjectBase::Flag getFlags() const noexcept
	{
		return cacheFlags.load(atomics::memory_order_relaxed);
	}

	// add new entry to the list
	static bool add(thread_db* tdbb, atomics::atomic<ListEntry*>& list, ListEntry* newVal)
	{
		HazardPtr<ListEntry> oldVal(list);

		do
		{
			while(oldVal && oldVal->isBusy(newVal->traNumber))
			{
				// modified in transaction oldVal->traNumber
				if (TransactionNumber::isNotActive(tdbb, oldVal->traNumber))
				{
					rollback(tdbb, list, oldVal->traNumber);
					oldVal.set(list);
				}
				else
					return false;
			}

			newVal->next.store(oldVal.getPointer());
		} while (!oldVal.replace(list, newVal));

		return true;
	}

	// insert newVal in the beginning of a list provided there is still oldVal at the top of the list
	static bool replace(atomics::atomic<ListEntry*>& list, ListEntry* newVal, ListEntry* oldVal) noexcept
	{
		if (oldVal && oldVal->isBusy(newVal->traNumber))	// modified in other transaction
			return false;

		newVal->next.store(oldVal, atomics::memory_order_acquire);
		return list.compare_exchange_strong(oldVal, newVal, std::memory_order_release, std::memory_order_acquire);
	}

	// remove too old objects - they are anyway can't be in use
	static TraNumber gc(thread_db* tdbb, atomics::atomic<ListEntry*>* list, const TraNumber oldest)
	{
		TraNumber rc = 0;
		for (HazardPtr<ListEntry> entry(*list); entry; list = &entry->next, entry.set(*list))
		{
			if (!(entry->getFlags() & CacheFlag::COMMITTED))
				continue;

			if (rc && entry->traNumber < oldest)
			{
				if (entry->cacheFlags.fetch_or(CacheFlag::RETIRED) & CacheFlag::RETIRED)
					break;	// someone else also performs GC

				// split remaining list off
				if (entry.replace(*list, nullptr))
				{
					while (entry)// && !(entry->cacheFlags.fetch_or(CacheFlag::RETIRED) & CacheFlag::RETIRED))
					{
						if (entry->object)
						{
							OBJ::destroy(tdbb, entry->object);
							entry->object = nullptr;
						}
						entry->retire();
						entry.set(entry->next);
						if (entry && (entry->cacheFlags.fetch_or(CacheFlag::RETIRED) & CacheFlag::RETIRED))
							break;
					}
				}
				break;
			}

			// store traNumber of last not removed list element
			rc = entry->traNumber;
		}

		return rc;		// 0 is returned in a case when list was empty
	}

	// created (erased) earlier object is OK and should become visible to the world
	// return true if object was erased
	bool commit(thread_db* tdbb, TraNumber currentTrans, TraNumber nextTrans)
	{
		// following assert is OK in general but breaks CREATE DATABASE
		// commented out till better solution
		// fb_assert((getFlags() & CacheFlag::COMMITTED) == 0);

		fb_assert(traNumber == currentTrans);

		traNumber = nextTrans;
		version = VersionSupport::next(tdbb);
		auto flags = cacheFlags.fetch_or(CacheFlag::COMMITTED);
		return flags & CacheFlag::ERASED;
	}

	// created earlier object is bad and should be destroyed
	static void rollback(thread_db* tdbb, atomics::atomic<ListEntry*>& list, const TraNumber currentTran)
	{
		// Take into an account that no other transaction except current (i.e. object creator)
		// can access uncommitted objects, only list entries may be accessed as hazard pointers.
		// Therefore rollback can retire such entries at once, a kind of pop() from stack.

		HazardPtr<ListEntry> entry(list);
		while (entry)
		{
			if (entry->getFlags() & CacheFlag::COMMITTED)
				break;
			fb_assert(entry->traNumber == currentTran);

			if (entry.replace(list, entry->next))
			{
				entry->next = nullptr;
				OBJ::destroy(tdbb, entry->object);
				entry->object = nullptr;
				entry->retire();

				entry = list;
			}
		}
	}

	void assertCommitted()
	{
		fb_assert(getFlags() & CacheFlag::COMMITTED);
	}

	template <typename F>
	void scanObject(F&& scanFunction, ObjectBase::Flag fl)
	{
		if (!(fl & CacheFlag::NOSCAN))
			bar.scanBar(std::forward<F>(scanFunction));
	}

	static bool scanCallback(thread_db* tdbb, OBJ* obj, bool rld, ObjectBase::Flag fl)
	{
		fb_assert(obj);
		return rld ? obj->reload(tdbb, fl) : obj->scan(tdbb, fl);
	}

	bool scanInProgress() const
	{
		return bar.scanInProgress();
	}

private:
	// object (nill/not nill) & ERASED bit in cacheFlags together control state of cache element
	//				|				 ERASED
	//----------------------------------|-----------------------------
	//		object	|		true		|			false
	//----------------------------------|-----------------------------
	//		nill	|	object dropped	|	cache to be loaded
	//	not nill	|	prohibited		|	cache is actual

	StartupBarrier bar;
	OBJ* object;
	atomics::atomic<ListEntry*> next = nullptr;
	TraNumber traNumber;		// when COMMITTED not set - stores transaction that created this list element
								// when COMMITTED is set - stores transaction after which older elements are not needed
								// traNumber to be changed BEFORE setting COMMITTED
	MdcVersion version = 0;		// version of metadata cache when object was added
	atomics::atomic<ObjectBase::Flag> cacheFlags;
};


typedef class Lock* MakeLock(thread_db*, MemoryPool&);

template <class V, class P>
class CacheElement : public ElementBase, public P
{
public:
	typedef V Versioned;
	typedef P Permanent;

	typedef atomics::atomic<CacheElement*> AtomicElementPointer;

	template <typename EXTEND>
	CacheElement(thread_db* tdbb, MemoryPool& p, MetaId id, MakeLock* makeLock, EXTEND extend) :
		Permanent(tdbb, p, id, makeLock, extend), list(nullptr), resetAt(0), ptrToClean(nullptr)
	{ }

	CacheElement(MemoryPool& p) :
		Permanent(p), list(nullptr), resetAt(0), ptrToClean(nullptr)
	{ }

	static void cleanup(thread_db* tdbb, CacheElement* element)
	{
		auto* ptr = element->list.load(atomics::memory_order_relaxed);
		if (ptr)
		{
			ptr->cleanup(tdbb);
			delete ptr;
		}

		if (element->ptrToClean)
			*element->ptrToClean = nullptr;

		if (!Permanent::destroy(tdbb, element))
		{
			// destroy() returns true if it completed removal of permamnet part (delete by pool)
			// if not - delete it ourself here
			delete element;
		}
	}

	void cleanup(thread_db* tdbb) override
	{
		cleanup(tdbb, this);
	}

	void setCleanup(AtomicElementPointer* clearPtr)
	{
		ptrToClean = clearPtr;
	}

	void reload(thread_db* tdbb, ObjectBase::Flag fl)
	{
		HazardPtr<ListEntry<Versioned>> listEntry(list);
		TraNumber cur = TransactionNumber::current(tdbb);
		if (listEntry)
		{
			Versioned* obj = ListEntry<Versioned>::getObject(tdbb, listEntry, cur, 0);
			if (obj)
			{
				listEntry->scanObject(
					[&](bool rld) { return ListEntry<Versioned>::scanCallback(tdbb, obj, rld, 0); },
				fl);
			}
		}
	}

	Versioned* getObject(thread_db* tdbb, ObjectBase::Flag fl)
	{
		TraNumber cur = TransactionNumber::current(tdbb);

		HazardPtr<ListEntry<Versioned>> listEntry(list);
		if (!listEntry)
		{
			if (!(fl & CacheFlag::AUTOCREATE))
				return nullptr;

			fb_assert(tdbb);

			// create almost empty object ...
			Versioned* obj = Versioned::create(tdbb, this->getPool(), this);

			// ... and new entry to store it in cache
			ListEntry<Versioned>* newEntry = nullptr;
			try
			{
				newEntry = FB_NEW_POOL(*getDefaultMemoryPool()) ListEntry<Versioned>(obj, cur, fl);
			}
			catch (const Firebird::Exception&)
			{
				Versioned::destroy(tdbb, obj);
				throw;
			}

			if (ListEntry<Versioned>::replace(list, newEntry, nullptr))
			{
				newEntry->scanObject(
					[&](bool rld)
					{
						return ListEntry<Versioned>::scanCallback(tdbb, obj, rld, fl);
					},
				fl);
				if (! (fl & CacheFlag::NOCOMMIT))
					newEntry->commit(tdbb, cur, TransactionNumber::next(tdbb));
				return obj;
			}

			newEntry->cleanup(tdbb);
			delete newEntry;
			fb_assert(list.load());
		}
		return ListEntry<Versioned>::getObject(tdbb, listEntry, cur, fl);
	}

	// return latest committed version or nullptr when does not exist
	Versioned* getLatestObject(thread_db* tdbb) const
	{
		HazardPtr<ListEntry<Versioned>> listEntry(list);
		if (!listEntry)
			return nullptr;

		return ListEntry<Versioned>::getObject(tdbb, listEntry, MAX_TRA_NUMBER, 0);
	}

	bool storeObject(thread_db* tdbb, Versioned* obj, ObjectBase::Flag fl)
	{
		TraNumber oldest = TransactionNumber::oldestActive(tdbb);
		TraNumber oldResetAt = resetAt.load(atomics::memory_order_acquire);
		if (oldResetAt && oldResetAt < oldest)
			setNewResetAt(oldResetAt, ListEntry<Versioned>::gc(tdbb, &list, oldest));

		TraNumber cur = TransactionNumber::current(tdbb);
		ListEntry<Versioned>* newEntry = FB_NEW_POOL(*getDefaultMemoryPool()) ListEntry<Versioned>(obj, cur, fl);
		if (!ListEntry<Versioned>::add(tdbb, list, newEntry))
		{
			newEntry->cleanup(tdbb);
			delete newEntry;
			return false;
		}

		setNewResetAt(oldResetAt, cur);
		if (obj)
		{
			newEntry->scanObject(
				[&](bool rld) { return ListEntry<Versioned>::scanCallback(tdbb, obj, rld, fl); },
			fl);
		}
		if (! (fl & CacheFlag::NOCOMMIT))
			newEntry->commit(tdbb, cur, TransactionNumber::next(tdbb));

		return true;
	}

	void commit(thread_db* tdbb)
	{
		HazardPtr<ListEntry<Versioned>> current(list);
		if (current)
		{
			if (current->commit(tdbb, TransactionNumber::current(tdbb), TransactionNumber::next(tdbb)))
				commitErase(tdbb);
		}
	}

	void rollback(thread_db* tdbb)
	{
		ListEntry<Versioned>::rollback(tdbb, list, TransactionNumber::current(tdbb));
	}
/*
	void gc()
	{
		list.load()->assertCommitted();
		ListEntry<Versioned>::gc(&list, MAX_TRA_NUMBER);
	}
 */
	void resetDependentObject(thread_db* tdbb, ResetType rt) override
	{
		switch (rt)
		{
		case ElementBase::ResetType::Recompile:
			{
				Versioned* newObj = Versioned::create(tdbb, CachePool::get(tdbb), this);
				if (!storeObject(tdbb, newObj, CacheFlag::NOCOMMIT))
				{
					Versioned::destroy(tdbb, newObj);
					busyError(tdbb, this->getId(), this->c_name(), V::objectFamily(this));
				}
			}
			break;

		case ElementBase::ResetType::Mark:
			// used in AST, therefore ignore error when saving empty object
			if (storeObject(tdbb, nullptr, 0))
				commit(tdbb);
			break;

		case ElementBase::ResetType::Commit:
			commit(tdbb);
			break;

		case ElementBase::ResetType::Rollback:
			rollback(tdbb);
			break;
		}
	}

	CacheElement* erase(thread_db* tdbb)
	{
		HazardPtr<ListEntry<Versioned>> l(list);
		fb_assert(l);
		if (!l)
			return nullptr;

		if (!storeObject(tdbb, nullptr, CacheFlag::ERASED | CacheFlag::NOCOMMIT))
		{
			Versioned* oldObj = getObject(tdbb, 0);
			busyError(tdbb, this->getId(), this->c_name(), V::objectFamily(this));
		}

		return this;
	}

	// Checking it does not protect from something to be added in this element at next cycle!!!
	bool hasData() const
	{
		return list.load(atomics::memory_order_relaxed);
	}

	bool isDropped() const
	{
		auto* l = list.load(atomics::memory_order_relaxed);
		return l && (l->getFlags() & CacheFlag::ERASED);
	}

	bool scanInProgress() const
	{
		HazardPtr<ListEntry<Versioned>> listEntry(list);
		if (!listEntry)
			return false;

		return listEntry->scanInProgress();
	}

	static int getObjectType()
	{
		return Versioned::objectType();
	}

private:
	void setNewResetAt(TraNumber oldVal, TraNumber newVal)
	{
		resetAt.compare_exchange_strong(oldVal, newVal,
			atomics::memory_order_release, atomics::memory_order_relaxed);
	}

private:
	atomics::atomic<ListEntry<Versioned>*> list;
	atomics::atomic<TraNumber> resetAt;
	AtomicElementPointer* ptrToClean;
};


struct NoData
{
	NoData() { }
};

template <class StoredElement, unsigned SUBARRAY_SHIFT = 8, typename EXTEND = NoData>
class CacheVector : public Firebird::PermanentStorage
{
public:
	static const unsigned SUBARRAY_SIZE = 1 << SUBARRAY_SHIFT;
	static const unsigned SUBARRAY_MASK = SUBARRAY_SIZE - 1;

	typedef typename StoredElement::Versioned Versioned;
	typedef typename StoredElement::Permanent Permanent;
	typedef typename StoredElement::AtomicElementPointer SubArrayData;
	typedef atomics::atomic<SubArrayData*> ArrayData;
	typedef SharedReadVector<ArrayData, 4> Storage;

	explicit CacheVector(MemoryPool& pool, EXTEND extend = NoData())
		: Firebird::PermanentStorage(pool),
		  m_objects(),
		  m_extend(extend)
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

		m_objects.grow(reqSize, false);
		auto wa = m_objects.writeAccessor();
		fb_assert(wa->getCapacity() >= reqSize);
		while (wa->getCount() < reqSize)
		{
			SubArrayData* sub = FB_NEW_POOL(getPool()) SubArrayData[SUBARRAY_SIZE];
			memset(sub, 0, sizeof(SubArrayData) * SUBARRAY_SIZE);
			wa->addStart()->store(sub, atomics::memory_order_release);
			wa->addComplete();
		}
	}

public:
	StoredElement* getDataNoChecks(MetaId id) const
	{
		SubArrayData* ptr = getDataPointer(id);
		return ptr ? ptr->load(atomics::memory_order_relaxed) : nullptr;
	}

	StoredElement* getData(thread_db* tdbb, MetaId id, ObjectBase::Flag fl) const
	{
		SubArrayData* ptr = getDataPointer(id);

		if (ptr)
		{
			StoredElement* rc = ptr->load(atomics::memory_order_relaxed);
			if (rc)
			{
				rc->getObject(tdbb, fl);
				return rc;
			}
		}

		return nullptr;
	}

	FB_SIZE_T getCount() const
	{
		return getCount(m_objects.readAccessor());
	}

	Versioned* getObject(thread_db* tdbb, MetaId id, ObjectBase::Flag fl)
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
				StoredElement* data = ptr->load(atomics::memory_order_acquire);
				if (data)
				{
					if (fl & CacheFlag::UPGRADE)
					{
						auto val = makeObject(tdbb, id, fl);
						if (val)
							return val;
						continue;
					}

					return data->getObject(tdbb, fl);
				}
			}

			if (!(fl & CacheFlag::AUTOCREATE))
				return nullptr;

			auto val = makeObject(tdbb, id, fl);
			if (val)
				return val;
		}
#ifdef DEV_BUILD
		(Firebird::Arg::Gds(isc_random) << "Object suddenly disappeared").raise();
#endif
	}

	StoredElement* erase(thread_db* tdbb, MetaId id)
	{
		auto ptr = getDataPointer(id);
		if (ptr)
		{
			StoredElement* data = ptr->load(atomics::memory_order_acquire);
			if (data)
				return data->erase(tdbb);
		}

		return nullptr;
	}

	Versioned* makeObject(thread_db* tdbb, MetaId id, ObjectBase::Flag fl)
	{
		if (id >= getCount())
			grow(id + 1);

		auto ptr = getDataPointer(id);
		fb_assert(ptr);

		StoredElement* data = ptr->load(atomics::memory_order_acquire);
		if (!data)
		{
			StoredElement* newData = FB_NEW_POOL(getPool())
				StoredElement(tdbb, getPool(), id, Versioned::makeLock, m_extend);
			if (ptr->compare_exchange_strong(data, newData,
				atomics::memory_order_release, atomics::memory_order_acquire))
			{
				newData->setCleanup(ptr);
				data = newData;
			}
			else
				StoredElement::cleanup(tdbb, newData);
		}

		auto obj = Versioned::create(tdbb, getPool(), *ptr);
		if (!obj)
			(Firebird::Arg::Gds(isc_random) << "Object create failed in makeObject()").raise();

		if (data->storeObject(tdbb, obj, fl))
			return obj;

		Versioned::destroy(tdbb, obj);
		return nullptr;
	}

	template <typename F>
	StoredElement* lookup(thread_db* tdbb, F&& cmp) const
	{
		auto a = m_objects.readAccessor();
		for (FB_SIZE_T i = 0; i < a->getCount(); ++i)
		{
			SubArrayData* const sub = a->value(i).load(atomics::memory_order_relaxed);
			if (!sub)
				continue;

			for (SubArrayData* end = &sub[SUBARRAY_SIZE]; sub < end--;)
			{
				StoredElement* ptr = end->load(atomics::memory_order_relaxed);
				if (ptr && cmp(ptr))
				{
					ptr->reload(tdbb, 0);
					return ptr;
				}
			}
		}

		return nullptr;
	}

	~CacheVector()
	{
		fb_assert(!m_objects.hasData());
	}

	void cleanup(thread_db* tdbb)
	{
		auto a = m_objects.writeAccessor();
		for (FB_SIZE_T i = 0; i < a->getCount(); ++i)
		{
			SubArrayData* const sub = a->value(i).load(atomics::memory_order_relaxed);
			if (!sub)
				continue;

			for (SubArrayData* end = &sub[SUBARRAY_SIZE]; sub < end--;)
			{
				StoredElement* elem = end->load(atomics::memory_order_relaxed);
				if (!elem)
					continue;

				StoredElement::cleanup(tdbb, elem);
				fb_assert(!end->load(atomics::memory_order_relaxed));
			}

			delete[] sub;		// no need using retire() here in CacheVector's cleanup
			a->value(i).store(nullptr, atomics::memory_order_relaxed);
		}

		m_objects.clear();
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

	HazardPtr<typename Storage::Generation> readAccessor() const
	{
		return m_objects.readAccessor();
	}

	class Iterator
	{
		static const FB_SIZE_T eof = ~0u;
		static const FB_SIZE_T endloop = ~0u;

	public:
		StoredElement* operator*()
		{
			return get();
		}

		Iterator& operator++()
		{
			index = locateData(index + 1);
			return *this;
		}

		bool operator==(const Iterator& itr) const
		{
			fb_assert(data == itr.data);
			return index == itr.index ||
				(index == endloop && itr.index == eof) ||
				(itr.index == endloop && index == eof);
		}

		bool operator!=(const Iterator& itr) const
		{
			fb_assert(data == itr.data);
			return !operator==(itr);
		}

	private:
		void* operator new(size_t) = delete;
		void* operator new[](size_t) = delete;

	public:
		enum class Location {Begin, End};
		Iterator(const CacheVector* v, Location loc)
			: data(v),
			  index(loc == Location::Begin ? locateData(0) : endloop)
		{ }

		StoredElement* get()
		{
			fb_assert(index != eof);
			if (index == eof)
				return nullptr;
			StoredElement* ptr = data->getDataNoChecks(index);
			fb_assert(ptr);
			return ptr;
		}

	private:
		FB_SIZE_T locateData(FB_SIZE_T i)
		{
			while (i < data->getCount())
			{
				if (data->getDataNoChecks(i))
					return i;
				++i;
			}

			return eof;
		}

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
	EXTEND m_extend;
};

template <typename T>
auto getPermanent(T* t) -> decltype(t->getPermanent())
{
	return t ? t->getPermanent() : nullptr;
}

} // namespace Jrd

#endif // JRD_HAZARDPTR_H
