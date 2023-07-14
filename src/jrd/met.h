/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		met.h
 *	DESCRIPTION:	Random meta-data stuff
 *
 * The contents of this file are subject to the Interbase Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy
 * of the License at http://www.Inprise.com/IPL.html
 *
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * rights and limitations under the License.
 *
 * The Original Code was created by Inprise Corporation
 * and its predecessors. Portions created by Inprise Corporation are
 * Copyright (C) Inprise Corporation.
 *
 * All Rights Reserved.
 * Contributor(s): ______________________________________.
 */

#ifndef JRD_MET_H
#define JRD_MET_H

#include "../jrd/HazardPtr.h"
#include <cds/container/michael_list_dhp.h>

#include "../common/StatusArg.h"

#include "../jrd/Relation.h"
#include "../jrd/Function.h"

#include "../jrd/val.h"
#include "../jrd/irq.h"
#include "../jrd/drq.h"

#include "../jrd/CharSetContainer.h"

// Record types for record summary blob records

enum rsr_t {
	RSR_field_id,
	RSR_field_name,
	RSR_view_context,
	RSR_base_field,
	RSR_computed_blr,
	RSR_missing_value,
	RSR_default_value,
	RSR_validation_blr,
	RSR_security_class,
	RSR_trigger_name,
	RSR_dimensions,
	RSR_array_desc,

	RSR_relation_id,			// The following are Gateway specific
	RSR_relation_name,			// and are used to speed the acquiring
	RSR_rel_sys_flag,			// of relation information
	RSR_view_blr,
	RSR_owner_name,
	RSR_field_type,				// The following are also Gateway
	RSR_field_scale,			// specific and relate to field info
	RSR_field_length,
	RSR_field_sub_type,
	RSR_field_not_null,
	RSR_field_generator_name,
	RSR_field_identity_type
};

// Temporary field block

class TemporaryField : public pool_alloc<type_tfb>
{
public:
	TemporaryField*	tfb_next;		// next block in chain
	USHORT			tfb_id;			// id of field in relation
	USHORT			tfb_flags;
	dsc				tfb_desc;
	Jrd::impure_value	tfb_default;
};

// tfb_flags

const int TFB_computed			= 1;
const int TFB_array				= 2;


const int TRIGGER_PRE_STORE		= 1;
const int TRIGGER_POST_STORE	= 2;
const int TRIGGER_PRE_MODIFY	= 3;
const int TRIGGER_POST_MODIFY	= 4;
const int TRIGGER_PRE_ERASE		= 5;
const int TRIGGER_POST_ERASE	= 6;
const int TRIGGER_MAX			= 7;

// trigger type prefixes
const int TRIGGER_PRE			= 0;
const int TRIGGER_POST			= 1;

// trigger type suffixes
const int TRIGGER_STORE			= 1;
const int TRIGGER_MODIFY		= 2;
const int TRIGGER_ERASE			= 3;

// that's how trigger action types are encoded
/*
	bit 0 = TRIGGER_PRE/TRIGGER_POST flag,
	bits 1-2 = TRIGGER_STORE/TRIGGER_MODIFY/TRIGGER_ERASE (slot #1),
	bits 3-4 = TRIGGER_STORE/TRIGGER_MODIFY/TRIGGER_ERASE (slot #2),
	bits 5-6 = TRIGGER_STORE/TRIGGER_MODIFY/TRIGGER_ERASE (slot #3),
	and finally the above calculated value is decremented

example #1:
	TRIGGER_POST_ERASE =
	= ((TRIGGER_ERASE << 1) | TRIGGER_POST) - 1 =
	= ((3 << 1) | 1) - 1 =
	= 0x00000110 (6)

example #2:
	TRIGGER_PRE_STORE_MODIFY =
	= ((TRIGGER_MODIFY << 3) | (TRIGGER_STORE << 1) | TRIGGER_PRE) - 1 =
	= ((2 << 3) | (1 << 1) | 0) - 1 =
	= 0x00010001 (17)

example #3:
	TRIGGER_POST_MODIFY_ERASE_STORE =
	= ((TRIGGER_STORE << 5) | (TRIGGER_ERASE << 3) | (TRIGGER_MODIFY << 1) | TRIGGER_POST) - 1 =
	= ((1 << 5) | (3 << 3) | (2 << 1) | 1) - 1 =
	= 0x00111100 (60)
*/

// that's how trigger types are decoded
#define TRIGGER_ACTION(value, shift) \
	(((((value + 1) >> shift) & 3) << 1) | ((value + 1) & 1)) - 1

#define TRIGGER_ACTION_SLOT(value, slot) \
	TRIGGER_ACTION(value, (slot * 2 - 1) )

const int TRIGGER_COMBINED_MAX	= 128;

#include "../jrd/exe_proto.h"
#include "../jrd/obj.h"
#include "../dsql/sym.h"

namespace Jrd {

// Procedure block

class jrd_prc : public Routine
{
public:
	const Format*	prc_record_format;
	prc_t			prc_type;					// procedure type

	const ExtEngineManager::Procedure* getExternal() const { return prc_external; }
	void setExternal(ExtEngineManager::Procedure* value) { prc_external = value; }

private:
	const ExtEngineManager::Procedure* prc_external;

	jrd_prc(MemoryPool& p, MetaId id)
		: Routine(p, id),
		  prc_record_format(NULL),
		  prc_type(prc_legacy),
		  prc_external(NULL)
	{
	}

public:
	explicit jrd_prc(MemoryPool& p)
		: Routine(p),
		  prc_record_format(NULL),
		  prc_type(prc_legacy),
		  prc_external(NULL)
	{
	}

public:
	virtual int getObjectType() const
	{
		return obj_procedure;
	}

	virtual SLONG getSclType() const
	{
		return obj_procedures;
	}

	virtual void releaseFormat()
	{
		delete prc_record_format;
		prc_record_format = NULL;
	}

private:
	virtual ~jrd_prc() override
	{
		delete prc_external;
	}

public:
	static jrd_prc* create(thread_db* tdbb, MetaId id, CacheObject::Flag flags);

	virtual bool checkCache(thread_db* tdbb) const;

	virtual void releaseExternal()
	{
		delete prc_external;
		prc_external = NULL;
	}

protected:
	virtual bool reload(thread_db* tdbb);	// impl is in met.epp
};


// Parameter block

class Parameter : public pool_alloc<type_prm>
{
public:
	USHORT		prm_number;
	dsc			prm_desc;
	NestConst<ValueExprNode>	prm_default_value;
	bool		prm_nullable;
	prm_mech_t	prm_mechanism;
	MetaName prm_name;
	MetaName prm_field_source;
	MetaName prm_type_of_column;
	MetaName prm_type_of_table;
	Nullable<USHORT> prm_text_type;
	FUN_T		prm_fun_mechanism;

public:
	explicit Parameter(MemoryPool& p)
		: prm_name(p),
		  prm_field_source(p),
		  prm_type_of_column(p),
		  prm_type_of_table(p)
	{
	}
};


struct index_desc;
struct DSqlCacheItem;

// index status
enum IndexStatus
{
	MET_object_active,
	MET_object_deferred_active,
	MET_object_inactive,
	MET_object_unknown
};

class CharSet;

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

	static const CacheObject::Flag IGNORE_MASK = COMMITTED | ERASED;
}

template <class OBJ>
class CacheList : public HazardObject
{
public:
	CacheList(OBJ* obj, TraNumber currentTrans, CacheObject::Flag fl = 0)
		: object(obj), next(nullptr), traNumber(currentTrans), cacheFlags(fl)
	{ }

	// find appropriate object in cache
	OBJ* getObject(TraNumber currentTrans, CacheObject::Flag flags) const
	{
		CacheObject::Flag f(cacheFlags.load() & ~(flags & CacheFlag::IGNORE_MASK));

		// object deleted, good bye
		if (f & CacheFlag::ERASED)
			return nullptr;

		// committed (i.e. confirmed) objects are freely available
		if (f & CacheFlag::COMMITTED)
			return object;

		// transaction that created an object can always access it
		if ((traNumber == currentTrans) && currentTrans)
			return object;

		// try next level
		CacheList* n = next.load(atomics::memory_order_acquire);
		return n ? n->getObject(currentTrans, flags) : nullptr;
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
			newVal->next.store(oldVal.getPointer(), atomics::memory_order_relaxed);
		} while (! oldVal.replace2(list, newVal));

		return true;
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
	void commit(TraNumber currentTrans, TraNumber nextTrans)
	{
		fb_assert(cacheFlags == 0);
		fb_assert(traNumber == currentTrans);
		traNumber = nextTrans;
		cacheFlags |= CacheFlag::COMMITTED;
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

	// mark as erased
	void erase()
	{
		cacheFlags |= CacheFlag::ERASED;
	}

	void assertCommitted()
	{
		fb_assert(cacheFlags & CacheFlag::COMMITTED);
	}

private:
	OBJ* object;
	atomics::atomic<CacheList*> next;
	TraNumber traNumber;	// when COMMITTED not set - stores transaction that created this list element
							// when COMMITTED is set - stores transaction after which older elements are not needed
							// traNumber to be changed BEFORE setting COMMITTED
	MdcVersion version;		// version of metadata cache when object was added
	atomics::atomic<CacheObject::Flag> cacheFlags;
};


class CurrentTransaction
{
public:
	static TraNumber getNumber(thread_db* tdbb);
/*	static TraNumber currentTraNumber(thread_db* tdbb)
	{
		jrd_tra* tra = tdbb->getTransaction();
		return tra ? tra->tra_number : 0;
	} */
};


template <class OBJ>
class CacheElement : public ObjectBase
{
	typedef CacheList<OBJ> CachedObj;

public:
	CacheElement(MetaId id) :
		list(nullptr), resetAt(0), myId(id)
	{ }

	~CacheElement()
	{
		cleanup();
	}

	OBJ* getObject(thread_db* tdbb, CacheObject::Flag flags = 0)
	{
		HazardPtr<CachedObj> l(list);
		if (!l)
			return nullptr;
		return l->getObject(CurrentTransaction::getNumber(tdbb), flags);
	}

	bool storeObject(thread_db* tdbb, OBJ* obj, CacheObject::Flag fl = 0)
	{
		TraNumber oldest = tdbb->getDatabase()->dbb_oldest_active;
		TraNumber oldResetAt = resetAt.load(atomics::memory_order_acquire);
		if (oldResetAt && oldResetAt < oldest)
			setNewResetAt(oldResetAt, CachedObj::cleanup(list, oldest));

		TraNumber current = CurrentTransaction::getNumber(tdbb);
		CachedObj* value = FB_NEW CachedObj(obj, current, fl & CacheFlag::IGNORE_MASK);
		if (!CachedObj::add(list, value))
		{
			delete value;
			return false;
		}

		setNewResetAt(oldResetAt, current);
		return true;
	}

	void commit(thread_db* tdbb)
	{
		HazardPtr<CachedObj> current(list);
		if (current)
			current->commit(CurrentTransaction::getNumber(tdbb), tdbb->getDatabase()->dbb_next_transaction);
	}

	void rollback(thread_db* tdbb)
	{
		CacheList<OBJ>::rollback(list, CurrentTransaction::getNumber(tdbb));
	}

	void cleanup()
	{
		list.load()->assertCommitted();
		CacheList<OBJ>::cleanup(list, MAX_TRA_NUMBER);
	}

	void resetDependentObject(thread_db* tdbb, ResetType rt) override
	{
		switch (rt)
		{
		case ObjectBase::ResetType::Recompile:
			{
				OBJ* newObj = OBJ::create(tdbb, myId, 0);
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

private:
	void setNewResetAt(TraNumber oldVal, TraNumber newVal)
	{
		resetAt.compare_exchange_strong(oldVal, newVal,
			atomics::memory_order_release, atomics::memory_order_relaxed);
	}

	atomics::atomic<CachedObj*> list;
	atomics::atomic<TraNumber> resetAt;
	MetaId myId;
};


template <class E, unsigned SUBARRAY_SHIFT = 8>
class CacheVector : public Firebird::PermanentStorage
{
public:
	static const unsigned SUBARRAY_SIZE = 1 << SUBARRAY_SHIFT;
	static const unsigned SUBARRAY_MASK = SUBARRAY_SIZE - 1;

	typedef CacheElement<E> StoredObject;
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
	StoredObject* getData(thread_db*, MetaId id)
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

			auto val = E::create(tdbb, id, flags);
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
			MemoryPool* pool = tdbb->getDatabase()->dbb_permanent;
			fb_assert(pool);
			StoredObject* newData = FB_NEW_POOL(*pool) StoredObject(id);
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

		delete a;
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

	class Snapshot;

	class Iterator
	{
	public:
		HazardPtr<E> operator*()
		{
			return get();
		}

		HazardPtr<E> operator->()
		{
			return get();
		}

		Iterator& operator++()
		{
			index = snap->locateData(index + 1);
			return *this;
		}

		bool operator==(const Iterator& itr) const
		{
			fb_assert(snap == itr.snap);
			return index == itr.index;
		}

		bool operator!=(const Iterator& itr) const
		{
			fb_assert(snap == itr.snap);
			return index != itr.index;
		}

	private:
		void* operator new(size_t);
		void* operator new[](size_t);

	public:
		enum class Location {Begin, End};
		Iterator(const Snapshot* s, Location loc)
			: snap(s),
			  index(loc == Location::Begin ? snap->locateData(0) :
			  	snap->data->getCount() << SUBARRAY_SHIFT)
		{ }

		HazardPtr<E> get()
		{
			HazardPtr<E> rc;
			if (!snap->load(index, rc))
				rc.clear();
			return rc;
		}

	private:
		const Snapshot* snap;
		FB_SIZE_T index;
	};

	class Snapshot
	{
	private:
		void* operator new(size_t);
		void* operator new[](size_t);

	public:
		Snapshot(const CacheVector* array)
			: data(array->readAccessor())
		{ }

		Iterator begin() const
		{
			return Iterator(this, Iterator::Location::Begin);
		}

		Iterator end() const
		{
			return Iterator(this, Iterator::Location::End);
		}

		FB_SIZE_T locateData(FB_SIZE_T index) const
		{
			for (FB_SIZE_T i = index >> SUBARRAY_SHIFT; i < data->getCount(); ++i, index = 0)
			{
				SubArrayData* const sub = data->value(i).load(atomics::memory_order_acquire);
				if (!sub)
					continue;

				for (FB_SIZE_T j = index & SUBARRAY_MASK; j < SUBARRAY_SIZE; ++j)
				{
					auto p = sub[j].load(atomics::memory_order_acquire);
					if (p && p->hasData())
						return (i << SUBARRAY_SHIFT) + j;
				}
			}
			return data->getCount() << SUBARRAY_SHIFT;
		}

		bool load(MetaId id, HazardPtr<E>& val) const
		{
			if (id < (data->getCount() << SUBARRAY_SHIFT))
			{
				SubArrayData* sub = data->value(id >> SUBARRAY_SHIFT).load(atomics::memory_order_acquire);
				if (sub)
				{
					val.set(sub[id & SUBARRAY_MASK]);
					if (val && val->hasData())
						return true;
				}
			}

			return false;
		}

		HazardPtr<typename Storage::Generation> data;
	};

	Snapshot snapshot() const
	{
		return Snapshot(this);
	}

private:
	Storage m_objects;
	Firebird::Mutex objectsGrowMutex;
};


class MetadataCache : public Firebird::PermanentStorage
{
	friend class CharSetContainer;
/*
	class ListNodeAllocator
	{
	public:
		typedef int value_type;

		T* allocate(std::size_t n);
		void deallocate(T* p, std::size_t n);
	};

	struct MetaTraits : public cds::container::michael_list::traits
	{
		typedef ListNodeAllocator allocator;
	};

	template <typename C>
	using MetaList = cds::container::MichaelList<cds::gc::DHP, C, MetaTraits>;
*/

public:
	MetadataCache(MemoryPool& pool)
		: Firebird::PermanentStorage(pool),
		  mdc_relations(getPool()),
		  mdc_procedures(getPool()),
		  mdc_functions(getPool()),
		  mdc_charsets(getPool()),
		  mdc_charset_ids(getPool())
	{
		memset(mdc_triggers, 0, sizeof(mdc_triggers));
		mdc_ddl_triggers = nullptr;
	}

	~MetadataCache();

/*
	// Objects are placed to this list after DROP OBJECT 
	// and wait for current OAT >= NEXT when DDL committed
	atomics::atomic<CacheList<ObjectBase>*> dropList;
	{
public:
	void drop(
}; ?????????????????????
*/


	void releaseIntlObjects(thread_db* tdbb);			// defined in intl.cpp
	void destroyIntlObjects(thread_db* tdbb);			// defined in intl.cpp

	void releaseRelations(thread_db* tdbb);
	void releaseLocks(thread_db* tdbb);
	void releaseGTTs(thread_db* tdbb);
	void runDBTriggers(thread_db* tdbb, TriggerAction action);
	void invalidateReplSet(thread_db* tdbb);
	Function* lookupFunction(thread_db* tdbb, const QualifiedName& name, USHORT setBits, USHORT clearBits);
	jrd_rel* getRelation(thread_db* tdbb, ULONG rel_id);
	void setRelation(thread_db* tdbb, ULONG rel_id, jrd_rel* rel);
	void releaseTrigger(thread_db* tdbb, USHORT triggerId, const MetaName& name);
	TrigVectorPtr* getTriggers(USHORT triggerId);

	MetaId relCount()
	{
		return mdc_relations.getCount();
	}

	Function* getFunction(thread_db* tdbb, MetaId id, bool noscan)
	{
		if (id >= mdc_functions.getCount())
			return nullptr;

		return mdc_functions.getObject(tdbb, id, CacheFlag::AUTOCREATE | (noscan ? CacheFlag::NOSCAN : 0));
	}

	bool makeFunction(thread_db* tdbb, MetaId id, Function* f)
	{
		return mdc_functions.storeObject(tdbb, id, f);
	}

	jrd_prc* getProcedure(thread_db* tdbb, MetaId id, bool grow = false)
	{
		if (id >= mdc_procedures.getCount() && !grow)
			return nullptr;

		return mdc_procedures.getObject(tdbb, id, CacheFlag::AUTOCREATE);
	}

	bool makeProcedure(thread_db* tdbb, MetaId id, jrd_prc* p)
	{
		return mdc_procedures.storeObject(tdbb, id, p);
	}

	CharSetContainer* getCharSet(thread_db* tdbb, MetaId id)
	{
		if (id >= mdc_charsets.getCount())
			return nullptr;

		return mdc_charsets.getObject(tdbb, id, 0);
	}

	bool makeCharSet(thread_db* tdbb, MetaId id, CharSetContainer* cs)
	{
		return mdc_charsets.storeObject(tdbb, id, cs);
	}

	// former met_proto.h
#ifdef DEV_BUILD
	static void verify_cache(thread_db* tdbb);
#else
	static void verify_cache(thread_db* tdbb) { }
#endif
	static void clear_cache(thread_db* tdbb);
	static void update_partners(thread_db* tdbb);
	static bool routine_in_use(thread_db* tdbb, HazardPtr<Routine> routine);
	void load_db_triggers(thread_db* tdbb, int type);
	void load_ddl_triggers(thread_db* tdbb);
	static jrd_prc* lookup_procedure(thread_db* tdbb, const QualifiedName& name, bool noscan);
	static jrd_prc* lookup_procedure_id(thread_db* tdbb, MetaId id, bool return_deleted, bool noscan, USHORT flags);
	static CacheElement<jrd_prc>* lookupProcedure(thread_db* tdbb, const MetaName& name);
	static CacheElement<jrd_prc>* lookupProcedure(thread_db* tdbb, MetaId id, bool noscan = false);
	static jrd_rel* lookup_relation(thread_db*, const MetaName&);
	static jrd_rel* lookup_relation_id(thread_db*, MetaId, bool);
	static CacheElement<jrd_rel>* lookupRelation(thread_db* tdbb, const MetaName& name);
	static CacheElement<jrd_rel>* lookupRelation(thread_db* tdbb, MetaId id, bool noscan = false);
	static void lookup_index(thread_db* tdbb, MetaName& index_name, const MetaName& relation_name, USHORT number);
	static ObjectBase::ReturnedId lookup_index_name(thread_db* tdbb, const MetaName& index_name,
								   MetaId* relation_id, IndexStatus* status);
	static void post_existence(thread_db* tdbb, jrd_rel* relation);
	static HazardPtr<jrd_prc> findProcedure(thread_db* tdbb, MetaId id, bool noscan, USHORT flags);
    static jrd_rel* findRelation(thread_db* tdbb, MetaId id);
	static bool get_char_coll_subtype(thread_db* tdbb, MetaId* id, const UCHAR* name, USHORT length);
	bool resolve_charset_and_collation(thread_db* tdbb, MetaId* id,
											  const UCHAR* charset, const UCHAR* collation);
	static DSqlCacheItem* get_dsql_cache_item(thread_db* tdbb, sym_type type, const QualifiedName& name);
	static void dsql_cache_release(thread_db* tdbb, sym_type type, const MetaName& name, const MetaName& package = "");
	static bool dsql_cache_use(thread_db* tdbb, sym_type type, const MetaName& name, const MetaName& package = "");
	// end of met_proto.h
	static bool checkRelation(thread_db* tdbb, jrd_rel* relation);

private:
	CacheVector<jrd_rel>			mdc_relations;
	CacheVector<jrd_prc>			mdc_procedures;
	TrigVectorPtr					mdc_triggers[DB_TRIGGER_MAX];
	TrigVectorPtr					mdc_ddl_triggers;
	CacheVector<Function>			mdc_functions;			// User defined functions
	CacheVector<CharSetContainer>	mdc_charsets;			// intl character set descriptions
	Firebird::GenericMap<Firebird::Pair<Firebird::Left<
		MetaName, USHORT> > > mdc_charset_ids;				// Character set ids

public:
	Firebird::Mutex mdc_db_triggers_mutex,					// Also used for load DDL triggers
					mdc_charset_mutex;						// Protects mdc_charset_ids
};

} // namespace Jrd

#endif // JRD_MET_H
