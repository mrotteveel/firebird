/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		Resource.h
 *	DESCRIPTION:	Resource used by request / transaction
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
 *
 * 2001.07.28: Added rse_skip to class RecordSelExpr to support LIMIT.
 * 2002.09.28 Dmitry Yemanov: Reworked internal_info stuff, enhanced
 *                            exception handling in SPs/triggers,
 *                            implemented ROWS_AFFECTED system variable
 * 2002.10.21 Nickolay Samofatov: Added support for explicit pessimistic locks
 * 2002.10.29 Nickolay Samofatov: Added support for savepoints
 * Adriano dos Santos Fernandes
 */

#ifndef JRD_RESOURCE_H
#define JRD_RESOURCE_H

#include "fb_blk.h"
#include "../jrd/HazardPtr.h"

namespace Jrd {

class RelationPermanent;
class RoutinePermanent;
class CharSetContainer;
class jrd_rel;
class jrd_prc;
class Function;
class DbTriggersHeader;
class DbTriggers;
class CharSetVers;

class Resources;

// Set of objects cached per particular MDC version

union VersionedPartPtr
{
	jrd_rel* relation;
	jrd_prc* procedure;
	Function* function;
};

class VersionedObjects : public pool_alloc_rpt<VersionedPartPtr>,
	public Firebird::RefCounted
{

public:
	VersionedObjects(FB_SIZE_T cnt, MdcVersion ver) :
		version(ver),
		capacity(cnt)
	{ }

	template <class C>
	void put(FB_SIZE_T n, C* obj)
	{
		fb_assert(n < capacity);
		fb_assert(!object<C>(n));
		object<C>(n) = obj;
	}

	template <class C>
	C* get(FB_SIZE_T n) const
	{
		fb_assert(n < capacity);
		return object<C>(n);
	}

	FB_SIZE_T getCapacity()
	{
		return capacity;
	}

	const MdcVersion version;		// version when created

private:
	FB_SIZE_T capacity;
	VersionedPartPtr data[1];

	template <class C> C*& object(FB_SIZE_T n);
	template <class C> C* object(FB_SIZE_T n) const;
};

// specialization
template <> Function*& VersionedObjects::object<Function>(FB_SIZE_T n) { return data[n].function; }
template <> jrd_prc*& VersionedObjects::object<jrd_prc>(FB_SIZE_T n) { return data[n].procedure; }
template <> jrd_rel*& VersionedObjects::object<jrd_rel>(FB_SIZE_T n) { return data[n].relation; }

template <> jrd_rel* VersionedObjects::object<jrd_rel>(FB_SIZE_T n) const { return data[n].relation; }

//template <> *& object<*>(FB_SIZE_T n) { check(n); return data[n].; }


template <class OBJ, class PERM>
class CachedResource
{
public:
	CachedResource(CacheElement<OBJ, PERM>* elem, FB_SIZE_T version)
		: cacheElement(elem), versionOffset(version)
	{ }

	CachedResource()
		: cacheElement(nullptr)
	{ }

	OBJ* operator()(const VersionedObjects* runTime) const
	{
		return runTime->get<OBJ>(versionOffset);
	}

	OBJ* operator()(thread_db* tdbb) const
	{
		return cacheElement->getObject(tdbb);
	}

	CacheElement<OBJ, PERM>* operator()() const
	{
		return cacheElement;
	}

	FB_SIZE_T getOffset() const
	{
		return versionOffset;
	}

	void clear()
	{
		cacheElement = nullptr;
	}

	bool isSet() const
	{
		return cacheElement != nullptr;
	}

	operator bool() const
	{
		return isSet();
	}

	bool operator!() const
	{
		return !isSet();
	}

private:
	CacheElement<OBJ, PERM>* cacheElement;
	FB_SIZE_T versionOffset;
};


class Resources
{
public:
	template <class OBJ, class PERM>
	class RscArray : public Firebird::Array<CachedResource<OBJ, PERM>>
	{
	public:
		RscArray(MemoryPool& p, FB_SIZE_T& pos)
			: Firebird::Array<CachedResource<OBJ, PERM>>(p),
			  versionCurrentPosition(pos)
		{ }

		CachedResource<OBJ, PERM>& registerResource(CacheElement<OBJ, PERM>* res)
		{
			FB_SIZE_T pos;
			if (!this->find([res](const CachedResource<OBJ, PERM>& elem) {
					const void* p1 = elem();
					const void* p2 = res;
					return p1 < p2 ? -1 : p1 == p2 ? 0 : 1;
				}, pos))
			{
				CachedResource<OBJ, PERM> newPtr(res, versionCurrentPosition++);
				pos = this->add(newPtr);
			}

			return this->getElement(pos);
		}

		void transfer(thread_db* tdbb, VersionedObjects* to)
		{
			for (auto& resource : *this)
				to->put(resource.getOffset(), resource()->getObject(tdbb));
		}

	private:
		FB_SIZE_T& versionCurrentPosition;
	};

	void transfer(thread_db* tdbb, VersionedObjects* to);	// Impl-ted in Statement.cpp

private:
	FB_SIZE_T versionCurrentPosition;

public:
	template <class OBJ, class PERM> const RscArray<OBJ, PERM>& objects() const;

	Resources(MemoryPool& p)
		: versionCurrentPosition(0),
		  charSets(p, versionCurrentPosition),
		  relations(p, versionCurrentPosition),
		  procedures(p, versionCurrentPosition),
		  functions(p, versionCurrentPosition),
		  triggers(p, versionCurrentPosition)
	{ }

	RscArray<CharSetVers, CharSetContainer> charSets;
	RscArray<jrd_rel, RelationPermanent> relations;
	RscArray<jrd_prc, RoutinePermanent> procedures;
	RscArray<Function, RoutinePermanent> functions;
	RscArray<DbTriggers, DbTriggersHeader> triggers;
};

// specialization
template <> const Resources::RscArray<jrd_rel, RelationPermanent>& Resources::objects() const { return relations; }
template <> const Resources::RscArray<jrd_prc, RoutinePermanent>& Resources::objects() const { return procedures; }
template <> const Resources::RscArray<Function, RoutinePermanent>& Resources::objects() const { return functions; }
template <> const Resources::RscArray<CharSetVers, CharSetContainer>& Resources::objects() const { return charSets; }
template <> const Resources::RscArray<DbTriggers, DbTriggersHeader>& Resources::objects() const { return triggers; }

namespace Rsc
{
	typedef CachedResource<jrd_rel, RelationPermanent> Rel;
	typedef CachedResource<jrd_prc, RoutinePermanent> Proc;
	typedef CachedResource<Function, RoutinePermanent> Fun;
	typedef CachedResource<CharSetVers, CharSetContainer> CSet;
	typedef CachedResource<DbTriggers, DbTriggersHeader> Trig;
}; //namespace Rsc





// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //
// -------------------------------------------------------------------------- //

/*

class jrd_rel;
class Routine;
class Collation;

struct Resource
{
	enum rsc_s : UCHAR
	{
		rsc_relation,
		rsc_index,
		rsc_collation,
		rsc_procedure,
		rsc_function,
		rsc_MAX
	};

	Resource(rsc_s type, USHORT id, jrd_rel* rel)
		: rsc_rel(rel), rsc_routine(nullptr), rsc_coll(nullptr),
		  rsc_id(id), rsc_type(type), rsc_state(State::Registered)
	{
		fb_assert(rsc_type == rsc_relation || rsc_type == rsc_index);
	}

	Resource(rsc_s type, USHORT id, Routine* routine)
		: rsc_rel(nullptr), rsc_routine(routine), rsc_coll(nullptr),
		  rsc_id(id), rsc_type(type), rsc_state(State::Registered)
	{
		fb_assert(rsc_type == rsc_procedure || rsc_type == rsc_function);
	}

	Resource(rsc_s type, USHORT id, Collation* coll)
		: rsc_rel(nullptr), rsc_routine(nullptr), rsc_coll(coll),
		  rsc_id(id), rsc_type(type), rsc_state(State::Registered)
	{
		fb_assert(rsc_type == rsc_collation);
	}

	Resource(rsc_s type)
		: rsc_rel(nullptr), rsc_routine(nullptr), rsc_coll(nullptr),
		  rsc_id(0), rsc_type(type), rsc_state(State::Registered)
	{ }

	static constexpr rsc_s next(rsc_s type)
	{
		fb_assert(type != rsc_MAX);
		return static_cast<rsc_s>(static_cast<int>(type) + 1);
	}

	// Resource state makes sense only for permanently (i.e. in some list) stored resource
	enum class State : UCHAR
	{
		Registered,
		Posted,
		Counted,
		Locked,
		Extra,
		Unlocking
	};

	jrd_rel*	rsc_rel;		// Relation block
	Routine*	rsc_routine;	// Routine block
	Collation*	rsc_coll;		// Collation block
	USHORT		rsc_id;			// Id of the resource
	rsc_s		rsc_type;		// Resource type
	State		rsc_state;		// What actions were taken with resource

	static bool greaterThan(const Resource& i1, const Resource& i2)
	{
		// A few places of the engine depend on fact that rsc_type
		// is the first field in ResourceList ordering
		if (i1.rsc_type != i2.rsc_type)
			return i1.rsc_type > i2.rsc_type;
		if (i1.rsc_type == rsc_index)
		{
			// Sort by relation ID for now
			if (i1.relId() != i2.relId())
				return i1.relId() > i2.relId();
		}
		return i1.rsc_id > i2.rsc_id;
	}

	bool operator>(const Resource& i2) const
	{
		return greaterThan(*this, i2);
	}

	USHORT relId() const;
};

*/

} // namespace Jrd

#endif // JRD_RESOURCE_H

