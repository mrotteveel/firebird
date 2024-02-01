/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		Resources.h
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

#ifndef JRD_RESOURCES_H
#define JRD_RESOURCES_H

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

namespace Cached
{
	// DB objects stored in cache vector
	typedef CacheElement<jrd_rel, RelationPermanent> Relation;
	typedef CacheElement<jrd_prc, RoutinePermanent> Procedure;
	typedef CacheElement<CharSetVers, CharSetContainer> Charset;
	typedef CacheElement<Function, RoutinePermanent> Function;
	typedef CacheElement<DbTriggers, DbTriggersHeader> Triggers;
}

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


} // namespace Jrd

#endif // JRD_RESOURCES_H

