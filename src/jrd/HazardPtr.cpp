/*
 *	PROGRAM:	Engine Code
 *	MODULE:		HazardPtr.cpp
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

#include "firebird.h"

#include "../jrd/HazardPtr.h"
#include "../jrd/jrd.h"

using namespace Jrd;
using namespace Firebird;

HazardObject::~HazardObject()
{ }

CacheObject* TRAP = nullptr;

int HazardObject::delayedDelete(thread_db* tdbb)
{
	HazardDelayedDelete* dd = HazardBase::getHazardDelayed(tdbb);
//	if (this == TRAP)
//		abort();
	dd->delayedDelete(this);
	return 0;
}

HazardDelayedDelete* HazardBase::getHazardDelayed(thread_db* tdbb)
{
	if (!tdbb)
		tdbb = JRD_get_thread_data();
	fb_assert(tdbb);

	Attachment* att = tdbb->getAttachment();
	if (att)
		return &att->att_delayed_delete;

	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	// what about locking object in database? (dbb_delayed_delete)
	Database* dbb = tdbb->getDatabase();
	fb_assert(dbb);
	return &dbb->dbb_delayed_delete;
}

HazardDelayedDelete* HazardBase::getHazardDelayed(Attachment* att)
{
	return &att->att_delayed_delete;
}

void HazardDelayedDelete::add(const void* ptr, const char* from)
{
	HZ_DEB(fprintf(stderr, "HazardDelayedDelete::add %s %p\n", from, ptr));
	// as long as we access our own hazard pointers use of write accessor is always OK
	auto hp = hazardPointers.writeAccessor();

	// 1. Search for holes
	for (unsigned n = 0; n < hp->getCount(); ++n)
	{
		if (hp->value(n) == nullptr)
		{
			// store
			hp->value(n) = ptr;
			return;
		}
	}

	// 2. Grow if needed
	if (!hp->hasSpace())
		hazardPointers.grow(this);

	// 3. Append
	hp = hazardPointers.writeAccessor();
	*(hp->add()) = ptr;
}

void HazardDelayedDelete::remove(const void* ptr, const char* from)
{
	HZ_DEB(fprintf(stderr, "HazardDelayedDelete::remove %s %p\n", from, ptr));
	// as long as we access our own hazard pointers use of write accessor is always OK
	auto hp = hazardPointers.writeAccessor();

	for (unsigned n = 0; n < hp->getCount(); ++n)
	{
		if (hp->value(n) == ptr)
		{
			hp->value(n) = nullptr;
			hp->truncate(nullptr);
			return;
		}
	}

	fb_assert(!"Required ptr not found in HazardDelayedDelete::remove");
}

void HazardDelayedDelete::delayedDelete(HazardObject* mem, bool gc)
{
	HZ_DEB(fprintf(stderr, "HazardDelayedDelete::delayedDelete %p\n", mem));

	if (mem)
		toDelete.push(mem);

	if (gc)
		garbageCollect(GarbageCollectMethod::GC_NORMAL);
}

void HazardDelayedDelete::copyHazardPointers(LocalHP& local, HazardPtr<HazardPointers>& from)
{
	for (unsigned n = 0; n < from->getCount(); ++n)
	{
		const void* ptr = from->value(n);
		if (ptr)
			local.push(ptr);
	}
}

void HazardDelayedDelete::copyHazardPointers(thread_db* tdbb, LocalHP& local, Attachment* from)
{
	for (Attachment* attachment = from; attachment; attachment = attachment->att_next)
	{
		HazardPtr<HazardPointers> hp = attachment->att_delayed_delete.hazardPointers.readAccessor(tdbb);
		copyHazardPointers(local, hp);
	}
}


void HazardDelayedDelete::garbageCollect(GarbageCollectMethod gcMethod)
{
	if (gcMethod == GarbageCollectMethod::GC_NORMAL && toDelete.getCount() < DELETED_LIST_SIZE)
		return;

	thread_db* tdbb = JRD_get_thread_data();
	Database* database = tdbb->getDatabase();

	// collect hazard pointers from all atachments
	LocalHP localCopy;
	localCopy.setSortMode(FB_ARRAY_SORT_MANUAL);
	{
		Sync dbbSync(&database->dbb_sync, FB_FUNCTION);
		if (!database->dbb_sync.ourExclusiveLock())
			dbbSync.lock(SYNC_SHARED);

		copyHazardPointers(tdbb, localCopy, database->dbb_attachments);
		copyHazardPointers(tdbb, localCopy, database->dbb_sys_attachments);

		HazardPtr<HazardPointers> hp = database->dbb_delayed_delete.hazardPointers.readAccessor(tdbb);
		copyHazardPointers(localCopy, hp);
	}
	localCopy.sort();

	// delete what can be deleted
	unsigned keep = 0;
	for (unsigned i = 0; i < toDelete.getCount(); ++i)
	{
		if (localCopy.exist(toDelete[i]))
			toDelete[keep++] = toDelete[i];
		else
			delete toDelete[i];

		if (i + 1 > keep)
			toDelete[i] = nullptr;
	}
	toDelete.shrink(keep);

	if (gcMethod != GarbageCollectMethod::GC_FORCE || keep == 0)
		return;

	// Pass remaining to Database
	MutexLockGuard g(database->dbb_dd_mutex, FB_FUNCTION);

	database->dbb_delayed_delete.garbageCollect(GarbageCollectMethod::GC_NORMAL);
	for (unsigned i = 0; i < toDelete.getCount(); ++i)
	{
		database->dbb_delayed_delete.add(toDelete[i], FB_FUNCTION);
		toDelete[i] = nullptr;
	}
	toDelete.shrink(0);
}

HazardDelayedDelete::HazardPointers* HazardDelayedDelete::getHazardPointers()
{
	// as long as we access our own hazard pointers single relaxed load is OK
	return hazardPointers.writeAccessor();
}

bool CacheObject::checkObject(thread_db*, Arg::StatusVector&)
{
	return true;
}

void CacheObject::afterUnlock(thread_db* tdbb, unsigned flags)
{
	// do nothing
}

void CacheObject::lockedExcl [[noreturn]] (thread_db* tdbb)
{
	fatal_exception::raise("Unspecified object locked exclusive for deletion");
}
