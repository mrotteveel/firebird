/*
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
 *  The Original Code was created by Vlad Khorsun
 *  for the Firebird Open Source RDBMS project.
 *
 *  Copyright (c) 2005 Vlad Khorsun <hvlad@users.sourceforge.net>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 */

#include "firebird.h"
#include "../jrd/Relation.h"

#include "../jrd/met.h"
#include "../jrd/tra.h"
#include "../jrd/btr_proto.h"
#include "../jrd/dpm_proto.h"
#include "../jrd/idx_proto.h"
#include "../jrd/lck.h"
#include "../jrd/met_proto.h"
#include "../jrd/pag_proto.h"
#include "../jrd/vio_debug.h"
#include "../jrd/ext_proto.h"
#include "../jrd/Statement.h"
#include "../common/StatusArg.h"

// Pick up relation ids
#include "../jrd/ini.h"

using namespace Jrd;
using namespace Firebird;


TrigArray::TrigArray(MemoryPool& p)
	: preErase(p), postErase(p), preModify(p),
	  postModify(p), preStore(p), postStore(p)
{ }

Triggers& TrigArray::operator[](int t)
{
	switch(t)
	{
	case TRIGGER_PRE_STORE:
		return preStore;

	case TRIGGER_POST_STORE:
		return postStore;

	case TRIGGER_PRE_MODIFY:
		return preModify;

	case TRIGGER_POST_MODIFY:
		return postModify;

	case TRIGGER_PRE_ERASE:
		return preErase;

	case TRIGGER_POST_ERASE:
		return postErase;
	}

	fb_assert(false);
	fatal_exception::raise("Invalid trigger type");
}

const char* DbTriggersHeader::c_name() const
{
	switch(type)
	{
	case TRIGGER_CONNECT:
		return "database connect";

	case TRIGGER_DISCONNECT:
		return "database disconnect";

	case TRIGGER_TRANS_START:
		return "transaction start";

	case TRIGGER_TRANS_COMMIT:
		return "transaction commit";

	case TRIGGER_TRANS_ROLLBACK:
		return "transaction rollback";
	}

	return "DDL";
}

const Triggers& TrigArray::operator[](int t) const
{
	switch(t)
	{
	case TRIGGER_PRE_STORE:
		return preStore;

	case TRIGGER_POST_STORE:
		return postStore;

	case TRIGGER_PRE_MODIFY:
		return preModify;

	case TRIGGER_POST_MODIFY:
		return postModify;

	case TRIGGER_PRE_ERASE:
		return preErase;

	case TRIGGER_POST_ERASE:
		return postErase;
	}

	fb_assert(false);
	fatal_exception::raise("Invalid trigger type");
}

jrd_rel::jrd_rel(MemoryPool& p, Cached::Relation* r)
	: rel_pool(&p),
	  rel_perm(r),
	  rel_current_fmt(0),
	  rel_current_format(nullptr),
	  rel_fields(nullptr),
	  rel_view_rse(nullptr),
	  rel_view_contexts(p),
	  rel_ss_definer(false),
	  rel_triggers(p)
{ }

RelationPermanent::RelationPermanent(thread_db* tdbb, MemoryPool& p, MetaId id, MakeLock* /*makeLock*/)
	: PermanentStorage(p),
	  rel_existence_lock(nullptr),
	  rel_partners_lock(nullptr),
	  rel_rescan_lock(nullptr),
	  rel_gc_lock(this),
	  rel_gc_records(p),
	  rel_sweep_count(0),
	  rel_scan_count(0),
	  rel_formats(nullptr),
	  rel_index_locks(),
	  rel_name(p),
	  rel_id(id),
	  rel_flags(0u),
	  rel_index_blocks(nullptr),
	  rel_pages_inst(nullptr),
	  rel_pages_base(p),
	  rel_pages_free(nullptr),
	  rel_file(nullptr)
{
	rel_partners_lock = FB_NEW_RPT(getPool(), 0)
		Lock(tdbb, sizeof(SLONG), LCK_rel_partners, this, partners_ast_relation);
	rel_partners_lock->setKey(rel_id);

	rel_rescan_lock = FB_NEW_RPT(getPool(), 0)
		Lock(tdbb, sizeof(SLONG), LCK_rel_rescan, this, rescan_ast_relation);
	rel_rescan_lock->setKey(rel_id);

	if (rel_id >= rel_MAX)
	{
		rel_existence_lock = FB_NEW_RPT(getPool(), 0)
			Lock(tdbb, sizeof(SLONG), LCK_rel_exist, this, blocking_ast_relation);
		rel_existence_lock->setKey(rel_id);
	}

}

RelationPermanent::~RelationPermanent()
{
	fb_assert(!(rel_existence_lock || rel_partners_lock || rel_rescan_lock));
}

bool RelationPermanent::destroy(thread_db* tdbb, RelationPermanent* rel)
{
	if (rel->rel_existence_lock)
	{
		LCK_release(tdbb, rel->rel_existence_lock);
		rel->rel_existence_lock = nullptr;
	}

	if (rel->rel_partners_lock)
	{
		LCK_release(tdbb, rel->rel_partners_lock);
		rel->rel_partners_lock = nullptr;
	}

	if (rel->rel_rescan_lock)
	{
		LCK_release(tdbb, rel->rel_rescan_lock);
		rel->rel_rescan_lock = nullptr;
	}

	delete rel->rel_file;
/*
	// delete by pool
	auto& pool = rel->getPool();
	tdbb->getDatabase()->deletePool(&pool);

	return true;*/
	return false;
}

bool RelationPermanent::isReplicating(thread_db* tdbb)
{
	Database* const dbb = tdbb->getDatabase();
	if (!dbb->isReplicating(tdbb))
		return false;

	Attachment* const attachment = tdbb->getAttachment();
	attachment->checkReplSetLock(tdbb);

	if (rel_repl_state.isUnknown())
		rel_repl_state = MET_get_repl_state(tdbb, getName());

	return rel_repl_state.value;
}

RelationPages* RelationPermanent::getPagesInternal(thread_db* tdbb, TraNumber tran, bool allocPages)
{
	if (tdbb->tdbb_flags & TDBB_use_db_page_space)
		return &rel_pages_base;

	Jrd::Attachment* attachment = tdbb->getAttachment();
	Database* dbb = tdbb->getDatabase();

	RelationPages::InstanceId inst_id;

	if (rel_flags & REL_temp_tran)
	{
		if (tran != 0 && tran != MAX_TRA_NUMBER)
			inst_id = tran;
		else if (tdbb->tdbb_temp_traid)
			inst_id = tdbb->tdbb_temp_traid;
		else if (tdbb->getTransaction())
			inst_id = tdbb->getTransaction()->tra_number;
		else // called without transaction, maybe from OPT or CMP ?
			return &rel_pages_base;
	}
	else
		inst_id = PAG_attachment_id(tdbb);

	MutexLockGuard relPerm(rel_pages_mutex, FB_FUNCTION);

	if (!rel_pages_inst)
		rel_pages_inst = FB_NEW_POOL(getPool()) RelationPagesInstances(getPool());

	FB_SIZE_T pos;
	if (!rel_pages_inst->find(inst_id, pos))
	{
		if (!allocPages)
			return 0;

		RelationPages* newPages = rel_pages_free;
		if (!newPages) {
			newPages = FB_NEW_POOL(getPool()) RelationPages(getPool());
		}
		else
		{
			rel_pages_free = newPages->rel_next_free;
			newPages->rel_next_free = 0;
		}

		fb_assert(newPages->useCount == 0);

		newPages->addRef();
		newPages->rel_instance_id = inst_id;
		newPages->rel_pg_space_id = dbb->dbb_page_manager.getTempPageSpaceID(tdbb);
		rel_pages_inst->add(newPages);

		// create primary pointer page and index root page
		DPM_create_relation_pages(tdbb, this, newPages);

#ifdef VIO_DEBUG
		VIO_trace(DEBUG_WRITES,
			"jrd_rel::getPages rel_id %u, inst %" UQUADFORMAT", ppp %" ULONGFORMAT", irp %" ULONGFORMAT", addr 0x%x\n",
			getId(),
			newPages->rel_instance_id,
			newPages->rel_pages ? (*newPages->rel_pages)[0] : 0,
			newPages->rel_index_root,
			newPages);
#endif

		// create indexes
		MemoryPool* pool = tdbb->getDefaultPool();
		const bool poolCreated = !pool;

		if (poolCreated)
			pool = dbb->createPool();
		Jrd::ContextPoolHolder context(tdbb, pool);

		jrd_tra* idxTran = tdbb->getTransaction();
		if (!idxTran)
			idxTran = attachment->getSysTransaction();

		jrd_rel* rel = MetadataCache::lookup_relation_id(tdbb, getId(), CacheFlag::AUTOCREATE);
		fb_assert(rel);

		IndexDescList indices;
		BTR_all(tdbb, getPermanent(rel), indices, &rel_pages_base);

		for (auto& idx : indices)
		{
			MetaName idx_name;
			MetadataCache::lookup_index(tdbb, idx_name, this->rel_name, idx.idx_id + 1);

			idx.idx_root = 0;
			SelectivityList selectivity(*pool);
			IDX_create_index(tdbb, rel, &idx, idx_name.c_str(), NULL, idxTran, selectivity);

#ifdef VIO_DEBUG
			VIO_trace(DEBUG_WRITES,
				"jrd_rel::getPages rel_id %u, inst %" UQUADFORMAT", irp %" ULONGFORMAT", idx %u, idx_root %" ULONGFORMAT", addr 0x%x\n",
				getId(),
				newPages->rel_instance_id,
				newPages->rel_index_root,
				idx.idx_id,
				idx.idx_root,
				newPages);
#endif
		}

		if (poolCreated)
			dbb->deletePool(pool);

		return newPages;
	}

	RelationPages* pages = (*rel_pages_inst)[pos];
	fb_assert(pages->rel_instance_id == inst_id);
	return pages;
}

bool RelationPermanent::delPages(thread_db* tdbb, TraNumber tran, RelationPages* aPages)
{
	RelationPages* pages = aPages ? aPages : getPages(tdbb, tran, false);
	if (!pages || !pages->rel_instance_id)
		return false;

	fb_assert(tran == 0 || tran == MAX_TRA_NUMBER || pages->rel_instance_id == tran);

	fb_assert(pages->useCount > 0);

	if (--pages->useCount)
		return false;

#ifdef VIO_DEBUG
	VIO_trace(DEBUG_WRITES,
		"jrd_rel::delPages rel_id %u, inst %" UQUADFORMAT", ppp %" ULONGFORMAT", irp %" ULONGFORMAT", addr 0x%x\n",
		getId(),
		pages->rel_instance_id,
		pages->rel_pages ? (*pages->rel_pages)[0] : 0,
		pages->rel_index_root,
		pages);
#endif

	FB_SIZE_T pos;
#ifdef DEV_BUILD
	const bool found =
#endif
		rel_pages_inst->find(pages->rel_instance_id, pos);
	fb_assert(found && ((*rel_pages_inst)[pos] == pages) );

	rel_pages_inst->remove(pos);

	if (pages->rel_index_root)
		IDX_delete_indices(tdbb, this, pages);

	if (pages->rel_pages)
		DPM_delete_relation_pages(tdbb, this, pages);

	pages->free(rel_pages_free);
	return true;
}

void RelationPermanent::retainPages(thread_db* tdbb, TraNumber oldNumber, TraNumber newNumber)
{
	fb_assert(rel_flags & REL_temp_tran);
	fb_assert(oldNumber != 0);
	fb_assert(newNumber != 0);

	if (!rel_pages_inst)
		return;

	SINT64 inst_id = oldNumber;
	FB_SIZE_T pos;
	if (!rel_pages_inst->find(oldNumber, pos))
		return;

	RelationPages* pages = (*rel_pages_inst)[pos];
	fb_assert(pages->rel_instance_id == oldNumber);

	rel_pages_inst->remove(pos);

	pages->rel_instance_id = newNumber;
	rel_pages_inst->add(pages);
}

void RelationPermanent::getRelLockKey(thread_db* tdbb, UCHAR* key)
{
	const ULONG val = getId();
	memcpy(key, &val, sizeof(ULONG));
	key += sizeof(ULONG);

	const RelationPages::InstanceId inst_id = getPages(tdbb)->rel_instance_id;
	memcpy(key, &inst_id, sizeof(inst_id));
}

USHORT constexpr RelationPermanent::getRelLockKeyLength()
{
	return sizeof(ULONG) + sizeof(SINT64);
}

void RelationPermanent::cleanUp()
{
	delete rel_pages_inst;
	rel_pages_inst = NULL;
}


void RelationPermanent::fillPagesSnapshot(RelPagesSnapshot& snapshot, const bool attachmentOnly)
{
	if (rel_pages_inst)
	{
		for (FB_SIZE_T i = 0; i < rel_pages_inst->getCount(); i++)
		{
			RelationPages* relPages = (*rel_pages_inst)[i];

			if (!attachmentOnly)
			{
				snapshot.add(relPages);
				relPages->addRef();
			}
			else if ((rel_flags & REL_temp_conn) &&
				PAG_attachment_id(snapshot.spt_tdbb) == relPages->rel_instance_id)
			{
				snapshot.add(relPages);
				relPages->addRef();
			}
			else if (rel_flags & REL_temp_tran)
			{
				const jrd_tra* tran = snapshot.spt_tdbb->getAttachment()->att_transactions;
				for (; tran; tran = tran->tra_next)
				{
					if (tran->tra_number == relPages->rel_instance_id)
					{
						snapshot.add(relPages);
						relPages->addRef();
					}
				}
			}
		}
	}
	else
		snapshot.add(&rel_pages_base);
}

void RelationPermanent::RelPagesSnapshot::clear()
{
#ifdef DEV_BUILD
	thread_db* tdbb = NULL;
	SET_TDBB(tdbb);
	fb_assert(tdbb == spt_tdbb);
#endif

	for (FB_SIZE_T i = 0; i < getCount(); i++)
	{
		RelationPages* relPages = (*this)[i];
		(*this)[i] = NULL;

		spt_relation->delPages(spt_tdbb, MAX_TRA_NUMBER, relPages);
	}

	inherited::clear();
}

/* ?????????????
bool jrd_rel::hasTriggers() const
{
	typedef const TrigVector* ctv;
	ctv trigs[6] = // non-const array, don't want optimization tricks by the compiler.
	{
		rel_pre_erase,
		rel_post_erase,
		rel_pre_modify,
		rel_post_modify,
		rel_pre_store,
		rel_post_store
	};

	for (int i = 0; i < 6; ++i)
	{
		if (trigs[i] && trigs[i]->getCount())
			return true;
	}
	return false;
}
 */

void jrd_rel::releaseTriggers(thread_db* tdbb, bool destroy)
{
	for (int n = 1; n < TRIGGER_MAX; ++n)
	{
		rel_triggers[n].release(tdbb, destroy);
	}
}

void Triggers::release(thread_db* tdbb, bool destroy)
{
/***********************************************
 *
 *      M E T _ r e l e a s e _ t r i g g e r s
 *
 ***********************************************
 *
 * Functional description
 *      Release a possibly null vector of triggers.
 *      If triggers are still active let someone
 *      else do the work.
 *
 **************************************/
	if (destroy)
	{
		Triggers::destroy(tdbb, this);
		return;
	}

/*	TrigVector* vector = vector_ptr->load(); !!!!!!!!!!!!!!!!!!!!!!!!!

	if (!vector)
		return;

	if (!destroy)
	{
		vector->decompile(tdbb);
		return;
	}

	*vector_ptr = NULL;

	if (vector->hasActive())
		return;

	vector->release(tdbb); */
}

Lock* RelationPermanent::createLock(thread_db* tdbb, lck_t lckType, bool noAst)
{
	return createLock(tdbb, getPool(), lckType, noAst);
}

Lock* RelationPermanent::createLock(thread_db* tdbb, MemoryPool& pool, lck_t lckType, bool noAst)
{
	const USHORT relLockLen = getRelLockKeyLength();

	Lock* lock = FB_NEW_RPT(pool, relLockLen)
		Lock(tdbb, relLockLen, lckType, lckType == LCK_relation ? (void*)this : (void*)&rel_gc_lock);
	getRelLockKey(tdbb, lock->getKeyPtr());

	lock->lck_type = lckType;
	switch (lckType)
	{
	case LCK_relation:
		break;

	case LCK_rel_gc:
		lock->lck_ast = noAst ? nullptr : GCLock::ast;
		break;

	default:
		fb_assert(false);
	}

	return lock;
}


void GCLock::blockingAst()
{
	/****
	 SR - gc forbidden, awaiting moment to re-establish SW lock
	 SW - gc allowed, usual state
	 PW - gc allowed to the one connection only
	****/

	Database* dbb = lck->lck_dbb;

	AsyncContextHolder tdbb(dbb, FB_FUNCTION, lck);

	unsigned oldFlags = flags.load(std::memory_order_acquire);
	do
	{
		fb_assert(oldFlags & GC_locked);
		if (!(oldFlags & GC_locked)) // work already done synchronously ?
			return;
	} while (!flags.compare_exchange_weak(oldFlags, oldFlags | GC_blocking,
										  std::memory_order_release, std::memory_order_acquire));

	if (oldFlags & GC_counterMask)
		return;

	if (oldFlags & GC_disabled)
	{
		// someone acquired EX lock

		fb_assert(lck->lck_id);
		fb_assert(lck->lck_physical == LCK_SR);

		LCK_release(tdbb, lck);
		flags.fetch_and(~(GC_disabled | GC_blocking | GC_locked));
	}
	else
	{
		// someone acquired PW lock

		fb_assert(lck->lck_id);
		fb_assert(lck->lck_physical == LCK_SW);

		flags.fetch_or(GC_disabled);
		downgrade(tdbb);
	}
}

[[noreturn]] void GCLock::incrementError()
{
	fatal_exception::raise("Overflow when changing GC lock atomic counter (guard bit set)");
}

bool GCLock::acquire(thread_db* tdbb, int wait)
{
	unsigned oldFlags = flags.load(std::memory_order_acquire);
	for(;;)
	{
		if (oldFlags & (GC_blocking | GC_disabled))		// lock should not be obtained
			return false;

		const unsigned newFlags = oldFlags + 1;
		if (newFlags & GC_guardBit)
			incrementError();

		if (!flags.compare_exchange_weak(oldFlags, newFlags, std::memory_order_release, std::memory_order_acquire))
			continue;

		if (oldFlags & GC_locked)			// lock was already taken when we checked flags
			return true;

		if (!(oldFlags & GC_counterMask))	// we must take lock
			break;

		// unstable state - someone else it getting a lock right now:
		--flags;													// decrement counter,
		Thread::yield();											// wait a bit
		oldFlags = flags.fetch_sub(1, std::memory_order_acquire);	// and retry
	}

	// We incremented counter from 0 to 1 - take care about lck
	if (!lck)
		lck = relPerm->createLock(tdbb, LCK_rel_gc, false);

	fb_assert(!lck->lck_id);

	ThreadStatusGuard temp_status(tdbb);

	bool ret;
	if (oldFlags & GC_disabled)
		ret = LCK_lock(tdbb, lck, LCK_SR, wait);
	else
	{
		ret = LCK_lock(tdbb, lck, LCK_SW, wait);
		if (ret)
		{
			flags.fetch_or(GC_locked);
			return true;
		}
		else
		{
			flags.fetch_or(GC_disabled);
			ret = LCK_lock(tdbb, lck, LCK_SR, wait);
		}
	}

	flags.fetch_sub(1, std::memory_order_release);
	if (!ret)
		flags.fetch_and(~GC_disabled, std::memory_order_release);
	return false;
}

void GCLock::downgrade(thread_db* tdbb)
{
	unsigned oldFlags = flags.load(std::memory_order_acquire);
	unsigned newFlags;
	do
	{
		newFlags = oldFlags - 1;
		if (newFlags & GC_guardBit)
			incrementError();

		if ((newFlags & GC_counterMask == 0) && (newFlags & GC_blocking))
		{
			fb_assert(oldFlags & GC_locked);
			fb_assert(lck->lck_id);
			fb_assert(lck->lck_physical == LCK_SW);

			LCK_downgrade(tdbb, lck);

			if (lck->lck_physical != LCK_SR)
			{
				newFlags &= ~GC_disabled;
				if (lck->lck_physical < LCK_SR)
					newFlags &= GC_locked;
			}
			else
				newFlags |= GC_disabled;

			newFlags &= ~GC_blocking;
		}
	} while (!flags.compare_exchange_weak(oldFlags, newFlags, std::memory_order_release, std::memory_order_acquire));
}

bool GCLock::disable(thread_db* tdbb, int wait, Lock*& tempLock)
{
	ThreadStatusGuard temp_status(tdbb);

	// if validation is already running - go out
	unsigned oldFlags = flags.load(std::memory_order_acquire);
	do {
		if (oldFlags & GC_disabled)
			return false;
	} while (flags.compare_exchange_weak(oldFlags, oldFlags | GC_disabled,
										 std::memory_order_release, std::memory_order_acquire));

	int sleeps = -wait * 10;
	while (flags.load(std::memory_order_relaxed) & GC_counterMask)
	{
		EngineCheckout cout(tdbb, FB_FUNCTION);
		Thread::sleep(100);

		if (wait < 0 && --sleeps == 0)
			break;
	}

	if (flags.load(std::memory_order_relaxed) & GC_counterMask)
	{
		flags.fetch_and(~GC_disabled);
		return false;
	}

	ensureReleased(tdbb);

	// we need no AST here
	if (!tempLock)
		tempLock = relPerm->createLock(tdbb, LCK_rel_gc, true);

	const bool ret = LCK_lock(tdbb, tempLock, LCK_PW, wait);
	if (!ret)
		flags.fetch_and(~GC_disabled);

	return ret;
}

void GCLock::ensureReleased(thread_db* tdbb)
{
	unsigned oldFlags = flags.load(std::memory_order_acquire);
	for (;;)
	{
		if (oldFlags & GC_locked)
		{
			if (!flags.compare_exchange_strong(oldFlags, oldFlags & ~GC_locked,
											   std::memory_order_release, std::memory_order_acquire))
			{
				continue;
			}

			// exactly one who cleared GC_locked bit releases a lock
			LCK_release(tdbb, lck);
		}

		return;
	}
}

void GCLock::forcedRelease(thread_db* tdbb)
{
	flags.fetch_and(~GC_locked);
	if (lck)
		LCK_release(tdbb, lck);
}

void GCLock::enable(thread_db* tdbb, Lock* tempLock)
{
	if (!lck || !lck->lck_id)
		return;

	fb_assert(flags.load() & GC_disabled);

	ensureReleased(tdbb);

	LCK_convert(tdbb, tempLock, LCK_EX, LCK_WAIT);
	flags.fetch_and(~GC_disabled);

	LCK_release(tdbb, tempLock);
}


/// RelationPages

void RelationPages::free(RelationPages*& nextFree)
{
	rel_next_free = nextFree;
	nextFree = this;

	if (rel_pages)
		rel_pages->clear();

	rel_index_root = rel_data_pages = 0;
	rel_slot_space = rel_pri_data_space = rel_sec_data_space = 0;
	rel_last_free_pri_dp = rel_last_free_blb_dp = 0;
	rel_instance_id = 0;

	dpMap.clear();
	dpMapMark = 0;
}


IndexLock* RelationPermanent::getIndexLock(thread_db* tdbb, USHORT id)
{
/**************************************
 *
 *	C M P _ g e t _ i n d e x _ l o c k
 *
 **************************************
 *
 * Functional description
 *	Get index lock block for index.  If one doesn't exist,
 *	make one.
 *
 **************************************/
	SET_TDBB(tdbb);
	Database* dbb = tdbb->getDatabase();

	if (getId() < (MetaId) rel_MAX)
		return nullptr;

	auto ra = rel_index_locks.readAccessor();
	if (id < ra->getCount())
	{
		IndexLock* indexLock = ra->value(id);
		if (indexLock)
			return indexLock;
	}

	MutexLockGuard g(index_locks_mutex, FB_FUNCTION);

	rel_index_locks.grow(id + 1, false);
	auto wa = rel_index_locks.writeAccessor();
	while (auto* dp = wa->addStart())
	{
		*dp = nullptr;
		wa->addComplete();
	}

	IndexLock* indexLock = wa->value(id);
	if (!indexLock)
	{
		indexLock = FB_NEW_POOL(getPool()) IndexLock(getPool(), tdbb, this, id);
		wa->value(id) = indexLock;
	}

	return indexLock;
}

IndexLock::IndexLock(MemoryPool& p, thread_db* tdbb, RelationPermanent* rel, USHORT id)
	: idl_relation(rel),
	  idl_lock(FB_NEW_RPT(p, 0) Lock(tdbb, sizeof(SLONG), LCK_idx_exist)),
	  idl_count(0)
{
	idl_lock->setKey((idl_relation->rel_id << 16) | id);
}

void IndexLock::sharedLock(thread_db* tdbb)
{
	MutexLockGuard g(idl_mutex, FB_FUNCTION);

	if (idl_count++ <= 0)
	{
		if (idl_count < 0)
		{
			--idl_count;
			errIndexGone();
		}

		LCK_lock(tdbb, idl_lock, LCK_SR, LCK_WAIT);
	}
}

bool IndexLock::exclusiveLock(thread_db* tdbb)
{
	MutexLockGuard g(idl_mutex, FB_FUNCTION);

	if (idl_count < 0)
		errIndexGone();

	if (idl_count > 0)
		MetadataCache::clear(tdbb);

	if (idl_count ||
		!LCK_lock(tdbb, idl_lock, LCK_EX, tdbb->getTransaction()->getLockWait()))
	{
		return false;
	}

	idl_mutex.enter(FB_FUNCTION);		// keep exclusive in-process
	idl_count = exclLock;
	return true;
}

bool IndexLock::exclusiveUnlock(thread_db* tdbb)
{
	MutexLockGuard g(idl_mutex, FB_FUNCTION);

	if (idl_count == exclLock)
	{
		idl_mutex.leave();
		idl_count = 0;
		LCK_release(tdbb, idl_lock);

		return true;
	}

	return false;
}

void IndexLock::sharedUnlock(thread_db* tdbb)
{
	MutexLockGuard g(idl_mutex, FB_FUNCTION);

	if (idl_count > 0)
		--idl_count;

	if (idl_count == 0)
		LCK_release(tdbb, idl_lock);
}

void IndexLock::unlockAll(thread_db* tdbb)
{
	MutexLockGuard g(idl_mutex, FB_FUNCTION);

	if (!exclusiveUnlock(tdbb))
		LCK_release(tdbb, idl_lock);

	idl_count = offTheLock;
}

void IndexLock::recreate(thread_db*)
{
	MutexLockGuard g(idl_mutex, FB_FUNCTION);

	idl_count = 0;
}

[[noreturn]] void IndexLock::errIndexGone()
{
	fatal_exception::raise("Index is gone unexpectedly");
}


void jrd_rel::destroy(thread_db* tdbb, jrd_rel* rel)
{
    rel->releaseTriggers(tdbb, true);

	// A lot more things to do !!!!!!!!!!!!!!!!

	delete rel;
}

jrd_rel* jrd_rel::create(thread_db* tdbb, MemoryPool& pool, Cached::Relation* rlp)
{
	return FB_NEW_POOL(pool) jrd_rel(pool, rlp);
}

const char* jrd_rel::objectFamily(RelationPermanent* perm)
{
	return perm->isView() ? "view" : "table";
}

int jrd_rel::objectType()
{
	return obj_relation;
}

void Triggers::destroy(thread_db* tdbb, Triggers* trigs)
{
	for (auto t : trigs->triggers)
	{
		t->free(tdbb, true);
		delete t;
	}
	trigs->triggers.clear();
}

void Trigger::free(thread_db* tdbb, bool force)
{
	if (extTrigger)
	{
		delete extTrigger;
		extTrigger = nullptr;
	}

	// dimitr:	We should never release triggers created by MET_parse_sys_trigger().
	//			System triggers do have BLR, but it's not stored inside the trigger object.
	//			However, triggers backing RI constraints are also marked as system,
	//			but they are loaded in a regular way and their BLR is present here.
	//			This is why we cannot simply check for sysTrigger, sigh.

	const bool sysTableTrigger = (blr.isEmpty() && engine.isEmpty());

	if ((sysTableTrigger && !force) || !statement || releaseInProgress)
		return;

	AutoSetRestore<bool> autoProgressFlag(&releaseInProgress, true);

	statement->release(tdbb);
	statement = nullptr;
}


// class DbTriggers

DbTriggersHeader::DbTriggersHeader(thread_db* tdbb, MemoryPool& p, MetaId& t, MakeLock* makeLock)
	: Firebird::PermanentStorage(p),
	  type(t),
	  lock(nullptr)
{
	lock = makeLock(tdbb, p);
	lock->setKey(type);
	lock->lck_object = this;
}

bool DbTriggersHeader::destroy(thread_db* tdbb, DbTriggersHeader* trigs)
{
	if (trigs->lock)
	{
		LCK_release(tdbb, trigs->lock);
		trigs->lock = nullptr;
	}

	return false;
}

int DbTriggersHeader::blockingAst(void* ast_object)
{
	auto* const trigs = static_cast<Cached::Triggers*>(ast_object);

	try
	{
		Database* const dbb = trigs->lock->lck_dbb;

		AsyncContextHolder tdbb(dbb, FB_FUNCTION, trigs->lock);

		LCK_release(tdbb, trigs->lock);
		trigs->resetDependentObject(tdbb, ElementBase::ResetType::Mark);
	}
	catch (const Firebird::Exception&)
	{} // no-op

	return 0;
}

Lock* DbTriggers::makeLock(thread_db* tdbb, MemoryPool& p)
{
	return FB_NEW_RPT(p, 0) Lock(tdbb, sizeof(SLONG), LCK_dbwide_triggers, nullptr, DbTriggersHeader::blockingAst);
}

int DbTriggers::objectType()
{
	return obj_relation;
}

