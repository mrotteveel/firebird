/*
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
 *
 */

#ifndef JRD_RELATION_H
#define JRD_RELATION_H

#include "../common/classes/RefCounted.h"

#include "../jrd/vec.h"
#include "../jrd/btr.h"
#include "../jrd/lck.h"
#include "../jrd/pag.h"
#include "../jrd/val.h"
#include "../jrd/Attachment.h"
#include "../jrd/HazardPtr.h"
#include "../jrd/ExtEngineManager.h"
#include "../jrd/met_proto.h"
#include "../jrd/Resources.h"

namespace Jrd
{

template <typename T> class vec;
class BoolExprNode;
class RseNode;
class StmtNode;
class jrd_fld;
class ExternalFile;
class RelationPermanent;
class jrd_rel;

// trigger types
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



// Relation trigger definition

class Trigger
{
public:
	Firebird::HalfStaticArray<UCHAR, 128> blr;			// BLR code
	Firebird::HalfStaticArray<UCHAR, 128> debugInfo;	// Debug info
	Statement* statement;							// Compiled statement
	bool		releaseInProgress;
	bool		sysTrigger;
	FB_UINT64	type;						// Trigger type
	USHORT		flags;						// Flags as they are in RDB$TRIGGERS table
	jrd_rel*	relation;					// Trigger parent relation
	MetaName	name;				// Trigger name
	MetaName	engine;				// External engine name
	Firebird::string	entryPoint;			// External trigger entrypoint
	Firebird::string	extBody;			// External trigger body
	ExtEngineManager::Trigger* extTrigger;	// External trigger
	Nullable<bool> ssDefiner;
	MetaName	owner;				// Owner for SQL SECURITY

	MemoryPool& getPool();

	bool isActive() const;

	void compile(thread_db*);				// Ensure that trigger is compiled
	void free(thread_db*, bool force);		// Try to free trigger request

	explicit Trigger(MemoryPool& p)
		: blr(p),
		  debugInfo(p),
		  releaseInProgress(false),
		  name(p),
		  engine(p),
		  entryPoint(p),
		  extBody(p),
		  extTrigger(NULL)
	{}

	virtual ~Trigger()
	{
		delete extTrigger;
	}
};

// Set of triggers (use separate arrays for triggers of different types)
class Triggers
{
public:
	explicit Triggers(MemoryPool& p)
		: triggers(p)
	{ }

	bool hasActive() const;
	void decompile(thread_db* tdbb);

	void addTrigger(thread_db*, Trigger* trigger)
	{
		triggers.add(trigger);
	}

	Trigger* const* begin() const
	{
		return triggers.begin();
	}

	Trigger* const* end() const
	{
		return triggers.end();
	}

	bool operator!() const
	{
		return !hasData();
	}

	operator bool() const
	{
		return hasData();
	}

	bool hasData() const
	{
		return triggers.hasData();
	}

	void release(thread_db* tdbb, bool destroy);

	static void destroy(thread_db* tdbb, Triggers* trigs);

private:
	Firebird::HalfStaticArray<Trigger*, 8> triggers;
};

class DbTriggersHeader : public Firebird::PermanentStorage
{
public:
	DbTriggersHeader(thread_db*, MemoryPool& p, MetaId& t, MakeLock* makeLock, NoData = NoData());

	MetaId getId()
	{
		return type;
	}

	static int blockingAst(void* ast_object);
	static bool destroy(thread_db* tdbb, DbTriggersHeader* trigs);

	const char* c_name() const;

private:
	MetaId type;
	Lock* lock;
};

class DbTriggers final : public Triggers, public ObjectBase
{
public:
	DbTriggers(DbTriggersHeader* hdr)
		: Triggers(hdr->getPool()),
		  ObjectBase(),
		  perm(hdr)
	{ }

	static DbTriggers* create(thread_db*, MemoryPool&, DbTriggersHeader* hdr)
	{
		return FB_NEW_POOL(hdr->getPool()) DbTriggers(hdr);
	}

	static void destroy(thread_db* tdbb, DbTriggers* trigs)
	{
		Triggers::destroy(tdbb, trigs);
		delete trigs;
	}

	static Lock* makeLock(thread_db* tdbb, MemoryPool& p);
	bool scan(thread_db* tdbb, ObjectBase::Flag flags);

	bool reload(thread_db* tdbb, ObjectBase::Flag flags)
	{
		return scan(tdbb, flags);
	}

	const char* c_name() const override
	{
		return perm->c_name();
	}

	static const char* objectFamily(void*)
	{
		return "set of database-wide triggers on";
	}

	static int objectType();

private:
	DbTriggersHeader* perm;

public:
	decltype(perm) getPermanent() const
	{
		return perm;
	}
};

class TrigArray
{
public:
	TrigArray(MemoryPool& p);
	Triggers& operator[](int t);
	const Triggers& operator[](int t) const;

private:
	Triggers preErase, postErase, preModify, postModify, preStore, postStore;
};


// view context block to cache view aliases

class ViewContext
{
public:
	explicit ViewContext(MemoryPool& p, const TEXT* context_name,
						 const TEXT* relation_name, USHORT context,
						 ViewContextType type)
	: vcx_context_name(p, context_name, fb_strlen(context_name)),
	  vcx_relation_name(relation_name),
	  vcx_context(context),
	  vcx_type(type)
	{
	}

	static USHORT generate(const ViewContext* vc)
	{
		return vc->vcx_context;
	}

	const Firebird::string vcx_context_name;
	const MetaName vcx_relation_name;
	const USHORT vcx_context;
	const ViewContextType vcx_type;
};

typedef Firebird::SortedArray<ViewContext*, Firebird::EmptyStorage<ViewContext*>,
		USHORT, ViewContext> ViewContexts;


class RelationPages
{
public:
	typedef FB_UINT64 InstanceId;

	// Vlad asked for this compile-time check to make sure we can contain a txn/att number here
	static_assert(sizeof(InstanceId) >= sizeof(TraNumber), "InstanceId must fit TraNumber");
	static_assert(sizeof(InstanceId) >= sizeof(AttNumber), "InstanceId must fit AttNumber");

	vcl* rel_pages;					// vector of pointer page numbers
	InstanceId rel_instance_id;		// 0 or att_attachment_id or tra_number

	ULONG rel_index_root;		// index root page number
	ULONG rel_data_pages;		// count of relation data pages
	ULONG rel_slot_space;		// lowest pointer page with slot space
	ULONG rel_pri_data_space;	// lowest pointer page with primary data page space
	ULONG rel_sec_data_space;	// lowest pointer page with secondary data page space
	ULONG rel_last_free_pri_dp;	// last primary data page found with space
	ULONG rel_last_free_blb_dp;	// last blob data page found with space
	USHORT rel_pg_space_id;

	RelationPages(Firebird::MemoryPool& pool)
		: rel_pages(NULL), rel_instance_id(0),
		  rel_index_root(0), rel_data_pages(0), rel_slot_space(0),
		  rel_pri_data_space(0), rel_sec_data_space(0),
		  rel_last_free_pri_dp(0), rel_last_free_blb_dp(0),
		  rel_pg_space_id(DB_PAGE_SPACE), rel_next_free(NULL),
		  useCount(0),
		  dpMap(pool),
		  dpMapMark(0)
	{}

	inline SLONG addRef()
	{
		return useCount++;
	}

	void free(RelationPages*& nextFree);

	static inline InstanceId generate(const RelationPages* item)
	{
		return item->rel_instance_id;
	}

	ULONG getDPNumber(ULONG dpSequence)
	{
		FB_SIZE_T pos;
		if (dpMap.find(dpSequence, pos))
		{
			if (dpMap[pos].mark != dpMapMark)
				dpMap[pos].mark = ++dpMapMark;
			return dpMap[pos].physNum;
		}

		return 0;
	}

	void setDPNumber(ULONG dpSequence, ULONG dpNumber)
	{
		FB_SIZE_T pos;
		if (dpMap.find(dpSequence, pos))
		{
			if (dpNumber)
			{
				dpMap[pos].physNum = dpNumber;
				dpMap[pos].mark = ++dpMapMark;
			}
			else
				dpMap.remove(pos);
		}
		else if (dpNumber)
		{
			dpMap.insert(pos, {dpSequence, dpNumber, ++dpMapMark});

			if (dpMap.getCount() == MAX_DPMAP_ITEMS)
				freeOldestMapItems();
		}
	}

	void freeOldestMapItems()
	{
		ULONG minMark = MAX_ULONG;
		FB_SIZE_T i;

		for (i = 0; i < dpMap.getCount(); i++)
		{
			if (minMark > dpMap[i].mark)
				minMark = dpMap[i].mark;
		}

		minMark = (minMark + dpMapMark) / 2;

		i = 0;
		while (i < dpMap.getCount())
		{
			if (dpMap[i].mark > minMark)
				dpMap[i++].mark -= minMark;
			else
				dpMap.remove(i);
		}

		dpMapMark -= minMark;
	}

private:
	RelationPages*	rel_next_free;
	SLONG			useCount;

	static const ULONG MAX_DPMAP_ITEMS = 64;

	struct DPItem
	{
		ULONG seqNum;
		ULONG physNum;
		ULONG mark;

		static ULONG generate(const DPItem& item)
		{
			return item.seqNum;
		}
	};

	Firebird::SortedArray<DPItem, Firebird::InlineStorage<DPItem, MAX_DPMAP_ITEMS>, ULONG, DPItem> dpMap;
	ULONG dpMapMark;

friend class RelationPermanent;
};


// Index block

class IndexPermanent : public Firebird::PermanentStorage
{
public:
	IndexPermanent(thread_db* tdbb, MemoryPool& p, MetaId id, MakeLock*, RelationPermanent* rel)
		: PermanentStorage(p),
		  idp_relation(rel),
		  idp_id(id)
	{ }

	~IndexPermanent()
	{
		fb_assert((!idp_lock) || (idp_lock->lck_physical == LCK_none && idp_lock->lck_logical == LCK_none));
	}

	static int indexReload(void* ast_object);

	static bool destroy(thread_db* tdbb, IndexPermanent* idp)
	{
		idp->unlock(tdbb);
		return false;
	}

	MetaId getId() const
	{
		return idp_id;
	}

	static const int REL_ID_KEY_OFFSET = 16;
	void createLock(thread_db* tdbb, MetaId relId, MetaId indId);

	bool exclusiveLock(thread_db* tdbb);
	bool sharedLock(thread_db* tdbb);
	void unlock(thread_db* tdbb);

	Lock* getRescanLock()
	{
		return nullptr;
	}

	RelationPermanent* getRelation()
	{
		return idp_relation;
	}

	const char* c_name();

public:
	MetaName			idp_name;		// used only as temp mirror for c_name() implementation

private:
	RelationPermanent*	idp_relation;
	Lock*				idp_lock = nullptr;
	MetaId				idp_id;

	[[noreturn]] void errIndexGone();
};

class IndexVersion final : public ObjectBase
{
public:
	IndexVersion(MemoryPool& p, Cached::Index* idp);

	static IndexVersion* create(thread_db* tdbb, MemoryPool& p, Cached::Index* idp)
	{
		return FB_NEW_POOL(p) IndexVersion(p, idp);
	}

	static void destroy(thread_db* tdbb, IndexVersion* idv)
	{
		delete idv;
	}

	static Lock* makeLock(thread_db* tdbb, MemoryPool& p)
	{
		return nullptr;
	}

	bool scan(thread_db* tdbb, ObjectBase::Flag flags);

	bool reload(thread_db* tdbb, ObjectBase::Flag flags)
	{
		return scan(tdbb, flags);
	}

	const char* c_name() const override
	{
		return idv_name.c_str();
	}

	MetaName getName() const
	{
		return idv_name;
	}

	static const char* objectFamily(void*)
	{
		return "index";
	}

	MetaName getForeignKey() const
	{
		return idv_foreignKey;
	}

	MetaId getId() const
	{
		return perm->getId();
	}

	Cached::Index* getPermanent() const
	{
		return perm;
	}

	bool getActive()
	{
		return !idv_inactive;
	}

private:
	Cached::Index* perm;
	MetaName idv_name;
	SSHORT idv_uniqFlag = 0;
	SSHORT idv_segmentCount = 0;
	SSHORT idv_type = 0;
	MetaName idv_foreignKey;						// FOREIGN RELATION NAME
	bool idv_inactive = false;

public:
	ValueExprNode* idv_expression = nullptr;		// node tree for index expression
	Statement* idv_expression_statement = nullptr;	// statement for index expression evaluation
	dsc			idv_expression_desc;				// descriptor for expression result
	BoolExprNode* idv_condition = nullptr;			// node tree for index condition
	Statement* idv_condition_statement = nullptr;	// statement for index condition evaluation
};


// Relation block; one is created for each relation referenced
// in the database, though it is not really filled out until
// the relation is scanned

class jrd_rel final : public ObjectBase
{
public:
	jrd_rel(MemoryPool& p, Cached::Relation* r);

	MemoryPool*		rel_pool;
	Cached::Relation*	rel_perm;
	USHORT			rel_current_fmt;	// Current format number
	Format*			rel_current_format;	// Current record format

	vec<jrd_fld*>*	rel_fields;			// vector of field blocks

	RseNode*		rel_view_rse;		// view record select expression
	ViewContexts	rel_view_contexts;	// sorted array of view contexts

	Nullable<bool>	rel_ss_definer;

	TrigArray rel_triggers;

	bool hasData() const;
	const char* c_name() const override;
	MetaId getId() const;
	RelationPages* getPages(thread_db* tdbb, TraNumber tran = MAX_TRA_NUMBER, bool allocPages = true);
	bool isTemporary() const;
	bool isView() const;
	bool isVirtual() const;
	bool isSystem() const;
	bool isReplicating(thread_db* tdbb);

	bool scan(thread_db* tdbb, ObjectBase::Flag flags);		// Scan the newly loaded relation for meta data
	MetaName getName() const;
	MemoryPool& getPool() const;
	MetaName getSecurityName() const;
	MetaName getOwnerName() const;
	ExternalFile* getExtFile() const;

	static void destroy(thread_db* tdbb, jrd_rel *rel);
	static jrd_rel* create(thread_db* tdbb, MemoryPool& p, Cached::Relation* perm);

	static Lock* makeLock(thread_db*, MemoryPool&)
	{
		return nullptr;		// ignored
	}

	bool reload(thread_db* tdbb, ObjectBase::Flag flags)
	{
		return scan(tdbb, flags);
	}

	static const char* objectFamily(RelationPermanent* perm);
	static int objectType();

public:
	// bool hasTriggers() const;  unused ???????????????????
	void releaseTriggers(thread_db* tdbb, bool destroy);
	const Trigger* findTrigger(const MetaName trig_name) const;

	decltype(rel_perm) getPermanent() const
	{
		return rel_perm;
	}
};

// rel_flags

const ULONG REL_system					= 0x0001;
const ULONG REL_get_dependencies		= 0x0002;	// New relation needs dependencies during scan
const ULONG REL_sys_triggers			= 0x0004;	// The relation has system triggers to compile
const ULONG REL_sql_relation			= 0x0008;	// Relation defined as sql table
const ULONG REL_check_partners			= 0x0010;	// Rescan primary dependencies and foreign references
const ULONG REL_sys_trigs_being_loaded	= 0x0020;	// System triggers being loaded
const ULONG REL_temp_tran				= 0x0040;	// relation is a GTT delete rows
const ULONG REL_temp_conn				= 0x0080;	// relation is a GTT preserve rows
const ULONG REL_virtual					= 0x0100;	// relation is virtual
const ULONG REL_jrd_view				= 0x0200;	// relation is VIEW
const ULONG REL_format					= 0x0400;	// new format version to be built

class GCLock
{
public:
	GCLock(RelationPermanent* rl)
		: lck(nullptr),
		  relPerm(rl),
		  flags(0u)
	{ }

	// This guard is used by regular code to prevent online validation while
	// dead- or back- versions is removed from disk.
	class Shared
	{
	public:
		Shared(thread_db* tdbb, RelationPermanent* rl);
		~Shared();

		bool gcEnabled() const
		{
			return m_gcEnabled;
		}

	private:
		thread_db*	m_tdbb;
		RelationPermanent*	m_rl;
		bool		m_gcEnabled;
	};

	// This guard is used by online validation to prevent any modifications of
	// table data while it is checked.
	class Exclusive
	{
	public:
		Exclusive(thread_db* tdbb, RelationPermanent* rl)
			: m_tdbb(tdbb), m_rl(rl), m_lock(nullptr)
		{ }

		~Exclusive()
		{
			release();
			delete m_lock;
		}

		bool acquire(int wait);
		void release();

	private:
		thread_db*		m_tdbb;
		RelationPermanent*		m_rl;
		Lock*			m_lock;
	};

public:
	bool acquire(thread_db* tdbb, int wait);
	void downgrade(thread_db* tdbb);
	void enable(thread_db* tdbb, Lock* tempLock);
	bool disable(thread_db* tdbb, int wait, Lock*& tempLock);

	static int ast(void* self)
	{
		try
		{
			reinterpret_cast<GCLock*>(self)->blockingAst();
		}
		catch(const Firebird::Exception&) { }

		return 0;
	}

	void forcedRelease(thread_db* tdbb);

private:
	void blockingAst();
	void ensureReleased(thread_db* tdbb);

	[[noreturn]] void incrementError();

private:
	Firebird::AutoPtr<Lock> lck;
	RelationPermanent* relPerm;

public:
	std::atomic<unsigned> flags;

	static const unsigned GC_counterMask =	0x0FFFFFFF;
	static const unsigned GC_guardBit =		0x10000000;
	static const unsigned GC_disabled =		0x20000000;
	static const unsigned GC_locked =		0x40000000;
	static const unsigned GC_blocking =		0x80000000;
};


// Non-versioned part of relation in cache

class RelationPermanent : public Firebird::PermanentStorage
{
	typedef CacheVector<Cached::Index, 4, RelationPermanent*> Indices;
	typedef Firebird::HalfStaticArray<Record*, 4> GCRecordList;

public:
	RelationPermanent(thread_db* tdbb, MemoryPool& p, MetaId id, MakeLock* makeLock, NoData);
	~RelationPermanent();
	static bool destroy(thread_db* tdbb, RelationPermanent* rel);

	void makeLocks(thread_db* tdbb, Cached::Relation* relation);
	static constexpr USHORT getRelLockKeyLength();
	Lock* createLock(thread_db* tdbb, lck_t, bool);
	Lock* createLock(thread_db* tdbb, MemoryPool& pool, lck_t, bool);
	void extFile(thread_db* tdbb, const TEXT* file_name);		// impl in ext.cpp

	IndexVersion* lookup_index(thread_db* tdbb, MetaId id, ObjectBase::Flag flags);
	Cached::Index* lookupIndex(thread_db* tdbb, MetaId id, ObjectBase::Flag flags);
	IndexVersion* lookup_index(thread_db* tdbb, MetaName name, ObjectBase::Flag flags);
	Cached::Index* lookupIndex(thread_db* tdbb, MetaName name, ObjectBase::Flag flags);

	void newIndexVersion(thread_db* tdbb, MetaId id)
	{
		auto chk = rel_indices.makeObject(tdbb, id, CacheFlag::NOCOMMIT);
		fb_assert(chk);
	}

	void oldIndexVersion(thread_db* tdbb, MetaId id)
	{
		auto chk = rel_indices.getObject(tdbb, id, CacheFlag::AUTOCREATE);
		fb_assert(chk);
	}

	void eraseIndex(thread_db* tdbb, MetaId id)		// oldIndex to be called before
	{
		auto chk = rel_indices.erase(tdbb, id);
		fb_assert(chk);
	}

	Lock*		rel_existence_lock;		// existence lock
	Lock*		rel_partners_lock;		// partners lock
	Lock*		rel_rescan_lock;		// lock forcing relation to be scanned
	GCLock		rel_gc_lock;			// garbage collection lock
	GCRecordList	rel_gc_records;		// records for garbage collection

	atomics::atomic<USHORT>	rel_sweep_count;	// sweep and/or garbage collector threads active
	atomics::atomic<SSHORT>	rel_scan_count;		// concurrent sequential scan count

	class RelPagesSnapshot : public Firebird::Array<RelationPages*>
	{
	public:
		typedef Firebird::Array<RelationPages*> inherited;

		RelPagesSnapshot(thread_db* tdbb, RelationPermanent* relation)
		{
			spt_tdbb = tdbb;
			spt_relation = relation;
		}

		~RelPagesSnapshot() { clear(); }

		void clear();
	private:
		thread_db*	spt_tdbb;
		RelationPermanent*	spt_relation;

	friend class RelationPermanent;
	};

	RelationPages* getPages(thread_db* tdbb, TraNumber tran = MAX_TRA_NUMBER, bool allocPages = true);
	bool	delPages(thread_db* tdbb, TraNumber tran = MAX_TRA_NUMBER, RelationPages* aPages = NULL);
	void	retainPages(thread_db* tdbb, TraNumber oldNumber, TraNumber newNumber);
	void	cleanUp();
	void	fillPagesSnapshot(RelPagesSnapshot&, const bool AttachmentOnly = false);
	void scan_partners(thread_db* tdbb);		// Foreign keys scan - impl. in met.epp

	RelationPages* getBasePages()
	{
		return &rel_pages_base;
	}


	bool hasData() const
	{
		return rel_name.hasData();
	}

	const char* c_name() const
	{
		return rel_name.c_str();
	}

	MetaName getName() const
	{
		return rel_name;
	}

	MetaId getId() const
	{
		return rel_id;
	}

	MetaName getSecurityName() const
	{
		return rel_security_name;
	}

	ExternalFile* getExtFile() const
	{
		return rel_file;
	}

	void setExtFile(ExternalFile* f)
	{
		fb_assert(!rel_file);
		rel_file = f;
	}


	void	getRelLockKey(thread_db* tdbb, UCHAR* key);

	bool isSystem() const;
	bool isTemporary() const;
	bool isVirtual() const;
	bool isView() const;
	bool isReplicating(thread_db* tdbb);

	static int partners_ast_relation(void* ast_object);
	static int rescan_ast_relation(void* ast_object);
	static int blocking_ast_relation(void* ast_object);

	vec<Format*>*	rel_formats;		// Known record formats
	Indices			rel_indices;		// Active indices
	MetaName		rel_name;			// ascii relation name
	MetaId			rel_id;

	MetaName		rel_owner_name;		// ascii owner
	MetaName		rel_security_name;	// security class name for relation
	ULONG			rel_flags;			// flags

	TriState		rel_repl_state;		// replication state

	PrimaryDeps*	rel_primary_dpnds = nullptr;	// foreign dependencies on this relation's primary key
	ForeignDeps*	rel_foreign_refs = nullptr;		// foreign references to other relations' primary keys

private:
	Firebird::Mutex			rel_pages_mutex;

	typedef Firebird::SortedArray<
				RelationPages*,
				Firebird::EmptyStorage<RelationPages*>,
				RelationPages::InstanceId,
				RelationPages>
			RelationPagesInstances;

	RelationPagesInstances* rel_pages_inst;
	RelationPages			rel_pages_base;
	RelationPages*			rel_pages_free;

	RelationPages* getPagesInternal(thread_db* tdbb, TraNumber tran, bool allocPages);

	ExternalFile* rel_file;
};


inline bool jrd_rel::hasData() const
{
	return rel_perm->rel_name.hasData();
}

inline const char* jrd_rel::c_name() const
{
	return rel_perm->rel_name.c_str();
}

inline MetaName jrd_rel::getName() const
{
	return rel_perm->rel_name;
}

inline MemoryPool& jrd_rel::getPool() const
{
	return rel_perm->getPool();
}

inline ExternalFile* jrd_rel::getExtFile() const
{
	return rel_perm->getExtFile();
}

inline MetaName jrd_rel::getSecurityName() const
{
	return rel_perm->rel_security_name;
}

inline MetaName jrd_rel::getOwnerName() const
{
	return rel_perm->rel_owner_name;
}

inline MetaId jrd_rel::getId() const
{
	return rel_perm->rel_id;
}

inline RelationPages* jrd_rel::getPages(thread_db* tdbb, TraNumber tran, bool allocPages)
{
	return rel_perm->getPages(tdbb, tran, allocPages);
}

inline bool jrd_rel::isTemporary() const
{
	return rel_perm->isTemporary();
}

inline bool jrd_rel::isView() const
{
	return rel_perm->isView();
}

inline bool jrd_rel::isVirtual() const
{
	return rel_perm->isVirtual();
}

inline bool jrd_rel::isSystem() const
{
	return rel_perm->isSystem();
}

inline bool jrd_rel::isReplicating(thread_db* tdbb)
{
	return rel_perm->isReplicating(tdbb);
}


inline bool RelationPermanent::isSystem() const
{
	return rel_flags & REL_system;
}

inline bool RelationPermanent::isTemporary() const
{
	return (rel_flags & (REL_temp_tran | REL_temp_conn));
}

inline bool RelationPermanent::isVirtual() const
{
	return (rel_flags & REL_virtual);
}

inline bool RelationPermanent::isView() const
{
	return (rel_flags & REL_jrd_view);
}

inline RelationPages* RelationPermanent::getPages(thread_db* tdbb, TraNumber tran, bool allocPages)
{
	if (!isTemporary())
		return &rel_pages_base;

	return getPagesInternal(tdbb, tran, allocPages);
}


/// class GCLock::Shared

inline GCLock::Shared::Shared(thread_db* tdbb, RelationPermanent* rl)
	: m_tdbb(tdbb),
	  m_rl(rl),
	  m_gcEnabled(m_rl->rel_gc_lock.acquire(m_tdbb, LCK_NO_WAIT))
{ }

inline GCLock::Shared::~Shared()
{
	if (m_gcEnabled)
		m_rl->rel_gc_lock.downgrade(m_tdbb);
}


/// class GCLock::Exclusive

inline bool GCLock::Exclusive::acquire(int wait)
{
	return m_rl->rel_gc_lock.disable(m_tdbb, wait, m_lock);
}

inline void GCLock::Exclusive::release()
{
	return m_rl->rel_gc_lock.enable(m_tdbb, m_lock);
}


// Field block, one for each field in a scanned relation

const USHORT FLD_parse_computed = 0x0001;		// computed expression is being parsed

class jrd_fld : public pool_alloc<type_fld>
{
public:
	BoolExprNode*	fld_validation;		// validation clause, if any
	BoolExprNode*	fld_not_null;		// if field cannot be NULL
	ValueExprNode*	fld_missing_value;	// missing value, if any
	ValueExprNode*	fld_computation;	// computation for virtual field
	ValueExprNode*	fld_source;			// source for view fields
	ValueExprNode*	fld_default_value;	// default value, if any
	ArrayField*	fld_array;			// array description, if array
	MetaName	fld_name;	// Field name
	MetaName	fld_security_name;	// security class name for field
	MetaName	fld_generator_name;	// identity generator name
	MetaNamePair	fld_source_rel_field;	// Relation/field source name
	Nullable<IdentityType> fld_identity_type;
	USHORT fld_flags;

public:
	explicit jrd_fld(MemoryPool& p)
		: fld_name(p),
		  fld_security_name(p),
		  fld_generator_name(p),
		  fld_source_rel_field(p)
	{
	}
};

}

#endif	// JRD_RELATION_H
