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
#include "../jrd/exe.h"

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
	MetaId			tfb_id;			// id of field in relation
	USHORT			tfb_flags;
	dsc				tfb_desc;
	Jrd::impure_value	tfb_default;
};

// tfb_flags

const int TFB_computed			= 1;
const int TFB_array				= 2;

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

public:
	explicit jrd_prc(RoutinePermanent* perm)
		: Routine(perm),
		  prc_record_format(NULL),
		  prc_type(prc_legacy),
		  prc_external(NULL)
	{
	}

public:
	int getObjectType() const override
	{
		return obj_procedure;
	}

	SLONG getSclType() const override
	{
		return obj_procedures;
	}

	void releaseFormat() override
	{
		delete prc_record_format;
		prc_record_format = NULL;
	}

private:
	virtual ~jrd_prc() override
	{
		delete prc_external;
	}

	static int blockingAst(void* ast_object);

public:
	static jrd_prc* create(thread_db* tdbb, MemoryPool& p, RoutinePermanent* perm);
	static Lock* makeLock(thread_db* tdbb, MemoryPool& p);
	void scan(thread_db* tdbb, CacheObject::Flag);

	bool checkCache(thread_db* tdbb) const override;

	void releaseExternal() override
	{
		delete prc_external;
		prc_external = NULL;
	}

protected:
	bool reload(thread_db* tdbb) override;	// impl is in met.epp
};


// Parameter block

class Parameter : public pool_alloc<type_prm>
{
public:
	MetaId		prm_number;
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

typedef atomics::atomic<CacheElement<DbTriggers, DbTriggersHeader>*> TriggersSet;

class MetadataCache : public Firebird::PermanentStorage
{
	friend class CharSetContainer;

public:
	typedef CacheVector<CharSetVers, CharSetContainer> Charsets;	// intl character set descriptions
	typedef Charsets::StoredElement CharsetElement;						// character set stored in cache vector

	MetadataCache(MemoryPool& pool)
		: Firebird::PermanentStorage(pool),
		  mdc_relations(getPool()),
		  mdc_procedures(getPool()),
		  mdc_functions(getPool()),
		  mdc_charsets(getPool()),
		  mdc_ddl_triggers(nullptr)/*,
		  mdc_charset_ids(getPool())*/
	{
		memset(mdc_triggers, 0, sizeof(mdc_triggers));
	}

	~MetadataCache();

/*
	// Objects are placed to this list after DROP OBJECT 
	// and wait for current OAT >= NEXT when DDL committed
	atomics::atomic<Cache List<ObjectBase>*> dropList;
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
	jrd_rel* getRelation(thread_db* tdbb, ULONG rel_id);
	void setRelation(thread_db* tdbb, ULONG rel_id, jrd_rel* rel);
	void releaseTrigger(thread_db* tdbb, MetaId triggerId, const MetaName& name);
	const Triggers* getTriggers(thread_db* tdbb, MetaId tType);

	MetaId relCount()
	{
		return mdc_relations.getCount();
	}

	Function* getFunction(thread_db* tdbb, MetaId id, CacheObject::Flag flags)
	{
		return mdc_functions.getObject(tdbb, id, flags);
	}
/* ??????????????
	bool makeFunction(thread_db* tdbb, MetaId id, Function* f)
	{
		return mdc_functions.storeObject(tdbb, id, f);
	}
*/
	jrd_prc* getProcedure(thread_db* tdbb, MetaId id)
	{
		return mdc_procedures.getObject(tdbb, id, CacheFlag::AUTOCREATE);
	}
/* ??????????
	bool makeProcedure(thread_db* tdbb, MetaId id, jrd_prc* p)
	{
		return mdc_procedures.storeObject(tdbb, id, p);
	}
*/
	static CharSetContainer* getCharSet(thread_db* tdbb, MetaId id, CacheObject::Flag flags);
// ??????????	bool makeCharSet(thread_db* tdbb, USHORT id, CharSetContainer* cs);

	// former met_proto.h
#ifdef DEV_BUILD
	static void verify_cache(thread_db* tdbb);
#else
	static void verify_cache(thread_db* tdbb) { }
#endif
	static void clear_cache(thread_db* tdbb);
	static void update_partners(thread_db* tdbb);
	void load_db_triggers(thread_db* tdbb, int type, bool force = false);
	void load_ddl_triggers(thread_db* tdbb, bool force = false);
	static jrd_prc* lookup_procedure(thread_db* tdbb, const QualifiedName& name);
	static jrd_prc* lookup_procedure_id(thread_db* tdbb, MetaId id, USHORT flags);
	static Function* lookup_function(thread_db* tdbb, const QualifiedName& name);
	static Function* lookup_function(thread_db* tdbb, MetaId id, CacheObject::Flag flags);
	static CacheElement<jrd_prc, RoutinePermanent>* lookupProcedure(thread_db* tdbb, const QualifiedName& name, CacheObject::Flag flags = 0);
	static CacheElement<jrd_prc, RoutinePermanent>* lookupProcedure(thread_db* tdbb, MetaId id, CacheObject::Flag flags = 0);
	static jrd_rel* lookup_relation(thread_db*, const MetaName&);
	static jrd_rel* lookup_relation_id(thread_db*, MetaId, CacheObject::Flag flags = CacheFlag::AUTOCREATE);
	static CachedRelation* lookupRelation(thread_db* tdbb, const MetaName& name, CacheObject::Flag flags = 0);
	static CachedRelation* lookupRelation(thread_db* tdbb, MetaId id, CacheObject::Flag flags = 0);
	CachedRelation* lookupRelation(MetaId id);
	static void lookup_index(thread_db* tdbb, MetaName& index_name, const MetaName& relation_name, USHORT number);
	static ObjectBase::ReturnedId lookup_index_name(thread_db* tdbb, const MetaName& index_name,
								   MetaId* relation_id, IndexStatus* status);
	static void post_existence(thread_db* tdbb, jrd_rel* relation);
	static jrd_prc* findProcedure(thread_db* tdbb, MetaId id, CacheObject::Flag flags);
    static jrd_rel* findRelation(thread_db* tdbb, MetaId id);
	static bool get_char_coll_subtype(thread_db* tdbb, MetaId* id, const UCHAR* name, USHORT length);
	bool resolve_charset_and_collation(thread_db* tdbb, MetaId* id,
											  const UCHAR* charset, const UCHAR* collation);
	static DSqlCacheItem* get_dsql_cache_item(thread_db* tdbb, sym_type type, const QualifiedName& name);
	static void dsql_cache_release(thread_db* tdbb, sym_type type, const MetaName& name, const MetaName& package = "");
	static bool dsql_cache_use(thread_db* tdbb, sym_type type, const MetaName& name, const MetaName& package = "");
	// end of met_proto.h

	static CharsetElement* lookupCharset(thread_db* tdbb, USHORT tt_id);

	MdcVersion getVersion()
	{
		return mdc_version.load(std::memory_order_relaxed);
	}

	MdcVersion nextVersion()
	{
		return ++mdc_version;
	}

private:
	CacheVector<jrd_rel, RelationPermanent>	mdc_relations;
	CacheVector<jrd_prc, RoutinePermanent>	mdc_procedures;
	CacheVector<Function, RoutinePermanent>	mdc_functions;	// User defined functions
	Charsets								mdc_charsets;	// intl character set descriptions
	TriggersSet								mdc_triggers[DB_TRIGGER_MAX];
	TriggersSet								mdc_ddl_triggers;

	std::atomic<MdcVersion>					mdc_version;	// Current version of metadata cache

public:
	Firebird::Mutex
					mdc_charset_mutex;						// Protects mdc_charset_ids
};

} // namespace Jrd

#endif // JRD_MET_H
