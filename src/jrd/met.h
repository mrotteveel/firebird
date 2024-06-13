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
	Cached::Procedure* cachedProcedure;
	const ExtEngineManager::Procedure* prc_external;

public:
	explicit jrd_prc(Cached::Procedure* perm)
		: Routine(perm->getPool()),
		  prc_record_format(NULL),
		  prc_type(prc_legacy),
		  cachedProcedure(perm),
		  prc_external(NULL)
	{
	}

	explicit jrd_prc(MemoryPool& p)
		: Routine(p),
		  prc_record_format(NULL),
		  prc_type(prc_legacy),
		  cachedProcedure(FB_NEW_POOL(p) Cached::Procedure(p)),
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
	static jrd_prc* create(thread_db* tdbb, MemoryPool& p, Cached::Procedure* perm);
	static Lock* makeLock(thread_db* tdbb, MemoryPool& p);
	bool scan(thread_db* tdbb, ObjectBase::Flag);

	void releaseExternal() override
	{
		delete prc_external;
		prc_external = NULL;
	}

	Cached::Procedure* getPermanent() const override
	{
		return cachedProcedure;
	}

	static const char* objectFamily(void*)
	{
		return "procedure";
	}

	bool reload(thread_db* tdbb) override;
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

typedef atomics::atomic<Cached::Triggers*> TriggersSet;

class MetadataCache : public Firebird::PermanentStorage
{
	friend class CharSetContainer;

public:
	MetadataCache(MemoryPool& pool)
		: Firebird::PermanentStorage(pool),
		  mdc_generators(getPool()),
		  mdc_relations(getPool()),
		  mdc_procedures(getPool()),
		  mdc_functions(getPool()),
		  mdc_charsets(getPool()),
		  mdc_ddl_triggers(nullptr),
		  mdc_version(0)
	{
		memset(mdc_triggers, 0, sizeof(mdc_triggers));
	}

	~MetadataCache();

/*
	// Objects are placed to this list after DROP OBJECT 
	// and wait for current OAT >= NEXT when DDL committed
	atomics::atomic<Cache List<ElementBase>*> dropList;
	{
public:
	void drop(
}; ?????????????????????
*/


	void releaseRelations(thread_db* tdbb);
	void releaseLocks(thread_db* tdbb);
	void releaseGTTs(thread_db* tdbb);
	void runDBTriggers(thread_db* tdbb, TriggerAction action);
	void invalidateReplSet(thread_db* tdbb);
	void setRelation(thread_db* tdbb, ULONG rel_id, jrd_rel* rel);
	void releaseTrigger(thread_db* tdbb, MetaId triggerId, const MetaName& name);
	const Triggers* getTriggers(thread_db* tdbb, MetaId tType);

	MetaId relCount()
	{
		return mdc_relations.getCount();
	}

	Function* getFunction(thread_db* tdbb, MetaId id, ObjectBase::Flag flags)
	{
		return mdc_functions.getObject(tdbb, id, flags);
	}

	jrd_prc* getProcedure(thread_db* tdbb, MetaId id)
	{
		return mdc_procedures.getObject(tdbb, id, CacheFlag::AUTOCREATE);
	}

	static Cached::CharSet* getCharSet(thread_db* tdbb, CSetId id, ObjectBase::Flag flags);

	void cleanup(Jrd::thread_db*);

	// former met_proto.h
#ifdef DEV_BUILD
	static void verify_cache(thread_db* tdbb);
#else
	static void verify_cache(thread_db* tdbb) { }
#endif
	static void clear(thread_db* tdbb);
	static void update_partners(thread_db* tdbb);
	void load_db_triggers(thread_db* tdbb, int type, bool force = false);
	void load_ddl_triggers(thread_db* tdbb, bool force = false);
	static jrd_prc* lookup_procedure(thread_db* tdbb, const QualifiedName& name, ObjectBase::Flag flags = CacheFlag::AUTOCREATE);
	static jrd_prc* lookup_procedure_id(thread_db* tdbb, MetaId id, ObjectBase::Flag flags);
	static Function* lookup_function(thread_db* tdbb, const QualifiedName& name);
	static Function* lookup_function(thread_db* tdbb, MetaId id, ObjectBase::Flag flags);
	static Cached::Procedure* lookupProcedure(thread_db* tdbb, const QualifiedName& name, ObjectBase::Flag flags);
	static Cached::Procedure* lookupProcedure(thread_db* tdbb, MetaId id, ObjectBase::Flag flags);
	static Cached::Function* lookupFunction(thread_db* tdbb, const QualifiedName& name, ObjectBase::Flag flags);
	static Cached::Function* lookupFunction(thread_db* tdbb, MetaId id, ObjectBase::Flag flags);
	static jrd_rel* lookup_relation(thread_db*, const MetaName&);
	static jrd_rel* lookup_relation_id(thread_db*, MetaId, ObjectBase::Flag flags);
	static Cached::Relation* lookupRelation(thread_db* tdbb, const MetaName& name, ObjectBase::Flag flags);
	static Cached::Relation* lookupRelation(thread_db* tdbb, MetaId id, ObjectBase::Flag flags);
	Cached::Relation* lookupRelation(thread_db* tdbb, MetaId id);
	Cached::Relation* lookupRelationNoChecks(MetaId id);
	static void lookup_index(thread_db* tdbb, MetaName& index_name, const MetaName& relation_name, USHORT number);
	static ElementBase::ReturnedId lookup_index_name(thread_db* tdbb, const MetaName& index_name,
													 MetaId* relation_id, IndexStatus* status);
	static void post_existence(thread_db* tdbb, jrd_rel* relation);
	static jrd_prc* findProcedure(thread_db* tdbb, MetaId id, ObjectBase::Flag flags);
    static jrd_rel* findRelation(thread_db* tdbb, MetaId id);

	template<typename ID>
	static bool get_char_coll_subtype(thread_db* tdbb, ID* id, const UCHAR* name, USHORT length)
	{
		fb_assert(id);

		TTypeId ttId;
		bool rc = get_texttype(tdbb, &ttId, name, length);
		*id = ttId;
		return rc;
	}

private:
	static bool get_texttype(thread_db* tdbb, TTypeId* id, const UCHAR* name, USHORT length);

public:
	bool resolve_charset_and_collation(thread_db* tdbb, TTypeId* id,
									   const UCHAR* charset, const UCHAR* collation);
	static DSqlCacheItem* get_dsql_cache_item(thread_db* tdbb, sym_type type, const QualifiedName& name);
	static void dsql_cache_release(thread_db* tdbb, sym_type type, const MetaName& name, const MetaName& package = "");
	static bool dsql_cache_use(thread_db* tdbb, sym_type type, const MetaName& name, const MetaName& package = "");
	// end of met_proto.h

	static CharSetVers* lookup_charset(thread_db* tdbb, CSetId id, ObjectBase::Flag flags);

	static void release_temp_tables(thread_db* tdbb, jrd_tra* transaction);
	static void retain_temp_tables(thread_db* tdbb, jrd_tra* transaction, TraNumber new_number);

	MdcVersion getVersion()
	{
		return mdc_version.load(std::memory_order_relaxed);
	}

	MdcVersion nextVersion()
	{
		return ++mdc_version;
	}

	SLONG lookupSequence(thread_db*, const MetaName& genName)
	{
		return mdc_generators.lookup(genName);
	}

	void setSequence(thread_db*, SLONG id, const MetaName& name)
	{
		mdc_generators.store(id, name);
	}

	bool getSequence(thread_db*, SLONG id, MetaName& name)
	{
		return mdc_generators.lookup(id, name);
	}

	static void oldVersion(thread_db* tdbb, ObjectType objType, MetaId id)
	{
		changeVersion(tdbb, true, objType, id);
	}

	static void newVersion(thread_db* tdbb, ObjectType objType, MetaId id)
	{
		changeVersion(tdbb, false, objType, id);
	}

private:
	static void changeVersion(thread_db* tdbb, bool loadOld, ObjectType objType, MetaId id);

	template <typename C>
	static void changeVersion(thread_db* tdbb, bool loadOld, CacheVector<C>& vector, MetaId id)
	{
		auto* ver = loadOld ? vector.getObject(tdbb, id, CacheFlag::AUTOCREATE) :
			vector.makeObject(tdbb, id, CacheFlag::NOCOMMIT);
		fb_assert(ver);
	}

	class GeneratorFinder
	{
		typedef Firebird::MutexLockGuard Guard;

	public:
		explicit GeneratorFinder(MemoryPool& pool)
			: m_objects(pool)
		{}

		void store(SLONG id, const MetaName& name)
		{
			fb_assert(id >= 0);
			fb_assert(name.hasData());

			Guard g(m_tx, FB_FUNCTION);

			if (id < (int) m_objects.getCount())
			{
				fb_assert(m_objects[id].isEmpty());
				m_objects[id] = name;
			}
			else
			{
				m_objects.resize(id + 1);
				m_objects[id] = name;
			}
		}

		bool lookup(SLONG id, MetaName& name)
		{
			Guard g(m_tx, FB_FUNCTION);

			if (id < (int) m_objects.getCount() && m_objects[id].hasData())
			{
				name = m_objects[id];
				return true;
			}

			return false;
		}

		SLONG lookup(const MetaName& name)
		{
			Guard g(m_tx, FB_FUNCTION);

			FB_SIZE_T pos;

			if (m_objects.find(name, pos))
				return (SLONG) pos;

			return -1;
		}

	private:
		Firebird::Array<MetaName> m_objects;
		Firebird::Mutex m_tx;
	};

	GeneratorFinder						mdc_generators;
	CacheVector<Cached::Relation>		mdc_relations;
	CacheVector<Cached::Procedure>		mdc_procedures;
	CacheVector<Cached::Function>		mdc_functions;	// User defined functions
	CacheVector<Cached::CharSet>		mdc_charsets;	// intl character set descriptions
	TriggersSet							mdc_triggers[DB_TRIGGER_MAX];
	TriggersSet							mdc_ddl_triggers;

	std::atomic<MdcVersion>				mdc_version;	// Current version of metadata cache (should have 2 nums???????????????)
};

} // namespace Jrd

#endif // JRD_MET_H
