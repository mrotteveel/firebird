/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		exe.h
 *	DESCRIPTION:	Execution block definitions
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

#ifndef JRD_EXE_H
#define JRD_EXE_H

#include <optional>
#include "../jrd/blb.h"
#include "../jrd/Relation.h"
#include "../common/classes/array.h"
#include "../jrd/MetaName.h"
#include "../common/classes/auto.h"
#include "../common/classes/fb_pair.h"
#include "../common/classes/NestConst.h"

#include "iberror.h"

#include "../common/dsc.h"

#include "../jrd/err_proto.h"
#include "../jrd/scl.h"
#include "../jrd/sbm.h"
#include "../jrd/sort.h"

#include "../jrd/DebugInterface.h"
#include "../common/classes/BlrReader.h"
#include "../dsql/Nodes.h"
#include "../dsql/Visitors.h"

// This macro enables DSQL tracing code
//#define CMP_DEBUG

#ifdef CMP_DEBUG
DEFINE_TRACE_ROUTINE(cmp_trace);
#define CMP_TRACE(args) cmp_trace args
#else
#define CMP_TRACE(args) // nothing
#endif

class VaryingString;

namespace Jrd {

class jrd_rel;
class Sort;
struct sort_key_def;
template <typename T> class vec;
class jrd_prc;
class Collation;
struct index_desc;
class Format;
class ForNode;
class Cursor;
class DeclareSubFuncNode;
class DeclareSubProcNode;
class DeclareVariableNode;
class ItemInfo;
class MessageNode;
class PlanNode;
class RecordSource;
class Select;

// Direction for each column in sort order
enum SortDirection { ORDER_ANY, ORDER_ASC, ORDER_DESC };

// Types of nulls placement for each column in sort order
enum NullsPlacement { NULLS_DEFAULT, NULLS_FIRST, NULLS_LAST };

// CompilerScratch.csb_g_flags' values.
inline constexpr int csb_internal			= 1;	// "csb_g_flag" switch
inline constexpr int csb_get_dependencies	= 2;	// we are retrieving dependencies
inline constexpr int csb_ignore_perm		= 4;	// ignore permissions checks
//inline constexpr int csb_blr_version4		= 8;	// the BLR is of version 4
inline constexpr int csb_pre_trigger		= 16;	// this is a BEFORE trigger
inline constexpr int csb_post_trigger		= 32;	// this is an AFTER trigger
inline constexpr int csb_validation			= 64;	// we're in a validation expression (RDB hack)
inline constexpr int csb_reuse_context		= 128;	// allow context reusage
inline constexpr int csb_subroutine			= 256;	// sub routine
inline constexpr int csb_reload				= 512;	// request's BLR should be loaded and parsed again
inline constexpr int csb_computed_field		= 1024;	// computed field expression
inline constexpr int csb_search_system_schema = 2048;	// search system schema

// CompilerScratch.csb_rpt[].csb_flags's values.
inline constexpr int csb_active			= 1;		// stream is active
inline constexpr int csb_used			= 2;		// context has already been defined (BLR parsing only)
inline constexpr int csb_view_update	= 4;		// view update w/wo trigger is in progress
inline constexpr int csb_trigger		= 8;		// NEW or OLD context in trigger
//inline constexpr int csb_no_dbkey		= 16;		// unused
inline constexpr int csb_store			= 32;		// we are processing a store statement
inline constexpr int csb_modify			= 64;		// we are processing a modify
inline constexpr int csb_sub_stream		= 128;		// a sub-stream of the RSE being processed
inline constexpr int csb_erase			= 256;		// we are processing an erase
inline constexpr int csb_unmatched		= 512;		// stream has conjuncts unmatched by any index
inline constexpr int csb_update			= 1024;		// erase or modify for relation
inline constexpr int csb_unstable		= 2048;		// unstable explicit cursor
inline constexpr int csb_skip_locked	= 4096;		// skip locked record


// Aggregate Sort Block (for DISTINCT aggregates)

class AggregateSort : protected Firebird::PermanentStorage, public Printable
{
public:
	explicit AggregateSort(Firebird::MemoryPool& p)
		: PermanentStorage(p),
		  keyItems(p)
	{
	}

public:
	virtual Firebird::string internalPrint(NodePrinter& printer) const
	{
		return "AggregateSort";
	}

public:
	dsc desc;
	ULONG length = 0;
	bool intl = false;
	ULONG impure = 0;
	Firebird::HalfStaticArray<sort_key_def, 2> keyItems;
};

// Inversion (i.e. nod_index) impure area

struct impure_inversion
{
	RecordBitmap* inv_bitmap;
};


// AggregateSort impure area

struct impure_agg_sort
{
	Sort* iasb_sort;
	ULONG iasb_dummy;
};


// Request resources

struct Resource
{
	enum rsc_s
	{
		rsc_relation,
		rsc_procedure,
		rsc_index,
		rsc_collation,
		rsc_function
	};

	rsc_s		rsc_type;
	USHORT		rsc_id;			// Id of the resource
	jrd_rel*	rsc_rel;		// Relation block
	Routine*	rsc_routine;	// Routine block
	Collation*	rsc_coll;		// Collation block

	static bool greaterThan(const Resource& i1, const Resource& i2) noexcept
	{
		// A few places of the engine depend on fact that rsc_type
		// is the first field in ResourceList ordering
		if (i1.rsc_type != i2.rsc_type)
			return i1.rsc_type > i2.rsc_type;
		if (i1.rsc_type == rsc_index)
		{
			// Sort by relation ID for now
			if (i1.rsc_rel->rel_id != i2.rsc_rel->rel_id)
				return i1.rsc_rel->rel_id > i2.rsc_rel->rel_id;
		}
		return i1.rsc_id > i2.rsc_id;
	}

	Resource(rsc_s type, USHORT id, jrd_rel* rel, Routine* routine, Collation* coll) noexcept
		: rsc_type(type), rsc_id(id), rsc_rel(rel), rsc_routine(routine), rsc_coll(coll)
	{ }
};

typedef Firebird::SortedArray<Resource, Firebird::EmptyStorage<Resource>,
	Resource, Firebird::DefaultKeyValue<Resource>, Resource> ResourceList;

// Access items
// In case we start to use MetaName with required pool parameter,
// access item to be reworked!
// This struct seems better located in scl.h.

struct AccessItem
{
	MetaName		acc_security_name;
	SLONG			acc_ss_rel_id;	// Relation Id which owner will be used to check permissions
	QualifiedName	acc_name;
	MetaName		acc_col_name;
	ObjectType		acc_type;
	SecurityClass::flags_t	acc_mask;

	static bool greaterThan(const AccessItem& i1, const AccessItem& i2)
	{
		int v;

		/* CVC: Disabled this horrible hack.
		// Relations and procedures should be sorted before
		// columns, hence such a tricky inverted condition
		if ((v = -strcmp(i1.acc_type, i2.acc_type)) != 0)
			return v > 0;
		*/
		if (i1.acc_type != i2.acc_type)
			return i1.acc_type > i2.acc_type;

		if ((v = i1.acc_security_name.compare(i2.acc_security_name)) != 0)
			return v > 0;

		if (i1.acc_ss_rel_id != i2.acc_ss_rel_id)
			return i1.acc_ss_rel_id > i2.acc_ss_rel_id;

		if (i1.acc_mask != i2.acc_mask)
			return i1.acc_mask > i2.acc_mask;

		if (i1.acc_name != i2.acc_name)
			return i1.acc_name > i2.acc_name;

		if (i1.acc_col_name != i2.acc_col_name)
			return i1.acc_col_name > i2.acc_col_name;

		return false; // Equal
	}

	AccessItem(const MetaName& security_name, SLONG view_id,
		const QualifiedName& name, ObjectType type,
		SecurityClass::flags_t mask, const MetaName& columnName)
		: acc_security_name(security_name), acc_ss_rel_id(view_id), acc_name(name),
			acc_col_name(columnName), acc_type(type), acc_mask(mask)
	{}
};

typedef Firebird::SortedArray<AccessItem, Firebird::EmptyStorage<AccessItem>,
	AccessItem, Firebird::DefaultKeyValue<AccessItem>, AccessItem> AccessItemList;

// Triggers and procedures the request accesses
struct ExternalAccess
{
	enum exa_act
	{
		exa_procedure,
		exa_function,
		exa_insert,
		exa_update,
		exa_delete
	};
	exa_act exa_action;
	USHORT exa_prc_id;
	USHORT exa_fun_id;
	USHORT exa_rel_id;
	USHORT exa_view_id;
	MetaName user;		// User which touch the recources.

	// Procedure
	ExternalAccess(exa_act action, USHORT id) :
		exa_action(action),
		exa_prc_id(action == exa_procedure ? id : 0),
		exa_fun_id(action == exa_function ? id : 0),
		exa_rel_id(0), exa_view_id(0)
	{ }

	// Trigger
	ExternalAccess(exa_act action, USHORT rel_id, USHORT view_id) :
		exa_action(action), exa_prc_id(0), exa_fun_id(0),
		exa_rel_id(rel_id), exa_view_id(view_id)
	{ }

	static bool greaterThan(const ExternalAccess& i1, const ExternalAccess& i2)
	{
		if (i1.exa_action != i2.exa_action)
			return i1.exa_action > i2.exa_action;
		if (i1.exa_prc_id != i2.exa_prc_id)
			return i1.exa_prc_id > i2.exa_prc_id;
		if (i1.exa_fun_id != i2.exa_fun_id)
			return i1.exa_fun_id > i2.exa_fun_id;
		if (i1.exa_rel_id != i2.exa_rel_id)
			return i1.exa_rel_id > i2.exa_rel_id;
		if (i1.exa_view_id != i2.exa_view_id)
			return i1.exa_view_id > i2.exa_view_id;
		if (i1.user != i2.user)
			return i1.user > i2.user;
		return false; // Equal
	}
};

typedef Firebird::SortedArray<ExternalAccess, Firebird::EmptyStorage<ExternalAccess>,
	ExternalAccess, Firebird::DefaultKeyValue<ExternalAccess>, ExternalAccess> ExternalAccessList;

// The three structs below are used for domains DEFAULT and constraints in PSQL
struct Item
{
	enum Type
	{
		TYPE_VARIABLE,
		TYPE_PARAMETER,
		TYPE_CAST
	};

	Item(Type aType, UCHAR aSubType, USHORT aIndex) noexcept
		: type(aType),
		  subType(aSubType),
		  index(aIndex)
	{
	}

	Item(Type aType, USHORT aIndex = 0) noexcept
		: type(aType),
		  subType(0),
		  index(aIndex)
	{
	}

	Type type;
	UCHAR subType;
	USHORT index;

	bool operator >(const Item& x) const noexcept
	{
		if (type == x.type)
		{
			if (subType == x.subType)
				return index > x.index;

			return subType > x.subType;
		}

		return type > x.type;
	}

	Firebird::string getDescription(Request* request, const ItemInfo* itemInfo) const;
};

struct FieldInfo
{
	FieldInfo() noexcept
		: nullable(false), defaultValue(NULL), validationExpr(NULL)
	{}

	bool nullable;
	NestConst<ValueExprNode> defaultValue;
	NestConst<BoolExprNode> validationExpr;
};

class ItemInfo : public Printable
{
public:
	ItemInfo(MemoryPool& p, const ItemInfo& o)
		: name(p, o.name),
		  field(p, o.field),
		  nullable(o.nullable),
		  explicitCollation(o.explicitCollation),
		  fullDomain(o.fullDomain)
	{
	}

	explicit ItemInfo(MemoryPool& p)
		: name(p),
		  field(p),
		  nullable(true),
		  explicitCollation(false),
		  fullDomain(false)
	{
	}

	ItemInfo() noexcept
		: name(),
		  field(),
		  nullable(true),
		  explicitCollation(false),
		  fullDomain(false)
	{
	}

public:
	bool isSpecial() const noexcept
	{
		return !nullable || fullDomain;
	}

	virtual Firebird::string internalPrint(NodePrinter& printer) const
	{
		/*** FIXME-PRINT:
		NODE_PRINT(printer, name);
		NODE_PRINT(printer, field);
		NODE_PRINT(printer, nullable);
		NODE_PRINT(printer, explicitCollation);
		NODE_PRINT(printer, fullDomain);
		***/

		return "ItemInfo";
	}

public:
	MetaName name;
	QualifiedNameMetaNamePair field;
	bool nullable;
	bool explicitCollation;
	bool fullDomain;
};

typedef Firebird::LeftPooledMap<QualifiedNameMetaNamePair, FieldInfo> MapFieldInfo;
typedef Firebird::RightPooledMap<Item, ItemInfo> MapItemInfo;

// Table value function block

class jrd_table_value_fun
{
public:
	explicit jrd_table_value_fun(MemoryPool& p) : recordFormat(nullptr), fields(p), funcId(0)
	{
	}

	SSHORT getId(const MetaName& fieldName) const
	{
		const SSHORT* const id = fields.get(fieldName);
		fb_assert(id);
		return *id;
	}

	const Format* recordFormat;
	Firebird::LeftPooledMap<MetaName, SSHORT> fields;
	MetaName name;
	USHORT funcId;
};

// Compile scratch block

class CompilerScratch : public pool_alloc<type_csb>
{
public:
	struct Dependency
	{
		explicit Dependency(int aObjType)
		{
			memset(this, 0, sizeof(*this));
			objType = aObjType;
		}

		int objType;

		union
		{
			jrd_rel* relation;
			const Function* function;
			const jrd_prc* procedure;
			const QualifiedName* name;
			SLONG number;
		};

		const MetaName* subName;
		SLONG subNumber;
	};

	explicit CompilerScratch(MemoryPool& p, CompilerScratch* aMainCsb = NULL)
	:	/*csb_node(0),
		csb_variables(0),
		csb_dependencies(0),
		csb_count(0),
		csb_n_stream(0),
		csb_msg_number(0),
		csb_impure(0),
		csb_g_flags(0),*/
#ifdef CMP_DEBUG
		csb_dump(p),
#endif
		mainCsb(aMainCsb),
		csb_external(p),
		csb_access(p),
		csb_resources(p),
		csb_dependencies(p),
		csb_fors(p),
		csb_localTables(p),
		csb_invariants(p),
		csb_current_nodes(p),
		csb_current_for_nodes(p),
		csb_forCursorNames(p),
		csb_computing_fields(p),
		csb_inner_booleans(p),
		csb_variables_used_in_subroutines(p),
		csb_pool(p),
		csb_map_field_info(p),
		csb_map_item_info(p),
		csb_message_pad(p),
		subFunctions(p),
		subProcedures(p),
		outerMessagesMap(p),
		outerVarsMap(p),
		csb_schema(p),
		csb_currentForNode(NULL),
		csb_currentDMLNode(NULL),
		csb_currentAssignTarget(NULL),
		csb_preferredDesc(NULL),
		csb_rpt(p)
	{
		csb_dbg_info = FB_NEW_POOL(p) Firebird::DbgInfo(p);
	}

	// Implemented in Statement.cpp
	ULONG allocImpure(ULONG align, ULONG size);

	template <typename T>
	ULONG allocImpure()
	{
		return allocImpure(alignof(T), sizeof(T));
	}

	StreamType nextStream(bool check = true)
	{
		if (csb_n_stream >= MAX_STREAMS && check)
			ERR_post(Firebird::Arg::Gds(isc_too_many_contexts));

		return csb_n_stream++;
	}

	bool collectingDependencies() const noexcept
	{
		return (mainCsb ? mainCsb : this)->csb_g_flags & csb_get_dependencies;
	}

	void addDependency(const Dependency& dependency)
	{
		auto& dependencies = mainCsb ? mainCsb->csb_dependencies : csb_dependencies;
		dependencies.add(dependency);
	}

	void qualifyExistingName(thread_db* tdbb, QualifiedName& name, ObjectType objType)
	{
		if (!(name.schema.isEmpty() && name.object.hasData()))
			return;

		const auto attachment = tdbb->getAttachment();

		if (csb_schema.hasData())
		{
			Firebird::ObjectsArray<Firebird::MetaString> schemaSearchPath;

			if (csb_g_flags & csb_search_system_schema)
				schemaSearchPath.push(SYSTEM_SCHEMA);

			schemaSearchPath.push(csb_schema);

			attachment->qualifyExistingName(tdbb, name, {objType}, &schemaSearchPath);
		}
		else
			attachment->qualifyExistingName(tdbb, name, {objType});
	}

#ifdef CMP_DEBUG
	void dump(const char* format, ...)
	{
		va_list params;
		va_start(params, format);

		Firebird::string s;
		s.vprintf(format, params);

		va_end(params);

		csb_dump += s;
	}

	Firebird::string csb_dump;
#endif

	CompilerScratch* mainCsb;
	Firebird::BlrReader	csb_blr_reader;
	DmlNode*		csb_node;
	ExternalAccessList csb_external;			// Access to outside procedures/triggers to be checked
	AccessItemList	csb_access;					// Access items to be checked
	vec<DeclareVariableNode*>*	csb_variables;	// Vector of variables, if any
	ResourceList	csb_resources;				// Resources (relations and indexes)
	Firebird::Array<Dependency>	csb_dependencies;	// objects that this statement depends upon
	Firebird::Array<const Select*> csb_fors;	// select expressions
	Firebird::Array<const DeclareLocalTableNode*> csb_localTables;	// local tables
	Firebird::Array<ULONG*> csb_invariants;		// stack of pointer to nodes invariant offsets
	Firebird::Array<ExprNode*> csb_current_nodes;	// RseNode's and other invariant
												// candidates within whose scope we are
	Firebird::Array<ForNode*> csb_current_for_nodes;
	Firebird::RightPooledMap<ForNode*, MetaName> csb_forCursorNames;
	Firebird::SortedArray<jrd_fld*> csb_computing_fields;	// Computed fields being compiled
	Firebird::Array<BoolExprNode*> csb_inner_booleans;	// Inner booleans at the current scope
	Firebird::SortedArray<USHORT> csb_variables_used_in_subroutines;
	StreamType		csb_n_stream;				// Next available stream
	USHORT			csb_msg_number;				// Highest used message number
	ULONG			csb_impure;					// Next offset into impure area
	USHORT			csb_g_flags;
	MemoryPool&		csb_pool;					// Memory pool to be used by csb
	Firebird::AutoPtr<Firebird::DbgInfo> csb_dbg_info;	// Debug information
	MapFieldInfo		csb_map_field_info;		// Map field name to field info
	MapItemInfo			csb_map_item_info;		// Map item to item info

	// Map of message number to field number to pad for external routines.
	Firebird::GenericMap<Firebird::Pair<Firebird::NonPooled<USHORT, USHORT> > > csb_message_pad;

	QualifiedName	csb_domain_validation;	// Parsing domain constraint in PSQL

	// used in cmp.cpp/pass1
	jrd_rel*	csb_view;
	StreamType	csb_view_stream;
	jrd_rel*	csb_parent_relation;
	unsigned	blrVersion;
	USHORT		csb_remap_variable;
	bool		csb_validate_expr;
	bool		csb_returning_expr;
	bool		csb_implicit_cursor;

	Firebird::LeftPooledMap<MetaName, DeclareSubFuncNode*> subFunctions;
	Firebird::LeftPooledMap<MetaName, DeclareSubProcNode*> subProcedures;
	Firebird::NonPooledMap<USHORT, USHORT> outerMessagesMap;	// <inner, outer>
	Firebird::NonPooledMap<USHORT, USHORT> outerVarsMap;		// <inner, outer>

	MetaName csb_schema;

	ForNode*	csb_currentForNode;
	StmtNode*	csb_currentDMLNode;		// could be StoreNode or ModifyNode
	ExprNode*	csb_currentAssignTarget;
	dsc*		csb_preferredDesc;		// expected by receiving side data format

	ULONG		csb_currentCursorId = 0;
	ULONG		csb_nextCursorId = 1;
	ULONG		csb_nextRecSourceId = 1;

	struct csb_repeat
	{
		// We must zero-initialize this one
		csb_repeat() noexcept;

		void activate() noexcept;
		void deactivate() noexcept;
		QualifiedName getName(bool allowEmpty = true) const;

		std::optional<USHORT> csb_cursor_number;	// Cursor number for this stream
		StreamType csb_stream;			// Map user context to internal stream
		StreamType csb_view_stream;		// stream number for view relation, below
		USHORT csb_flags;

		jrd_rel* csb_relation;
		Firebird::string* csb_alias;	// SQL alias name for this instance of relation
		jrd_prc* csb_procedure;
		jrd_rel* csb_view;				// parent view

		IndexDescList* csb_idx;			// Packed description of indices
		MessageNode* csb_message;		// Msg for send/receive
		const Format* csb_format;		// Default Format for stream
		Format* csb_internal_format;	// Statement internal format
		UInt32Bitmap* csb_fields;		// Fields referenced
		double csb_cardinality;			// Cardinality of relation
		PlanNode* csb_plan;				// user-specified plan for this relation
		StreamType* csb_map;			// Stream map for views
		RecordSource** csb_rsb_ptr;		// point to rsb for nod_stream
		jrd_table_value_fun* csb_table_value_fun;  // Table value function
	};

	typedef csb_repeat* rpt_itr;
	typedef const csb_repeat* rpt_const_itr;
	Firebird::HalfStaticArray<csb_repeat, 5> csb_rpt;
};

	// We must zero-initialize this one
inline CompilerScratch::csb_repeat::csb_repeat() noexcept
	: csb_stream(0),
	  csb_view_stream(0),
	  csb_flags(0),
	  csb_relation(0),
	  csb_alias(0),
	  csb_procedure(0),
	  csb_view(0),
	  csb_idx(0),
	  csb_message(0),
	  csb_format(0),
	  csb_internal_format(0),
	  csb_fields(0),
	  csb_cardinality(0.0),	// TMN: Non-natural cardinality?!
	  csb_plan(0),
	  csb_map(0),
	  csb_rsb_ptr(0),
	  csb_table_value_fun(0)
{
}

inline void CompilerScratch::csb_repeat::activate() noexcept
{
	csb_flags |= csb_active;
}

inline void CompilerScratch::csb_repeat::deactivate() noexcept
{
	csb_flags &= ~csb_active;
}

inline QualifiedName CompilerScratch::csb_repeat::getName(bool allowEmpty) const
{
	if (csb_relation)
		return csb_relation->rel_name;
	else if (csb_procedure)
		return csb_procedure->getName();
	else if (csb_table_value_fun)
		return QualifiedName(csb_table_value_fun->name);
	//// TODO: LocalTableSourceNode
	//// TODO: JsonTableSourceNode
	else
	{
		fb_assert(allowEmpty);
		return {};
	}
}


class AutoSetCurrentCursorId : private Firebird::AutoSetRestore<ULONG>
{
public:
	explicit AutoSetCurrentCursorId(CompilerScratch* csb)
		: AutoSetRestore(&csb->csb_currentCursorId,
			(csb->csb_currentCursorId == 0 ? csb->csb_nextCursorId++ : csb->csb_currentCursorId))
	{
	}
};


class StatusXcp
{
	Firebird::FbLocalStatus status;

public:
	StatusXcp();

	void clear();
	void init(const Jrd::FbStatusVector*) noexcept;
	void copyTo(Jrd::FbStatusVector*) const noexcept;
	bool success() const;
	SLONG as_gdscode() const;
	SLONG as_sqlcode() const;
	void as_sqlstate(char*) const;
	SLONG as_xcpcode() const;
	Firebird::string as_text() const;
};

// must correspond to the declared size of RDB$EXCEPTIONS.RDB$MESSAGE
inline constexpr unsigned XCP_MESSAGE_LENGTH = 1023;

// Array which stores relative pointers to impure areas of invariant nodes
typedef Firebird::SortedArray<ULONG> VarInvariantArray;

} // namespace Jrd

#endif // JRD_EXE_H
