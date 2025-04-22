/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		met_proto.h
 *	DESCRIPTION:	Prototype header file for met.cpp
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

#ifndef JRD_MET_PROTO_H
#define JRD_MET_PROTO_H

#include "../common/classes/array.h"
#include "../common/classes/TriState.h"
#include "../jrd/MetaName.h"
#include "../jrd/Resources.h"

#include <functional>

struct dsc;

namespace Jrd
{
	class jrd_tra;
	class Request;
	class Statement;
	class jrd_prc;
	class Format;
	class jrd_rel;
	class CompilerScratch;
	struct Dependency;
	class DmlNode;
	class Database;
	struct bid;
	struct index_desc;
	class jrd_fld;
	class Shadow;
	class DeferredWork;
	struct FieldInfo;
	class ExceptionItem;
	class GeneratorItem;
	class BlobFilter;
	class RelationPermanent;
	class Triggers;
	class TrigArray;

	typedef Firebird::HalfStaticArray<Jrd::MetaName, 4> CharsetVariants;

	struct SubtypeInfo
	{
		SubtypeInfo()
			: attributes(0),
			  ignoreAttributes(true)
		{
		}

		CharsetVariants charsetName;
		MetaName collationName;
		MetaName baseCollationName;
		USHORT attributes;
		bool ignoreAttributes;
		Firebird::UCharBuffer specificAttributes;
	};
}

void		MET_activate_shadow(Jrd::thread_db*);
ULONG		MET_align(const dsc*, ULONG);
Jrd::DeferredWork*	MET_change_fields(Jrd::thread_db*, Jrd::jrd_tra*, const dsc*);
void		MET_delete_dependencies(Jrd::thread_db*, const Jrd::MetaName&, int, Jrd::jrd_tra*);
void		MET_delete_shadow(Jrd::thread_db*, USHORT);
void		MET_error(const TEXT*, ...);
Jrd::Format*	MET_format(Jrd::thread_db*, Jrd::RelationPermanent*, USHORT);
bool		MET_get_char_coll_subtype_info(Jrd::thread_db*, USHORT, Jrd::SubtypeInfo* info);
Jrd::DmlNode*	MET_get_dependencies(Jrd::thread_db*, Jrd::jrd_rel*, const UCHAR*, const ULONG,
								Jrd::CompilerScratch*, Jrd::bid*, Jrd::Statement**,
								Jrd::CompilerScratch**, const Jrd::MetaName&, int, USHORT,
								Jrd::jrd_tra*, const Jrd::MetaName& = Jrd::MetaName());
Jrd::jrd_fld*	MET_get_field(const Jrd::jrd_rel*, USHORT);
ULONG		MET_get_rel_flags_from_TYPE(USHORT);
bool		MET_get_repl_state(Jrd::thread_db*, const Jrd::MetaName&);
void		MET_get_shadow_files(Jrd::thread_db*, bool);
bool		MET_load_exception(Jrd::thread_db*, Jrd::ExceptionItem&);
void		MET_load_trigger(Jrd::thread_db*, Jrd::jrd_rel*, const Jrd::MetaName&, std::function<Jrd::Triggers&(int)>);
void		MET_lookup_cnstrt_for_index(Jrd::thread_db*, Jrd::MetaName& constraint, const Jrd::MetaName& index_name);
void		MET_lookup_cnstrt_for_trigger(Jrd::thread_db*, Jrd::MetaName&, Jrd::MetaName&, const Jrd::MetaName&);
void		MET_lookup_exception(Jrd::thread_db*, SLONG, /* OUT */ Jrd::MetaName&, /* OUT */ Firebird::string*);
int			MET_lookup_field(Jrd::thread_db*, Jrd::jrd_rel*, const Jrd::MetaName&);
Jrd::BlobFilter*	MET_lookup_filter(Jrd::thread_db*, SSHORT, SSHORT);
bool		MET_load_generator(Jrd::thread_db*, Jrd::GeneratorItem&, bool* sysGen = 0, SLONG* step = 0);
SLONG		MET_lookup_generator(Jrd::thread_db*, const Jrd::MetaName&, bool* sysGen = 0, SLONG* step = 0);
bool		MET_lookup_generator_id(Jrd::thread_db*, SLONG, Jrd::MetaName&, bool* sysGen = 0);
void		MET_update_generator_increment(Jrd::thread_db* tdbb, SLONG gen_id, SLONG step);
void		MET_lookup_index_code(Jrd::thread_db* tdbb, Jrd::Cached::Relation* relation, Jrd::index_desc* idx);
bool		MET_lookup_index_expr_cond_blr(Jrd::thread_db* tdbb, const Jrd::MetaName& index_name, Jrd::bid& expr_blob_id, Jrd::bid& cond_blob_id);
bool		MET_lookup_partner(Jrd::thread_db* tdbb, Jrd::RelationPermanent* relation, Jrd::index_desc* idx,
							   const TEXT* index_name);
Jrd::DmlNode*	MET_parse_blob(Jrd::thread_db*, Jrd::Cached::Relation*, Jrd::bid*, Jrd::CompilerScratch**,
							   Jrd::Statement**, bool, bool);
void		MET_prepare(Jrd::thread_db*, Jrd::jrd_tra*, USHORT, const UCHAR*);
void		MET_release_existence(Jrd::thread_db*, Jrd::jrd_rel*);
void		MET_revoke(Jrd::thread_db*, Jrd::jrd_tra*, const Jrd::MetaName&,
	const Jrd::MetaName&, const Firebird::string&);
void		MET_scan_partners(Jrd::thread_db*, Jrd::RelationPermanent*);
void		MET_store_dependencies(Jrd::thread_db*, Firebird::Array<Jrd::Dependency>&, Jrd::jrd_rel*,
	const Jrd::MetaName&, int, Jrd::jrd_tra*);
void		MET_trigger_msg(Jrd::thread_db*, Firebird::string&, const Jrd::MetaName&, USHORT);
void		MET_update_shadow(Jrd::thread_db*, Jrd::Shadow*, USHORT);
void		MET_update_transaction(Jrd::thread_db*, Jrd::jrd_tra*, const bool);
void		MET_get_domain(Jrd::thread_db*, MemoryPool& csbPool, const Jrd::MetaName&, dsc*,
	Jrd::FieldInfo*);
Jrd::MetaName MET_get_relation_field(Jrd::thread_db*, MemoryPool& csbPool,
	const Jrd::MetaName&, const Jrd::MetaName&, dsc*, Jrd::FieldInfo*);
int			MET_get_linger(Jrd::thread_db*);
Firebird::TriState	MET_get_ss_definer(Jrd::thread_db*);

#endif // JRD_MET_PROTO_H
