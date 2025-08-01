/*
 *	PROGRAM:	JRD Backup and Restore Program
 *	MODULE:		burp.h
 *	DESCRIPTION:	Burp file format
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
 * 2002.10.29 Sean Leyne - Removed obsolete "Netware" port
 *
// 2002.10.30 Sean Leyne - Removed support for obsolete "PC_PLATFORM" define
//
 */

#ifndef BURP_BURP_H
#define BURP_BURP_H

#include <optional>
#include <stdio.h>
#include "ibase.h"
#include "firebird/Interface.h"
#include "firebird/Message.h"
#include "../common/dsc.h"
#include "../burp/misc_proto.h"
#include "../burp/mvol_proto.h"
#include "../yvalve/gds_proto.h"
#include "../common/ThreadData.h"
#include "../common/UtilSvc.h"
#include "../common/classes/array.h"
#include "../common/classes/fb_pair.h"
#include "../common/classes/GenericMap.h"
#include "../common/classes/MetaString.h"
#include "../common/classes/QualifiedMetaString.h"
#include "../common/SimilarToRegex.h"
#include "../common/status.h"
#include "../common/sha.h"
#include "../common/classes/ImplementHelper.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#if defined(HAVE_ZLIB_H)
#define WIRE_COMPRESS_SUPPORT 1
#endif

#ifdef WIRE_COMPRESS_SUPPORT
#include <zlib.h>
//#define COMPRESS_DEBUG 1
#endif // WIRE_COMPRESS_SUPPORT

inline constexpr int GDS_NAME_LEN = METADATA_IDENTIFIER_CHAR_LEN * 4 /* max bytes per char */ + 1;
typedef TEXT GDS_NAME[GDS_NAME_LEN];

enum redirect_vals {
	NOREDIRECT = 0,
	REDIRECT = 1,
	NOOUTPUT = 2
};

static inline constexpr int burp_msg_fac = FB_IMPL_MSG_FACILITY_GBAK;

// Record types in backup file

enum rec_type {
	rec_burp,				// Restore program attributes
	rec_database,			// Logical database parameters
	rec_global_field,		// Global field description
	rec_relation,			// Relation description
	rec_field,				// Local field description
	rec_index,				// Index description
	rec_data,				// Data for relation
	rec_blob,				// Blob
	rec_relation_data,		// Standalone data header
	rec_relation_end,		// End of data for relation
	rec_end,				// End of file
	rec_view,				// View attributes
	rec_security_class,		// Security class acl
	rec_trigger,			// Trigger definition
	rec_physical_db,		// Physical database parameters
	rec_function,			// Function description
	rec_function_arg,		// Function arguement description
	rec_function_end,		// End of function and its args
	rec_gen_id,				// From blr_gen_id
	rec_system_type,		// Type of field
	rec_filter,				// Filter
	rec_trigger_message,	// Trigger message texts
	rec_user_privilege,		// User privilege
	rec_array,				// Array blob (23)
	rec_field_dimensions,	// Array field dimensions
	rec_files,				// files for shadowing
	rec_generator,			// another format for gen-ids
	rec_procedure,			// Stored procedure
	rec_procedure_prm,		// Stored procedure parameters
	rec_procedure_end,		// End of procedure and its args
	rec_exception,			// Exception
	rec_rel_constraint,		// Relation constraints
	rec_ref_constraint,		// Referential constraints
	rec_chk_constraint,		// Check constraints
	rec_charset,			// Character sets
	rec_collation,			// Collations
	rec_sql_roles,			// SQL roles
	rec_mapping,			// Mapping of security names
	rec_package,			// Package
	rec_db_creator,			// Database creator
	rec_publication,		// Publication
	rec_pub_table,			// Publication table
	rec_schema				// Schema
};


/* The order of battle for major records is:

	[<rec_physical_database>] <rec_database> <global fields> <field dimensions> <relation> <function>
	 <types> <filters> <relation data> <trigger-new> <trigger messages> <user privileges> <security> rec_end

where each relation is:

	<rec_relation> <rel name> <att_end> <local fields> <view> <rec_relation_end>

where local fields is:
	<rec_field> <att_field_... att_field_dimensions, att_field_range_low, att_field_range_high...> <att_end>

where each relation data is:

	<rec_relation_data> <rel attributes> <gen id> <indices> <data> <trigger-old> <rec_relation_end>

where data is:
	<rec_data> <rec length> [<xdr_length>]  <data attr>  [ <blob>, <array>...]

and <blob> is

	<rec_blob> <blob_field_number> <max_sigment> <blob_type> <number_segments> <blob_data>

and <array> is

	<rec_array> <blob_field_number> <att_array_dimensions> <att_array_range_low>
	<att_array_range_high> <blob_data> [<att_xdr_array>]


where each function is:

	<rec_function> <function attributes> <att_end> [<function_arguments>] <rec_function_end>

and <function_arguments> is

	<rec_function_arg> <function argument attributes> <att_end>

where trigger-old is:
	<rec_trigger> <trig_type> <trig_blr> <trig_source> <att_end>

and trigger-new is:
	<rec_trigger> <trig_type> <trig_blr> <trig_source> <trigger name> <relation name> <trig_seqence>
				  <description> <system flag> <att_end>


*/

// Attributes within major record

/*
   CAREFUL not to pull the lastest version into maint version without
   modifying the att_backup_format to be one version back


Version 6: IB6, FB1, FB1.5.
			Supports SQL Time & Date columns.
			  RDB$FIELD_PRECISION
			  SQL Dialect from database header
			  SQL_INT64 columns and generator values

Version 7: FB2.0.
			RDB$DESCRIPTION in roles and generators.
			RDB$BASE_COLLATION_NAME and RDB$SPECIFIC_ATTRIBUTES in collations

Version 8: FB2.1.
			RDB$RELATION_TYPE in relations
			RDB$PROCEDURE_TYPE and RDB$VALID_BLR in procedures
			RDB$VALID_BLR in triggers
			RDB$DEFAULT_VALUE, RDB$DEFAULT_SOURCE and RDB$COLLATION_ID in procedure_parameters

Version 9: FB2.5.
			RDB$MESSAGE domain was enlarged from 78 to 1021 in FB2.0 and to 1023 in FB2.5,
			but gbak wasn't adjusted accordingly and thus it cannot store reliably text that's
			longer than 255 bytes.
			We anyway tried a recovery routine in v2.5 that may be backported.

Version 10: FB3.0.
			See backup_capabilities in OdsDetection.h.

Version 11: FB4.0.
			SQL SECURITY feature, tables RDB$PUBLICATIONS/RDB$PUBLICATION_TABLES.

Version 12: FB6.0.
			Schemas.
*/

inline constexpr int ATT_BACKUP_FORMAT = 12;

// max array dimension

inline constexpr int MAX_DIMENSION = 16;

inline constexpr int SERIES = 1;

inline constexpr USHORT MAX_UPDATE_DBKEY_RECURSION_DEPTH = 16;


enum att_type {
	att_end = 0,		// end of major record

	// Backup program attributes

	att_backup_date = SERIES,	// date of backup
	att_backup_format,		// backup format version
	att_backup_os,		// backup operating system
	att_backup_compress,
	att_backup_transportable,	// XDR datatypes for user data
	att_backup_blksize,		// backup block size
	att_backup_file,		// database file name
	att_backup_volume,		// backup volume number
	att_backup_keyname,		// name of crypt key
	att_backup_zip,			// zipped backup file
	att_backup_hash,		// hash of crypt key
	att_backup_crypt,		// name of crypt plugin

	// Database attributes

	att_file_name = SERIES,	// database file name (physical)
	att_file_size,		// size of original database (physical)
	att_jrd_version,		// jrd version (physical)
	att_creation_date,		// database creation date (physical)
	att_page_size,		// page size of original database (physical)
	att_database_description,	// description from RDB$DATABASE (logical)
	att_database_security_class,	// database level security (logical)
	att_sweep_interval,		// sweep interval
	att_no_reserve,		// don't reserve space for versions
	att_database_description2,
	att_database_dfl_charset,	// default character set from RDB$DATABASE
	att_forced_writes,		// syncronous writes flag
	att_page_buffers,		// page buffers for buffer cache
	att_SQL_dialect,		// SQL dialect that it speaks
	att_db_read_only,		// Is the database ReadOnly?
	att_database_linger,	// Disconnection timeout
	att_database_sql_security_deprecated,	// can be removed later
	att_replica_mode,		// replica mode
	att_database_sql_security,	// default sql security value
	att_default_pub_active, // default publication status
	att_default_pub_auto_enable,
	att_database_dfl_charset_schema_name,	// default character set schema name

	// Relation attributes

	att_relation_name = SERIES,
	att_relation_view_blr,
	att_relation_description,
	att_relation_record_length,	// Record length in file
	att_relation_view_relation,
	att_relation_view_context,
	att_relation_system_flag,
	att_relation_security_class,
	att_relation_view_source,
	att_relation_dummy,		 // this space available
	att_relation_ext_description,
	att_relation_owner_name,
	att_relation_description2,
	att_relation_view_source2,
	att_relation_ext_description2,
	att_relation_flags,
	att_relation_ext_file_name, // name of file for external tables
	att_relation_type,
	att_relation_sql_security_deprecated,	// can be removed later
	att_relation_sql_security,
	att_relation_schema_name,

	// Field attributes (used for both global and local fields)

	att_field_name = SERIES,	// name of field
	att_field_source,		// Global field name for local field

	att_base_field,		// Source field for view
	att_view_context,		// Context variable for view definition

	att_field_query_name,	// Query attributes
	att_field_query_header,
	att_field_edit_string,

	att_field_type,		// Physical attributes
	att_field_sub_type,
	att_field_length,		// 10
	att_field_scale,
	att_field_segment_length,
	att_field_position,		// Field position in relation (not in file)
	att_field_offset,		// Offset in data record (local fields only)

	att_field_default_value,	// Fluff
	att_field_description,
	att_field_missing_value,
	att_field_computed_blr,
	att_field_computed_source,
	att_field_validation_blr,	// 20
	att_field_validation_source,
	att_field_number,		// Field number to match up blobs
	att_field_computed_flag,	// Field is computed, not real
	att_field_system_flag,	// Interesting system flag
	att_field_security_class,

	att_field_external_length,
	att_field_external_type,
	att_field_external_scale,
	att_field_dimensions,	   // 29
	att_field_ranges,		   // this space for rent
	att_field_complex_name,	 // relation field attribute
	att_field_range_low,		// low range for array
	att_field_range_high,	   // high range for array
	att_field_update_flag,
	att_field_description2,
	att_field_validation_source2,
	att_field_computed_source2,
	att_field_null_flag,	// If field can be null
	att_field_default_source,	// default source for field (new fmt only)
	att_field_missing_source,	// missing source for field (new fmt only)
	att_field_character_length,	// length of field in characters
	att_field_character_set,	// Charset id of field
	att_field_collation_id,	// Collation id of field
	att_field_precision,	// numeric field precision of RDB$FIELDS (44)

	// beware that several items are shared between rdb$fields and rdb$relation_fields,
	// hence the new atributes for rdb$fields may be already present
	// att_field_security_class, // already used for relation_fields
	att_field_owner_name, // FB3.0, ODS12_0,
	att_field_generator_name,
	att_field_identity_type,
	att_field_schema_name,

	// Index attributes

	att_index_name = SERIES,
	att_segment_count,
	att_index_inactive,
	att_index_unique_flag,
	att_index_field_name,
	att_index_description,
	att_index_type,
	att_index_foreign_key,
	att_index_description2,
	att_index_expression_source,
	att_index_expression_blr,
	att_index_condition_source,
	att_index_condition_blr,
	att_index_foreign_key_schema_name,

	// Data record

	att_data_length = SERIES,
	att_data_data,

	// Blob record

	att_blob_field_number = SERIES + 2,	// Field number of blob field
	att_blob_type,			// Segmented = 0, stream = 1
	att_blob_number_segments,		// Number of segments
	att_blob_max_segment,		// Longest segment
	att_blob_data,

	// View attributes

	att_view_relation_name = SERIES + 7,
	att_view_context_id,
	att_view_context_name,
	att_view_context_type,
	att_view_context_package,

	// Security class attributes

	att_class_security_class = SERIES + 10,
	att_class_acl,
	att_class_description,


	// Array attributes

	att_array_dimensions = SERIES + 13,
	att_array_range_low,
	att_array_range_high,

	// XDR encoded data attributes

	att_xdr_length = SERIES + 16,
	att_xdr_array,

	att_class_description2,
	att_view_relation_schema_name,

	// Trigger attributes

	att_trig_type = SERIES,
	att_trig_blr,
	att_trig_source,
	att_trig_name,
	att_trig_relation_name,
	att_trig_sequence,
	att_trig_description,
	att_trig_system_flag,
	att_trig_inactive,
	att_trig_source2,
	att_trig_description2,
	att_trig_flags,
	att_trig_valid_blr,
	att_trig_debug_info,
	att_trig_engine_name,
	att_trig_entrypoint,
	att_trig_type2,
	att_trig_sql_security_deprecated,	// can be removed later
	att_trig_sql_security,
	att_trig_schema_name,

	// Function attributes

	att_function_name = SERIES,
	att_function_description,
	att_function_class,
	att_function_module_name,
	att_function_entrypoint,
	att_function_return_arg,
	att_function_query_name,
	att_function_type,
	att_function_description2,
	att_function_engine_name, // FB3.0, ODS12_0
	att_function_package_name,
	att_function_private_flag,
	att_function_blr,
	att_function_source,
	att_function_valid_blr,
	att_function_debug_info,
	att_function_security_class,
	att_function_owner_name,
	att_function_legacy_flag,
	att_function_deterministic_flag,
	att_function_sql_security_deprecated,	// can be removed later
	att_function_sql_security,
	att_function_schema_name,

	// Function argument attributes

	att_functionarg_name = SERIES,
	att_functionarg_position,
	att_functionarg_passing_mechanism, // by value, ref, descriptor
	att_functionarg_field_type,
	att_functionarg_field_scale,
	att_functionarg_field_length,
	att_functionarg_field_sub_type,
	att_functionarg_character_set,
	att_functionarg_field_precision,
	att_functionarg_package_name, // FB3.0, ODS12_0
	att_functionarg_arg_name,
	att_functionarg_field_source,
	att_functionarg_default_value,
	att_functionarg_default_source,
	att_functionarg_collation_id,
	att_functionarg_null_flag,
	att_functionarg_type_mechanism, // type inheritance
	att_functionarg_field_name,
	att_functionarg_relation_name,
	att_functionarg_description,
	att_functionarg_schema_name,
	att_functionarg_field_source_schema_name,
	att_functionarg_relation_schema_name,

	// TYPE relation attributes
	att_type_name = SERIES,
	att_type_type,
	att_type_field_name,
	att_type_description,
	att_type_system_flag,
	// Also see att_type_description2 below!

	// Filter attributes
	att_filter_name,
	att_filter_description,
	att_filter_module_name,
	att_filter_entrypoint,
	att_filter_input_sub_type,
	att_filter_output_sub_type,
	att_filter_description2,
	att_type_description2,

	// Trigger message attributes
	att_trigmsg_name = SERIES,
	att_trigmsg_number,
	att_trigmsg_text,
	att_trigmsg_schema_name,

	// User privilege attributes
	att_priv_user = SERIES,
	att_priv_grantor,
	att_priv_privilege,
	att_priv_grant_option,
	att_priv_object_name,
	att_priv_field_name,
	att_priv_user_type,
	att_priv_obj_type,
	att_priv_user_schema_name,
	att_priv_object_schema_name,

	// files for shadowing purposes
	att_file_filename = SERIES,
	att_file_sequence,
	att_file_start,
	att_file_length,
	att_file_flags,
	att_shadow_number,

	// Attributes for gen_id
	att_gen_generator = SERIES,
	att_gen_value,
	att_gen_value_int64,
	att_gen_description,
	att_gen_security_class, // FB3.0, ODS12_0
	att_gen_owner_name,
	att_gen_sysflag,
	att_gen_init_val,
	att_gen_id_increment,
	att_gen_schema_name,

	// Stored procedure attributes

	att_procedure_name = SERIES,
	att_procedure_inputs,
	att_procedure_outputs,
	att_procedure_description,
	att_procedure_description2,
	att_procedure_source,
	att_procedure_source2,
	att_procedure_blr,
	att_procedure_security_class,
	att_procedure_owner_name,
	att_procedure_type,
	att_procedure_valid_blr,
	att_procedure_debug_info,
	att_procedure_engine_name,
	att_procedure_entrypoint,
	att_procedure_package_name,
	att_procedure_private_flag,
	att_procedure_sql_security_deprecated,	// can be removed later
	att_procedure_sql_security,
	att_procedure_schema_name,

	// Stored procedure parameter attributes

	att_procedureprm_name = SERIES,
	att_procedureprm_number,
	att_procedureprm_type,
	att_procedureprm_field_source,
	att_procedureprm_description,
	att_procedureprm_description2,
	att_procedureprm_default_value,
	att_procedureprm_default_source,
	att_procedureprm_collation_id,
	att_procedureprm_null_flag,
	att_procedureprm_mechanism,
	att_procedureprm_field_name,
	att_procedureprm_relation_name,
	att_procedureprm_field_source_schema_name,
	att_procedureprm_relation_schema_name,

	// Exception attributes

	att_exception_name = SERIES,
	att_exception_msg,
	att_exception_description,
	att_exception_description2,
	att_exception_msg2,
	att_exception_security_class, // FB3.0, ODS12_0
	att_exception_owner_name,
	att_exception_schema_name,	// FB6.0, ODS14_0

	// Relation constraints attributes

	att_rel_constraint_name = SERIES,
	att_rel_constraint_type,
	att_rel_constraint_rel_name,
	att_rel_constraint_defer,
	att_rel_constraint_init,
	att_rel_constraint_index,
	att_rel_constraint_schema_name,

	// Referential constraints attributes

	att_ref_constraint_name = SERIES,
	att_ref_unique_const_name,
	att_ref_match_option,
	att_ref_update_rule,
	att_ref_delete_rule,
	att_ref_schema_name,
	att_ref_unique_const_schema_name,

	// SQL roles attributes
	att_role_name = SERIES,
	att_role_owner_name,
	att_role_description,
	att_role_sys_priveleges,

	// Check constraints attributes
	att_chk_constraint_name = SERIES,
	att_chk_trigger_name,
	att_chk_schema_name,

	// Character Set attributes
	att_charset_name = SERIES,
	att_charset_form,
	att_charset_numchar,
	att_charset_coll,
	att_charset_id,
	att_charset_sysflag,
	att_charset_description,
	att_charset_funct,
	att_charset_bytes_char,
	att_charset_security_class, // FB3.0, ODS12_0
	att_charset_owner_name,
	att_charset_schema_name,
	att_charset_coll_schema_name,

	att_coll_name = SERIES,
	att_coll_id,
	att_coll_cs_id,
	att_coll_attr,
	att_coll_subtype,		// Unused: 93-11-12 Daves
	att_coll_sysflag,
	att_coll_description,
	att_coll_funct,
	att_coll_base_collation_name,
	att_coll_specific_attr,
	att_coll_security_class, // FB3.0, ODS12_0
	att_coll_owner_name,
	att_coll_schema_name,

	// Names mapping
	att_map_name = SERIES,
	att_map_using,
	att_map_plugin,
	att_auto_map_role,		// Keep it at pos.4 - ODS11.2 compatibility issue
	att_map_db,
	att_map_from_type,
	att_map_from,
	att_map_to_type,
	att_map_to,
	att_map_description,

	// Package attributes
	att_package_name = SERIES,
	att_package_header_source,
	att_package_body_source,
	att_package_valid_body_flag,
	att_package_security_class,
	att_package_owner_name,
	att_package_description,
	att_package_sql_security_deprecated,	// can be removed later
	att_package_sql_security,
	att_package_schema_name,

	// Database creators
	att_dbc_user = SERIES,
	att_dbc_type,

	// Publications
	att_pub_name = SERIES,
	att_pub_owner_name,
	att_pub_active_flag,
	att_pub_auto_enable,

	// Publication tables
	att_ptab_pub_name = SERIES,
	att_ptab_table_name,
	att_ptab_table_schema_name,

	// Schema attributes
	att_schema_name = SERIES,
	att_schema_charset_schema_name,
	att_schema_charset_name,
	att_schema_sql_security,
	att_schema_security_class,
	att_schema_owner_name,
	att_schema_description,
};



// Trigger types

enum trig_t {
	trig_pre_store = 1,   // default
	trig_pre_modify,	  // default
	trig_post_erase	   // default
};

// these types to go away when recognized by gpre as
// <relation>.<field>.<type>  some time in the future

inline constexpr int TRIG_TYPE_PRE_STORE = 1;
inline constexpr int TRIG_TYPE_PRE_MODIFY = 3;
inline constexpr int TRIG_TYPE_POST_ERASE = 6;

// default trigger name templates

inline constexpr int TRIGGER_SEQUENCE_DEFAULT = 0;

// common structure definitions

// field block, used to hold local field definitions

struct burp_fld
{
	burp_fld*	fld_next;
	SSHORT		fld_type;
	SSHORT		fld_sub_type;
	FLD_LENGTH	fld_length;
	FLD_LENGTH	fld_total_len;	// including additional 2 bytes for VARYING CHAR
	SSHORT		fld_scale;
	SSHORT		fld_position;
	SSHORT		fld_parameter;
	SSHORT		fld_missing_parameter;
	SSHORT		fld_id;
	RCRD_OFFSET	fld_offset;
	RCRD_OFFSET	fld_missing_offset;
	RCRD_OFFSET	fld_old_offset;
	SSHORT		fld_number;
	SSHORT		fld_system_flag;
	SSHORT		fld_name_length;
	TEXT		fld_name [GDS_NAME_LEN];
	Firebird::QualifiedMetaString fld_source;
	TEXT		fld_base [GDS_NAME_LEN];
	TEXT		fld_query_name [GDS_NAME_LEN];
	TEXT		fld_security_class [GDS_NAME_LEN];
	TEXT		fld_generator[GDS_NAME_LEN];
	SSHORT		fld_identity_type;
	//SSHORT	fld_edit_length;
	SSHORT		fld_view_context;
	SSHORT		fld_update_flag;
	SSHORT		fld_flags;
	// Can't do here
	// BASED_ON RDB$RDB$RELATION_FIELDS.RDB$EDIT_STRING fld_edit_string;
	TEXT		fld_edit_string[128]; // was [256]
	ISC_QUAD	fld_description;
	ISC_QUAD	fld_query_header;
	TEXT		fld_complex_name [GDS_NAME_LEN];
	SSHORT		fld_dimensions;
	SLONG		fld_ranges [2 * MAX_DIMENSION];
	SSHORT		fld_null_flag;
	ISC_QUAD	fld_default_value;
	ISC_QUAD	fld_default_source;
	SSHORT		fld_character_set_id;
	SSHORT		fld_collation_id;
	RCRD_OFFSET	fld_sql;
	RCRD_OFFSET	fld_null;
};

enum fld_flags_vals {
	FLD_computed			= 1,
	FLD_position_missing	= 2,
	FLD_array				= 4,
	FLD_update_missing		= 8,
	FLD_null_flag			= 16,
	FLD_charset_flag		= 32,	// column has global charset
	FLD_collate_flag		= 64	// local column has specific collation
};

// relation definition - holds useful relation type stuff

struct burp_rel
{
	burp_rel*	rel_next;
	burp_fld*	rel_fields;
	SSHORT		rel_flags;
	SSHORT		rel_id;
	Firebird::QualifiedMetaString rel_name;
	GDS_NAME	rel_owner;		// relation owner, if not us
	ULONG		rel_max_pp;		// max pointer page sequence number
};

enum burp_rel_flags_vals {
	REL_view		= 1,
	REL_external	= 2
};

// package definition
struct burp_pkg
{
	burp_pkg*	pkg_next;
	Firebird::QualifiedMetaString pkg_name;
	GDS_NAME	pkg_owner;
};

// procedure definition - holds useful procedure type stuff

struct burp_prc
{
	burp_prc*	prc_next;
	Firebird::QualifiedMetaString prc_name;
	GDS_NAME	prc_owner;		// relation owner, if not us
};


struct gfld
{
	Firebird::QualifiedMetaString gfld_name;
	ISC_QUAD	gfld_vb;
	ISC_QUAD	gfld_vs;
	ISC_QUAD	gfld_vs2;
	ISC_QUAD	gfld_computed_blr;
	ISC_QUAD	gfld_computed_source;
	ISC_QUAD	gfld_computed_source2;
	gfld*		gfld_next;
	USHORT		gfld_flags;
};

enum gfld_flags_vals {
	GFLD_validation_blr		= 1,
	GFLD_validation_source	= 2,
	GFLD_validation_source2	= 4,
	GFLD_computed_blr		= 8,
	GFLD_computed_source	= 16,
	GFLD_computed_source2	= 32
};


struct burp_meta_obj
{
	burp_meta_obj*	obj_next;
	USHORT			obj_type;
	Firebird::QualifiedMetaString obj_name;
	bool			obj_class;
};

// CVC: Could use MAXPATHLEN, but what about restoring in a different system?
// I need to review if we tolerate different lengths for different OS's here.
inline constexpr unsigned int MAX_FILE_NAME_SIZE = 256;

#include "../burp/std_desc.h"

#ifdef WIN_NT

inline static void close_platf(DESC file)
{
	CloseHandle(file);
}

inline static void unlink_platf(const TEXT* file_name)
{
	DeleteFile(file_name);
}

inline static void flush_platf(DESC file)
{
	FlushFileBuffers(file);
}

#else // WIN_NT

void close_platf(DESC file);

inline static void unlink_platf(const TEXT* file_name)
{
	unlink(file_name);
}

inline static void flush_platf(DESC file)
{
#if defined(HAVE_FDATASYNC)
	fdatasync(file);
#elif defined(HAVE_FSYNC)
	fsync(file);
#endif
}

#endif // WIN_NT

// File block -- for multi-file databases

enum SIZE_CODE {
	size_n = 0,	// none
	size_k,		// k = 1024
	size_m,		// m = k x 1024
	size_g,		// g = m x 1024
	size_e		// error
};

class burp_fil
{
public:
	burp_fil*	fil_next;
	Firebird::PathName	fil_name;
	FB_UINT64	fil_length;
	DESC		fil_fd;
	USHORT		fil_seq;
	SIZE_CODE	fil_size_code;

burp_fil(Firebird::MemoryPool& p)
	: fil_next(0), fil_name(p), fil_length(0),
	  fil_fd(INVALID_HANDLE_VALUE), fil_seq(0), fil_size_code(size_n) { }
};

// Split & Join stuff

enum act_t {
	ACT_unknown, // action is unknown
	ACT_backup,
	ACT_backup_split,
	ACT_backup_fini,
	ACT_restore,
	ACT_restore_join
};

struct burp_act
{
		USHORT		act_total;
		burp_fil*	act_file;
		act_t		act_action;
};

inline constexpr size_t ACT_LEN = sizeof(burp_act);

inline constexpr ULONG MAX_LENGTH = ~FB_CONST64(0);	// Keep in sync with burp_fil.fil_length

// This structure has been cloned from spit.cpp

struct hdr_split
{
	TEXT hdr_split_tag[18];
	TEXT hdr_split_timestamp[30];
	TEXT hdr_split_text1[11];
	TEXT hdr_split_sequence[4];  // File sequence number
	TEXT hdr_split_text2[4];
	TEXT hdr_split_total[4];	 // Total number of files
	TEXT hdr_split_text3[2];
	TEXT hdr_split_name[27];	 // File name
};


// NOTE: size of the hdr_split_tag and HDR_SPLIT_TAG must be the same and equal
// to 18. Otherwise we will not be able to join the gbk files v5.x

inline constexpr size_t HDR_SPLIT_SIZE	= sizeof(hdr_split);
static inline constexpr char HDR_SPLIT_TAG5[]	= "InterBase/gsplit, ";
static inline constexpr char HDR_SPLIT_TAG6[]	= "InterBase/gbak,   ";
// CVC: Don't convert to const char* or you will have to fix the sizeof()'s!!!
#define HDR_SPLIT_TAG HDR_SPLIT_TAG6
inline constexpr FB_UINT64 MIN_SPLIT_SIZE	= FB_CONST64(2048);		// bytes


// Global switches and data

struct BurpCrypt;


class GblPool
{
private:
	// Moved it to separate class in order to ensure 'first create/last destroy' order
	Firebird::MemoryPool* gbl_pool;
public:
	Firebird::MemoryPool& getPool()
	{
		fb_assert(gbl_pool);
		return *gbl_pool;
	}

	explicit GblPool(bool ownPool)
		: gbl_pool(ownPool ? MemoryPool::createPool(getDefaultMemoryPool()) : getDefaultMemoryPool())
	{ }

	~GblPool()
	{
		if (gbl_pool != getDefaultMemoryPool())
			Firebird::MemoryPool::deletePool(gbl_pool);
	}
};

// forward declarations
namespace Burp
{
	class BurpTaskItem;
};

class BurpGlobals : public Firebird::ThreadData, public GblPool
{
public:
	explicit BurpGlobals(Firebird::UtilSvc* us)
		: ThreadData(ThreadData::tddGBL),
		  GblPool(us->isService()),
		  gbl_sw_par_workers(1),
		  defaultCollations(getPool()),
		  systemFields(getPool()),
		  gbl_dpb_data(*getDefaultMemoryPool()),
		  uSvc(us),
		  master(true),
		  taskItem(NULL),
		  verboseInterval(10000),
		  flag_on_line(true),
		  firstMap(true),
		  firstDbc(true),
		  stdIoMode(false)
	{
		// this is VERY dirty hack to keep current (pre-FB2) behaviour
		memset (&gbl_database_file_name, 0,
			&veryEnd - reinterpret_cast<char*>(&gbl_database_file_name));

		// normal code follows
		gbl_stat_flags = 0;
		gbl_stat_header = false;
		gbl_stat_done = false;
		memset(gbl_stats, 0, sizeof(gbl_stats));
		gbl_stats[TIME_TOTAL] = gbl_stats[TIME_DELTA] = fb_utils::query_performance_counter();

		exit_code = FINI_ERROR;	// prevent FINI_OK in case of unknown error thrown
								// would be set to FINI_OK (==0) in exit_local
	}

	const TEXT*	gbl_database_file_name;
	TEXT		gbl_backup_start_time[30];
	bool		gbl_sw_verbose;
	bool		gbl_sw_ignore_limbo;
	bool		gbl_sw_meta;
	bool		gbl_sw_novalidity;
	USHORT		gbl_sw_page_size;
	bool		gbl_sw_compress;
	bool		gbl_sw_version;
	bool		gbl_sw_transportable;
	bool		gbl_sw_incremental;
	bool		gbl_sw_deactivate_indexes;
	bool		gbl_sw_kill;
	USHORT		gbl_sw_blk_factor;
	USHORT		gbl_dialect;
	const SCHAR*	gbl_sw_fix_fss_data;
	USHORT			gbl_sw_fix_fss_data_id;
	const SCHAR*	gbl_sw_fix_fss_metadata;
	USHORT			gbl_sw_fix_fss_metadata_id;
	bool		gbl_sw_no_reserve;
	bool		gbl_sw_old_descriptions;
	bool		gbl_sw_convert_ext_tables;
	bool		gbl_sw_mode;
	bool		gbl_sw_mode_val;
	bool		gbl_sw_overwrite;
	bool		gbl_sw_direct_io;
	bool		gbl_sw_zip;
	const SCHAR*	gbl_sw_keyholder;
	const SCHAR*	gbl_sw_crypt;
	const SCHAR*	gbl_sw_keyname;
	SCHAR			gbl_hdr_keybuffer[MAX_SQL_IDENTIFIER_SIZE];
	SCHAR			gbl_hdr_cryptbuffer[MAX_SQL_IDENTIFIER_SIZE];
	const SCHAR*	gbl_sw_sql_role;
	const SCHAR*	gbl_sw_user;
	const SCHAR*	gbl_sw_password;
	SLONG		gbl_sw_skip_count;
	SLONG		gbl_sw_page_buffers;
	burp_fil*	gbl_sw_files;
	burp_fil*	gbl_sw_backup_files;
	int			gbl_sw_par_workers;
	gfld*		gbl_global_fields;
	unsigned	gbl_network_protocol;
	burp_act*	action;
	BurpCrypt*	gbl_crypt;
	ULONG		io_buffer_size;
	redirect_vals	sw_redirect;
	bool		burp_throw;
	std::optional<ReplicaMode>	gbl_sw_replica;

	UCHAR*		blk_io_ptr;
	int			blk_io_cnt;

	void put(const UCHAR c)
	{
		if (gbl_io_cnt <= 0)
			MVOL_write(this);

		--gbl_io_cnt;
		*gbl_io_ptr++ = c;
	}

	UCHAR get()
	{
		if (gbl_io_cnt <= 0)
			MVOL_read(this);

		--gbl_io_cnt;
		return *gbl_io_ptr++;
	}

#ifdef WIRE_COMPRESS_SUPPORT
	z_stream	gbl_stream;
#endif
	UCHAR*		gbl_io_ptr;
	int			gbl_io_cnt;
	UCHAR*		gbl_compress_buffer;
	UCHAR*		gbl_crypt_buffer;
	ULONG		gbl_crypt_left;
	UCHAR*      gbl_decompress;
	bool		gbl_default_pub_active = false;
	bool		gbl_default_pub_auto_enable = false;

	burp_rel*	relations;
	burp_pkg*	packages;
	burp_prc*	procedures;
	burp_meta_obj*	miss_privs;
	// ODS of the target server (not necessarily the same version as gbak)
	int			runtimeODS;
	// Format of the backup being read on restore; gbak always creates it using the latest version
	// but it can read backups created by previous versions.
	USHORT		RESTORE_format;
	ULONG		mvol_io_buffer_size;
	ULONG		mvol_actual_buffer_size;
	FB_UINT64	mvol_cumul_count;
	UCHAR*		mvol_io_ptr;
	int			mvol_io_cnt;
	UCHAR*		mvol_io_memory;		// as allocated, not aligned pointer
	UCHAR*		mvol_io_buffer;
	UCHAR*		mvol_io_volume;
	UCHAR*		mvol_io_header;
	UCHAR*		mvol_io_data;
	TEXT		mvol_db_name_buffer [MAX_FILE_NAME_SIZE];
	SCHAR		mvol_old_file [MAX_FILE_NAME_SIZE];
	int			mvol_volume_count;
	bool		mvol_empty_file;
	TEXT		mvol_keyname_buffer[MAX_FILE_NAME_SIZE];
	const TEXT*	mvol_keyname;
	TEXT		mvol_crypt_buffer[MAX_FILE_NAME_SIZE];
	const TEXT*	mvol_crypt;
	TEXT		gbl_key_hash[(Firebird::Sha1::HASH_SIZE + 1) * 4 / 3 + 1];	// take into an account base64
	Firebird::IAttachment*	db_handle;
	Firebird::ITransaction*	tr_handle;
	Firebird::ITransaction*	global_trans;
	TraNumber	tr_snapshot;
	DESC		file_desc;
	int			exit_code;
	UCHAR*		head_of_mem_list;
	FILE*		output_file;

	// Link list of global fields that were converted from V3 sub_type
	// to V4 char_set_id/collate_id. Needed for local fields conversion.
	// burp_fld*	v3_cvt_fld_list;

	// The handles_get... are for restore.
	Firebird::IRequest*	handles_get_character_sets_req_handle1;
	Firebird::IRequest*	handles_get_chk_constraint_req_handle1;
	Firebird::IRequest*	handles_get_collation_req_handle1;
	Firebird::IRequest*	handles_get_db_creators_req_handle1;
	Firebird::IRequest*	handles_get_exception_req_handle1;
	Firebird::IRequest*	handles_get_field_dimensions_req_handle1;
	Firebird::IRequest*	handles_get_field_req_handle1;
	Firebird::IRequest*	handles_get_fields_req_handle1;
	Firebird::IRequest*	handles_get_fields_req_handle2;
	Firebird::IRequest*	handles_get_fields_req_handle3;
	Firebird::IRequest*	handles_get_fields_req_handle4;
	Firebird::IRequest*	handles_get_fields_req_handle5;
	Firebird::IRequest*	handles_get_fields_req_handle6;
	Firebird::IRequest*	handles_get_files_req_handle1;
	Firebird::IRequest*	handles_get_filter_req_handle1;
	Firebird::IRequest*	handles_get_function_arg_req_handle1;
	Firebird::IRequest*	handles_get_function_req_handle1;
	Firebird::IRequest*	handles_get_global_field_req_handle1;
	Firebird::IRequest*	handles_get_index_req_handle1;
	Firebird::IRequest*	handles_get_index_req_handle2;
	Firebird::IRequest*	handles_get_index_req_handle3;
	Firebird::IRequest*	handles_get_index_req_handle4;
	Firebird::IRequest*	handles_get_mapping_req_handle1;
	Firebird::IRequest*	handles_get_package_req_handle1;
	Firebird::IRequest*	handles_get_procedure_prm_req_handle1;
	Firebird::IRequest*	handles_get_procedure_req_handle1;
	Firebird::IRequest*	handles_get_pub_req_handle1;
	Firebird::IRequest*	handles_get_pub_tab_req_handle1;
	Firebird::IRequest*	handles_get_ranges_req_handle1;
	Firebird::IRequest*	handles_get_ref_constraint_req_handle1;
	Firebird::IRequest*	handles_get_rel_constraint_req_handle1;
	Firebird::IRequest*	handles_get_relation_req_handle1;
	Firebird::IRequest*	handles_get_schema_req_handle1;
	Firebird::IRequest*	handles_get_security_class_req_handle1;
	Firebird::IRequest*	handles_get_sql_roles_req_handle1;
	Firebird::IRequest*	handles_get_trigger_message_req_handle1;
	Firebird::IRequest*	handles_get_trigger_message_req_handle2;
	Firebird::IRequest*	handles_get_trigger_old_req_handle1;
	Firebird::IRequest*	handles_get_trigger_req_handle1;
	Firebird::IRequest*	handles_get_trigger_req_handle2;
	Firebird::IRequest*	handles_get_type_req_handle1;
	Firebird::IRequest*	handles_get_user_privilege_req_handle1;
	Firebird::IRequest*	handles_get_view_req_handle1;
	Firebird::IRequest* handles_activateIndex_req_handle1;

	// The handles_put.. are for backup.
	Firebird::IRequest*	handles_put_index_req_handle1;
	Firebird::IRequest*	handles_put_index_req_handle2;
	Firebird::IRequest*	handles_put_index_req_handle3;
	Firebird::IRequest*	handles_put_index_req_handle4;
	Firebird::IRequest*	handles_put_index_req_handle5;
	Firebird::IRequest*	handles_put_index_req_handle6;
	Firebird::IRequest*	handles_put_index_req_handle7;
	Firebird::IRequest*	handles_put_relation_req_handle1;
	Firebird::IRequest*	handles_put_relation_req_handle2;
	Firebird::IRequest*	handles_put_relation_req_handle3;
	Firebird::IRequest*	handles_store_blr_gen_id_req_handle1;
	Firebird::IRequest*	handles_write_function_args_req_handle1;
	Firebird::IRequest*	handles_write_function_args_req_handle2;
	Firebird::IRequest*	handles_write_procedure_prms_req_handle1;
	Firebird::IRequest*	handles_fix_security_class_name_req_handle1;

	bool			hdr_forced_writes;
	TEXT			database_security_class[GDS_NAME_LEN]; // To save database security class for deferred update
	unsigned		batchInlineBlobLimit;

	static inline BurpGlobals* getSpecific()
	{
		return (BurpGlobals*) ThreadData::getSpecific();
	}
	static inline void putSpecific(BurpGlobals* tdgbl)
	{
		tdgbl->ThreadData::putSpecific();
	}
	static inline void restoreSpecific()
	{
		ThreadData::restoreSpecific();
	}
	void setupSkipIncludePattern(const Firebird::string& regexp, USHORT alreadySetErrorCode,
		Firebird::AutoPtr<Firebird::SimilarToRegex>& matcher);
	bool skipRelation(const Firebird::QualifiedMetaString& name);

	char veryEnd;
	//starting after this members must be initialized in constructor explicitly

	Firebird::FbLocalStatus status_vector;
	Firebird::ThrowLocalStatus throwStatus;

	Firebird::NonPooledMap<Firebird::QualifiedMetaString, Firebird::QualifiedMetaString> defaultCollations;
	Firebird::SortedArray<Firebird::QualifiedMetaString> systemFields;
	Firebird::Array<UCHAR> gbl_dpb_data;
	Firebird::UtilSvc* uSvc;
	bool master;			// set for master thread only
	Burp::BurpTaskItem* taskItem;			// current task item, if any.
	ULONG verboseInterval;	// How many records should be backed up or restored before we show this message
	bool flag_on_line;		// indicates whether we will bring the database on-line
	bool firstMap;			// this is the first time we entered get_mapping()
	bool firstDbc;			// this is the first time we entered get_db_creators()
	bool stdIoMode;			// stdin or stdout is used as backup file
	Firebird::AutoPtr<Firebird::SimilarToRegex> skipSchemaDataMatcher;
	Firebird::AutoPtr<Firebird::SimilarToRegex> skipDataMatcher;
	Firebird::AutoPtr<Firebird::SimilarToRegex> includeSchemaDataMatcher;
	Firebird::AutoPtr<Firebird::SimilarToRegex> includeDataMatcher;

public:
	Firebird::string toSystem(const Firebird::PathName& from);

	enum StatCounter { TIME_TOTAL = 0, TIME_DELTA, READS, WRITES, LAST_COUNTER};

	void read_stats(SINT64* stats);
	void print_stats(USHORT number);
	void print_stats_header();

	int gbl_stat_flags;					// bitmask, bit numbers see at enum StatCounter
	bool gbl_stat_header;				// true, if stats header was printed
	bool gbl_stat_done;					// true, if main process is done, stop to collect db-level stats
	SINT64 gbl_stats[LAST_COUNTER];

	bool gbl_use_no_auto_undo = true;
	bool gbl_use_auto_release_temp_blobid = true;
};

// CVC: This aux routine declared here to not force inclusion of burp.h with burp_proto.h
// in other modules.
void	BURP_exit_local(int code, BurpGlobals* tdgbl);

// database is not on-line due to failure to activate one or more indices
inline constexpr int FINI_DB_NOT_ONLINE = 2;

/* Burp will always write a backup in multiples of the following number
 * of bytes.  The initial value is the smallest which ensures that writes
 * to fixed-block SCSI tapes such as QIC-150 will work.  The value should
 * always be a multiple of 512 for that reason.
 * If you change to a value which is NOT a power of 2, then change the
 * BURP_UP_TO_BLOCK macro to use division and multiplication instead of
 * bit masking.
 */

inline constexpr int BURP_BLOCK = 512;
static inline constexpr ULONG BURP_UP_TO_BLOCK(const ULONG size)
{
	return (((size) + BURP_BLOCK - 1) & ~(BURP_BLOCK - 1));
}

// Move the read and write mode declarations in here from burp.cpp
// so that other files can see them for multivolume opens

#ifdef WIN_NT
static inline constexpr ULONG MODE_READ = GENERIC_READ;
static inline constexpr ULONG MODE_WRITE = GENERIC_WRITE;
#else
static inline constexpr ULONG MODE_READ = O_RDONLY;
static inline constexpr ULONG MODE_WRITE = O_WRONLY | O_CREAT;
#endif


// Burp Messages

enum burp_messages_vals {
	msgVerbose_write_charsets		= 211,
	msgVerbose_write_collations		= 212,
	msgErr_restore_charset			= 213,
	msgVerbose_restore_charset		= 214,
	msgErr_restore_collation		= 215,
	msgVerbose_restore_collation	= 216
};

// BLOB buffer
typedef Firebird::HalfStaticArray<UCHAR, 1024> BlobBuffer;

class BurpSql : public Firebird::AutoStorage
{
public:
	BurpSql(BurpGlobals* g, const char* sql)
		: Firebird::AutoStorage(),
		  tdgbl(g), stmt(nullptr)
	{
		stmt = tdgbl->db_handle->prepare(&tdgbl->throwStatus, tdgbl->tr_handle, 0, sql, 3, 0);
	}

	~BurpSql()
	{
		if (stmt)
		{
			stmt->free(&tdgbl->status_vector);
			stmt->release();
		}
	}

	template <typename M>
	void singleSelect(Firebird::ITransaction* trans, M* msg)
	{
		stmt->execute(&tdgbl->throwStatus, tdgbl->tr_handle, nullptr, nullptr, msg->getMetadata(), msg->getData());
	}

	template <typename M>
	void execute(Firebird::ITransaction* trans, M* msg)
	{
		stmt->execute(&tdgbl->throwStatus, tdgbl->tr_handle, msg->getMetadata(), msg->getData(), nullptr, nullptr);
	}

	void execute(Firebird::ITransaction* trans)
	{
		stmt->execute(&tdgbl->throwStatus, tdgbl->tr_handle, nullptr, nullptr, nullptr, nullptr);
	}

private:
	BurpGlobals* tdgbl;
	Firebird::IStatement* stmt;
};

class OutputVersion : public Firebird::IVersionCallbackImpl<OutputVersion, Firebird::CheckStatusWrapper>
{
public:
	OutputVersion(const char* printFormat)
		: format(printFormat)
	{ }

	void callback(Firebird::CheckStatusWrapper* status, const char* text);

private:
	const char* format;
};

static inline UCHAR* BURP_alloc(ULONG size)
{
	BurpGlobals* tdgbl = BurpGlobals::getSpecific();
	return (UCHAR*)(tdgbl->getPool().allocate(size ALLOC_ARGS));
}

static inline UCHAR* BURP_alloc_zero(ULONG size)
{
	BurpGlobals* tdgbl = BurpGlobals::getSpecific();
	return (UCHAR*)(tdgbl->getPool().calloc(size ALLOC_ARGS));
}

static inline void BURP_free(void* block) noexcept
{
	MemoryPool::globalFree(block);
}

#endif // BURP_BURP_H
