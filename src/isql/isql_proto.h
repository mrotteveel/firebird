/*
 *	PROGRAM:	Interactive SQL utility
 *	MODULE:		isql_proto.h
 *	DESCRIPTION:	Prototype header file for isql.epp
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

#ifndef ISQL_ISQL_PROTO_H
#define ISQL_ISQL_PROTO_H

#include <firebird/Interface.h>
#include "../common/classes/fb_string.h"
#include "../common/classes/MetaString.h"
#include "../common/classes/QualifiedMetaString.h"
#include <optional>

struct IsqlVar;

void	ISQL_array_dimensions(const Firebird::QualifiedMetaString&);
//void	ISQL_build_table_list(void**, FILE*, FILE*, FILE*);
//void	ISQL_build_view_list(void**, FILE*, FILE*, FILE*);
//int	ISQL_commit_work(int, FILE*, FILE*, FILE*);
bool	ISQL_dbcheck();
void	ISQL_disconnect_database(bool);
bool	ISQL_errmsg(Firebird::IStatus*);
void	ISQL_warning(Firebird::IStatus*);
void	ISQL_exit_db();
// CVC: Not found.
//int		ISQL_extract(TEXT*, int, FILE*, FILE*, FILE*);
int		ISQL_frontend_command(TEXT*, FILE*, FILE*, FILE*);
bool	ISQL_get_base_column_null_flag(const Firebird::QualifiedMetaString&, const SSHORT, const Firebird::MetaString&);
// Shall become obsolete when collation become a part of data type as in SQL standard
enum class Get
{
	CHARSET_ONLY,
	COLLATE_ONLY,
	BOTH
};
bool	ISQL_get_character_sets(
	SSHORT char_set_id, SSHORT collation,
	SSHORT default_char_set_id, Get what,
	bool not_null, Firebird::string& text);
SSHORT	ISQL_get_default_char_set_id();
void	ISQL_get_domain_default_source(const Firebird::QualifiedMetaString&, ISC_QUAD*);
SSHORT	ISQL_get_field_length(const Firebird::QualifiedMetaString&);
SSHORT	ISQL_get_char_length(
	SSHORT fieldLength,
	SSHORT characterLengthNull, SSHORT characterLength,
	SSHORT characterSetIdNull, SSHORT characterSetId);
SLONG	ISQL_get_index_segments(TEXT*, const size_t, const Firebird::QualifiedMetaString&);
bool	ISQL_get_null_flag(const Firebird::QualifiedMetaString&, const Firebird::MetaString&);
void	ISQL_get_version(bool);
SSHORT	ISQL_init(FILE*, FILE*);
int		ISQL_main(int, char**);
bool	ISQL_printNumericType(const Firebird::QualifiedMetaString& fieldName, const int fieldType,
	const int fieldSubType, const int fieldPrecision, const int fieldScale);
void	ISQL_print_validation(FILE*, ISC_QUAD*, bool, Firebird::ITransaction*);
//void	ISQL_query_database(SSHORT*, FILE*, FILE*, FILE*);
//void	ISQL_reset_settings();
void	ISQL_ri_action_print(const TEXT*, const TEXT*, bool);
//int	ISQL_sql_statement(TEXT*, FILE*, FILE*, FILE*);
//void	ISQL_win_err(const char*);
processing_state ISQL_print_item_blob(FILE*, const IsqlVar*, Firebird::ITransaction*, int subtype);
processing_state ISQL_fill_var(IsqlVar*, Firebird::IMessageMetadata*, unsigned, UCHAR*);
bool ISQL_statement_ends_in_comment(const char* statement);

#endif // ISQL_ISQL_PROTO_H
