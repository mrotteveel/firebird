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
 *  The Original Code was created by Claudio Valderrama on 23-Jul-2009
 *  for the Firebird Open Source RDBMS project.
 *
 *  Copyright (c) 2009 Claudio Valderrama
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 *
 */


#ifndef ISQL_ISQLSWI_H
#define ISQL_ISQLSWI_H

#include "ibase.h"
#include "../jrd/constants.h"

enum isql_switches
{
	IN_SW_ISQL_0			= 0,
	IN_SW_ISQL_EXTRACTALL	= 1,
	IN_SW_ISQL_AUTOTERM		= 2,
	IN_SW_ISQL_BAIL 		= 3,
	IN_SW_ISQL_CACHE		= 4,
	IN_SW_ISQL_CHARSET		= 5,
	IN_SW_ISQL_DATABASE 	= 6,
	IN_SW_ISQL_ECHO 		= 7,
	IN_SW_ISQL_EXTRACT		= 8,
	IN_SW_ISQL_FETCHPASS	= 9,
	IN_SW_ISQL_INPUT		= 10,
	IN_SW_ISQL_MERGE		= 11,
	IN_SW_ISQL_MERGE2		= 12,
	IN_SW_ISQL_NOAUTOCOMMIT = 13,
	IN_SW_ISQL_NODBTRIGGERS = 14,
	IN_SW_ISQL_NOWARN		= 15,
	IN_SW_ISQL_OUTPUT		= 16,
	IN_SW_ISQL_PAGE 		= 17,
	IN_SW_ISQL_PASSWORD 	= 18,
	IN_SW_ISQL_QUIET		= 19,
	IN_SW_ISQL_ROLE 		= 20,
	IN_SW_ISQL_ROLE2		= 21,
	IN_SW_ISQL_SEARCH_PATH	= 22,
	IN_SW_ISQL_SQLDIALECT	= 23,
	IN_SW_ISQL_TERM 		= 24,
#ifdef TRUSTED_AUTH
	IN_SW_ISQL_TRUSTED		= 25,
#endif
	IN_SW_ISQL_USER 		= 26,
	IN_SW_ISQL_VERSION		= 27,
#ifdef DEV_BUILD
	IN_SW_ISQL_EXTRACTTBL	= 28,
#endif
	IN_SW_ISQL_HELP 		= 29
};


enum IsqlOptionType { iqoArgNone, iqoArgInteger, iqoArgString };

static const Switches::in_sw_tab_t isql_in_sw_table[] =
{
	{IN_SW_ISQL_EXTRACTALL	, 0, "ALL"				, 0, 0, 0, false, false, 11		, 1, NULL, iqoArgNone},
	{IN_SW_ISQL_AUTOTERM	, 0, "AUTOTERM"			, 0, 0, 0, false, false, 206	, 5, NULL, iqoArgNone},
	{IN_SW_ISQL_BAIL 		, 0, "BAIL"				, 0, 0, 0, false, false, 104	, 1, NULL, iqoArgNone},
	{IN_SW_ISQL_CACHE		, 0, "CACHE"			, 0, 0, 0, false, false, 111	, 1, NULL, iqoArgInteger},
	{IN_SW_ISQL_CHARSET		, 0, "CHARSET"			, 0, 0, 0, false, false, 122	, 2, NULL, iqoArgString},
	{IN_SW_ISQL_DATABASE 	, 0, "DATABASE"			, 0, 0, 0, false, false, 123	, 1, NULL, iqoArgString},
	{IN_SW_ISQL_ECHO 		, 0, "ECHO"				, 0, 0, 0, false, false, 124	, 1, NULL, iqoArgNone},
	{IN_SW_ISQL_EXTRACT		, 0, "EXTRACT"			, 0, 0, 0, false, false, 125	, 2, NULL, iqoArgNone},
	{IN_SW_ISQL_FETCHPASS	, 0, "FETCH_PASSWORD"	, 0, 0, 0, false, false, 161	, 1, NULL, iqoArgString},
	{IN_SW_ISQL_INPUT		, 0, "INPUT"			, 0, 0, 0, false, false, 126	, 1, NULL, iqoArgString},
	{IN_SW_ISQL_MERGE		, 0, "MERGE"			, 0, 0, 0, false, false, 127	, 1, NULL, iqoArgNone},
	{IN_SW_ISQL_MERGE2		, 0, "M2"				, 0, 0, 0, false, false, 128	, 2, NULL, iqoArgNone},
	{IN_SW_ISQL_NOAUTOCOMMIT, 0, "NOAUTOCOMMIT"		, 0, 0, 0, false, false, 129	, 1, NULL, iqoArgNone},
	{IN_SW_ISQL_NODBTRIGGERS, 0, "NODBTRIGGERS"		, 0, 0, 0, false, false, 154	, 3, NULL, iqoArgNone},
	{IN_SW_ISQL_NOWARN		, 0, "NOWARNINGS"		, 0, 0, 0, false, false, 130	, 3, NULL, iqoArgNone},
	{IN_SW_ISQL_OUTPUT		, 0, "OUTPUT"			, 0, 0, 0, false, false, 131	, 1, NULL, iqoArgString},
	{IN_SW_ISQL_PAGE 		, 0, "PAGELENGTH"		, 0, 0, 0, false, false, 132	, 3, NULL, iqoArgInteger},
	{IN_SW_ISQL_PASSWORD 	, 0, "PASSWORD"			, 0, 0, 0, false, false, 133	, 1, NULL, iqoArgString},
	{IN_SW_ISQL_QUIET		, 0, "QUIET"			, 0, 0, 0, false, false, 134	, 1, NULL, iqoArgNone},
	{IN_SW_ISQL_ROLE 		, 0, "ROLE"				, 0, 0, 0, false, false, 135	, 1, NULL, iqoArgString},
	{IN_SW_ISQL_ROLE2		, 0, "R2"				, 0, 0, 0, false, false, 136	, 2, NULL, iqoArgString},
	{IN_SW_ISQL_SEARCH_PATH	, 0, "SEARCH_PATH"		, 0, 0, 0, false, false, 210	, 2, NULL, iqoArgString},
	{IN_SW_ISQL_SQLDIALECT	, 0, "SQLDIALECT"		, 0, 0, 0, false, false, 137	, 1, NULL, iqoArgInteger},
	{IN_SW_ISQL_SQLDIALECT	, 0, "SQL_DIALECT"		, 0, 0, 0, false, false, 0		, 1, NULL, iqoArgInteger},
	{IN_SW_ISQL_TERM 		, 0, "TERMINATOR"		, 0, 0, 0, false, false, 138	, 1, NULL, iqoArgString},
#ifdef TRUSTED_AUTH
	{IN_SW_ISQL_TRUSTED		, 0, "TRUSTED"			, 0, 0, 0, false, false, 155	, 2, NULL, iqoArgNone},
#endif
	{IN_SW_ISQL_USER 		, 0, "USER"				, 0, 0, 0, false, false, 139	, 1, NULL, iqoArgString},
	{IN_SW_ISQL_EXTRACT		, 0, "X"				, 0, 0, 0, false, false, 140	, 1, NULL, iqoArgNone},
#ifdef DEV_BUILD
	{IN_SW_ISQL_EXTRACTTBL	, 0, "XT"				, 0, 0, 0, false, false, 0		, 2, NULL, iqoArgNone},
#endif
	{IN_SW_ISQL_VERSION		, 0, "Z"				, 0, 0, 0, false, false, 141	, 1, NULL, iqoArgNone},
	{IN_SW_ISQL_HELP 		, 0, "?"				, 0, 0, 0, false, false, 0		, 1, NULL, iqoArgNone},
	{IN_SW_ISQL_0			, 0, NULL				, 0, 0, 0, false, false, 0		, 0, NULL, iqoArgNone}
};

#endif // ISQL_ISQLSWI_H
