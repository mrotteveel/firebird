/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		ppg.cpp
 *	DESCRIPTION:	Database page print module
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
 * 2001.08.07 Sean Leyne - Code Cleanup, removed "#ifdef READONLY_DATABASE"
 *                         conditionals, second attempt
 */

#include "firebird.h"
#include <stdio.h>
#include <string.h>
#include "../common/classes/timestamp.h"
#include "ibase.h"
#include "../jrd/ods.h"
#include "../jrd/ods_proto.h"
#include "../common/os/guid.h"
#include "../yvalve/gds_proto.h"
#include "../common/classes/DbImplementation.h"

#include "../utilities/gstat/ppg_proto.h"

// gstat directly reads database files, therefore
using namespace Ods;
using Firebird::Guid;

void PPG_print_header(const header_page* header, bool nocreation, Firebird::UtilSvc* uSvc)
{
/**************************************
 *
 *	P P G _ p r i n t _ h e a d e r
 *
 **************************************
 *
 * Functional description
 *	Print database header page.
 *
 **************************************/
	uSvc->printf(false, "Database header page information:\n");

	uSvc->printf(false, "\tFlags\t\t\t%d\n", header->hdr_header.pag_flags);
	//uSvc->printf("\tChecksum\t\t%d\n", header->hdr_header.pag_checksum);
	uSvc->printf(false, "\tGeneration\t\t%" ULONGFORMAT"\n", header->hdr_header.pag_generation);
	uSvc->printf(false, "\tSystem Change Number\t%" ULONGFORMAT"\n", header->hdr_header.pag_scn);
	uSvc->printf(false, "\tPage size\t\t%d\n", header->hdr_page_size);
	uSvc->printf(false, "\tODS version\t\t%d.%d\n",
			header->hdr_ods_version & ~ODS_FIREBIRD_FLAG, header->hdr_ods_minor);
	uSvc->printf(false, "\tOldest transaction\t%" SQUADFORMAT"\n", header->hdr_oldest_transaction);
	uSvc->printf(false, "\tOldest active\t\t%" SQUADFORMAT"\n", header->hdr_oldest_active);
	uSvc->printf(false, "\tOldest snapshot\t\t%" SQUADFORMAT"\n", header->hdr_oldest_snapshot);
	uSvc->printf(false, "\tNext transaction\t%" SQUADFORMAT"\n", header->hdr_next_transaction);
	uSvc->printf(false, "\tNext attachment ID\t%" SQUADFORMAT"\n", header->hdr_attachment_id);

	Firebird::DbImplementation imp(header);
	uSvc->printf(false, "\tImplementation\t\tHW=%s %s-endian OS=%s CC=%s\n",
						 imp.cpu(), imp.endianess(), imp.os(), imp.cc());
	uSvc->printf(false, "\tShadow count\t\t%" SLONGFORMAT"\n", header->hdr_shadow_count);
	uSvc->printf(false, "\tPage buffers\t\t%" ULONGFORMAT"\n", header->hdr_page_buffers);

#ifdef DEV_BUILD
	uSvc->printf(false, "\tClumplet End\t\t%d\n", header->hdr_end);
#endif

	// If the database dialect is not set to 3, then we need to
	// assume it was set to 1.  The reason for this is that a dialect
	// 1 database has no dialect information written to the header.
	if (header->hdr_flags & hdr_SQL_dialect_3)
		uSvc->printf(false, "\tDatabase dialect\t3\n");
	else
		uSvc->printf(false, "\tDatabase dialect\t1\n");

	if (!nocreation)
	{
		const Guid guid(header->hdr_guid);
		uSvc->printf(false, "\tDatabase GUID:\t%s\n", guid.toString().c_str());

		struct tm time;
		isc_decode_timestamp(reinterpret_cast<const ISC_TIMESTAMP*>(header->hdr_creation_date),
						&time);
		uSvc->printf(false, "\tCreation date\t\t%s %d, %d %d:%02d:%02d\n",
				FB_SHORT_MONTHS[time.tm_mon], time.tm_mday, time.tm_year + 1900,
				time.tm_hour, time.tm_min, time.tm_sec);
	}

	uSvc->printf(false, "\tAttributes\t\t");
	const auto flags = header->hdr_flags;
	const auto nbakMode = header->hdr_backup_mode;
	const auto shutMode = header->hdr_shutdown_mode;
	const auto replMode = header->hdr_replica_mode;

	if (flags || nbakMode || shutMode || replMode)
	{
		int count = 0;

		if (flags & hdr_force_write)
		{
			uSvc->printf(false, "force write");
			count++;
		}

		if (flags & hdr_no_reserve)
		{
			if (count++)
				uSvc->printf(false, ", ");
			uSvc->printf(false, "no reserve");
		}

		if (flags & hdr_active_shadow)
		{
			if (count++)
				uSvc->printf(false, ", ");
			uSvc->printf(false, "active shadow");
		}

		if (flags & hdr_encrypted)
		{
			if (count++)
				uSvc->printf(false, ", ");
			uSvc->printf(false, "encrypted");
		}

		if (flags & hdr_crypt_process)
		{
			if (count++)
				uSvc->printf(false, ", ");
			uSvc->printf(false, "crypt process");
		}

		if (flags & (hdr_encrypted | hdr_crypt_process))
		{
			if (count++)
				uSvc->printf(false, ", ");
			uSvc->printf(false, "plugin %s", header->hdr_crypt_plugin);
		}

		if (flags & hdr_read_only)
		{
			if (count++)
				uSvc->printf(false, ", ");
			uSvc->printf(false, "read only");
		}

		if (shutMode)
		{
			if (count++)
				uSvc->printf(false, ", ");

			switch (shutMode)
			{
			case hdr_shutdown_multi:
				uSvc->printf(false, "multi-user maintenance");
				break;
			case hdr_shutdown_single:
				uSvc->printf(false, "single-user maintenance");
				break;
			case hdr_shutdown_full:
				uSvc->printf(false, "full shutdown");
				break;
			default:
				uSvc->printf(false, "wrong shutdown state %d", (int) shutMode);
			}
		}

		if (nbakMode)
		{
			if (count++)
				uSvc->printf(false, ", ");

			switch (nbakMode)
			{
			case Ods::hdr_nbak_stalled:
				uSvc->printf(false, "backup lock");
				break;
			case Ods::hdr_nbak_merge:
				uSvc->printf(false, "backup merge");
				break;
			default:
				uSvc->printf(false, "wrong backup state %d", (int) nbakMode);
			}
		}

		if (replMode)
		{
			if (count++)
				uSvc->printf(false, ", ");

			switch (replMode)
			{
			case Ods::hdr_replica_read_only:
				uSvc->printf(false, "read-only replica");
				break;
			case Ods::hdr_replica_read_write:
				uSvc->printf(false, "read-write replica");
				break;
			default:
				uSvc->printf(false, "wrong replica state %d", (int) replMode);
			}
		}

		uSvc->printf(false, "\n");
	}

	uSvc->printf(false, "\n    Variable header data:\n");

	TEXT temp[257];

	const UCHAR* p = header->hdr_data;
	for (const auto end = reinterpret_cast<const UCHAR*>(header) + header->hdr_page_size;
		p < end && *p != HDR_end; p += 2 + p[1])
	{
		SLONG number;

		switch (*p)
		{
		case HDR_root_file_name:
			memcpy(temp, p + 2, p[1]);
			temp[p[1]] = '\0';
			uSvc->printf(false, "\tRoot file name:\t\t%s\n", temp);
			break;

		case HDR_sweep_interval:
			memcpy(&number, p + 2, sizeof(number));
			uSvc->printf(false, "\tSweep interval:\t\t%ld\n", number);
			break;

		case HDR_difference_file:
			memcpy(temp, p + 2, p[1]);
			temp[p[1]] = '\0';
			uSvc->printf(false, "\tBackup difference file:\t%s\n", temp);
			break;

		case HDR_backup_guid:
		{
			fb_assert(p[1] == Guid::SIZE);
			const Guid guid(p + 2);
			uSvc->printf(false, "\tDatabase backup GUID:\t%s\n", guid.toString().c_str());
			break;
		}

		case HDR_crypt_key:
			uSvc->printf(false, "\tEncryption key name:\t%*.*s\n", p[1], p[1], p + 2);
			break;

		case HDR_crypt_hash:
			uSvc->printf(false, "\tKey hash:\t%*.*s\n", p[1], p[1], p + 2);
			break;

		case HDR_crypt_checksum:
			uSvc->printf(false, "\tCrypt checksum:\t%*.*s\n", p[1], p[1], p + 2);
			break;

		case HDR_repl_seq:
		{
			FB_UINT64 sequence;
			memcpy(&sequence, p + 2, sizeof(sequence));
			uSvc->printf(false, "\tReplication sequence:\t%" UQUADFORMAT"\n", sequence);
			break;
		}

		default:
			if (*p > HDR_max)
				uSvc->printf(false, "\tUnrecognized option %d, length %d\n", p[0], p[1]);
			else
				uSvc->printf(false, "\tEncoded option %d, length %d\n", p[0], p[1]);
			break;
		}
	}

	uSvc->printf(false, "\t*END*\n");
}
