/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		svc.cpp
 *	DESCRIPTION:	Service manager functions
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
 * 2002.02.15 Sean Leyne - Code Cleanup, removed obsolete "EPSON" define
 * 2002.02.15 Sean Leyne - Code Cleanup, removed obsolete "IMP" port
 *
 * 2002.10.27 Sean Leyne - Completed removal of obsolete "DELTA" port
 * 2002.10.27 Sean Leyne - Completed removal of obsolete "IMP" port
 *
 * 2002.10.29 Sean Leyne - Removed obsolete "Netware" port
 *
 * 2008		Alex Peshkoff - refactored services code for MT safe engine
 */

#include "firebird.h"
#include <stdio.h>
#include <string.h>
#include "../common/file_params.h"
#include <stdarg.h>
#include "../jrd/jrd.h"
#include "../jrd/svc.h"
#include "../jrd/constants.h"
#include "iberror.h"
#include "../jrd/license.h"
#include "../jrd/err_proto.h"
#include "../yvalve/gds_proto.h"
#include "../jrd/inf_proto.h"
#include "../common/isc_proto.h"
#include "../jrd/jrd_proto.h"
#include "../jrd/mov_proto.h"
#include "../yvalve/why_proto.h"
#include "../jrd/jrd_proto.h"
#include "../common/classes/alloc.h"
#include "../common/classes/init.h"
#include "../common/classes/ClumpletWriter.h"
#include "../common/utils_proto.h"
#include "../common/db_alias.h"
#include "../jrd/scl.h"
#include "../common/msg_encode.h"
#include "../jrd/trace/TraceManager.h"
#include "../jrd/trace/TraceObjects.h"
#include "../jrd/EngineInterface.h"
#include "../jrd/Mapping.h"
#include "../common/classes/RefMutex.h"
#include "../common/os/os_utils.h"

#include "../common/classes/DbImplementation.h"

// The switches tables. Needed only for utilities that run as service, too.
#include "../common/classes/Switches.h"
#include "../alice/aliceswi.h"
#include "../burp/burpswi.h"
#include "../utilities/gsec/gsecswi.h"
#include "../utilities/gstat/dbaswi.h"
#include "../utilities/nbackup/nbkswi.h"
#include "../jrd/trace/traceswi.h"
#include "../jrd/val_proto.h"
#include "../jrd/ThreadCollect.h"

// Service threads
#include "../burp/burp_proto.h"
#include "../alice/alice_proto.h"
int main_gstat(Firebird::UtilSvc* uSvc);
#include "../utilities/nbackup/nbk_proto.h"
#include "../utilities/gsec/gsec_proto.h"
#include "../jrd/trace/TraceService.h"
#include "../jrd/val_proto.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#ifdef HAVE_SYS_WAIT_H
# include <sys/wait.h>
#endif

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef HAVE_VFORK_H
#include <vfork.h>
#endif

#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#if !defined(WIN_NT)
#  include <signal.h>
#  include <sys/param.h>
#  include <errno.h>
#else
#  include <windows.h>
#  include <io.h> // _open, _get_osfhandle
#  include <stdlib.h>
#endif

#include <sys/stat.h>

#define statistics	stat


using namespace Firebird;
using namespace Jrd;

inline constexpr int SVC_user_dba	= 2;
inline constexpr int SVC_user_any	= 1;
inline constexpr int SVC_user_none	= 0;

inline constexpr int GET_LINE		= 1;
inline constexpr int GET_EOF		= 2;
inline constexpr int GET_BINARY		= 4;
inline constexpr int GET_ONCE		= 8;

inline constexpr const char* SPB_SEC_USERNAME = "isc_spb_sec_username";

namespace {

	// Generic mutex to synchronize services
	GlobalPtr<Mutex> globalServicesMutex;

	// All that we need to shutdown service threads when shutdown in progress
	typedef Array<Service*> AllServices;
	GlobalPtr<AllServices> allServices;	// protected by globalServicesMutex
	volatile bool svcShutdown = false;
	GlobalPtr<ThreadCollect> threadCollect;

	void spbVersionError()
	{
		ERR_post(Arg::Gds(isc_bad_spb_form) <<
				 Arg::Gds(isc_wrospbver));
	}

} // anonymous namespace


namespace {
inline constexpr serv_entry services[] =
{
	{ isc_action_svc_backup, "Backup Database", BURP_main },
	{ isc_action_svc_restore, "Restore Database", BURP_main },
	{ isc_action_svc_repair, "Repair Database", ALICE_main },
	{ isc_action_svc_add_user, "Add User", GSEC_main },
	{ isc_action_svc_delete_user, "Delete User", GSEC_main },
	{ isc_action_svc_modify_user, "Modify User", GSEC_main },
	{ isc_action_svc_display_user, "Display User", GSEC_main },
	{ isc_action_svc_properties, "Database Properties", ALICE_main },
	{ isc_action_svc_db_stats, "Database Stats", main_gstat },
	{ isc_action_svc_get_fb_log, "Get Log File", Service::readFbLog },
	{ isc_action_svc_nbak, "Incremental Backup Database", NBACKUP_main },
	{ isc_action_svc_nrest, "Incremental Restore Database", NBACKUP_main },
	{ isc_action_svc_nfix, "Fixup Database after FS Copy", NBACKUP_main },
	{ isc_action_svc_trace_start, "Start Trace Session", TRACE_main },
	{ isc_action_svc_trace_stop, "Stop Trace Session", TRACE_main },
	{ isc_action_svc_trace_suspend, "Suspend Trace Session", TRACE_main },
	{ isc_action_svc_trace_resume, "Resume Trace Session", TRACE_main },
	{ isc_action_svc_trace_list, "List Trace Sessions", TRACE_main },
	{ isc_action_svc_set_mapping, "Set Domain Admins Mapping to RDB$ADMIN", GSEC_main },
	{ isc_action_svc_drop_mapping, "Drop Domain Admins Mapping to RDB$ADMIN", GSEC_main },
	{ isc_action_svc_display_user_adm, "Display User with Admin Info", GSEC_main },
	{ isc_action_svc_validate, "Validate Database", VAL_service},
	{ 0, NULL, NULL }
};

}

Service::Validate::Validate(Service* svc)
	: sharedGuard(globalServicesMutex, FB_FUNCTION)
{
	sharedGuard.enter();

	if (! (svc && svc->locateInAllServices()))
	{
		// Service is null or so old that it's even missing in allServices array
		Arg::Gds(isc_bad_svc_handle).raise();
	}

	// Appears we have correct service object, may use it later to lock mutex
}

Service::SafeMutexLock::SafeMutexLock(Service* svc, const char* f)
	: Validate(svc),
	  existenceMutex(svc->svc_existence),
	  from(f)
{
	sharedGuard.leave();
}

bool Service::SafeMutexLock::lock()
{
	existenceMutex->enter(from);
	return existenceMutex->link;
}

Service::ExistenceGuard::ExistenceGuard(Service* svc, const char* from)
	: SafeMutexLock(svc, from)
{
	if (!lock())
	{
		// could not lock service
		existenceMutex->leave();
		Arg::Gds(isc_bad_svc_handle).raise();
	}
}

Service::ExistenceGuard::~ExistenceGuard()
{
	try
	{
		existenceMutex->leave();
	}
	catch (const Exception&)
	{
		DtorException::devHalt();
	}
}

Service::UnlockGuard::UnlockGuard(Service* svc, const char* from)
	: SafeMutexLock(svc, from), locked(false), doLock(false)
{
	existenceMutex->leave();
	doLock = true;
}

bool Service::UnlockGuard::enter()
{
	if (doLock)
	{
		locked = lock();
		doLock = false;
	}

	return locked;
}

Service::UnlockGuard::~UnlockGuard()
{
	if (!enter())
	{
		// could not lock service
		DtorException::devHalt();
	}
}


void Service::getOptions(ClumpletReader& spb)
{
	svc_spb_version = spb.getBufferTag();

	for (spb.rewind(); !(spb.isEof()); spb.moveNext())
	{
		switch (spb.getClumpTag())
		{
		case isc_spb_user_name:
			spb.getString(svc_username);
			fb_utils::dpbItemUpper(svc_username);
			break;

		case isc_spb_sql_role_name:
			spb.getString(svc_sql_role);
			break;

		case isc_spb_auth_block:
			svc_auth_block.clear();
			svc_auth_block.add(spb.getBytes(), spb.getClumpLength());
			break;

		case isc_spb_command_line:
			spb.getString(svc_command_line);
			break;

		case isc_spb_expected_db:
			spb.getPath(svc_expected_db);
			break;

		case isc_spb_address_path:
			spb.getData(svc_address_path);
			{
				ClumpletReader address_stack(ClumpletReader::UnTagged,
					spb.getBytes(), spb.getClumpLength());
				while (!address_stack.isEof())
				{
					if (address_stack.getClumpTag() != isc_dpb_address)
					{
						address_stack.moveNext();
						continue;
					}

					ClumpletReader address(ClumpletReader::UnTagged,
						address_stack.getBytes(), address_stack.getClumpLength());

					while (!address.isEof())
					{
						switch (address.getClumpTag())
						{
						case isc_dpb_addr_protocol:
							address.getString(svc_network_protocol);
							break;
						case isc_dpb_addr_endpoint:
							address.getString(svc_remote_address);
							break;
						default:
							break;
						}

						address.moveNext();
					}

					break;
				}
			}

			break;

		case isc_spb_process_name:
			spb.getString(svc_remote_process);
			break;

		case isc_spb_process_id:
			svc_remote_pid = spb.getInt();
			break;

		case isc_spb_utf8_filename:
			svc_utf8 = true;
			break;
		}
	}
}

void Service::parseSwitches()
{
	svc_parsed_sw = svc_switches;
	svc_parsed_sw.trim();
	argv.clear();
	argv.push("service");	// why not use it for argv[0]

	if (svc_parsed_sw.isEmpty())
	{
		return;
	}

	bool inStr = false;
	for (FB_SIZE_T i = 0; i < svc_parsed_sw.length(); ++i)
	{
		switch (svc_parsed_sw[i])
		{
		case SVC_TRMNTR:
			svc_parsed_sw.erase(i, 1);
			if (inStr)
			{
				if (i < svc_parsed_sw.length() && svc_parsed_sw[i] != SVC_TRMNTR)
				{
					inStr = false;
					--i;
				}
			}
			else
			{
				inStr = true;
				--i;
			}
			break;

		case ' ':
			if (!inStr)
			{
				svc_parsed_sw[i] = 0;
			}
			break;
		}
	}

	argv.push(svc_parsed_sw.c_str());

	for (const char* p = svc_parsed_sw.begin(); p < svc_parsed_sw.end(); ++p)
	{
		if (!*p)
		{
			argv.push(p + 1);
		}
	}
}

void Service::outputVerbose(const char* text)
{
	if (!usvcDataMode)
	{
		const ULONG len = static_cast<ULONG>(strlen(text));
		enqueue(reinterpret_cast<const UCHAR*>(text), len);
	}
}

void Service::outputError(const char* /*text*/)
{
	fb_assert(false);
}

void Service::outputData(const void* data, FB_SIZE_T len)
{
	fb_assert(usvcDataMode);
	enqueue(reinterpret_cast<const UCHAR*>(data), len);
}

void Service::printf(bool err, const SCHAR* format, ...)
{
	// Errors are returned from services as vectors
	fb_assert(!err);
	if (err || usvcDataMode)
	{
		return;
	}

	// Ensure that service is not detached.
	if (svc_flags & SVC_detached)
	{
		return;
	}

	string buf;
	va_list arglist;
	va_start(arglist, format);
	buf.vprintf(format, arglist);
	va_end(arglist);

	enqueue(reinterpret_cast<const UCHAR*>(buf.begin()), buf.length());
}

bool Service::isService()
{
	return true;
}

void Service::started()
{
	// ExistenceGuard guard(this, FB_FUNCTION);
	// Not needed here - lock is taken by thread waiting for us

	if (!(svc_flags & SVC_evnt_fired))
	{
		svc_flags |= SVC_evnt_fired;
		svcStart.release();
	}
}

void Service::putLine(char tag, const char* val)
{
	const ULONG len = strlen(val) & 0xFFFF;

	UCHAR buf[3];
	buf[0] = tag;
	buf[1] = len;
	buf[2] = len >> 8;
	enqueue(buf, sizeof buf);

	enqueue(reinterpret_cast<const UCHAR*>(val), len);
}

void Service::putSLong(char tag, SLONG val)
{
	UCHAR buf[5];
	buf[0] = tag;
	buf[1] = val;
	buf[2] = val >> 8;
	buf[3] = val >> 16;
	buf[4] = val >> 24;
	enqueue(buf, sizeof buf);
}

void Service::putSInt64(char tag, SINT64 val)
{
	UCHAR buf[9];
	buf[0] = tag;
	buf[1] = val;
	buf[2] = val >> 8;
	buf[3] = val >> 16;
	buf[4] = val >> 24;
	buf[5] = val >> 32;
	buf[6] = val >> 40;
	buf[7] = val >> 48;
	buf[8] = val >> 56;
	enqueue(buf, sizeof buf);
}

void Service::putChar(char tag, char val)
{
	UCHAR buf[2];
	buf[0] = tag;
	buf[1] = val;
	enqueue(buf, sizeof buf);
}

void Service::putBytes(const UCHAR* bytes, FB_SIZE_T len)
{
	enqueue(bytes, len);
}

void Service::setServiceStatus(const ISC_STATUS* status_vector)
{
	if (checkForShutdown() || checkForFailedStart())
	{
		return;
	}

	Arg::StatusVector passed(status_vector);
	MutexLockGuard g(svc_status_mutex, FB_FUNCTION);
	ERR_post_nothrow(passed, &svc_status);
}

void Service::setServiceStatus(const USHORT facility, const USHORT errcode,
	const MsgFormat::SafeArg& args)
{
	if (checkForShutdown() || checkForFailedStart())
	{
		return;
	}

	// Append error codes to the status vector
	Arg::StatusVector status;

	// stuff the error code
	status << Arg::Gds(ENCODE_ISC_MSG(errcode, facility));

	// stuff params
	svc_arg_ptr = svc_arg_conv;
	for (unsigned int loop = 0; loop < args.getCount(); ++loop)
	{
		put_status_arg(status, args.getCell(loop));
	}

	MutexLockGuard g(svc_status_mutex, FB_FUNCTION);
	ERR_post_nothrow(status, &svc_status);
}


void Service::put_status_arg(Arg::StatusVector& status, const MsgFormat::safe_cell& value)
{
	using MsgFormat::safe_cell;

	switch (value.type)
	{
	case safe_cell::at_int64:
	case safe_cell::at_uint64:
		status << Arg::Num(static_cast<SLONG>(value.i_value)); // May truncate number!
		break;
	case safe_cell::at_str:
		status << value.st_value.s_string;
		break;
	case safe_cell::at_char:
		svc_arg_ptr[0] = value.c_value;
		svc_arg_ptr[1] = 0;
		status << svc_arg_ptr;
		svc_arg_ptr += 2;
		break;
	default:
		fb_assert(false);
		break;
	}
}


void Service::hidePasswd(ArgvType&, int)
{
	// no action
}

Service::StatusAccessor Service::getStatusAccessor()
{
	return StatusAccessor(svc_status_mutex, &svc_status, this);
}

void Service::checkService()
{
	// no action
}

unsigned int Service::getAuthBlock(const unsigned char** bytes)
{
	*bytes = svc_auth_block.hasData() ? svc_auth_block.begin() : NULL;
	return svc_auth_block.getCount();
}

void Service::fillDpb(ClumpletWriter& dpb)
{
	dpb.insertString(isc_dpb_config, EMBEDDED_PROVIDERS, fb_strlen(EMBEDDED_PROVIDERS));
	if (svc_address_path.hasData())
	{
		dpb.insertData(isc_dpb_address_path, svc_address_path);
	}
	if (svc_utf8)
	{
		dpb.insertTag(isc_dpb_utf8_filename);
	}
	if (svc_crypt_callback)
	{
		// That's not DPB-related, but anyway should be done before attach/create DB
		ISC_STATUS_ARRAY status;
		if (fb_database_crypt_callback(status, svc_crypt_callback) != 0)
		{
			status_exception::raise(status);
		}
	}
	if (svc_remote_process.hasData())
	{
		dpb.insertString(isc_dpb_process_name, svc_remote_process);
	}
	if (svc_remote_pid)
	{
		dpb.insertInt(isc_dpb_process_id, svc_remote_pid);
	}
}

bool Service::utf8FileNames()
{
	return svc_utf8;
}

Firebird::ICryptKeyCallback* Service::getCryptCallback()
{
	return svc_crypt_callback;
}

void Service::need_admin_privs(Arg::StatusVector& status, const char* message) noexcept
{
	status << Arg::Gds(isc_insufficient_svc_privileges) << Arg::Str(message);
}

bool Service::ck_space_for_numeric(UCHAR*& info, const UCHAR* const end) noexcept
{
	if ((info + 1 + sizeof(ULONG)) > end)
	{
		if (info < end)
			*info++ = isc_info_truncated;
		if (info < end)
			*info++ = isc_info_end;
		return false;
	}
	return true;
}


// The SERVER_CAPABILITIES_FLAG is used to mark architectural
// differences across servers.  This allows applications like server
// manager to disable features as necessary.
namespace
{
	inline ULONG getServerCapabilities()
	{
		ULONG val = REMOTE_HOP_SUPPORT;
#ifdef WIN_NT
		val |= QUOTED_FILENAME_SUPPORT;
#endif // WIN_NT

		Firebird::MasterInterfacePtr master;
		switch (master->serverMode(-1))
		{
		case 1:		// super
			val |= MULTI_CLIENT_SUPPORT;
			break;
		case 0:		// classic
			val |= NO_SERVER_SHUTDOWN_SUPPORT;
			break;
		default:	// none-server mode
			break;
		}

		return val;
	}
}

Service::Service(const TEXT* service_name, USHORT spb_length, const UCHAR* spb_data,
				 Firebird::ICryptKeyCallback* crypt_callback)
	: svc_status(getPool()), svc_parsed_sw(getPool()),
	svc_stdout_head(0), svc_stdout_tail(0), svc_service_run(NULL),
	svc_resp_alloc(getPool()), svc_resp_buf(0), svc_resp_ptr(0), svc_resp_buf_len(0),
	svc_resp_len(0), svc_flags(SVC_finished), svc_user_flag(0), svc_spb_version(0),
	svc_shutdown_server(false), svc_shutdown_request(false),
	svc_shutdown_in_progress(false), svc_timeout(false),
	svc_username(getPool()), svc_sql_role(getPool()), svc_auth_block(getPool()),
	svc_expected_db(getPool()), svc_trusted_role(false), svc_utf8(false),
	svc_switches(getPool()), svc_perm_sw(getPool()), svc_address_path(getPool()),
	svc_command_line(getPool()), svc_parallel_workers(0),
	svc_network_protocol(getPool()), svc_remote_address(getPool()), svc_remote_process(getPool()),
	svc_remote_pid(0), svc_trace_manager(NULL), svc_crypt_callback(crypt_callback),
	svc_existence(FB_NEW_POOL(*getDefaultMemoryPool()) SvcMutex(this)),
	svc_stdin_size_requested(0), svc_stdin_buffer(NULL), svc_stdin_size_preload(0),
	svc_stdin_preload_requested(0), svc_stdin_user_size(0), svc_thread(0)
#ifdef DEV_BUILD
	, svc_debug(false)
#endif
{
	svc_status->init();

	{	// scope
		// Account service block in global array
		MutexLockGuard guard(globalServicesMutex, FB_FUNCTION);
		checkForShutdown();
		allServices->add(this);
	}

	// Since this moment we should remove this service from allServices in case of error thrown
	try
	{
#ifdef DEV_BUILD
		// If the service name begins with a slash, ignore it.
		if (*service_name == '/' || *service_name == '\\')
			service_name++;
		string svcname(service_name);
		if (svcname == "@@@")
			svc_debug = true;
#endif
		// Could be overrided in SPB
		svc_parallel_workers = Config::getParallelWorkers();

		// Process the service parameter block.
		ClumpletReader spb(ClumpletReader::spbList, spb_data, spb_length, spbVersionError);
		dumpAuthBlock("Jrd::Service() ctor", &spb, isc_spb_auth_block);
		getOptions(spb);

#ifdef DEV_BUILD
		if (svc_debug)
		{
			svc_trace_manager = FB_NEW_POOL(*getDefaultMemoryPool()) TraceManager(this);
			svc_user_flag = SVC_user_dba;
			return;
		}
#endif

		// Perhaps checkout the user in the security database.
		USHORT user_flag = 0;

		if (!svc_username.hasData())
		{
			if (svc_auth_block.hasData())
			{
				// remote connection - use svc_auth_block
				PathName dummy;
				RefPtr<const Config> config;
				expandDatabaseName(svc_expected_db, dummy, &config);

				Mapping mapping(Mapping::MAP_THROW_NOT_FOUND, svc_crypt_callback);
				mapping.needAuthBlock(svc_auth_block);

				mapping.setAuthBlock(svc_auth_block);
				mapping.setSqlRole(svc_sql_role);
				mapping.setErrorMessagesContextName("services manager");
				mapping.setSecurityDbAlias(config->getSecurityDatabase(), nullptr);

				string trusted_role;
				mapping.mapUser(svc_username, trusted_role);
				trusted_role.upper();
				svc_trusted_role = trusted_role == ADMIN_ROLE;
			}
			else
			{
				// we have embedded service connection, check OS auth
				if (ISC_get_user(&svc_username, NULL, NULL))
					svc_username = DBA_USER_NAME;
			}
		}

		if (!svc_username.hasData())
		{
			// user name and password are required while
			// attaching to the services manager
			status_exception::raise(Arg::Gds(isc_service_att_err) << Arg::Gds(isc_svcnouser));
		}

		if (svc_username.length() > USERNAME_LENGTH)
		{
			status_exception::raise(Arg::Gds(isc_long_login) <<
				Arg::Num(svc_username.length()) << Arg::Num(USERNAME_LENGTH));
		}

		// Check that the validated user has the authority to access this service
		if (svc_username != DBA_USER_NAME && !svc_trusted_role)
			user_flag = SVC_user_any;
		else
			user_flag = SVC_user_dba | SVC_user_any;

		// move service switches in
		string switches = svc_command_line;
		svc_flags |= switches.hasData() ? SVC_cmd_line : 0;
		svc_perm_sw = switches;
		svc_user_flag = user_flag;

		svc_trace_manager = FB_NEW_POOL(*getDefaultMemoryPool()) TraceManager(this);
	}	// try
	catch (const Firebird::Exception& ex)
	{
		TraceManager* trace_manager = NULL;
		FbLocalStatus status_vector;

		try
		{
			// Use created trace manager if it's possible
			const bool hasTrace = svc_trace_manager != NULL;
			if (hasTrace)
				trace_manager = svc_trace_manager;
			else
				trace_manager = FB_NEW_POOL(*getDefaultMemoryPool()) TraceManager(this);

			if (trace_manager->needs(ITraceFactory::TRACE_EVENT_SERVICE_ATTACH))
			{
				ex.stuffException(&status_vector);
				const ISC_STATUS exc = status_vector[1];
				const bool no_priv = (exc == isc_login || exc == isc_no_priv);

				TraceServiceImpl service(this);
				trace_manager->event_service_attach(&service,
					no_priv ? ITracePlugin::RESULT_UNAUTHORIZED : ITracePlugin::RESULT_FAILED);
			}

			if (!hasTrace)
				delete trace_manager;
		}
		catch (const Firebird::Exception&)
		{
		}

		removeFromAllServices();
		throw;
	}

	if (svc_trace_manager->needs(ITraceFactory::TRACE_EVENT_SERVICE_ATTACH))
	{
		TraceServiceImpl service(this);
		svc_trace_manager->event_service_attach(&service, ITracePlugin::RESULT_SUCCESS);
	}
}


static THREAD_ENTRY_DECLARE svcShutdownThread(THREAD_ENTRY_PARAM)
{
	if (fb_shutdown(10 * 1000 /* 10 seconds */, fb_shutrsn_services) == FB_SUCCESS)
	{
		InstanceControl::registerShutdown(0);
		exit(0);
	}

	return 0;
}


void Service::detach()
{
	ExistenceGuard guard(this, FB_FUNCTION);

	if (svc_flags & SVC_detached)
	{
		// Service was already detached
		Arg::Gds(isc_bad_svc_handle).raise();
	}

	// save it cause after call to finish() we can't access class members any more
	const bool localDoShutdown = svc_shutdown_server;

	if (svc_trace_manager->needs(ITraceFactory::TRACE_EVENT_SERVICE_DETACH))
	{
		TraceServiceImpl service(this);
		svc_trace_manager->event_service_detach(&service, ITracePlugin::RESULT_SUCCESS);
	}

	// Mark service as detached.
	finish(SVC_detached);

	if (localDoShutdown)
	{
		// run in separate thread to avoid blocking in remote
		Thread::start(svcShutdownThread, 0, THREAD_medium);
	}
}


Service::~Service()
{
	removeFromAllServices();

	delete svc_trace_manager;
	svc_trace_manager = NULL;

	fb_assert(svc_existence->locked());
	svc_existence->link = NULL;
}


void Service::removeFromAllServices()
{
	MutexLockGuard guard(globalServicesMutex, FB_FUNCTION);

	FB_SIZE_T pos;
	if (locateInAllServices(&pos))
	{
		allServices->remove(pos);
		return;
	}

	fb_assert(false);
}


bool Service::locateInAllServices(FB_SIZE_T* posPtr)
{
	MutexLockGuard guard(globalServicesMutex, FB_FUNCTION);
	AllServices& all(allServices);

	for (FB_SIZE_T pos = 0; pos < all.getCount(); ++pos)
	{
		if (all[pos] == this)
		{
			if (posPtr)
			{
				*posPtr = pos;
			}
			return true;
		}
	}

	return false;
}


ULONG Service::totalCount()
{
	MutexLockGuard guard(globalServicesMutex, FB_FUNCTION);
	AllServices& all(allServices);
	ULONG cnt = 0;

	// don't count already detached services
	for (FB_SIZE_T i = 0; i < all.getCount(); i++)
	{
		if (!(all[i]->svc_flags & SVC_detached))
			cnt++;
	}

	return cnt;
}


bool Service::checkForShutdown()
{
	if (svcShutdown || svc_shutdown_request)
	{
		if (svc_shutdown_in_progress)
		{
			// Here we avoid multiple exceptions thrown
			return true;
		}

		svc_shutdown_in_progress = true;
		status_exception::raise(Arg::Gds(isc_att_shutdown));
	}

	return false;
}


bool Service::checkForFailedStart() noexcept
{
	if ((svc_flags & SVC_evnt_fired) == 0)
	{
		// Service has not been started but we have got an error
		svc_flags |= SVC_failed_start;
	}
	else if ((svc_flags & SVC_failed_start) != 0)
	{
		// Service has started with an error but we are trying to write one more error
		return true;
	}

	return false;
}

void Service::cancel(thread_db* /*tdbb*/)
{
	svc_shutdown_request = true;

	// signal once
	if (!(svc_flags & SVC_finished))
		svc_detach_sem.release();
	if (svc_stdin_size_requested)
		svc_stdin_semaphore.release();

	svc_sem_full.release();
}


void Service::shutdownServices()
{
	svcShutdown = true;

	MutexLockGuard guard(globalServicesMutex, FB_FUNCTION);
	AllServices& all(allServices);

	unsigned int pos;

	// signal once for every still running service
	for (pos = 0; pos < all.getCount(); pos++)
	{
		if (!(all[pos]->svc_flags & SVC_finished))
			all[pos]->svc_detach_sem.release();
		if (all[pos]->svc_stdin_size_requested)
			all[pos]->svc_stdin_semaphore.release();
	}

	for (pos = 0; pos < all.getCount(); )
	{
		if (!(all[pos]->svc_flags & SVC_finished))
		{
			globalServicesMutex->leave();
			Thread::sleep(1);
			globalServicesMutex->enter(FB_FUNCTION);
			pos = 0;
			continue;
		}

		++pos;
	}

	threadCollect->join();
}


ISC_STATUS Service::query2(thread_db* /*tdbb*/,
						   USHORT send_item_length,
						   const UCHAR* send_items,
						   USHORT recv_item_length,
						   const UCHAR* recv_items,
						   USHORT buffer_length,
						   UCHAR* info)
{
	ExistenceGuard guard(this, FB_FUNCTION);

	if (svc_flags & SVC_detached)
	{
		// Service was already detached
		Arg::Gds(isc_bad_svc_handle).raise();
	}

	UCHAR item;
	UCHAR buffer[MAXPATHLEN];
	USHORT l, length, get_flags;
	UCHAR* stdin_request_notification = NULL;

	// Setup the status vector
	Arg::StatusVector status;

	ULONG requestFromPut = 0;

	try
	{
	// Process the send portion of the query first.
	USHORT timeout = 0;
	const UCHAR* items = send_items;
	const UCHAR* const end_items = items + send_item_length;

	while (items < end_items && *items != isc_info_end)
	{
		switch ((item = *items++))
		{
		case isc_info_end:
			break;

		default:
			if (items + 2 <= end_items)
			{
				l = (USHORT) gds__vax_integer(items, 2);
				items += 2;
				if (items + l <= end_items)
				{
					switch (item)
					{
					case isc_info_svc_line:
						requestFromPut = put(items, l);
						break;
					case isc_info_svc_message:
						//put(items - 3, l + 3);
						break;
					case isc_info_svc_timeout:
						timeout = (USHORT) gds__vax_integer(items, l);
						break;
					case isc_info_svc_version:
						break;
					}
				}
				items += l;
			}
			else
				items += 2;
			break;
		}
	}

	// Process the receive portion of the query now.
	const UCHAR* const end = info + buffer_length;
	items = recv_items;
	const UCHAR* const end_items2 = items + recv_item_length;

	UCHAR* start_info;

	if (*items == isc_info_length)
	{
		start_info = info;
		items++;
	}
	else {
		start_info = NULL;
	}

	while (items < end_items2 && *items != isc_info_end && info < end)
	{
		// if we attached to the "anonymous" service we allow only following queries:

		// isc_info_svc_get_config     - called from within remote/ibconfig.cpp
		// isc_info_svc_dump_pool_info - called from within utilities/print_pool.cpp
		if (svc_user_flag == SVC_user_none)
		{
			switch (*items)
			{
			case isc_info_svc_get_config:
			case isc_info_svc_dump_pool_info:
				break;
			default:
				status_exception::raise(Arg::Gds(isc_bad_spb_form) <<
										Arg::Gds(isc_info_access));
				break;
			}
		}

		switch ((item = *items++))
		{
		case isc_info_end:
			break;

		case isc_info_svc_svr_db_info:
			if (svc_user_flag & SVC_user_dba)
			{
				PathNameList databases(*getDefaultMemoryPool());
				ULONG num_dbs, num_att, num_svc;
				JRD_enum_attachments(&databases, num_att, num_dbs, num_svc);
				fb_assert(num_dbs == databases.getCount());

				*info++ = item;
				// Move the number of attachments into the info buffer
				if (!ck_space_for_numeric(info, end))
					return 0;
				*info++ = isc_spb_num_att;
				ADD_SPB_NUMERIC(info, num_att);

				// Move the number of databases in use into the info buffer
				if (!ck_space_for_numeric(info, end))
					return 0;
				*info++ = isc_spb_num_db;
				ADD_SPB_NUMERIC(info, num_dbs);

				// Move db names into the info buffer
				for (FB_SIZE_T i = 0; i < databases.getCount(); i++)
				{
					if (!(info = INF_put_item(isc_spb_dbname,
											  (USHORT) databases[i].length(),
											  databases[i].c_str(),
											  info, end)))
					{
						return 0;
					}
				}

				if (info < end)
					*info++ = isc_info_flag_end;
			}
			else
				need_admin_privs(status, "isc_info_svc_svr_db_info");

			break;

		case isc_info_svc_svr_online:
			*info++ = item;
			if (svc_user_flag & SVC_user_dba)
			{
				svc_shutdown_server = false;
			}
			else
				need_admin_privs(status, "isc_info_svc_svr_online");
			break;

		case isc_info_svc_svr_offline:
			*info++ = item;
			if (svc_user_flag & SVC_user_dba)
			{
				svc_shutdown_server = true;
			}
			else
				need_admin_privs(status, "isc_info_svc_svr_offline");
			break;

		// The following 3 service commands (or items) stuff the response
		// buffer 'info' with values of environment variable FIREBIRD,
		// FIREBIRD_LOCK or FIREBIRD_MSG. If the environment variable
		// is not set then default value is returned.
		case isc_info_svc_get_env:
		case isc_info_svc_get_env_lock:
		case isc_info_svc_get_env_msg:
			if (svc_user_flag & SVC_user_dba)
			{
				char* const auxBuf = reinterpret_cast<char*>(buffer);
				switch (item)
				{
				case isc_info_svc_get_env:
					gds__prefix(auxBuf, "");
					break;
				case isc_info_svc_get_env_lock:
					iscPrefixLock(auxBuf, "", false);
					break;
				case isc_info_svc_get_env_msg:
					gds__prefix_msg(auxBuf, "");
				}

				// Note: it is safe to use strlen to get a length of "buffer"
				// because gds_prefix[_lock|_msg] returns a zero-terminated
				// string.
				info = INF_put_item(item, static_cast<USHORT>(strlen(auxBuf)), buffer, info, end);
				if (!info)
				{
					return 0;
				}
			}
			else
			{
				need_admin_privs(status, "isc_info_svc_get_env");
			}
			break;

		case isc_info_svc_dump_pool_info:
			{
				char fname[MAXPATHLEN];
				size_t length2 = gds__vax_integer(items, sizeof(USHORT));
				if (length2 >= sizeof(fname))
					length2 = sizeof(fname) - 1; // truncation
				items += sizeof(USHORT);
				strncpy(fname, (const char*) items, length2);
				fname[length2] = 0;
				break;
			}

		case isc_info_svc_get_config:
			// TODO: iterate through all integer-based config values
			//		 and return them to the client
			break;
		/*
		case isc_info_svc_default_config:
			*info++ = item;
			if (svc_user_flag & SVC_user_dba) {
				// TODO: reset the config values to defaults
			}
			else
				need_admin_privs(status, "isc_info_svc_default_config");
			break;

		case isc_info_svc_set_config:
			*info++ = item;
			if (svc_user_flag & SVC_user_dba) {
				// TODO: set the config values
			}
			else {
				need_admin_privs(status, "isc_info_svc_set_config");
			}
			break;
		*/
		case isc_info_svc_version:
			// The version of the service manager
			if (!ck_space_for_numeric(info, end))
				return 0;
			*info++ = item;
			ADD_SPB_NUMERIC(info, SERVICE_VERSION);
			break;

		case isc_info_svc_capabilities:
			// bitmask defining any specific architectural differences
			if (!ck_space_for_numeric(info, end))
				return 0;
			*info++ = item;
			ADD_SPB_NUMERIC(info, getServerCapabilities());
			break;

		case isc_info_svc_running:
			// Returns the (inversed) status of the flag SVC_finished
			if (!ck_space_for_numeric(info, end))
				return 0;
			*info++ = item;

			if (svc_flags & SVC_finished)
				ADD_SPB_NUMERIC(info, FALSE)
			else
				ADD_SPB_NUMERIC(info, TRUE)

			break;

		case isc_info_svc_server_version:
			// The version of the server engine
			{ // scope
				info = INF_put_item(item, static_cast<USHORT>(strlen(FB_VERSION)), FB_VERSION, info, end);
				if (!info) {
					return 0;
				}
			} // scope
			break;

		case isc_info_svc_implementation:
			// The server implementation - e.g. Firebird/sun4
			{ // scope
				const string buf2 = DbImplementation::current.implementation();
				if (!(info = INF_put_item(item, buf2.length(), buf2.c_str(), info, end)))
					return 0;
			} // scope
			break;

		case isc_info_svc_stdin:
			// Check - is stdin data required for server
			if (!ck_space_for_numeric(info, end))
			{
				return 0;
			}
			*info++ = item;
			if (!stdin_request_notification)
			{
				stdin_request_notification = info;
			}
			ADD_SPB_NUMERIC(info, 0);

			break;

		case isc_info_svc_user_dbpath:
			if (svc_user_flag & SVC_user_dba)
			{
				// The path to the user security database
				PathName secDb;
				RefPtr<const Config> config;
				expandDatabaseName(svc_expected_db, secDb, &config);
				expandDatabaseName(config->getSecurityDatabase(), secDb, nullptr);

				if (!(info = INF_put_item(item, secDb.length(), secDb.c_str(), info, end)))
					return 0;
			}
			else
				need_admin_privs(status, "isc_info_svc_user_dbpath");
			break;

		case isc_info_svc_response:
			svc_resp_len = 0;
			if (info + 4 >= end)
			{
				*info++ = isc_info_truncated;
				break;
			}
			//put(&item, 1);
			get(&item, 1, GET_BINARY, 0, &length);
			get(buffer, 2, GET_BINARY, 0, &length);
			l = (USHORT) gds__vax_integer(buffer, 2);
			length = MIN(end - (info + 5), l);
			get(info + 3, length, GET_BINARY, 0, &length);
			info = INF_put_item(item, length, info + 3, info, end);
			if (length != l)
			{
				*info++ = isc_info_truncated;
				l -= length;
				if (l > svc_resp_buf_len)
				{
					try {
						svc_resp_buf = svc_resp_alloc.getBuffer(l);
					}
					catch (const BadAlloc&)
					{
						// NOMEM:
						DEV_REPORT("SVC_query: out of memory");
						// NOMEM: not really handled well
						l = 0;	// set the length to zero
					}
					svc_resp_buf_len = l;
				}
				get(svc_resp_buf, l, GET_BINARY, 0, &length);
				svc_resp_ptr = svc_resp_buf;
				svc_resp_len = l;
			}
			break;

		case isc_info_svc_response_more:
			if ( (l = length = svc_resp_len) )
				length = MIN(end - (info + 5), l);
			if (!(info = INF_put_item(item, length, svc_resp_ptr, info, end)))
				return 0;
			svc_resp_ptr += length;
			svc_resp_len -= length;
			if (length != l)
				*info++ = isc_info_truncated;
			break;

		case isc_info_svc_total_length:
			//put(&item, 1);
			get(&item, 1, GET_BINARY, 0, &length);
			get(buffer, 2, GET_BINARY, 0, &length);
			l = (USHORT) gds__vax_integer(buffer, 2);
			get(buffer, l, GET_BINARY, 0, &length);
			if (!(info = INF_put_item(item, length, buffer, info, end)))
				return 0;
			break;

		case isc_info_svc_line:
		case isc_info_svc_to_eof:
		case isc_info_svc_limbo_trans:
		case isc_info_svc_get_users:
			if (info + 4 >= end)
			{
				*info++ = isc_info_truncated;
				break;
			}

			switch (item)
			{
			case isc_info_svc_line:
				get_flags = GET_LINE;
				break;
			case isc_info_svc_to_eof:
				get_flags = GET_EOF;
				break;
			default:
				get_flags = GET_BINARY;
				break;
			}

			if (requestFromPut)
			{
				get_flags |= GET_ONCE;
			}

			get(info + 3, end - (info + 5), get_flags, timeout, &length);

			// If the read timed out, return the data, if any, & a timeout
			// item.  If the input buffer was not large enough
			// to store a read to eof, return the data that was read along
			// with an indication that more is available.

			if (!(info = INF_put_item(item, length, info + 3, info, end))) {
				return 0;
			}

			if (svc_timeout)
			{
				*info++ = isc_info_svc_timeout;
			}
			else //if (!svc_stdin_size_requested)
			{
				if (!length && !(svc_flags & SVC_finished))
					*info++ = isc_info_data_not_ready;
				else if (item == isc_info_svc_to_eof && !(svc_flags & SVC_finished))
					*info++ = isc_info_truncated;
			}
			break;

		default:
			status << Arg::Gds(isc_wish_list);
			break;
		}

		if (svc_user_flag == SVC_user_none)
			break;
	}

	if (info < end)
		*info++ = isc_info_end;

	if (start_info && (end - info >= 7))
	{
		const SLONG number = info - start_info;
		fb_assert(number > 0);
		memmove(start_info + 7, start_info, number);
		if (stdin_request_notification)
			stdin_request_notification += 7;
		const USHORT length2 = INF_convert(number, buffer);
		fb_assert(length2 == 4); // We only accept SLONG
		INF_put_item(isc_info_length, length2, buffer, start_info, end, true);
	}

	if (svc_trace_manager->needs(ITraceFactory::TRACE_EVENT_SERVICE_QUERY))
	{
		TraceServiceImpl service(this);
		svc_trace_manager->event_service_query(&service, send_item_length, send_items,
			recv_item_length, recv_items, ITracePlugin::RESULT_SUCCESS);
	}

	if (!requestFromPut)
	{
		requestFromPut = svc_stdin_size_requested;
	}

	if (requestFromPut)
	{
		if (stdin_request_notification)
		{
			ADD_SPB_NUMERIC(stdin_request_notification, requestFromPut);
		}
		else
		{
			(Arg::Gds(isc_svc_no_stdin)).raise();
		}
	}

	if (status.hasData())
	{
		status.raise();
	}

	}	// try
	catch (const Firebird::Exception& ex)
	{
		if (svc_trace_manager->needs(ITraceFactory::TRACE_EVENT_SERVICE_QUERY))
		{
			FbLocalStatus status_vector;
			ex.stuffException(&status_vector);

			const ISC_STATUS exc = status_vector[1];
			const bool no_priv = (exc == isc_login || exc == isc_no_priv ||
							exc == isc_insufficient_svc_privileges);

			TraceServiceImpl service(this);
			svc_trace_manager->event_service_query(&service, send_item_length, send_items,
				recv_item_length, recv_items,
				(no_priv ? ITracePlugin::RESULT_UNAUTHORIZED : ITracePlugin::RESULT_FAILED));
		}
		throw;
	}

	// no need locking svc_status_mutex - check single element of status vector
	return svc_status[1];
}


void Service::query(USHORT			send_item_length,
					const UCHAR*	send_items,
					USHORT			recv_item_length,
					const UCHAR*	recv_items,
					USHORT			buffer_length,
					UCHAR*			info)
{
	ExistenceGuard guard(this, FB_FUNCTION);

	if (svc_flags & SVC_detached)
	{
		// Service was already detached
		Arg::Gds(isc_bad_svc_handle).raise();
	}

	UCHAR item, *p;
	UCHAR buffer[256];
	USHORT l, length, get_flags;

	try
	{
	// Process the send portion of the query first.
	USHORT timeout = 0;
	const UCHAR* items = send_items;
	const UCHAR* const end_items = items + send_item_length;
	while (items < end_items && *items != isc_info_end)
	{
		switch ((item = *items++))
		{
		case isc_info_end:
			break;

		default:
			if (items + 2 <= end_items)
			{
				l = (USHORT) gds__vax_integer(items, 2);
				items += 2;
				if (items + l <= end_items)
				{
					switch (item)
					{
					case isc_info_svc_line:
						//put(items, l);
						break;
					case isc_info_svc_message:
						//put(items - 3, l + 3);
						break;
					case isc_info_svc_timeout:
						timeout = (USHORT) gds__vax_integer(items, l);
						break;
					case isc_info_svc_version:
						break;
					}
				}
				items += l;
			}
			else
				items += 2;
			break;
		}
	}

	// Process the receive portion of the query now.
	const UCHAR* const end = info + buffer_length;

	items = recv_items;
	const UCHAR* const end_items2 = items + recv_item_length;
	while (items < end_items2 && *items != isc_info_end)
	{
		switch ((item = *items++))
		{
		case isc_info_end:
			break;

		case isc_info_svc_svr_db_info:
			if (svc_user_flag & SVC_user_dba)
			{
				PathNameList databases(*getDefaultMemoryPool());
				ULONG num_dbs, num_att, num_svc;
				JRD_enum_attachments(&databases, num_att, num_dbs, num_svc);
				fb_assert(num_dbs == databases.getCount());

				length = INF_convert(num_att, buffer);
				if (!(info = INF_put_item(item, length, buffer, info, end)))
					return;

				length = INF_convert(num_dbs, buffer);
				if (!(info = INF_put_item(item, length, buffer, info, end)))
					return;
			}
			// Can not return error for service v.1 => simply ignore request
			// else
			//	need_admin_privs(status, "isc_info_svc_svr_db_info");
			break;

		case isc_info_svc_svr_online:
			*info++ = item;
			if (svc_user_flag & SVC_user_dba)
			{
				svc_shutdown_server = false;
				*info++ = 0;	// Success
			}
			else
				*info++ = 2;	// No user authority
			break;

		case isc_info_svc_svr_offline:
			*info++ = item;
			if (svc_user_flag & SVC_user_dba)
			{
				svc_shutdown_server = true;
				*info++ = 0;	// Success
			}
			else
				*info++ = 2;	// No user authority
			break;

		// The following 3 service commands (or items) stuff the response
		// buffer 'info' with values of environment variable FIREBIRD,
		// FIREBIRD_LOCK or FIREBIRD_MSG. If the environment variable
		// is not set then default value is returned.
		case isc_info_svc_get_env:
		case isc_info_svc_get_env_lock:
		case isc_info_svc_get_env_msg:
			if (svc_user_flag & SVC_user_dba)
			{
				TEXT pathBuffer[MAXPATHLEN];
				switch (item)
				{
				case isc_info_svc_get_env:
					gds__prefix(pathBuffer, "");
					break;
				case isc_info_svc_get_env_lock:
					iscPrefixLock(pathBuffer, "", false);
					break;
				case isc_info_svc_get_env_msg:
					gds__prefix_msg(pathBuffer, "");
				}

				// Note: it is safe to use strlen to get a length of "buffer"
				// because gds_prefix[_lock|_msg] return a zero-terminated
				// string.
				if (!(info = INF_put_item(item, fb_strlen(pathBuffer), pathBuffer, info, end)))
					return;
			}
			// Can not return error for service v.1 => simply ignore request
			// else
			//	need_admin_privs(status, "isc_info_svc_get_env");
			break;

		case isc_info_svc_dump_pool_info:
			{
				char fname[MAXPATHLEN];
				size_t length2 = gds__vax_integer(items, sizeof(USHORT));
				if (length2 >= sizeof(fname))
					length2 = sizeof(fname) - 1; // truncation
				items += sizeof(USHORT);
				memcpy(fname, items, length2);
				fname[length2] = 0;
				break;
			}
		/*
		case isc_info_svc_get_config:
			// TODO: iterate through all integer-based config values
			//		 and return them to the client
			break;

		case isc_info_svc_default_config:
			*info++ = item;
			if (svc_user_flag & SVC_user_dba) {
				// TODO: reset the config values to defaults
			}
			*
			 * Can not return error for service v.1 => simply ignore request
			else
				need_admin_privs(status, "isc_info_svc_default_config:");
			 *
			break;

		case isc_info_svc_set_config:
			*info++ = item;
			if (svc_user_flag & SVC_user_dba) {
				// TODO: set the config values
			}
			*
			 * Can not return error for service v.1 => simply ignore request
			else
				need_admin_privs(status, "isc_info_svc_set_config:");
			 *
			break;
		*/
		case isc_info_svc_version:
			// The version of the service manager

			length = INF_convert(SERVICE_VERSION, buffer);
			info = INF_put_item(item, length, buffer, info, end);
			if (!info)
				return;
			break;

		case isc_info_svc_capabilities:
			// bitmask defining any specific architectural differences

			length = INF_convert(getServerCapabilities(), buffer);
			info = INF_put_item(item, length, buffer, info, end);
			if (!info)
				return;
			break;

		case isc_info_svc_server_version:
			{
				// The version of the server engine

				p = buffer;
				*p++ = 1;			// Count
				*p++ = sizeof(FB_VERSION) - 1;
				for (const TEXT* gvp = FB_VERSION; *gvp; p++, gvp++)
					*p = *gvp;
				if (!(info = INF_put_item(item, p - buffer, buffer, info, end)))
				{
					return;
				}
				break;
			}

		case isc_info_svc_implementation:
			// The server implementation - e.g. Firebird/sun4

			p = buffer;
			*p++ = 1;			// Count
			*p++ = DbImplementation::current.backwardCompatibleImplementation();
			if (!(info = INF_put_item(item, p - buffer, buffer, info, end)))
			{
				return;
			}
			break;


		case isc_info_svc_user_dbpath:
            if (svc_user_flag & SVC_user_dba)
            {
				// The path to the user security database
				PathName secDb;
				RefPtr<const Config> config;
				expandDatabaseName(svc_expected_db, secDb, &config);
				expandDatabaseName(config->getSecurityDatabase(), secDb, nullptr);

				if (!(info = INF_put_item(item, secDb.length(), secDb.c_str(), info, end)))
					return;
			}
			// Can not return error for service v.1 => simply ignore request
			// else
			//	need_admin_privs(status, "isc_info_svc_user_dbpath");
			break;

		case isc_info_svc_response:
			svc_resp_len = 0;
			if (info + 4 > end)
			{
				*info++ = isc_info_truncated;
				break;
			}
			//put(&item, 1);
			get(&item, 1, GET_BINARY, 0, &length);
			get(buffer, 2, GET_BINARY, 0, &length);
			l = (USHORT) gds__vax_integer(buffer, 2);
			length = MIN(end - (info + 4), l);
			get(info + 3, length, GET_BINARY, 0, &length);
			info = INF_put_item(item, length, info + 3, info, end);
			if (length != l)
			{
				*info++ = isc_info_truncated;
				l -= length;
				if (l > svc_resp_buf_len)
				{
					try {
						svc_resp_buf = svc_resp_alloc.getBuffer(l);
					}
					catch (const BadAlloc&)
					{
						// NOMEM:
						DEV_REPORT("SVC_query: out of memory");
						// NOMEM: not really handled well
						l = 0;	// set the length to zero
					}
					svc_resp_buf_len = l;
				}
				get(svc_resp_buf, l, GET_BINARY, 0, &length);
				svc_resp_ptr = svc_resp_buf;
				svc_resp_len = l;
			}
			break;

		case isc_info_svc_response_more:
			if ( (l = length = svc_resp_len) )
				length = MIN(end - (info + 4), l);
			if (!(info = INF_put_item(item, length, svc_resp_ptr, info, end)))
				return;
			svc_resp_ptr += length;
			svc_resp_len -= length;
			if (length != l)
				*info++ = isc_info_truncated;
			break;

		case isc_info_svc_total_length:
			//put(&item, 1);
			get(&item, 1, GET_BINARY, 0, &length);
			get(buffer, 2, GET_BINARY, 0, &length);
			l = (USHORT) gds__vax_integer(buffer, 2);
			get(buffer, l, GET_BINARY, 0, &length);
			if (!(info = INF_put_item(item, length, buffer, info, end)))
				return;
			break;

		case isc_info_svc_line:
		case isc_info_svc_to_eof:
			if (info + 4 > end)
			{
				*info++ = isc_info_truncated;
				break;
			}
			get_flags = (item == isc_info_svc_line) ? GET_LINE : GET_EOF;
			get(info + 3, end - (info + 4), get_flags, timeout, &length);

			// If the read timed out, return the data, if any, & a timeout
			// item.  If the input buffer was not large enough
			// to store a read to eof, return the data that was read along
			// with an indication that more is available.

			info = INF_put_item(item, length, info + 3, info, end);

			if (svc_timeout)
				*info++ = isc_info_svc_timeout;
			else
			{
				if (!length && !(svc_flags & SVC_finished))
					*info++ = isc_info_data_not_ready;
				else
				{
					if (item == isc_info_svc_to_eof && !(svc_flags & SVC_finished))
						*info++ = isc_info_truncated;
				}
			}
			break;
		}
	}

	if (info < end)
	{
		*info = isc_info_end;
	}
	}	// try
	catch (const Firebird::Exception& ex)
	{
		if (svc_trace_manager->needs(ITraceFactory::TRACE_EVENT_SERVICE_QUERY))
		{
			FbLocalStatus status_vector;
			ex.stuffException(&status_vector);

			const ISC_STATUS exc = status_vector[1];
			const bool no_priv = (exc == isc_login || exc == isc_no_priv);

			// Report to Trace API that query failed
			TraceServiceImpl service(this);
			svc_trace_manager->event_service_query(&service, send_item_length, send_items,
				recv_item_length, recv_items,
				(no_priv ? ITracePlugin::RESULT_UNAUTHORIZED : ITracePlugin::RESULT_FAILED));
		}
		throw;
	}

	if (svc_flags & SVC_finished)
	{
		if ((svc_flags & SVC_detached) &&
			svc_trace_manager->needs(ITraceFactory::TRACE_EVENT_SERVICE_QUERY))
		{
			TraceServiceImpl service(this);
			svc_trace_manager->event_service_query(&service, send_item_length, send_items,
				recv_item_length, recv_items, ITracePlugin::RESULT_SUCCESS);
		}
	}
}


THREAD_ENTRY_DECLARE Service::run(THREAD_ENTRY_PARAM arg)
{
	int exit_code = -1;
	try
	{
		Service* svc = (Service*)arg;
		RefPtr<SvcMutex> ref(svc->svc_existence);
		exit_code = svc->svc_service_run->serv_thd(svc);

		Thread::Handle thrHandle = svc->svc_thread;
		svc->started();
		svc->unblockQueryGet();
		svc->finish(SVC_finished);

		threadCollect->ending(thrHandle);
	}
	catch (const Exception& ex)
	{
		// Not much we can do here
		exit_code = -1;
		iscLogException("Exception in Service::run():", ex);
	}

	return (THREAD_ENTRY_RETURN)(IPTR) exit_code;
}


void Service::start(USHORT spb_length, const UCHAR* spb_data)
{
	ExistenceGuard guard(this, FB_FUNCTION);

	if (svc_flags & SVC_detached)
	{
		// Service was already detached
		Arg::Gds(isc_bad_svc_handle).raise();
	}

	try
	{
	if (!svcShutdown)
		svc_shutdown_request = svc_shutdown_in_progress = false;

	ClumpletReader spb(ClumpletReader::SpbStart, spb_data, spb_length);

	// The name of the service is the first element of the buffer
	if (spb.isEof())
	{
		status_exception::raise(Arg::Gds(isc_service_att_err) << Arg::Gds(isc_spb_no_id));
	}
	const UCHAR svc_id = spb.getClumpTag();
	const serv_entry* serv;
	for (serv = services; serv->serv_action; serv++)
	{
		if (serv->serv_action == svc_id)
			break;
	}

	if (!serv->serv_name)
		status_exception::raise(Arg::Gds(isc_service_att_err) << Arg::Gds(isc_service_not_supported));

	svc_service_run = serv;

	// currently we do not use "anonymous" service for any purposes but isc_service_query()
	if (svc_user_flag == SVC_user_none)
	{
		status_exception::raise(Arg::Gds(isc_bad_spb_form) <<
								Arg::Gds(isc_svc_start_failed));
	}

	if (!(svc_flags & SVC_finished))
		status_exception::raise(Arg::Gds(isc_svc_in_use) << Arg::Str(serv->serv_name));

	// Another service may have been started with this service block.
	// If so, we must reset the service flags.
	svc_switches.erase();
	/***
	if (!(svc_flags & SVC_detached))
	{
		svc_flags = 0;
	}
	***/

	if (!svc_perm_sw.hasData())
	{
		// If svc_perm_sw is not used -- call a command-line parsing utility
		conv_switches(spb, svc_switches);
	}
	else
	{
		// Command line options (isc_spb_options) is used.
		// Currently the only case in which it might happen is -- gbak utility
		// is called with a "-server" switch.
		svc_switches = svc_perm_sw;
	}

	// Only need to add username and password information to those calls which need
	// to make a database connection

	const bool flNeedUser =
		svc_id == isc_action_svc_backup ||
		svc_id == isc_action_svc_restore ||
		svc_id == isc_action_svc_nbak ||
		svc_id == isc_action_svc_nrest ||
		svc_id == isc_action_svc_nfix ||
		svc_id == isc_action_svc_repair ||
		svc_id == isc_action_svc_db_stats ||
		svc_id == isc_action_svc_properties ||
		svc_id == isc_action_svc_trace_start ||
		svc_id == isc_action_svc_trace_stop ||
		svc_id == isc_action_svc_trace_suspend ||
		svc_id == isc_action_svc_trace_resume ||
		svc_id == isc_action_svc_trace_list ||
		svc_id == isc_action_svc_add_user ||
		svc_id == isc_action_svc_delete_user ||
		svc_id == isc_action_svc_modify_user ||
		svc_id == isc_action_svc_display_user ||
		svc_id == isc_action_svc_display_user_adm ||
		svc_id == isc_action_svc_set_mapping ||
		svc_id == isc_action_svc_drop_mapping ||
		svc_id == isc_action_svc_validate;

	if (flNeedUser)
	{
		// add the username to the end of svc_switches if needed
		if (svc_switches.hasData() && !svc_auth_block.hasData())
		{
			if (svc_username.hasData())
			{
				string auth = "-user ";
				auth += svc_username;
				auth += ' ';
				svc_switches = auth + svc_switches;
			}
		}

		if (svc_sql_role.hasData())
		{
			string auth = "-role ";
			auth += svc_sql_role;
			auth += ' ';
			svc_switches = auth + svc_switches;
		}
	}

#ifdef DEV_BUILD
	if (svc_debug)
	{
		::fprintf(stderr, "%s %s\n", svc_service_run->serv_name, svc_switches.c_str());
		return;
	}
#endif

	// All services except for get_ib_log require switches
	spb.rewind();
	if ((!svc_switches.hasData()) && actionNeedsArg(svc_id))
	{
		status_exception::raise(Arg::Gds(isc_bad_spb_form) <<
								Arg::Gds(isc_svc_no_switches));
	}

	// Do not let everyone look at server log
	if (svc_id == isc_action_svc_get_fb_log && !(svc_user_flag & SVC_user_dba))
    {
       	status_exception::raise(Arg::Gds(isc_adm_task_denied) << Arg::Gds(isc_not_dba));
    }

	// Break up the command line into individual arguments.
	parseSwitches();

	// The service block can be reused hence init a status vector.
	{
		MutexLockGuard g(svc_status_mutex, FB_FUNCTION);
		svc_status->init();
	}

	if (serv->serv_thd)
	{
		svc_flags &= ~(SVC_evnt_fired | SVC_finished);

		svc_stdout_head = svc_stdout_tail = 0;

		Thread::start(run, this, THREAD_medium, &svc_thread);

		// good time for housekeeping while new thread starts
		threadCollect->houseKeeping();

		// Check for the service being detached. This will prevent the thread
		// from waiting infinitely if the client goes away.
		while (!(svc_flags & SVC_detached))
		{
			// The semaphore will be released once the particular service
			// has reached a point in which it can start to return
			// information to the client.  This will allow isc_service_start
			// to include in its status vector information about the service's
			// ability to start.
			// This is needed since Thread::start() will almost always succeed.
			//
			// Do not unlock mutex here - one can call start not doing this.
			if (svcStart.tryEnter(60))
			{
				// started() was called
				break;
			}
		}
	}
	else
	{
		status_exception::raise(Arg::Gds(isc_svcnotdef) << Arg::Str(serv->serv_name));
	}

	}	// try
	catch (const Firebird::Exception& ex)
	{
		if (svc_trace_manager->needs(ITraceFactory::TRACE_EVENT_SERVICE_START))
		{
			FbLocalStatus status_vector;
			ex.stuffException(&status_vector);

			const ISC_STATUS exc = status_vector[1];
			const bool no_priv = (exc == isc_login || exc == isc_no_priv);

			TraceServiceImpl service(this);
			svc_trace_manager->event_service_start(&service,
				this->svc_switches.length(), this->svc_switches.c_str(),
				no_priv ? ITracePlugin::RESULT_UNAUTHORIZED : ITracePlugin::RESULT_FAILED);
		}
		throw;
	}

	if (svc_trace_manager->needs(ITraceFactory::TRACE_EVENT_SERVICE_START))
	{
		TraceServiceImpl service(this);
		this->svc_trace_manager->event_service_start(&service,
			this->svc_switches.length(), this->svc_switches.c_str(),
			(this->svc_status->getState() & IStatus::STATE_ERRORS ?
				ITracePlugin::RESULT_FAILED : ITracePlugin::RESULT_SUCCESS));
	}
}


int Service::readFbLog(UtilSvc* arg)
{
	Service* service = (Service*) arg;
	service->readFbLog();
	return 0;
}

void Service::readFbLog()
{
	bool svc_started = false;

	Firebird::PathName name = fb_utils::getPrefix(IConfigManager::DIR_LOG, LOGFILE);
	FILE* file = os_utils::fopen(name.c_str(), "r");

	try
	{
		if (file != NULL)
		{
			{
				MutexLockGuard g(svc_status_mutex, FB_FUNCTION);
				svc_status->init();
			}
			started();
			svc_started = true;
			TEXT buffer[100];
			setDataMode(true);
			size_t n;

			while ((n = fread(buffer, sizeof(buffer[0]), FB_NELEM(buffer), file)) > 0)
			{
				outputData(buffer, static_cast<FB_SIZE_T>(n));
				if (checkForShutdown())
					break;
			}

			setDataMode(false);
		}

		if (!file || (file && ferror(file)))
		{
			MutexLockGuard g(svc_status_mutex, FB_FUNCTION);
			(Arg::Gds(isc_sys_request) << Arg::Str(file ? "fgets" : "fopen") <<
										  SYS_ERR(errno)).copyTo(&svc_status);
			if (!svc_started)
			{
				started();
			}
		}
	}
	catch (const Firebird::Exception& e)
	{
		setDataMode(false);

		MutexLockGuard g(svc_status_mutex, FB_FUNCTION);
		e.stuffException(&svc_status);
	}

	if (file)
	{
		fclose(file);
	}
}


void Service::start(const serv_entry* service_run)
{
	// Break up the command line into individual arguments.
	parseSwitches();

	svc_service_run = service_run;
	Thread::start(run, this, THREAD_medium);
}


ULONG Service::add_one(ULONG i) noexcept
{
	return (i + 1) % SVC_STDOUT_BUFFER_SIZE;
}


ULONG Service::add_val(ULONG i, ULONG val) noexcept
{
	return (i + val) % SVC_STDOUT_BUFFER_SIZE;
}


bool Service::empty(ULONG head) const noexcept
{
	return svc_stdout_tail == head;
}


bool Service::full() const noexcept
{
	return add_one(svc_stdout_tail) == svc_stdout_head;
}

#define ENQUEUE_DEQUEUE_DELAY 1

void Service::enqueue(const UCHAR* s, ULONG len)
{
	if (checkForShutdown() || (svc_flags & SVC_detached))
	{
		unblockQueryGet();
		return;
	}

	while (len)
	{
		// Wait for space in buffer
		bool flagFirst = true;
		while (full())
		{
			if (flagFirst)
			{
				unblockQueryGet(true);
				flagFirst = false;
			}
			svc_sem_empty.tryEnter(1, 0);
			if (checkForShutdown() || (svc_flags & SVC_detached))
			{
				unblockQueryGet();
				return;
			}
		}

		const ULONG head = svc_stdout_head;
		ULONG cnt = (head > svc_stdout_tail ? head : sizeof(svc_stdout)) - 1;
		if (add_one(cnt) != head)
		{
			++cnt;
		}
		cnt -= svc_stdout_tail;
		if (cnt > len)
		{
			cnt = len;
		}

		memcpy(&svc_stdout[svc_stdout_tail], s, cnt);
		svc_stdout_tail = add_val(svc_stdout_tail, cnt);
		s += cnt;
		len -= cnt;
	}
	unblockQueryGet();
}


void Service::unblockQueryGet(bool over)
{
	svc_output_overflow = over;
	svc_sem_full.release();
}


void Service::get(UCHAR* buffer, USHORT length, USHORT flags, USHORT timeout, USHORT* return_length)
{
#ifdef HAVE_GETTIMEOFDAY
	struct timeval start_time, end_time;
	GETTIMEOFDAY(&start_time);
#else
	time_t start_time, end_time;
	time(&start_time);
#endif

	*return_length = 0;
	svc_timeout = false;
	bool flagFirst = true;
	ULONG head = svc_stdout_head;

	while (length)
	{
		if ((empty(head) && (svc_flags & SVC_finished)) || checkForShutdown())
		{
			break;
		}

		if (empty(head))
		{
			if (svc_stdin_size_requested && (!(flags & GET_BINARY)))
			{
				// service needs data from user - notify him
				break;
			}

			if (flagFirst)
			{
				svc_sem_empty.release();
				flagFirst = false;
			}

			if (flags & GET_ONCE)
			{
				break;
			}

			if (full())
			{
				// buffer is full but LF is not present in it
				break;
			}

			UnlockGuard guard(this, FB_FUNCTION);
			svc_sem_full.tryEnter(1, 0);
			if (!guard.enter())
				Arg::Gds(isc_bad_svc_handle).raise();
		}
#ifdef HAVE_GETTIMEOFDAY
		GETTIMEOFDAY(&end_time);
		const time_t elapsed_time = end_time.tv_sec - start_time.tv_sec;
#else
		time(&end_time);
		const time_t elapsed_time = end_time - start_time;
#endif
		if (timeout && elapsed_time >= timeout)
		{
			ExistenceGuard guard(this, FB_FUNCTION);
			svc_timeout = true;
			break;
		}

		while ((!empty(head)) && length > 0)
		{
			flagFirst = true;
			const UCHAR ch = svc_stdout[head];
			head = add_one(head);
			length--;

			// If returning a line of information, replace all new line
			// characters with a space.  This will ensure that the output is
			// consistent when returning a line or to eof.
			if ((flags & GET_LINE) && ch == '\n')
			{
				buffer[(*return_length)++] = ' ';
				length = 0;
				break;
			}

			buffer[(*return_length)++] = ch;
		}

		if (svc_output_overflow || !(flags & GET_LINE))
		{
			svc_output_overflow = false;
			svc_stdout_head = head;
		}
	}

	if (flags & GET_LINE)
	{
		if (length == 0 || full())
			svc_stdout_head = head;
		else
			*return_length = 0;
	}

	svc_sem_empty.release();
}


ULONG Service::put(const UCHAR* buffer, ULONG length)
{
	MutexLockGuard guard(svc_stdin_mutex, FB_FUNCTION);

	// check length correctness
	if (length > svc_stdin_size_requested && length > svc_stdin_preload_requested)
	{
		(Arg::Gds(isc_svc_bad_size)).raise();
	}

	if (svc_stdin_size_requested)		// service waits for data from us
	{
		svc_stdin_user_size = MIN(length, svc_stdin_size_requested);
		memcpy(svc_stdin_buffer, buffer, svc_stdin_user_size);
		// reset satisfied request
		const ULONG blockSize = svc_stdin_size_requested;
		svc_stdin_size_requested = 0;
		// let data be used
		svc_stdin_semaphore.release();

		if (length == 0)
		{
			return 0;
		}

		// reset used block of data
		length -= svc_stdin_user_size;
		buffer += svc_stdin_user_size;

		if (length == 0)				// ask user to preload next block of data
		{
			if (!svc_stdin_preload)
			{
				svc_stdin_preload.reset(FB_NEW_POOL(getPool()) UCHAR[PRELOAD_BUFFER_SIZE]);
			}

			svc_stdin_preload_requested = MIN(blockSize, PRELOAD_BUFFER_SIZE);
			return svc_stdin_preload_requested;
		}
	}

	// Store data in preload buffer
	fb_assert(length <= PRELOAD_BUFFER_SIZE);
	fb_assert(length <= svc_stdin_preload_requested);
	fb_assert(svc_stdin_size_preload == 0);

	memcpy(svc_stdin_preload, buffer, length);
	svc_stdin_size_preload = length;
	return 0;
}


ULONG Service::getBytes(UCHAR* buffer, ULONG size)
{
	{	// Guard scope
		MutexLockGuard guard(svc_stdin_mutex, FB_FUNCTION);

		if (svc_flags & SVC_detached)			// no more data for this service please
		{
			return 0;
		}

		if (svc_stdin_size_preload != 0)		// use data, preloaded by user
		{
			// Use data from preload buffer
			size = MIN(size, svc_stdin_size_preload);
			memcpy(buffer, svc_stdin_preload, size);
			if (size < svc_stdin_size_preload)
			{
				// not good, client should not request so small block
				svc_stdin_size_preload -= size;
				memmove(svc_stdin_preload, svc_stdin_preload + size, svc_stdin_size_preload);
			}
			else
			{
				svc_stdin_size_preload = 0;
			}
			return size;
		}

		// Request new data portion
		svc_stdin_size_requested = size;
		svc_stdin_buffer = buffer;
		// Wakeup Service::query() if it waits for data from service
		unblockQueryGet();
	}

	// Wait for data from client
	svc_stdin_semaphore.enter();
	return svc_stdin_user_size;
}


void Service::finish(USHORT flag)
{
	if (flag == SVC_finished || flag == SVC_detached)
	{
		ExistenceGuard guard(this, FB_FUNCTION);

		svc_flags |= flag;
		if ((svc_flags & SVC_finished) && (svc_flags & SVC_detached))
		{
			delete this;
			return;
		}

		if (svc_flags & SVC_detached)
		{
			svc_sem_empty.release();

			// if service waits for data from us - return EOF
			{	// guard scope
				MutexLockGuard guard(svc_stdin_mutex, FB_FUNCTION);

				if (svc_stdin_size_requested)
				{
					svc_stdin_user_size = 0;
					svc_stdin_semaphore.release();
				}
			}
		}

		if (svc_flags & SVC_finished)
		{
			unblockQueryGet();
		}
		else
		{
			svc_detach_sem.release();
		}
	}
}


void Service::conv_switches(ClumpletReader& spb, string& switches)
{
	spb.rewind();
	const UCHAR test = spb.getClumpTag();
	if (test < isc_action_min || test >= isc_action_max) {
		return;	// error - action not defined
	}

	// convert to string
	string sw;
	if (! process_switches(spb, sw)) {
		return;
	}

	switches = sw;
}


const TEXT* Service::find_switch(int in_spb_sw, const Switches::in_sw_tab_t* table, bool bitmask)
{
	for (const Switches::in_sw_tab_t* in_sw_tab = table; in_sw_tab->in_sw_name; in_sw_tab++)
	{
		if (in_spb_sw == in_sw_tab->in_spb_sw && bitmask == in_sw_tab->in_sw_option)
			return in_sw_tab->in_sw_name;
	}

#ifdef DEV_BUILD
	if (isatty(fileno(stderr)))
		fprintf(stderr, "Miss %d %s\n", in_spb_sw, bitmask ? "option" : "switch");
#endif

	return NULL;
}


bool Service::actionNeedsArg(UCHAR action) noexcept
{
	switch (action)
	{
	case isc_action_svc_get_fb_log:
		return false;
	default:
		return true;
	}
}


bool Service::process_switches(ClumpletReader& spb, string& switches)
{
	if (spb.getBufferLength() == 0)
		return false;

	spb.rewind();
	const UCHAR svc_action = spb.getClumpTag();
	spb.moveNext();

	string burp_database, burp_backup;
	int burp_options = 0;

	string nbk_database, nbk_file, nbk_guid;
	int nbk_level = -1;

	bool cleanHistory = false, keepHistory = false;

	bool val_database = false;
	bool found = false;
	string::size_type userPos = string::npos;

	do
	{
		bool bigint = false;

		switch (svc_action)
		{
		case isc_action_svc_nbak:
		case isc_action_svc_nrest:
			found = true;

			switch (spb.getClumpTag())
			{
			case isc_spb_dbname:
				if (nbk_database.hasData())
				{
					(Arg::Gds(isc_unexp_spb_form) << Arg::Str("only one isc_spb_dbname")).raise();
				}
				get_action_svc_string(spb, nbk_database);
				break;

			case isc_spb_nbk_level:
#ifdef DEV_BUILD
				if (!svc_debug)
#endif
				if (nbk_level >= 0 || nbk_guid.hasData())
				{
					(Arg::Gds(isc_unexp_spb_form) << Arg::Str("only one isc_spb_nbk_level or isc_spb_nbk_guid")).raise();
				}
				nbk_level = spb.getInt();
				break;

			case isc_spb_nbk_guid:
#ifdef DEV_BUILD
				if (!svc_debug)
#endif
				if (nbk_level >= 0 || nbk_guid.hasData())
				{
					(Arg::Gds(isc_unexp_spb_form) <<
						Arg::Str("only one isc_spb_nbk_level or isc_spb_nbk_guid")).raise();
				}
				get_action_svc_string(spb, nbk_guid);
				break;

			case isc_spb_nbk_file:
				if (nbk_file.hasData() && svc_action != isc_action_svc_nrest)
				{
					(Arg::Gds(isc_unexp_spb_form) << Arg::Str("only one isc_spb_nbk_file")).raise();
				}
				get_action_svc_string(spb, nbk_file);
				break;

			case isc_spb_options:
				if (!get_action_svc_bitmask(spb, nbackup_in_sw_table, switches))
				{
					return false;
				}
				break;

			case isc_spb_nbk_direct:
				if (!get_action_svc_parameter(spb.getClumpTag(), nbackup_in_sw_table, switches))
				{
					return false;
				}
				get_action_svc_string(spb, switches);
				break;

			case isc_spb_nbk_clean_history:
				if (cleanHistory)
				{
					(Arg::Gds(isc_unexp_spb_form) << Arg::Str("only one isc_spb_nbk_clean_history")).raise();
				}
				if (!get_action_svc_parameter(spb.getClumpTag(), nbackup_action_in_sw_table, switches))
				{
					return false;
				}
				cleanHistory = true;
				break;

			case isc_spb_nbk_keep_days:
			case isc_spb_nbk_keep_rows:
				if (keepHistory)
				{
					(Arg::Gds(isc_unexp_spb_form) << Arg::Str("only one isc_spb_nbk_keep_days or isc_spb_nbk_keep_rows")).raise();
				}
				switches += "-KEEP ";
				get_action_svc_data(spb, switches, false);
				switches += spb.getClumpTag() == isc_spb_nbk_keep_days ? "DAYS " : "ROWS ";
				keepHistory = true;
				break;

			default:
				return false;
			}
			break;

		case isc_action_svc_nfix:
			found = true;

			switch (spb.getClumpTag())
			{
			case isc_spb_dbname:
				if (nbk_database.hasData())
				{
					(Arg::Gds(isc_unexp_spb_form) << Arg::Str("only one isc_spb_dbname")).raise();
				}
				get_action_svc_string(spb, nbk_database);
				break;

			case isc_spb_options:
				if (!get_action_svc_bitmask(spb, nbackup_in_sw_table, switches))
				{
					return false;
				}
				break;

			default:
				return false;
			}
			break;

		case isc_action_svc_set_mapping:
		case isc_action_svc_drop_mapping:
			if (!found)
			{
				if (!get_action_svc_parameter(svc_action, gsec_action_in_sw_table, switches))
				{
					return false;
				}

				found = true;
				if (spb.isEof())
				{
					break;
				}
			}

			switch (spb.getClumpTag())
			{
			case isc_spb_sql_role_name:
			case isc_spb_dbname:
				if (!get_action_svc_parameter(spb.getClumpTag(), gsec_in_sw_table, switches))
				{
					return false;
				}
				get_action_svc_string(spb, switches);
				break;

			default:
				return false;
			}
			break;

		case isc_action_svc_delete_user:
		case isc_action_svc_display_user:
		case isc_action_svc_display_user_adm:
			if (!found)
			{
				if (!get_action_svc_parameter(svc_action, gsec_action_in_sw_table, switches))
				{
					return false;
				}
				found = true;

				if (spb.isEof() && (svc_action == isc_action_svc_display_user ||
									svc_action == isc_action_svc_display_user_adm))
				{
					// in case of "display all users" the spb buffer contains
					// nothing but isc_action_svc_display_user or isc_spb_dbname
					break;
				}

				if (spb.getClumpTag() != isc_spb_sec_username)
				{
					userPos = switches.getCount();
				}
			}

			switch (spb.getClumpTag())
			{
			case isc_spb_sql_role_name:
			case isc_spb_dbname:
				if (!get_action_svc_parameter(spb.getClumpTag(), gsec_in_sw_table, switches))
				{
					return false;
				}
				get_action_svc_string(spb, switches);
				break;

			case isc_spb_sec_username:
				get_action_svc_string_pos(spb, switches, userPos);
				userPos = string::npos;
				break;

			default:
				fatal_exception::raise("Invalid item in service parameter block - invalid code for security database operation");
				// no return
			}
			break;

		case isc_action_svc_add_user:
		case isc_action_svc_modify_user:
			if (!found)
			{
				if (!get_action_svc_parameter(svc_action, gsec_action_in_sw_table, switches))
				{
					return false;
				}
				found = true;

				if (spb.getClumpTag() != isc_spb_sec_username)
				{
					userPos = switches.getCount();
				}
			}

			switch (spb.getClumpTag())
			{
			case isc_spb_sec_userid:
			case isc_spb_sec_groupid:
				if (!get_action_svc_parameter(spb.getClumpTag(), gsec_in_sw_table, switches))
				{
					return false;
				}
				get_action_svc_data(spb, switches, bigint);
				break;

			case isc_spb_sec_admin:
				if (!get_action_svc_parameter(spb.getClumpTag(), gsec_in_sw_table, switches))
				{
					return false;
				}
				switches += (spb.getInt() ? "Yes " : "No ");
				break;

			case isc_spb_sql_role_name:
			case isc_spb_sec_password:
			case isc_spb_sec_groupname:
			case isc_spb_sec_firstname:
			case isc_spb_sec_middlename:
			case isc_spb_sec_lastname:
			case isc_spb_dbname:
				if (!get_action_svc_parameter(spb.getClumpTag(), gsec_in_sw_table, switches))
				{
					return false;
				}
				get_action_svc_string(spb, switches);
				break;

			case isc_spb_sec_username:
				get_action_svc_string_pos(spb, switches, userPos);
				userPos = string::npos;
				break;

			default:
				return false;
			}
			break;

		case isc_action_svc_db_stats:
			switch (spb.getClumpTag())
			{
			case isc_spb_sts_table:
			case isc_spb_sts_schema:
				if (!get_action_svc_parameter(spb.getClumpTag(), dba_in_sw_table, switches))
				{
					return false;
				}
				[[fallthrough]];
			case isc_spb_dbname:
				get_action_svc_string(spb, switches);
				break;

			case isc_spb_options:
				if (!get_action_svc_bitmask(spb, dba_in_sw_table, switches))
				{
					return false;
				}
				break;

			case isc_spb_command_line:
				{
					string s;
					spb.getString(s);

					bool inStr = false;
					for (FB_SIZE_T i = 0; i < s.length(); )
					{
						if (s[i] == SVC_TRMNTR)
						{
							s.erase(i, 1);
							if (inStr)
							{
								if (i < s.length() && s[i] != SVC_TRMNTR)
								{
									inStr = false;
									continue;
								}
							}
							else
							{
								inStr = true;
								continue;
							}
						}
						++i;
					}

					switches += s;
					switches += ' ';
				}
				break;

			default:
				return false;
			}
			break;

		case isc_action_svc_backup:
		case isc_action_svc_restore:
			switch (spb.getClumpTag())
			{
			case isc_spb_bkp_file:
				get_action_svc_string(spb, burp_backup);
				break;
			case isc_spb_dbname:
				get_action_svc_string(spb, burp_database);
				break;
			case isc_spb_options:
				burp_options |= spb.getInt();
				if (!get_action_svc_bitmask(spb, reference_burp_in_sw_table, switches))
				{
					return false;
				}
				break;
			case isc_spb_bkp_length:
				get_action_svc_data(spb, burp_backup, bigint);
				break;
			case isc_spb_res_length:
				get_action_svc_data(spb, burp_database, bigint);
				break;
			case isc_spb_bkp_factor:
			case isc_spb_bkp_parallel_workers:
			case isc_spb_res_buffers:
			case isc_spb_res_page_size:
			case isc_spb_verbint:
				if (!get_action_svc_parameter(spb.getClumpTag(), reference_burp_in_sw_table, switches))
				{
					return false;
				}
				get_action_svc_data(spb, switches, bigint);
				break;
			case isc_spb_res_access_mode:
				if (!get_action_svc_parameter(*(spb.getBytes()), reference_burp_in_sw_table, switches))
				{
					return false;
				}
				break;
			case isc_spb_res_replica_mode:
				if (get_action_svc_parameter(spb.getClumpTag(), reference_burp_in_sw_table, switches))
				{
					const unsigned int val = spb.getInt();
					if (val >= FB_NELEM(burp_repl_mode_sw_table))
					{
						return false;
					}
					switches += burp_repl_mode_sw_table[val];
					switches += " ";
					break;
				}
				return false;
			case isc_spb_verbose:
				if (!get_action_svc_parameter(spb.getClumpTag(), reference_burp_in_sw_table, switches))
				{
					return false;
				}
				break;
			case isc_spb_res_fix_fss_data:
			case isc_spb_res_fix_fss_metadata:
			case isc_spb_bkp_stat:
			case isc_spb_bkp_skip_data:
			case isc_spb_bkp_include_data:
			case isc_spb_bkp_keyholder:
			case isc_spb_bkp_keyname:
			case isc_spb_bkp_crypt:
				if (!get_action_svc_parameter(spb.getClumpTag(), reference_burp_in_sw_table, switches))
				{
					return false;
				}
				get_action_svc_string(spb, switches);
				break;
			default:
				return false;
			}
			break;

		case isc_action_svc_repair:
		case isc_action_svc_properties:
			switch (spb.getClumpTag())
			{
			case isc_spb_dbname:
                get_action_svc_string(spb, switches);
				break;
			case isc_spb_options:
				if (!get_action_svc_bitmask(spb, alice_in_sw_table, switches))
				{
					return false;
				}
				break;
			case isc_spb_rpr_commit_trans_64:
			case isc_spb_rpr_rollback_trans_64:
			case isc_spb_rpr_recover_two_phase_64:
				bigint = true;
				[[fallthrough]];
			case isc_spb_prp_page_buffers:
			case isc_spb_prp_sweep_interval:
			case isc_spb_prp_shutdown_db:
			case isc_spb_prp_deny_new_attachments:
			case isc_spb_prp_deny_new_transactions:
			case isc_spb_prp_force_shutdown:
			case isc_spb_prp_attachments_shutdown:
			case isc_spb_prp_transactions_shutdown:
			case isc_spb_prp_set_sql_dialect:
			case isc_spb_rpr_commit_trans:
			case isc_spb_rpr_rollback_trans:
			case isc_spb_rpr_recover_two_phase:
			case isc_spb_rpr_par_workers:
				if (!get_action_svc_parameter(spb.getClumpTag(), alice_in_sw_table, switches))
				{
					return false;
				}
				get_action_svc_data(spb, switches, bigint);
				break;
			case isc_spb_prp_write_mode:
			case isc_spb_prp_access_mode:
			case isc_spb_prp_reserve_space:
				if (!get_action_svc_parameter(*(spb.getBytes()), alice_in_sw_table, switches))
				{
					return false;
				}
				break;
			case isc_spb_prp_shutdown_mode:
			case isc_spb_prp_online_mode:
				if (get_action_svc_parameter(spb.getClumpTag(), alice_in_sw_table, switches))
				{
					const unsigned int val = spb.getInt();
					if (val >= FB_NELEM(alice_shut_mode_sw_table))
					{
						return false;
					}
					switches += alice_shut_mode_sw_table[val];
					switches += " ";
					break;
				}
				return false;
			case isc_spb_prp_replica_mode:
				if (get_action_svc_parameter(spb.getClumpTag(), alice_in_sw_table, switches))
				{
					const unsigned int val = spb.getInt();
					if (val >= FB_NELEM(alice_repl_mode_sw_table))
					{
						return false;
					}
					switches += alice_repl_mode_sw_table[val];
					switches += " ";
					break;
				}
				return false;
			default:
				return false;
			}
			break;

		case isc_action_svc_trace_start:
		case isc_action_svc_trace_stop:
		case isc_action_svc_trace_suspend:
		case isc_action_svc_trace_resume:
		case isc_action_svc_trace_list:
			if (!found)
			{
				if (!get_action_svc_parameter(svc_action, trace_action_in_sw_table, switches)) {
					return false;
				}
				found = true;
			}

			if (svc_action == isc_action_svc_trace_list)
				break;

			if (!get_action_svc_parameter(spb.getClumpTag(), trace_option_in_sw_table, switches)) {
				return false;
			}

			switch (spb.getClumpTag())
			{
			case isc_spb_trc_cfg:
			case isc_spb_trc_name:
				get_action_svc_string(spb, switches);
				break;
			case isc_spb_trc_id:
				get_action_svc_data(spb, switches, bigint);
				break;
			default:
				return false;
			}
			break;

		case isc_action_svc_validate:
			if (!get_action_svc_parameter(spb.getClumpTag(), val_option_in_sw_table, switches)) {
				return false;
			}

			switch (spb.getClumpTag())
			{
			case isc_spb_dbname:
				if (val_database) {
					(Arg::Gds(isc_unexp_spb_form) << Arg::Str("only one isc_spb_dbname")).raise();
				}
				val_database = true;
				[[fallthrough]];
			case isc_spb_val_sch_incl:
			case isc_spb_val_sch_excl:
			case isc_spb_val_tab_incl:
			case isc_spb_val_tab_excl:
			case isc_spb_val_idx_incl:
			case isc_spb_val_idx_excl:
				get_action_svc_string(spb, switches);
				break;
			case isc_spb_val_lock_timeout:
				get_action_svc_data(spb, switches, bigint);
				break;
			}
			break;

		default:
			return false;
		}

		spb.moveNext();
	} while (! spb.isEof());

	if (userPos != string::npos && svc_action != isc_action_svc_display_user &&
		svc_action != isc_action_svc_display_user_adm)
	{
		// unexpected item in service parameter block, expected @1
		status_exception::raise(Arg::Gds(isc_unexp_spb_form) << Arg::Str(SPB_SEC_USERNAME));
	}

	// postfixes for burp & nbackup
	switch (svc_action)
	{
	case isc_action_svc_backup:
		switches += (burp_database + burp_backup);
		break;
	case isc_action_svc_restore:
		if (! (burp_options & (isc_spb_res_create | isc_spb_res_replace)))
		{
			// user not specified create or replace database
			// default to create for restore
			switches += "-CREATE_DATABASE ";
		}
		switches += (burp_backup + burp_database);
		break;

	case isc_action_svc_nbak:
	case isc_action_svc_nrest:
		if (nbk_database.isEmpty())
		{
			(Arg::Gds(isc_missing_required_spb) << Arg::Str("isc_spb_dbname")).raise();
		}
		if (nbk_file.isEmpty())
		{
			(Arg::Gds(isc_missing_required_spb) << Arg::Str("isc_spb_nbk_file")).raise();
		}

		if (!get_action_svc_parameter(svc_action, nbackup_action_in_sw_table, switches))
		{
			return false;
		}
		if (svc_action == isc_action_svc_nbak)
		{
			if (nbk_level < 0 && nbk_guid.isEmpty())
			{
				(Arg::Gds(isc_missing_required_spb) << Arg::Str("isc_spb_nbk_level or isc_spb_nbk_guid")).raise();
			}
			if (nbk_level >= 0)
			{
				string temp;
				temp.printf("%d ", nbk_level);
				switches += temp;
			}
			else
				switches += nbk_guid;

			if (!cleanHistory && keepHistory)
			{
				(Arg::Gds(isc_missing_required_spb) << Arg::Str("isc_spb_nbk_clean_history")).raise();
			}

			if (cleanHistory && !keepHistory)
			{
				(Arg::Gds(isc_missing_required_spb) << Arg::Str("isc_spb_nbk_keep_days or isc_spb_nbk_keep_rows")).raise();
			}
		}
		switches += nbk_database;
		switches += nbk_file;
		break;

	case isc_action_svc_nfix:
		if (nbk_database.isEmpty())
		{
			(Arg::Gds(isc_missing_required_spb) << Arg::Str("isc_spb_dbname")).raise();
		}

		if (!get_action_svc_parameter(svc_action, nbackup_action_in_sw_table, switches))
		{
			return false;
		}
		switches += nbk_database;
		break;

	case isc_action_svc_validate:
		if (!val_database)
		{
			(Arg::Gds(isc_missing_required_spb) << Arg::Str("isc_spb_dbname")).raise();
		}
		break;
	}

	switches.rtrim();
	return switches.length() > 0;
}


bool Service::get_action_svc_bitmask(const ClumpletReader& spb,
									 const Switches::in_sw_tab_t* table,
									 string& switches)
{
	const int opt = spb.getInt();
	ISC_ULONG mask = 1;
	for (int count = (sizeof(ISC_ULONG) * 8) - 1; count--; mask <<= 1)
	{
		if (opt & mask)
		{
			const TEXT* s_ptr = find_switch((opt & mask), table, true);
			if (!s_ptr)
			{
				return false;
			}

			switches += '-';
			switches += s_ptr;
			switches += ' ';
		}
	}

	return true;
}


void Service::get_action_svc_string(const ClumpletReader& spb, string& switches)
{
	string s;
	spb.getString(s);
	addStringWithSvcTrmntr(s, switches);
}


void Service::get_action_svc_string_pos(const ClumpletReader& spb, string& switches, string::size_type p)
{
	if (p == string::npos)
		get_action_svc_string(spb, switches);
	else
	{
		string s;
		get_action_svc_string(spb, s);
		switches.insert(p, s);
	}
}


void Service::get_action_svc_data(const ClumpletReader& spb, string& switches, bool bigint)
{
	string s;
	s.printf("%" SQUADFORMAT" ", (bigint ? spb.getBigInt() : (SINT64) spb.getInt()));
	switches += s;
}


bool Service::get_action_svc_parameter(UCHAR action,
									   const Switches::in_sw_tab_t* table,
									   string& switches)
{
	const TEXT* s_ptr = find_switch(action, table, false);
	if (!s_ptr)
	{
		return false;
	}

	switches += '-';
	switches += s_ptr;
	switches += ' ';

	return true;
}

const char* Service::getServiceMgr() const noexcept
{
	return "service_mgr";
}

const char* Service::getServiceName() const noexcept
{
	return svc_service_run ? svc_service_run->serv_name : NULL;
}

bool Service::getUserAdminFlag() const noexcept
{
	return (svc_user_flag & SVC_user_dba);
}
