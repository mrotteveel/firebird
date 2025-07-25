/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		isc_sync.cpp
 *	DESCRIPTION:	OS-dependent IPC: shared memory, mutex and event.
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
 * 2002.02.15 Sean Leyne - Code Cleanup, removed obsolete "XENIX" port
 * 2002.02.15 Sean Leyne - Code Cleanup, removed obsolete "DELTA" port
 * 2002.02.15 Sean Leyne - Code Cleanup, removed obsolete "IMP" port
 *
 * 2002-02-23 Sean Leyne - Code Cleanup, removed old M88K and NCR3000 port
 *
 * 2002.10.27 Sean Leyne - Completed removal of obsolete "DG_X86" port
 * 2002.10.27 Sean Leyne - Completed removal of obsolete "M88K" port
 *
 * 2002.10.28 Sean Leyne - Completed removal of obsolete "DGUX" port
 * 2002.10.28 Sean Leyne - Code cleanup, removed obsolete "DecOSF" port
 * 2002.10.28 Sean Leyne - Code cleanup, removed obsolete "SGI" port
 *
 * 2002.10.29 Sean Leyne - Removed obsolete "Netware" port
 *
 */

#include "firebird.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef SOLARIS
#include "../common/gdsassert.h"
#define PER_THREAD_RWLOCK
#endif

#ifdef HPUX
#include <sys/pstat.h>
#endif

#include "iberror.h"
#include "../yvalve/gds_proto.h"
#include "../common/isc_proto.h"
#include "../common/os/isc_i_proto.h"
#include "../common/os/os_utils.h"
#include "../common/os/mac_utils.h"
#include "../common/isc_s_proto.h"
#include "../common/file_params.h"
#include "../common/gdsassert.h"
#include "../common/config/config.h"
#include "../common/utils_proto.h"
#include "../common/StatusArg.h"
#include "../common/ThreadData.h"
#include "../common/ThreadStart.h"
#include "../common/classes/GenericMap.h"
#include "../common/classes/RefMutex.h"
#include "../common/classes/array.h"
#include "../common/classes/condition.h"
#include "../common/StatusHolder.h"

#ifdef WIN_NT
static int process_id;
#endif

// Unix specific stuff

#ifdef UNIX
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>

#ifdef HAVE_SYS_SIGNAL_H
#include <sys/signal.h>
#endif

#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif


#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include <sys/mman.h>

#endif // UNIX

#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#ifndef WIN_NT
#ifndef HAVE_GETPAGESIZE
static size_t getpagesize()
{
	return PAGESIZE;
}
#endif
#endif

//#define DEBUG_IPC
#ifdef DEBUG_IPC
#define IPC_TRACE(x)	{ /*time_t t; time(&t); printf("%s", ctime(&t) ); printf x; fflush (stdout);*/ gds__log x; }
#else
#define IPC_TRACE(x)
#endif


// Windows NT

#ifdef WIN_NT

#include <process.h>
#include <windows.h>
#include <psapi.h>

#endif

using namespace Firebird;

static void		error(CheckStatusWrapper*, const TEXT*, ISC_STATUS);
static bool		event_blocked(const event_t* event, const SLONG value);

#ifdef UNIX

#ifdef FILELOCK_DEBUG

#include <stdarg.h>

void DEB_FLOCK(const char* format, ...)
{
	va_list params;
	va_start(params, format);
	::vfprintf(stderr, format, params);
	va_end(params);
}

#else

void DEB_FLOCK(const char* format, ...) { }


#endif //FILELOCK_DEBUG

namespace {

#ifdef USE_FCNTL
	const char* NAME = "fcntl";
#else
	const char* NAME = "flock";
#endif

	class FileLockHolder
	{
	public:
		explicit FileLockHolder(FileLock* l)
			: lock(l)
		{
			if (!lock)
				return;
			LocalStatus ls;
			CheckStatusWrapper status(&ls);
			if (!lock->setlock(&status, FileLock::FLM_EXCLUSIVE))
				status_exception::raise(&status);
		}

		~FileLockHolder()
		{
			if (lock)
				lock->unlock();
		}

	private:
		FileLock* lock;
	};

	class DevNode
	{
	public:
		DevNode()
			: f_dev(0), f_ino(0)
		{ }

		DevNode(dev_t d, ino_t i)
			: f_dev(d), f_ino(i)
		{ }

		DevNode(const DevNode& v)
			: f_dev(v.f_dev), f_ino(v.f_ino)
		{ }

		dev_t	f_dev;
		ino_t	f_ino;

		bool operator==(const DevNode& v) const
		{
			return f_dev == v.f_dev && f_ino == v.f_ino;
		}

		bool operator>(const DevNode& v) const
		{
			return f_dev > v.f_dev ? true :
				   f_dev < v.f_dev ? false :
				   f_ino > v.f_ino;
		}

		const DevNode& operator=(const DevNode& v)
		{
			f_dev = v.f_dev;
			f_ino = v.f_ino;
			return *this;
		}
	};

	DevNode getNode(const char* name)
	{
		struct STAT statistics;
		if (os_utils::stat(name, &statistics) != 0)
		{
			if (errno == ENOENT)
			{
				//file not found
				return DevNode();
			}

			system_call_failed::raise("stat");
		}

		return DevNode(statistics.st_dev, statistics.st_ino);
	}

	DevNode getNode(int fd)
	{
		struct STAT statistics;
		if (os_utils::fstat(fd, &statistics) != 0)
			system_call_failed::raise("stat");

		return DevNode(statistics.st_dev, statistics.st_ino);
	}

	GlobalPtr<Mutex> openFdInit;

} // anonymous namespace


namespace Firebird {

class SharedFileInfo : public RefCounted
{
	SharedFileInfo(int f, const DevNode& id)
		: counter(0), threadId(0), fd(f), devNode(id)
	{ }

	~SharedFileInfo()
	{
		fb_assert(sharedFilesMutex->locked());
		fb_assert(counter == 0);

		DEB_FLOCK("~ %p\n", this);
		sharedFiles->remove(devNode);
		close(fd);
	}

public:
	static SharedFileInfo* get(const char* fileName)
	{
		DevNode id(getNode(fileName));

		MutexLockGuard g(sharedFilesMutex, FB_FUNCTION);

		SharedFileInfo* file = nullptr;
		if (id.f_ino)
		{
			SharedFileInfo** got = sharedFiles->get(id);
			if (got)
			{
				file = *got;
				DEB_FLOCK("'%s': in map %p\n", fileName, file);
				file->assertNonZero();
			}
		}

		if (!file)
		{
			int fd = os_utils::openCreateSharedFile(fileName, 0);
			id = getNode(fd);
			file = FB_NEW_POOL(*getDefaultMemoryPool()) SharedFileInfo(fd, id);
			SharedFileInfo** put = sharedFiles->put(id);
			fb_assert(put);
			*put = file;
			DEB_FLOCK("'%s': new %p\n", fileName, file);
		}

		file->addRef();
		return file;
	}

	int release() const override
	{
		// Release should be executed under mutex protection
		// in order to modify reference counter & map atomically
		MutexLockGuard guard(sharedFilesMutex, FB_FUNCTION);

		return RefCounted::release();
	}

	int lock(bool shared, bool wait, FileLock::InitFunction* init)
	{
		MutexEnsureUnlock guard(mutex, FB_FUNCTION);
		if (wait)
			guard.enter();
		else if (!guard.tryEnter())
			return -1;

		DEB_FLOCK("%d lock %p %c%c\n", Thread::getId(), this, shared ? 's' : 'X', wait ? 'W' : 't');

		while (counter != 0)	// file lock belongs to our process
		{
			// check for compatible locks
			if (shared && counter > 0)
			{
				// one more shared lock
				++counter;
				DEB_FLOCK("%d fast %p c=%d\n", Thread::getId(), this, counter);
				return 0;
			}
			if ((!shared) && counter < 0 && threadId == Thread::getId())
			{
				// recursive excl lock
				--counter;
				DEB_FLOCK("%d fast %p c=%d\n", Thread::getId(), this, counter);
				return 0;
			}

			// non compatible lock needed
			// wait for another thread to release a lock
			if (!wait)
			{
				DEB_FLOCK("%d failed internally %p c=%d rc -1\n", Thread::getId(), this, counter);
				return -1;
			}

			DEB_FLOCK("%d wait %p c=%d\n", Thread::getId(), this, counter);
			waitOn.wait(mutex);
		}

		// Take lock on a file
		fb_assert(counter == 0);
#ifdef USE_FCNTL
		struct FLOCK lock;
		lock.l_type = shared ? F_RDLCK : F_WRLCK;
		lock.l_whence = SEEK_SET;
		lock.l_start = 0;
		lock.l_len = 1;
		if (fcntl(fd, wait ? F_SETLKW : F_SETLK, &lock) == -1)
		{
			int rc = errno;
			if (!wait && (rc == EACCES || rc == EAGAIN))
				rc = -1;
#else
		if (flock(fd, (shared ? LOCK_SH : LOCK_EX) | (wait ? 0 : LOCK_NB)))
		{
			int rc = errno;
			if (!wait && (rc == EWOULDBLOCK))
				rc = -1;
#endif
			DEB_FLOCK("%d failed on file %p c=%d rc %d\n", Thread::getId(), this, counter, rc);
			return rc;
		}

		if (!shared)
		{
			threadId = Thread::getId();

			// call init() when needed
			if (init && !shared)
				init(fd);
		}

		// mark lock as taken
		counter = shared ? 1 : -1;
		DEB_FLOCK("%d filelock %p c=%d\n", Thread::getId(), this, counter);
		return 0;
	}

	void unlock()
	{
		fb_assert(counter != 0);

		MutexEnsureUnlock guard(mutex, FB_FUNCTION);
		guard.enter();

		DEB_FLOCK("%d UNlock %p c=%d\n", Thread::getId(), this, counter);

		if (counter < 0)
			++counter;
		else
			--counter;

		if (counter != 0)
		{
			DEB_FLOCK("%d done %p c=%d\n", Thread::getId(), this, counter);
			return;
		}

		// release file lock
#ifdef USE_FCNTL
		struct FLOCK lock;
		lock.l_type = F_UNLCK;
		lock.l_whence = SEEK_SET;
		lock.l_start = 0;
		lock.l_len = 1;

		if (fcntl(fd, F_SETLK, &lock) != 0)
		{
#else
		if (flock(fd, LOCK_UN) != 0)
		{
#endif
			LocalStatus ls;
			CheckStatusWrapper local(&ls);
			error(&local, NAME, errno);
			iscLogStatus("Unlock error", &local);
		}

		DEB_FLOCK("%d file-done %p\n", Thread::getId(), this);
		waitOn.notifyAll();
	}

	int getFd()
	{
		return fd;
	}

private:
	typedef GenericMap<Pair<NonPooled<DevNode, SharedFileInfo*> > > SharedFiles;
	static GlobalPtr<SharedFiles> sharedFiles;
	static GlobalPtr<Mutex> sharedFilesMutex;

	Condition waitOn;
	Mutex mutex;
	int counter;
	ThreadId threadId;
	int fd;
	DevNode devNode;
};

GlobalPtr<SharedFileInfo::SharedFiles> SharedFileInfo::sharedFiles;
GlobalPtr<Mutex> SharedFileInfo::sharedFilesMutex;

} // namespace Firebird


FileLock::FileLock(const char* fileName, InitFunction* init)
	: file(REF_NO_INCR, SharedFileInfo::get(fileName)), initFunction(init), level(LCK_NONE)
{ }

FileLock::~FileLock()
{
	unlock();
}

int FileLock::getFd()
{
	return file->getFd();
}

int FileLock::setlock(const LockMode mode)
{
	bool shared = true, wait = true;
	switch (mode)
	{
		case FLM_TRY_EXCLUSIVE:
			wait = false;
			// fall through
		case FLM_EXCLUSIVE:
			shared = false;
			break;
		case FLM_SHARED:
			break;
	}

	const LockLevel newLevel = shared ? LCK_SHARED : LCK_EXCL;
	if (newLevel == level)
	{
		return 0;
	}
	if (level != LCK_NONE)
	{
		return wait ? EBUSY : -1;
	}

	int rc = file->lock(shared, wait, initFunction);
	if (rc == 0)	// lock taken
		level = newLevel;

	return rc;
}

bool FileLock::setlock(CheckStatusWrapper* status, const LockMode mode)
{
	int rc = setlock(mode);
	if (rc != 0)
	{
		if (rc > 0)
		{
			error(status, NAME, rc);
		}
		return false;
	}
	return true;
}

void FileLock::unlock()
{
	if (level == LCK_NONE)
	{
		return;
	}

	file->unlock();
	level = LCK_NONE;
}

#endif // UNIX

#if defined(WIN_NT)
static bool make_object_name(TEXT*, size_t, const TEXT*, const TEXT*);
#endif


namespace {

	int isPthreadError(int rc, const char* function)
	{
		if (rc == 0)
			return 0;
		iscLogStatus("Pthread Error",
			(Arg::Gds(isc_sys_request) << Arg::Str(function) << Arg::Unix(rc)).value());
		return rc;
	}

}

#define PTHREAD_ERROR(x) if (isPthreadError((x), #x)) return FB_FAILURE
#define PTHREAD_ERRNO(x) { int tmpState = (x); if (isPthreadError(tmpState, #x)) return tmpState; }
#define LOG_PTHREAD_ERROR(x) isPthreadError((x), #x)
#define PTHREAD_ERR_STATUS(x, v) { int tmpState = (x); if (tmpState) { error(v, #x, tmpState); return false; } }
#define PTHREAD_ERR_RAISE(x) { int tmpState = (x); if (tmpState) { system_call_failed::raise(#x, tmpState); } }


bool MemoryHeader::check(const char* name, USHORT type, USHORT version, bool raiseError) const
{
	if (mhb_type == type && mhb_header_version == HEADER_VERSION && mhb_version == version)
		return true;

	if (!raiseError)
		return false;

	string found, expected;

	found.printf("%d/%d:%d", mhb_type, mhb_header_version, mhb_version);
	expected.printf("%d/%d:%d", type, HEADER_VERSION, version);

	// @1: inconsistent shared memory type/version; found @2, expected @3
	(Arg::Gds(isc_wrong_shmem_ver) <<
		Arg::Str(name) << Arg::Str(found) << Arg::Str(expected)).raise();
}

int SharedMemoryBase::eventInit(event_t* event)
{
/**************************************
 *
 *	I S C _ e v e n t _ i n i t
 *
 **************************************
 *
 * Functional description
 *	Prepare an event object for use.
 *
 **************************************/

#if defined(WIN_NT)

	static AtomicCounter idCounter;

	event->event_id = ++idCounter;

	event->event_pid = process_id = getpid();
	event->event_count = 0;

	event->event_handle = ISC_make_signal(true, true, process_id, event->event_id);

	return (event->event_handle) ? FB_SUCCESS : FB_FAILURE;

#else // pthread-based event

	event->event_count = 0;
	event->event_pid = getpid();

	// Prepare an Inter-Process event block
	pthread_mutexattr_t mattr;
	pthread_condattr_t cattr;

	PTHREAD_ERROR(pthread_mutexattr_init(&mattr));
	PTHREAD_ERROR(pthread_condattr_init(&cattr));
#ifdef PTHREAD_PROCESS_SHARED
	if (!isSandboxed())
	{
		PTHREAD_ERROR(pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED));
		PTHREAD_ERROR(pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED));
	}
#else
#error Your system must support PTHREAD_PROCESS_SHARED to use firebird.
#endif
	PTHREAD_ERROR(pthread_mutex_init(event->event_mutex, &mattr));
	PTHREAD_ERROR(pthread_cond_init(event->event_cond, &cattr));
	PTHREAD_ERROR(pthread_mutexattr_destroy(&mattr));
	PTHREAD_ERROR(pthread_condattr_destroy(&cattr));

	return FB_SUCCESS;

#endif // OS-dependent choice

}


void SharedMemoryBase::eventFini(event_t* event)
{
/**************************************
 *
 *	I S C _ e v e n t _ f i n i
 *
 **************************************
 *
 * Functional description
 *	Discard an event object.
 *
 **************************************/

#if defined(WIN_NT)

	if (event->event_pid == process_id)
	{
		CloseHandle(event->event_handle);
	}

#else // pthread-based event

	if (event->event_pid == getpid())
	{
		LOG_PTHREAD_ERROR(pthread_mutex_destroy(event->event_mutex));
		LOG_PTHREAD_ERROR(pthread_cond_destroy(event->event_cond));
	}

#endif // OS-dependent choice

}


SLONG SharedMemoryBase::eventClear(event_t* event)
{
/**************************************
 *
 *	I S C _ e v e n t _ c l e a r
 *
 **************************************
 *
 * Functional description
 *	Clear an event preparatory to waiting on it.  The order of
 *	battle for event synchronization is:
 *
 *	    1.  Clear event.
 *	    2.  Test data structure for event already completed
 *	    3.  Wait on event.
 *
 **************************************/

	fb_assert(event->event_pid == getpid());

#if defined(WIN_NT)

	ResetEvent(event->event_handle);

	return event->event_count + 1;

#else // pthread-based event

	LOG_PTHREAD_ERROR(pthread_mutex_lock(event->event_mutex));
	const SLONG ret = event->event_count + 1;
	LOG_PTHREAD_ERROR(pthread_mutex_unlock(event->event_mutex));
	return ret;

#endif // OS-dependent choice

}


int SharedMemoryBase::eventWait(event_t* event, const SLONG value, const SLONG micro_seconds)
{
/**************************************
 *
 *	I S C _ e v e n t _ w a i t
 *
 **************************************
 *
 * Functional description
 *	Wait on an event.
 *
 **************************************/

	fb_assert(event->event_pid == getpid());

	// If we're not blocked, the rest is a gross waste of time

	if (!event_blocked(event, value)) {
		return FB_SUCCESS;
	}

#if defined(WIN_NT)

	// Go into wait loop

	const DWORD timeout = (micro_seconds > 0) ? micro_seconds / 1000 : INFINITE;

	for (;;)
	{
		if (!event_blocked(event, value))
			return FB_SUCCESS;

		const DWORD status = WaitForSingleObject(event->event_handle, timeout);

		if (status != WAIT_OBJECT_0)
			return FB_FAILURE;
	}

#else // pthread-based event

	// Set up timers if a timeout period was specified.

	struct timespec timer;
	if (micro_seconds > 0)
	{
#if defined(HAVE_CLOCK_GETTIME)
		clock_gettime(CLOCK_REALTIME, &timer);
#elif defined(HAVE_GETTIMEOFDAY)
		struct timeval tp;
		GETTIMEOFDAY(&tp);
		timer.tv_sec = tp.tv_sec;
		timer.tv_nsec = tp.tv_usec * 1000;
#else
		struct timeb time_buffer;
		ftime(&time_buffer);
		timer.tv_sec = time_buffer.time;
		timer.tv_nsec = time_buffer.millitm * 1000000;
#endif
		const SINT64 BILLION = 1000000000;
		const SINT64 nanos = (SINT64) timer.tv_sec * BILLION + timer.tv_nsec +
			(SINT64) micro_seconds * 1000;
		timer.tv_sec = nanos / BILLION;
		timer.tv_nsec = nanos % BILLION;
	}

	int ret = FB_SUCCESS;
	pthread_mutex_lock(event->event_mutex);
	for (;;)
	{
		if (!event_blocked(event, value))
		{
			ret = FB_SUCCESS;
			break;
		}

		// The Posix pthread_cond_wait & pthread_cond_timedwait calls
		// atomically release the mutex and start a wait.
		// The mutex is reacquired before the call returns.
		if (micro_seconds > 0)
		{
			ret = pthread_cond_timedwait(event->event_cond, event->event_mutex, &timer);

			if (ret == ETIMEDOUT)
			{

				// The timer expired - see if the event occurred and return
				// FB_SUCCESS or FB_FAILURE accordingly.

				if (event_blocked(event, value))
					ret = FB_FAILURE;
				else
					ret = FB_SUCCESS;
				break;
			}
		}
		else
			ret = pthread_cond_wait(event->event_cond, event->event_mutex);
	}
	pthread_mutex_unlock(event->event_mutex);
	return ret;

#endif // OS-dependent choice

}


int SharedMemoryBase::eventPost(event_t* event)
{
/**************************************
 *
 *	I S C _ e v e n t _ p o s t
 *
 **************************************
 *
 * Functional description
 *	Post an event to wake somebody else up.
 *
 **************************************/

#if defined(WIN_NT)

	++event->event_count;

	if (event->event_pid != process_id)
		return ISC_kill(event->event_pid, event->event_id, event->event_handle) == 0 ? FB_SUCCESS : FB_FAILURE;

	return SetEvent(event->event_handle) ? FB_SUCCESS : FB_FAILURE;

#else // pthread-based event

	PTHREAD_ERROR(pthread_mutex_lock(event->event_mutex));
	++event->event_count;
	const int ret = pthread_cond_broadcast(event->event_cond);
	PTHREAD_ERROR(pthread_mutex_unlock(event->event_mutex));
	if (ret)
	{
		gds__log ("ISC_event_post: pthread_cond_broadcast failed with errno = %d", ret);
		return FB_FAILURE;
	}

	return FB_SUCCESS;

#endif // OS-dependent choice

} // anonymous namespace


#ifdef UNIX
ULONG ISC_exception_post(ULONG sig_num, const TEXT* err_msg, ISC_STATUS& /*isc_error*/)
{
/**************************************
 *
 *	I S C _ e x c e p t i o n _ p o s t ( U N I X )
 *
 **************************************
 *
 * Functional description
 *     When we got a sync exception, fomulate the error code
 *     write it to the log file, and abort.
 *
 * 08-Mar-2004, Nickolay Samofatov.
 *   This function is dangerous and requires rewrite using signal-safe operations only.
 *   Main problem is that we call a lot of signal-unsafe functions from this signal handler,
 *   examples are gds__alloc, gds__log, etc... sprintf is safe on some BSD platforms,
 *   but not on Linux. This may result in lock-up during signal handling.
 *
 **************************************/
	// If there's no err_msg, we asumed the switch() finds no case or we crash.
	// Too much goodwill put on the caller. Weak programming style.
	// Therefore, lifted this safety net from the NT version.
	if (!err_msg)
	{
		err_msg = "";
	}

	const size_t msgsz = strlen(err_msg) + 256;
	TEXT* const log_msg = (TEXT *) gds__alloc(static_cast<SLONG>(msgsz));
	// NOMEM: crash!
	log_msg[0] = '\0';

	switch (sig_num)
	{
	case SIGSEGV:
		snprintf(log_msg, msgsz, "%s Segmentation Fault.\n"
				"\t\tThe code attempted to access memory\n"
				"\t\twithout privilege to do so.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case SIGBUS:
		snprintf(log_msg, msgsz, "%s Bus Error.\n"
				"\t\tThe code caused a system bus error.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case SIGILL:

		snprintf(log_msg, msgsz, "%s Illegal Instruction.\n"
				"\t\tThe code attempted to perform an\n"
				"\t\tillegal operation."
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;

	case SIGFPE:
		snprintf(log_msg, msgsz, "%s Floating Point Error.\n"
				"\t\tThe code caused an arithmetic exception\n"
				"\t\tor floating point exception."
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	default:
		snprintf(log_msg, msgsz, "%s Unknown Exception.\n"
				"\t\tException number %" ULONGFORMAT"."
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg, sig_num);
		break;
	}

	if (err_msg)
	{
		gds__log(log_msg);
	}

	gds__free(log_msg);

	abort();

	return 0;	// compiler silencer
}
#endif // UNIX


#ifdef WIN_NT
ULONG ISC_exception_post(ULONG except_code, const TEXT* err_msg, ISC_STATUS& isc_error)
{
/**************************************
 *
 *	I S C _ e x c e p t i o n _ p o s t ( W I N _ N T )
 *
 **************************************
 *
 * Functional description
 *     When we got a sync exception, fomulate the error code
 *     write it to the log file, and abort. Note: We can not
 *     actually call "abort" since in windows this will cause
 *     a dialog to appear stating the obvious!  Since on NT we
 *     would not get a core file, there is actually no difference
 *     between abort() and exit(3).
 *
 **************************************/
	ULONG result = 0;
	bool is_critical = true;
	isc_error = 0;

	if (!err_msg)
	{
		err_msg = "";
	}

	const size_t msgsz = strlen(err_msg) + 256;
	TEXT* const log_msg = (TEXT*)gds__alloc(static_cast<SLONG>(msgsz));
	// NOMEM: crash!
	log_msg[0] = '\0';

	switch (except_code)
	{
	case EXCEPTION_ACCESS_VIOLATION:
		snprintf(log_msg, msgsz, "%s Access violation.\n"
				"\t\tThe code attempted to access a virtual\n"
				"\t\taddress without privilege to do so.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_DATATYPE_MISALIGNMENT:
		snprintf(log_msg, msgsz, "%s Datatype misalignment.\n"
				"\t\tThe attempted to read or write a value\n"
				"\t\tthat was not stored on a memory boundary.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_ARRAY_BOUNDS_EXCEEDED:
		snprintf(log_msg, msgsz, "%s Array bounds exceeded.\n"
				"\t\tThe code attempted to access an array\n"
				"\t\telement that is out of bounds.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_FLT_DENORMAL_OPERAND:
		snprintf(log_msg, msgsz, "%s Float denormal operand.\n"
				"\t\tOne of the floating-point operands is too\n"
				"\t\tsmall to represent as a standard floating-point\n"
				"\t\tvalue.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_FLT_DIVIDE_BY_ZERO:
		snprintf(log_msg, msgsz, "%s Floating-point divide by zero.\n"
				"\t\tThe code attempted to divide a floating-point\n"
				"\t\tvalue by a floating-point divisor of zero.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_FLT_INEXACT_RESULT:
		snprintf(log_msg, msgsz, "%s Floating-point inexact result.\n"
				"\t\tThe result of a floating-point operation cannot\n"
				"\t\tbe represented exactly as a decimal fraction.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_FLT_INVALID_OPERATION:
		snprintf(log_msg, msgsz, "%s Floating-point invalid operand.\n"
				"\t\tAn indeterminant error occurred during a\n"
				"\t\tfloating-point operation.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_FLT_OVERFLOW:
		snprintf(log_msg, msgsz, "%s Floating-point overflow.\n"
				"\t\tThe exponent of a floating-point operation\n"
				"\t\tis greater than the magnitude allowed.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_FLT_STACK_CHECK:
		snprintf(log_msg, msgsz, "%s Floating-point stack check.\n"
				"\t\tThe stack overflowed or underflowed as the\n"
				"result of a floating-point operation.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_FLT_UNDERFLOW:
		snprintf(log_msg, msgsz, "%s Floating-point underflow.\n"
				"\t\tThe exponent of a floating-point operation\n"
				"\t\tis less than the magnitude allowed.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_INT_DIVIDE_BY_ZERO:
		snprintf(log_msg, msgsz, "%s Integer divide by zero.\n"
				"\t\tThe code attempted to divide an integer value\n"
				"\t\tby an integer divisor of zero.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_INT_OVERFLOW:
		snprintf(log_msg, msgsz, "%s Interger overflow.\n"
				"\t\tThe result of an integer operation caused the\n"
				"\t\tmost significant bit of the result to carry.\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg);
		break;
	case EXCEPTION_STACK_OVERFLOW:
		isc_error = isc_exception_stack_overflow;
		result = EXCEPTION_EXECUTE_HANDLER;
		is_critical = false;
		break;

	case EXCEPTION_BREAKPOINT:
	case EXCEPTION_SINGLE_STEP:
	case EXCEPTION_NONCONTINUABLE_EXCEPTION:
	case EXCEPTION_INVALID_DISPOSITION:
	case EXCEPTION_PRIV_INSTRUCTION:
	case EXCEPTION_IN_PAGE_ERROR:
	case EXCEPTION_ILLEGAL_INSTRUCTION:
	case EXCEPTION_GUARD_PAGE:
		// Pass these exception on to someone else, probably the OS or the debugger,
		// since there isn't a dam thing we can do with them
		result = EXCEPTION_CONTINUE_SEARCH;
		is_critical = false;
		break;
	case 0xE06D7363: // E == Exception. 0x6D7363 == "msc". Intel and Borland use the same code to be compatible
		// If we've caught our own software exception,
		// continue rewinding the stack to properly handle it
		// and deliver an error information to the client side
		result = EXCEPTION_CONTINUE_SEARCH;
		is_critical = false;
		break;
	default:
		snprintf(log_msg, msgsz, "%s An exception occurred that does\n"
				"\t\tnot have a description.  Exception number %" XLONGFORMAT".\n"
				"\tThis exception will cause the Firebird server\n"
				"\tto terminate abnormally.", err_msg, except_code);
		break;
	}

	if (is_critical)
	{
		gds__log(log_msg);
	}

	gds__free(log_msg);

	if (is_critical)
	{
		if (Config::getBugcheckAbort())
		{
			// Pass exception to outer handler in case debugger is present to collect memory dump
			return EXCEPTION_CONTINUE_SEARCH;
		}

		// Silently exit so guardian or service manager can restart the server.
		// If exception is getting out of the application Windows displays a message
		// asking if you want to send report to Microsoft or attach debugger,
		// application is not terminated until you press some button on resulting window.
		// This happens even if you run application as non-interactive service on
		// "server" OS like Windows Server 2003.

		fb_shutdown(0, fb_shutrsn_emergency);
		_exit(3);
	}

	return result;
}
#endif // WIN_NT


void SharedMemoryBase::removeMapFile()
{
	fb_assert(sh_mem_header);

	if (!sh_mem_header->isDeleted())
	{
#ifndef WIN_NT
		FileLockHolder initLock(initFile);
		if (!sh_mem_header->isDeleted())
		{
			unlinkFile();
			sh_mem_header->markAsDeleted();
		}
#else
		fb_assert(!sh_mem_unlink);
		sh_mem_unlink = true;
		sh_mem_header->markAsDeleted();
#endif // WIN_NT
	}
}

void SharedMemoryBase::unlinkFile()
{
	TEXT expanded_filename[MAXPATHLEN];
	iscPrefixLock(expanded_filename, sh_mem_name, false);

	unlinkFile(expanded_filename);
}

PathName SharedMemoryBase::getMapFileName()
{
	TEXT expanded_filename[MAXPATHLEN];
	iscPrefixLock(expanded_filename, sh_mem_name, false);

	return PathName(expanded_filename);
}

void SharedMemoryBase::unlinkFile(const TEXT* expanded_filename) noexcept
{
	// We can't do much (specially in dtors) when it fails
	// therefore do not check for errors - at least it's just /tmp.

#ifdef WIN_NT
	// Delete file only if it is not used by anyone else
	HANDLE hFile = CreateFile(expanded_filename,
		DELETE,
		0,
		NULL,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL | FILE_FLAG_DELETE_ON_CLOSE,
		NULL);

	if (hFile != INVALID_HANDLE_VALUE)
		CloseHandle(hFile);
#else
	unlink(expanded_filename);
#endif // WIN_NT
}


#ifdef UNIX

static inline void reportError(const char* func, CheckStatusWrapper* statusVector)
{
	if (!statusVector)
		system_call_failed::raise(func);
	else
		error(statusVector, func, errno);
}

bool allocFileSpace(int fd, off_t offset, FB_SIZE_T length, CheckStatusWrapper* statusVector)
{
#if defined(HAVE_LINUX_FALLOC_H) && defined(HAVE_FALLOCATE)
	if (fallocate(fd, 0, offset, length) == 0)
		return true;

	if (errno != EOPNOTSUPP && errno != ENOSYS)
	{
		reportError("fallocate", statusVector);
		return false;
	}
	// fallocate is not supported by this kernel or file system
	// take the long way around
#endif
	static const FB_SIZE_T buf128KSize = 131072;
	HalfStaticArray<UCHAR, BUFFER_LARGE> buf;
	const FB_SIZE_T bufSize = length < buf128KSize ? length : buf128KSize;

	memset(buf.getBuffer(bufSize), 0, bufSize);
	os_utils::lseek(fd, LSEEK_OFFSET_CAST offset, SEEK_SET);

	while (length)
	{
		const FB_SIZE_T cnt = length < bufSize ? length : bufSize;
		if (write(fd, buf.begin(), cnt) != (ssize_t) cnt)
		{
			reportError("write", statusVector);
			return false;
		}
		length -= cnt;
	}

	if (fsync(fd))
	{
		reportError("fsync", statusVector);
		return false;
	}

	return true;
}


void SharedMemoryBase::internalUnmap()
{
	if (sh_mem_header)
	{
		munmap(sh_mem_header, sh_mem_length_mapped);
		sh_mem_header = NULL;
	}
}

SharedMemoryBase::SharedMemoryBase(const TEXT* filename, ULONG length, IpcObject* callback, bool skipLock)
	:
#ifdef HAVE_SHARED_MUTEX_SECTION
	sh_mem_mutex(0),
#endif
	sh_mem_length_mapped(0), sh_mem_header(NULL),
	sh_mem_callback(callback)
{
/**************************************
 *
 *	c t o r		( U N I X - m m a p )
 *
 **************************************
 *
 * Functional description
 *	Try to map a given file.  If we are the first (i.e. only)
 *	process to map the file, call a given initialization
 *	routine (if given) or punt (leaving the file unmapped).
 *
 **************************************/
	LocalStatus ls;
	CheckStatusWrapper statusVector(&ls);

	sh_mem_name[0] = '\0';

	TEXT expanded_filename[MAXPATHLEN];
	iscPrefixLock(expanded_filename, filename, true);

	// make the complete filename for the init file this file is to be used as a
	// master lock to eliminate possible race conditions with just a single file
	// locking. The race condition is caused as the conversion of an EXCLUSIVE
	// lock to a SHARED lock is not atomic

	TEXT init_filename[MAXPATHLEN];
	iscPrefixLock(init_filename, INIT_FILE, true);

	const bool trunc_flag = (length != 0);

	// open the init lock file
	MutexLockGuard guard(openFdInit, FB_FUNCTION);

	if (!skipLock)
		initFile.reset(FB_NEW_POOL(*getDefaultMemoryPool()) FileLock(init_filename));

	// get an exclusive lock on the INIT file with blocking except TransactionStatusBlock
	// since its initialized under FileLock
	FileLockHolder initLock(initFile);

	// create lock in order to have file autoclosed on error
	mainLock.reset(FB_NEW_POOL(*getDefaultMemoryPool()) FileLock(expanded_filename));

	if (length == 0)
	{
		// Get and use the existing length of the shared segment
		struct STAT file_stat;

		if (os_utils::fstat(mainLock->getFd(), &file_stat) == -1)
			system_call_failed::raise("fstat");

		length = file_stat.st_size;

		if (length == 0)
		{
			// keep old text of message here -  will be assigned a bit later
			(Arg::Gds(isc_random) << "shmem_data->sh_mem_length_mapped is 0").raise();
		}
	}

	// map file to memory
	void* const address = os_utils::mmap(0, length,
		PROT_READ | PROT_WRITE, MAP_SHARED, mainLock->getFd(), 0);

	if ((U_IPTR) address == (U_IPTR) -1)
	{
		system_call_failed::raise("mmap", errno);
	}

	// this class is needed to cleanup mapping in case of error
	class AutoUnmap
	{
	public:
		explicit AutoUnmap(SharedMemoryBase* sm) : sharedMemory(sm)
		{ }

		void success()
		{
			sharedMemory = NULL;
		}

		~AutoUnmap()
		{
			if (sharedMemory)
			{
				sharedMemory->internalUnmap();
			}
		}

	private:
		SharedMemoryBase* sharedMemory;
	};

	AutoUnmap autoUnmap(this);

	sh_mem_header = (MemoryHeader*) address;
	sh_mem_length_mapped = length;
	strcpy(sh_mem_name, filename);

#ifdef HAVE_SHARED_MUTEX_SECTION
#ifdef USE_MUTEX_MAP
	sh_mem_mutex = (mtx*) mapObject(&statusVector, offsetof(MemoryHeader, mhb_mutex), sizeof(mtx));
	if (!sh_mem_mutex)
	{
		system_call_failed::raise("mmap");
	}
#else
	sh_mem_mutex = &sh_mem_header->mhb_mutex;
#endif
#endif // HAVE_SHARED_MUTEX_SECTION


	// Try to get an exclusive lock on the lock file.  This will
	// fail if somebody else has the exclusive or shared lock

	if (mainLock->setlock(&statusVector, FileLock::FLM_TRY_EXCLUSIVE))
	{
		if (trunc_flag)
		{
			FB_UNUSED(os_utils::ftruncate(mainLock->getFd(), length));
			allocFileSpace(mainLock->getFd(), 0, length, NULL);
		}

		if (callback->initialize(this, true))
		{
#ifdef HAVE_SHARED_MUTEX_SECTION

#if (defined(HAVE_PTHREAD_MUTEXATTR_SETPROTOCOL) || defined(USE_ROBUST_MUTEX)) && defined(LINUX)
// glibc in linux does not conform to the posix standard. When there is no RT kernel,
// ENOTSUP is returned not by pthread_mutexattr_setprotocol(), but by
// pthread_mutex_init(). Use a hack to deal with this broken error reporting.
#define BUGGY_LINUX_MUTEX
#endif

			int state = 0;

#ifdef BUGGY_LINUX_MUTEX
			static volatile bool staticBugFlag = false;

			do
			{
				bool bugFlag = staticBugFlag;
#endif

				pthread_mutexattr_t mattr;

				PTHREAD_ERR_RAISE(pthread_mutexattr_init(&mattr));
#ifdef PTHREAD_PROCESS_SHARED
				if (!isSandboxed())
					PTHREAD_ERR_RAISE(pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED));
#else
#error Your system must support PTHREAD_PROCESS_SHARED to use pthread shared futex in Firebird.
#endif

#ifdef HAVE_PTHREAD_MUTEXATTR_SETPROTOCOL
#ifdef BUGGY_LINUX_MUTEX
				if (!bugFlag)
				{
#endif
					int protocolRc = pthread_mutexattr_setprotocol(&mattr, PTHREAD_PRIO_INHERIT);
					if (protocolRc && (protocolRc != ENOTSUP))
					{
						iscLogStatus("Pthread Error", (Arg::Gds(isc_sys_request) <<
							"pthread_mutexattr_setprotocol" << Arg::Unix(protocolRc)).value());
					}
#ifdef BUGGY_LINUX_MUTEX
				}
#endif
#endif // HAVE_PTHREAD_MUTEXATTR_SETPROTOCOL

#ifdef USE_ROBUST_MUTEX
#ifdef BUGGY_LINUX_MUTEX
				if (!bugFlag)
				{
#endif
					LOG_PTHREAD_ERROR(pthread_mutexattr_setrobust(&mattr, PTHREAD_MUTEX_ROBUST));
#ifdef BUGGY_LINUX_MUTEX
				}
#endif
#endif

				memset(sh_mem_mutex->mtx_mutex, 0, sizeof(*(sh_mem_mutex->mtx_mutex)));
				//int state = LOG_PTHREAD_ERROR(pthread_mutex_init(sh_mem_mutex->mtx_mutex, &mattr));
				state = pthread_mutex_init(sh_mem_mutex->mtx_mutex, &mattr);

				if (state
#ifdef BUGGY_LINUX_MUTEX
					&& (state != ENOTSUP || bugFlag)
#endif
					)
				{
					iscLogStatus("Pthread Error", (Arg::Gds(isc_sys_request) <<
						"pthread_mutex_init" << Arg::Unix(state)).value());
				}

				LOG_PTHREAD_ERROR(pthread_mutexattr_destroy(&mattr));

#ifdef BUGGY_LINUX_MUTEX
				if (state == ENOTSUP && !bugFlag)
				{
					staticBugFlag = true;
					continue;
				}

			} while (false);
#endif

			if (state)
			{
				callback->mutexBug(state, "pthread_mutex_init");
				system_call_failed::raise("pthread_mutex_init", state);
			}

#endif // HAVE_SHARED_MUTEX_SECTION

			mainLock->unlock();
			if (!mainLock->setlock(&statusVector, FileLock::FLM_SHARED))
			{
				if (statusVector.hasData())
					status_exception::raise(&statusVector);
				else
					(Arg::Gds(isc_random) << "Unknown error in setlock(SHARED)").raise();
			}
		}
	}
	else
	{
		if (callback->initialize(this, false))
		{
			if (!mainLock->setlock(&statusVector, FileLock::FLM_SHARED))
			{
				if (statusVector.hasData())
					status_exception::raise(&statusVector);
				else
					(Arg::Gds(isc_random) << "Unknown error in setlock(SHARED)").raise();
			}
		}
	}



	autoUnmap.success();
}

#endif // UNIX

#ifdef WIN_NT

// Get name of the file that was mapped into memory at given address.
// This routine should not throw as caller is not ready currently.
static bool getMappedFileName(void* addr, PathName& mappedName)
{
	char* mapName = mappedName.getBuffer(MAXPATHLEN + 1);
	const DWORD mapLen = GetMappedFileName(GetCurrentProcess(), addr, mapName, MAXPATHLEN);
	mappedName.resize(mapLen);
	if (!mapLen)
		//system_call_failed::raise("GetMappedFileName");
		return false;

	char dosDevice[] = {'A', ':', 0};

	DWORD drives = GetLogicalDrives();
	for (; drives; drives >>= 1, dosDevice[0]++)
		if (drives & 1)
		{
			char ntDevice[MAXPATHLEN + 1];
			DWORD ntLen = QueryDosDevice(dosDevice, ntDevice, MAXPATHLEN);

			if (!ntLen)
				//system_call_failed::raise("QueryDosDevice");
				return false;

			ntLen = static_cast<DWORD>(strlen(ntDevice));

			if (ntLen <= mapLen &&
				_memicmp(ntDevice, mapName, ntLen) == 0 &&
				mapName[ntLen] == '\\')
			{
				mappedName.replace(0, ntLen, dosDevice);
				return true;
			}
		}

	return false;
}


void SharedMemoryBase::internalUnmap()
{
	if (!UnmapViewOfFile(sh_mem_header))
		return;

	sh_mem_header = NULL;
	CloseHandle(sh_mem_object);
	CloseHandle(sh_mem_handle);

	CloseHandle(sh_mem_interest);

	if (!UnmapViewOfFile(sh_mem_hdr_address))
		return;

	sh_mem_hdr_address = NULL;
	CloseHandle(sh_mem_hdr_object);

	ISC_mutex_fini(&sh_mem_winMutex);
	sh_mem_mutex = NULL;

	if (sh_mem_unlink)
		unlinkFile();
}

SharedMemoryBase::SharedMemoryBase(const TEXT* filename, ULONG length, IpcObject* cb, bool /*skipLock*/)
  :	sh_mem_mutex(0), sh_mem_length_mapped(0),
	sh_mem_handle(INVALID_HANDLE_VALUE), sh_mem_object(0), sh_mem_interest(0), sh_mem_hdr_object(0),
	sh_mem_hdr_address(0), sh_mem_header(NULL), sh_mem_callback(cb), sh_mem_unlink(false)
{
/**************************************
 *
 *	c t o r		( W I N _ N T )
 *
 **************************************
 *
 * Functional description
 *	Try to map a given file.  If we are the first (i.e. only)
 *	process to map the file, call a given initialization
 *	routine (if given) or punt (leaving the file unmapped).
 *
 **************************************/
	fb_assert(sh_mem_callback);
	sh_mem_name[0] = '\0';

	HANDLE event_handle = 0;
	int retry_count = 0;

	TEXT expanded_filename[MAXPATHLEN];
	iscPrefixLock(expanded_filename, filename, true);

	bool init_flag = false;
	DWORD err = 0;

	// retry to attach to mmapped file if the process initializing dies during initialization.

  retry:
	if (retry_count++ > 0)
		Thread::sleep(10);

	// Create an event that can be used to determine if someone has already
	// initialized shared memory.

	TEXT object_name[MAXPATHLEN];
	if (!make_object_name(object_name, sizeof(object_name), filename, "_event"))
	{
		system_call_failed::raise("make_object_name");
	}

	if (!init_flag)
	{
		event_handle = CreateEvent(ISC_get_security_desc(), TRUE, FALSE, object_name);
		err = GetLastError();
		if (!event_handle)
		{
			system_call_failed::raise("CreateEvent", err);
		}

		init_flag = (err != ERROR_ALREADY_EXISTS);

		SetHandleInformation(event_handle, HANDLE_FLAG_INHERIT, 0);
	}

	// All but the initializer will wait until the event is set.  That
	// is done after initialization is complete.

	if (!init_flag)
	{
		// Wait for 10 seconds.  Then retry

		const DWORD ret_event = WaitForSingleObject(event_handle, 10000);

		// If we timed out, just retry.  It is possible that the
		// process doing the initialization died before setting the event.

		if (ret_event == WAIT_TIMEOUT)
		{
			CloseHandle(event_handle);
			if (retry_count > 10)
				(Arg::Gds(isc_random) << Arg::Str("Wait for shared memory initialization timed out.")).raise();

			goto retry;
		}
	}

	HANDLE file_handle = CreateFile(expanded_filename,
							 GENERIC_READ | GENERIC_WRITE,
							 FILE_SHARE_READ | FILE_SHARE_WRITE,
							 NULL,
							 OPEN_ALWAYS,
							 FILE_ATTRIBUTE_NORMAL,
							 NULL);
	err = GetLastError();

	if (file_handle == INVALID_HANDLE_VALUE)
	{
		if ((err == ERROR_SHARING_VIOLATION) || (err == ERROR_ACCESS_DENIED))
		{
			if (!init_flag) {
				CloseHandle(event_handle);
			}

			if (retry_count < 200)	// 2 sec
				goto retry;
		}

		CloseHandle(event_handle);
		system_call_failed::raise("CreateFile", err);
	}

	if (init_flag)
	{
		if (err == ERROR_ALREADY_EXISTS)
		{
			const DWORD file_size = GetFileSize(file_handle, NULL);
			if (file_size == INVALID_FILE_SIZE)
			{
				err = GetLastError();
				CloseHandle(event_handle);
				CloseHandle(file_handle);
				system_call_failed::raise("GetFileSize", err);
			}

			if (length == 0)
			{
				length = file_size;
			}
			else if (file_size &&
					 SetFilePointer(file_handle, 0, NULL, FILE_BEGIN) == INVALID_SET_FILE_POINTER ||
					 !SetEndOfFile(file_handle) || !FlushFileBuffers(file_handle))
			{
				err = GetLastError();
				CloseHandle(file_handle);

				if (err == ERROR_USER_MAPPED_FILE)
				{
					if (retry_count < 50)	// 0.5 sec
						goto retry;

					CloseHandle(event_handle);
					Arg::Gds(isc_instance_conflict).raise();
				}
				else
				{
					CloseHandle(event_handle);
					system_call_failed::raise("SetFilePointer", err);
				}
			}
		}

		if (length == 0)
		{
			CloseHandle(event_handle);
			CloseHandle(file_handle);

			if (err == 0)
			{
				strcpy(sh_mem_name, filename);
				unlinkFile();
			}

			(Arg::Gds(isc_random) << Arg::Str("File for memory mapping is empty.")).raise();
		}

		LARGE_INTEGER offset;
		offset.QuadPart = length;

		if (SetFilePointer(file_handle, offset.LowPart, &offset.HighPart, FILE_BEGIN) == INVALID_SET_FILE_POINTER ||
			!SetEndOfFile(file_handle) || !FlushFileBuffers(file_handle))
		{
			err = GetLastError();
			CloseHandle(event_handle);
			CloseHandle(file_handle);
			system_call_failed::raise("SetFilePointer", err);
		}
	}

	// Create a file mapping object that will be used to make remapping possible.
	// The current length of real mapped file and its name are saved in it.

	if (!make_object_name(object_name, sizeof(object_name), filename, "_mapping"))
	{
		err = GetLastError();
		CloseHandle(event_handle);
		CloseHandle(file_handle);
		system_call_failed::raise("make_object_name", err);
	}

	HANDLE header_obj = CreateFileMapping(INVALID_HANDLE_VALUE,
										  ISC_get_security_desc(),
										  PAGE_READWRITE,
										  0, 2 * sizeof(ULONG),
										  object_name);
	err = GetLastError();
	if (header_obj == NULL)
	{
		CloseHandle(event_handle);
		CloseHandle(file_handle);
		system_call_failed::raise("CreateFileMapping", err);
	}

	if (!init_flag && err != ERROR_ALREADY_EXISTS)
	{
		// We have made header_obj but we are not initializing.
		// Previous owner is closed and clear all header_data.
		// One need to retry.
		CloseHandle(header_obj);
		CloseHandle(event_handle);
		CloseHandle(file_handle);
		goto retry;
	}

	SetHandleInformation(header_obj, HANDLE_FLAG_INHERIT, 0);

	ULONG* const header_address = (ULONG*) MapViewOfFile(header_obj, FILE_MAP_WRITE, 0, 0, 0);

	if (header_address == NULL)
	{
		err = GetLastError();
		CloseHandle(header_obj);
		CloseHandle(event_handle);
		CloseHandle(file_handle);
		system_call_failed::raise("MapViewOfFile", err);
	}

	// Set or get the true length of the file depending on whether or not we are the first user.

	if (init_flag)
	{
		header_address[0] = length;
		header_address[1] = 0;
	}
	else if (header_address[0] == 0)
	{
		UnmapViewOfFile(header_address);
		CloseHandle(header_obj);
		CloseHandle(event_handle);
		CloseHandle(file_handle);
		goto retry;
	}
	else
		length = header_address[0];

	fb_assert(length);

	// Create the real file mapping object.

	TEXT mapping_name[64]; // enough for int32 as text
	snprintf(mapping_name, sizeof(mapping_name), "_mapping_%" ULONGFORMAT, header_address[1]);

	if (!make_object_name(object_name, sizeof(object_name), filename, mapping_name))
	{
		err = GetLastError();
		UnmapViewOfFile(header_address);
		CloseHandle(header_obj);
		CloseHandle(event_handle);
		CloseHandle(file_handle);
		system_call_failed::raise("make_object_name", err);
	}

	HANDLE file_obj = CreateFileMapping(file_handle,
										ISC_get_security_desc(),
										PAGE_READWRITE,
										0, length,
										object_name);
	if (file_obj == NULL)
	{
		err = GetLastError();
		UnmapViewOfFile(header_address);
		CloseHandle(header_obj);
		CloseHandle(event_handle);
		CloseHandle(file_handle);
		system_call_failed::raise("CreateFileMapping", err);
	}

	SetHandleInformation(file_obj, HANDLE_FLAG_INHERIT, 0);

	UCHAR* const address = (UCHAR*) MapViewOfFile(file_obj, FILE_MAP_WRITE, 0, 0, 0);

	if (address == NULL)
	{
		err = GetLastError();
		CloseHandle(file_obj);
		UnmapViewOfFile(header_address);
		CloseHandle(header_obj);
		CloseHandle(event_handle);
		CloseHandle(file_handle);
		system_call_failed::raise("MapViewOfFile", err);
	}

	PathName mappedName;
	if (!getMappedFileName(address, mappedName) || mappedName != expanded_filename)
	{
		UnmapViewOfFile(address);
		CloseHandle(file_obj);
		UnmapViewOfFile(header_address);
		CloseHandle(header_obj);
		CloseHandle(event_handle);
		CloseHandle(file_handle);

		gds__log("Wrong file for memory mapping:\n"
				 "\t      expected %s\n"
				 "\talready mapped %s\n"
				 "\tCheck for presence of another Firebird instance with different lock directory",
				 expanded_filename, mappedName.c_str());

		(Arg::Gds(isc_random) << Arg::Str("Wrong file for memory mapping, see details in firebird.log")).raise();
	}

	ISC_mutex_init(&sh_mem_winMutex, filename);
	sh_mem_mutex = &sh_mem_winMutex;

	sh_mem_header = (MemoryHeader*) address;
	sh_mem_length_mapped = length;
	sh_mem_handle = file_handle;
	sh_mem_object = file_obj;
	sh_mem_interest = event_handle;
	sh_mem_hdr_object = header_obj;
	sh_mem_hdr_address = header_address;
	strcpy(sh_mem_name, filename);

	try
	{
		sh_mem_callback->initialize(this, init_flag);
	}
	catch (const Exception&)
	{
		internalUnmap();
		throw;
	}

	if (init_flag)
	{
		if (!FlushViewOfFile(address, 0))
		{
			err = GetLastError();
			internalUnmap();
			system_call_failed::raise("FlushViewOfFile", err);
		}

		SetEvent(event_handle);
	}
}
#endif


#ifdef HAVE_MMAP

UCHAR* SharedMemoryBase::mapObject(CheckStatusWrapper* statusVector, ULONG object_offset, ULONG object_length)
{
/**************************************
 *
 *	I S C _ m a p _ o b j e c t
 *
 **************************************
 *
 * Functional description
 *	Try to map an object given a file mapping.
 *
 **************************************/

	// Get system page size as this is the unit of mapping.

#ifdef SOLARIS
	const long ps = sysconf(_SC_PAGESIZE);
	if (ps == -1)
	{
		error(statusVector, "sysconf", errno);
		return NULL;
	}
#else
	const int ps = getpagesize();
	if (ps == -1)
	{
		error(statusVector, "getpagesize", errno);
		return NULL;
	}
#endif
	const ULONG page_size = (ULONG) ps;

	// Compute the start and end page-aligned offsets which contain the object being mapped.

	const ULONG start = (object_offset / page_size) * page_size;
	const ULONG end = FB_ALIGN(object_offset + object_length, page_size);
	const ULONG length = end - start;

	UCHAR* address = (UCHAR*) os_utils::mmap(0, length,
		PROT_READ | PROT_WRITE, MAP_SHARED, mainLock->getFd(), start);

	if ((U_IPTR) address == (U_IPTR) -1)
	{
		error(statusVector, "mmap", errno);
		return NULL;
	}

	// Return the virtual address of the mapped object.

	IPC_TRACE(("ISC_map_object in %p to %p %p\n", shmem_data->sh_mem_address, address, address + length));

	return address + (object_offset - start);
}


void SharedMemoryBase::unmapObject(CheckStatusWrapper* statusVector, UCHAR** object_pointer, ULONG object_length)
{
/**************************************
 *
 *	I S C _ u n m a p _ o b j e c t
 *
 **************************************
 *
 * Functional description
 *	Try to unmap an object given a file mapping.
 *	Zero the object pointer after a successful unmap.
 *
 **************************************/
	// Get system page size as this is the unit of mapping.

#ifdef SOLARIS
	const long ps = sysconf(_SC_PAGESIZE);
	if (ps == -1)
	{
		error(statusVector, "sysconf", errno);
		return;
	}
#else
	const int ps = getpagesize();
	if (ps == -1)
	{
		error(statusVector, "getpagesize", errno);
		return;
	}
#endif
	const size_t page_size = (ULONG) ps;

	// Compute the start and end page-aligned addresses which contain the mapped object.

	char* const start = (char*) ((U_IPTR) (*object_pointer) & ~(page_size - 1));
	char* const end =
		(char*) ((U_IPTR) ((*object_pointer + object_length) + (page_size - 1)) & ~(page_size - 1));
	const size_t length = end - start;

	if (munmap(start, length) == -1)
	{
		error(statusVector, "munmap", errno);
		return; // false;
	}

	*object_pointer = NULL;
	return; // true;
}
#endif // HAVE_MMAP


#ifdef WIN_NT

UCHAR* SharedMemoryBase::mapObject(CheckStatusWrapper* statusVector,
								   ULONG object_offset,
								   ULONG object_length)
{
/**************************************
 *
 *	I S C _ m a p _ o b j e c t
 *
 **************************************
 *
 * Functional description
 *	Try to map an object given a file mapping.
 *
 **************************************/

	SYSTEM_INFO sys_info;
	GetSystemInfo(&sys_info);
	const ULONG page_size = sys_info.dwAllocationGranularity;

	// Compute the start and end page-aligned offsets which
	// contain the object being mapped.

	const ULONG start = (object_offset / page_size) * page_size;
	const ULONG end = FB_ALIGN(object_offset + object_length, page_size);
	const ULONG length = end - start;
	const HANDLE handle = sh_mem_object;

	UCHAR* address = (UCHAR*) MapViewOfFile(handle, FILE_MAP_WRITE, 0, start, length);

	if (address == NULL)
	{
		error(statusVector, "MapViewOfFile", GetLastError());
		return NULL;
	}

	// Return the virtual address of the mapped object.

	return (address + (object_offset - start));
}


void SharedMemoryBase::unmapObject(CheckStatusWrapper* statusVector,
								   UCHAR** object_pointer, ULONG /*object_length*/)
{
/**************************************
 *
 *	I S C _ u n m a p _ o b j e c t
 *
 **************************************
 *
 * Functional description
 *	Try to unmap an object given a file mapping.
 *	Zero the object pointer after a successful unmap.
 *
 **************************************/
	SYSTEM_INFO sys_info;
	GetSystemInfo(&sys_info);
	const size_t page_size = sys_info.dwAllocationGranularity;

	// Compute the start and end page-aligned offsets which
	// contain the object being mapped.

	const UCHAR* start = (UCHAR*) ((U_IPTR) *object_pointer & ~(page_size - 1));
	if (!UnmapViewOfFile(start))
	{
		error(statusVector, "UnmapViewOfFile", GetLastError());
		return;
	}

	*object_pointer = NULL;
}
#endif


#ifdef WIN_NT

static constexpr LPCSTR FAST_MUTEX_EVT_NAME	= "%s_FM_EVT";
static constexpr LPCSTR FAST_MUTEX_MAP_NAME	= "%s_FM_MAP";

static constexpr int DEFAULT_INTERLOCKED_SPIN_COUNT	= 0;
static constexpr int DEFAULT_INTERLOCKED_SPIN_COUNT_SMP	= 200;

static SLONG pid = 0;

typedef WINBASEAPI BOOL (WINAPI *pfnSwitchToThread) ();
static inline BOOL switchToThread()
{
	static pfnSwitchToThread fnSwitchToThread = NULL;
	static bool bInit = false;

	if (!bInit)
	{
		HMODULE hLib = GetModuleHandle("kernel32.dll");
		if (hLib)
			fnSwitchToThread = (pfnSwitchToThread) GetProcAddress(hLib, "SwitchToThread");

		bInit = true;
	}

	BOOL res = FALSE;

	if (fnSwitchToThread)
	{
		const HANDLE hThread = GetCurrentThread();
		SetThreadPriority(hThread, THREAD_PRIORITY_ABOVE_NORMAL);

		res = (*fnSwitchToThread)();

		SetThreadPriority(hThread, THREAD_PRIORITY_NORMAL);
	}

	return res;
}


// MinGW has the wrong declaration for the operating system function.
#if defined __GNUC__
// Cast away volatile
#define FIX_TYPE(arg) const_cast<LPLONG>(arg)
#else
#define FIX_TYPE(arg) arg
#endif


static inline void lockSharedSection(volatile FAST_MUTEX_SHARED_SECTION* lpSect, ULONG SpinCount)
{
	while (InterlockedExchange(FIX_TYPE(&lpSect->lSpinLock), 1) != 0)
	{
		ULONG j = SpinCount;
		while (j != 0)
		{
			if (lpSect->lSpinLock == 0)
				goto next;
			j--;
		}
		switchToThread();
next:;
	}
}

static inline bool tryLockSharedSection(volatile FAST_MUTEX_SHARED_SECTION* lpSect)
{
	return (InterlockedExchange(FIX_TYPE(&lpSect->lSpinLock), 1) == 0);
}

static inline void unlockSharedSection(volatile FAST_MUTEX_SHARED_SECTION* lpSect)
{
	InterlockedExchange(FIX_TYPE(&lpSect->lSpinLock), 0);
}

static DWORD enterFastMutex(FAST_MUTEX* lpMutex, DWORD dwMilliseconds)
{
	volatile FAST_MUTEX_SHARED_SECTION* lpSect = lpMutex->lpSharedInfo;

	while (true)
	{
		if (dwMilliseconds == 0)
		{
			if (!tryLockSharedSection(lpSect))
				return WAIT_TIMEOUT;
		}
		else {
			lockSharedSection(lpSect, lpMutex->lSpinCount);
		}

		if (lpSect->lAvailable > 0)
		{
			lpSect->lAvailable--;
			lpSect->lOwnerPID = pid;
#ifdef DEV_BUILD
			lpSect->lThreadId = GetCurrentThreadId();
#endif
			unlockSharedSection(lpSect);
			return WAIT_OBJECT_0;
		}

		if (dwMilliseconds == 0)
		{
			unlockSharedSection(lpSect);
			return WAIT_TIMEOUT;
		}

		InterlockedIncrement(FIX_TYPE(&lpSect->lThreadsWaiting));
		unlockSharedSection(lpSect);

		// TODO actual timeout can be of any length
		const DWORD tm = (dwMilliseconds == INFINITE || dwMilliseconds > 5000) ? 5000 : dwMilliseconds;
		const DWORD dwResult = WaitForSingleObject(lpMutex->hEvent, tm);

		InterlockedDecrement(FIX_TYPE(&lpSect->lThreadsWaiting));

		if (dwMilliseconds != INFINITE)
			dwMilliseconds -= tm;

//		if (dwResult != WAIT_OBJECT_0)
//			return dwResult;

		if (dwResult == WAIT_OBJECT_0)
			continue;
		if (dwResult == WAIT_ABANDONED)
			return dwResult;
		if (dwResult == WAIT_TIMEOUT && !dwMilliseconds)
			return dwResult;

		lockSharedSection(lpSect, lpMutex->lSpinCount);
		if (lpSect->lOwnerPID > 0 && !lpSect->lAvailable)
		{
			if (!ISC_check_process_existence(lpSect->lOwnerPID))
			{
#ifdef DEV_BUILD
				gds__log("enterFastMutex: dead process detected, pid = %d", lpSect->lOwnerPID);
				lpSect->lThreadId = 0;
#endif
				lpSect->lOwnerPID = 0;
				lpSect->lAvailable++;
			}
		}
		unlockSharedSection(lpSect);
	}
}

static bool leaveFastMutex(FAST_MUTEX* lpMutex)
{
	volatile FAST_MUTEX_SHARED_SECTION* lpSect = lpMutex->lpSharedInfo;

	lockSharedSection(lpSect, lpMutex->lSpinCount);
	if (lpSect->lAvailable >= 1)
	{
		unlockSharedSection(lpSect);
		SetLastError(ERROR_INVALID_PARAMETER);
		return false;
	}
	lpSect->lAvailable++;
	if (lpSect->lThreadsWaiting)
		SetEvent(lpMutex->hEvent);
	fb_assert(lpSect->lOwnerPID == pid);
	lpSect->lOwnerPID = -lpSect->lOwnerPID;
#ifdef DEV_BUILD
	fb_assert(lpSect->lThreadId == GetCurrentThreadId());
	lpSect->lThreadId = -lpSect->lThreadId;
#endif
	unlockSharedSection(lpSect);

	return true;
}

static inline void deleteFastMutex(FAST_MUTEX* lpMutex)
{
	UnmapViewOfFile((FAST_MUTEX_SHARED_SECTION*)lpMutex->lpSharedInfo);
	CloseHandle(lpMutex->hFileMap);
	CloseHandle(lpMutex->hEvent);
}

static inline void setupMutex(FAST_MUTEX* lpMutex)
{
	SYSTEM_INFO si;
	GetSystemInfo(&si);

	if (si.dwNumberOfProcessors > 1)
		lpMutex->lSpinCount = DEFAULT_INTERLOCKED_SPIN_COUNT_SMP;
	else
		lpMutex->lSpinCount = DEFAULT_INTERLOCKED_SPIN_COUNT;
}

static bool initializeFastMutex(FAST_MUTEX* lpMutex, LPSECURITY_ATTRIBUTES lpAttributes,
								BOOL bInitialState, LPCSTR lpName)
{
	if (pid == 0)
		pid = GetCurrentProcessId();

	LPCSTR name = lpName;

	if (lpName && strlen(lpName) + strlen(FAST_MUTEX_EVT_NAME) - 2 >= MAXPATHLEN)
	{
		// this is the same error which CreateEvent will return for long name
		SetLastError(ERROR_FILENAME_EXCED_RANGE);
		return false;
	}

	setupMutex(lpMutex);

	char sz[MAXPATHLEN];
	if (lpName)
	{
		snprintf(sz, sizeof(sz), FAST_MUTEX_EVT_NAME, lpName);
		name = sz;
	}

#ifdef DONT_USE_FAST_MUTEX
	lpMutex->lpSharedInfo = NULL;
	lpMutex->hEvent = CreateMutex(lpAttributes, bInitialState, name);
	return (lpMutex->hEvent != NULL);
#else
	lpMutex->hEvent = CreateEvent(lpAttributes, FALSE, FALSE, name);
	DWORD dwLastError = GetLastError();

	if (lpMutex->hEvent)
	{
		SetHandleInformation(lpMutex->hEvent, HANDLE_FLAG_INHERIT, 0);

		if (lpName)
			snprintf(sz, sizeof(sz), FAST_MUTEX_MAP_NAME, lpName);

		lpMutex->hFileMap = CreateFileMapping(
			INVALID_HANDLE_VALUE,
			lpAttributes,
			PAGE_READWRITE,
			0,
			sizeof(FAST_MUTEX_SHARED_SECTION),
			name);

		dwLastError = GetLastError();

		if (lpMutex->hFileMap)
		{
			SetHandleInformation(lpMutex->hFileMap, HANDLE_FLAG_INHERIT, 0);

			lpMutex->lpSharedInfo = (FAST_MUTEX_SHARED_SECTION*)
				MapViewOfFile(lpMutex->hFileMap, FILE_MAP_WRITE, 0, 0, 0);

			if (lpMutex->lpSharedInfo)
			{
				if (dwLastError != ERROR_ALREADY_EXISTS)
				{
					lpMutex->lpSharedInfo->lSpinLock = 0;
					lpMutex->lpSharedInfo->lThreadsWaiting = 0;
					lpMutex->lpSharedInfo->lAvailable = bInitialState ? 0 : 1;
					lpMutex->lpSharedInfo->lOwnerPID = bInitialState ? pid : 0;
#ifdef DEV_BUILD
					lpMutex->lpSharedInfo->lThreadId = bInitialState ? GetCurrentThreadId() : 0;
#endif
					InterlockedExchange(FIX_TYPE(&lpMutex->lpSharedInfo->fInitialized), 1);
				}
				else
				{
					while (!lpMutex->lpSharedInfo->fInitialized)
						switchToThread();
				}

				SetLastError(dwLastError);
				return true;
			}
			CloseHandle(lpMutex->hFileMap);
		}
		CloseHandle(lpMutex->hEvent);
	}

	SetLastError(dwLastError);
	return false;
#endif // DONT_USE_FAST_MUTEX
}

static inline void setFastMutexSpinCount(FAST_MUTEX* lpMutex, ULONG SpinCount)
{
	lpMutex->lSpinCount = SpinCount;
}


int ISC_mutex_init(struct mtx* mutex, const TEXT* mutex_name)
{
/**************************************
 *
 *	I S C _ m u t e x _ i n i t	( W I N _ N T )
 *
 **************************************
 *
 * Functional description
 *	Initialize a mutex.
 *
 **************************************/
	char name_buffer[MAXPATHLEN];

	if (!make_object_name(name_buffer, sizeof(name_buffer), mutex_name, "_mutex"))
	{
		return FB_FAILURE;
	}

	if (initializeFastMutex(&mutex->mtx_fast, ISC_get_security_desc(), FALSE, name_buffer))
		return FB_SUCCESS;

	fb_assert(GetLastError() != 0);
	return GetLastError();
}


void ISC_mutex_fini(struct mtx *mutex)
{
/**************************************
 *
 *	m u t e x _ f i n i ( W I N _ N T )
 *
 **************************************
 *
 * Functional description
 *	Destroy a mutex.
 *
 **************************************/
	if (mutex->mtx_fast.lpSharedInfo)
		deleteFastMutex(&mutex->mtx_fast);
}


int ISC_mutex_lock(struct mtx* mutex)
{
/**************************************
 *
 *	I S C _ m u t e x _ l o c k	( W I N _ N T )
 *
 **************************************
 *
 * Functional description
 *	Seize a mutex.
 *
 **************************************/

	const DWORD status = (mutex->mtx_fast.lpSharedInfo) ?
		enterFastMutex(&mutex->mtx_fast, INFINITE) :
			WaitForSingleObject(mutex->mtx_fast.hEvent, INFINITE);

    return (status == WAIT_OBJECT_0 || status == WAIT_ABANDONED) ? FB_SUCCESS : FB_FAILURE;
}


int ISC_mutex_lock_cond(struct mtx* mutex)
{
/**************************************
 *
 *	I S C _ m u t e x _ l o c k _ c o n d	( W I N _ N T )
 *
 **************************************
 *
 * Functional description
 *	Conditionally seize a mutex.
 *
 **************************************/

	const DWORD status = (mutex->mtx_fast.lpSharedInfo) ?
		enterFastMutex(&mutex->mtx_fast, 0) : WaitForSingleObject(mutex->mtx_fast.hEvent, 0L);

    return (status == WAIT_OBJECT_0 || status == WAIT_ABANDONED) ? FB_SUCCESS : FB_FAILURE;
}


int ISC_mutex_unlock(struct mtx* mutex)
{
/**************************************
 *
 *	I S C _ m u t e x _ u n l o c k		( W I N _ N T )
 *
 **************************************
 *
 * Functional description
 *	Release a mutex.
 *
 **************************************/

	if (mutex->mtx_fast.lpSharedInfo) {
		return !leaveFastMutex(&mutex->mtx_fast);
	}

	return !ReleaseMutex(mutex->mtx_fast.hEvent);
}


void ISC_mutex_set_spin_count (struct mtx *mutex, ULONG spins)
{
	if (mutex->mtx_fast.lpSharedInfo)
		setFastMutexSpinCount(&mutex->mtx_fast, spins);
}

#endif // WIN_NT


#ifdef UNIX
#ifdef HAVE_MMAP
#define ISC_REMAP_FILE_DEFINED
bool SharedMemoryBase::remapFile(CheckStatusWrapper* statusVector, ULONG new_length, bool flag)
{
/**************************************
 *
 *	I S C _ r e m a p _ f i l e		( U N I X - m m a p )
 *
 **************************************
 *
 * Functional description
 *	Try to re-map a given file.
 *
 **************************************/
	if (!new_length)
	{
		error(statusVector, "Zero new_length is requested", 0);
		return false;
	}

	if (flag)
	{
		FB_UNUSED(os_utils::ftruncate(mainLock->getFd(), new_length));

		if (new_length > sh_mem_length_mapped)
		{
			if (!allocFileSpace(mainLock->getFd(), sh_mem_length_mapped,
				new_length - sh_mem_length_mapped, statusVector))
			{
				return false;
			}
		}
	}

	MemoryHeader* const address = (MemoryHeader*) os_utils::mmap(0, new_length,
		PROT_READ | PROT_WRITE, MAP_SHARED, mainLock->getFd(), 0);

	if ((U_IPTR) address == (U_IPTR) -1)
	{
		error(statusVector, "mmap() failed", errno);
		return false;
	}

	munmap(sh_mem_header, sh_mem_length_mapped);

	IPC_TRACE(("ISC_remap_file %p to %p %d\n", sh_mem_header, address, new_length));

	sh_mem_header = (MemoryHeader*) address;
	sh_mem_length_mapped = new_length;

#if defined(HAVE_SHARED_MUTEX_SECTION) && !defined(USE_MUTEX_MAP)
	sh_mem_mutex = &sh_mem_header->mhb_mutex;
#endif

	return address;
}
#endif // HAVE_MMAP
#endif // UNIX


#ifdef WIN_NT
#define ISC_REMAP_FILE_DEFINED
bool SharedMemoryBase::remapFile(CheckStatusWrapper* statusVector,
								 ULONG new_length, bool flag)
{
/**************************************
 *
 *	I S C _ r e m a p _ f i l e		( W I N _ N T )
 *
 **************************************
 *
 * Functional description
 *	Try to re-map a given file.
 *
 **************************************/

	if (flag)
	{
		LARGE_INTEGER offset;
		offset.QuadPart = new_length;

		if (SetFilePointer(sh_mem_handle, offset.LowPart, &offset.HighPart, FILE_BEGIN) == INVALID_SET_FILE_POINTER ||
			!SetEndOfFile(sh_mem_handle) ||
			!FlushViewOfFile(sh_mem_header, 0))
		{
			error(statusVector, "SetFilePointer", GetLastError());
			return false;
		}
	}

	/* If the remap file exists, remap does not occur correctly.
	* The file number is local to the process and when it is
	* incremented and a new filename is created, that file may
	* already exist.  In that case, the file is not expanded.
	* This will happen when the file is expanded more than once
	* by concurrently running processes.
	*
	* The problem will be fixed by making sure that a new file name
	* is generated with the mapped file is created.
	*/

	HANDLE file_obj = NULL;

	while (true)
	{
		TEXT mapping_name[64]; // enough for int32 as text
		snprintf(mapping_name, sizeof(mapping_name), "_mapping_%" ULONGFORMAT, sh_mem_hdr_address[1] + 1);

		TEXT object_name[MAXPATHLEN];
		if (!make_object_name(object_name, sizeof(object_name), sh_mem_name, mapping_name))
			break;

		file_obj = CreateFileMapping(sh_mem_handle,
									 ISC_get_security_desc(),
									 PAGE_READWRITE,
									 0, new_length,
									 object_name);

		if (!(GetLastError() == ERROR_ALREADY_EXISTS && flag))
			break;

		CloseHandle(file_obj);
		file_obj = NULL;
		sh_mem_hdr_address[1]++;
	}

	if (file_obj == NULL)
	{
		error(statusVector, "CreateFileMapping", GetLastError());
		return false;
	}

	MemoryHeader* const address = (MemoryHeader*) MapViewOfFile(file_obj, FILE_MAP_WRITE, 0, 0, 0);

	if (!address)
	{
		error(statusVector, "MapViewOfFile", GetLastError());
		CloseHandle(file_obj);
		return false;
	}

	if (flag)
	{
		sh_mem_hdr_address[0] = new_length;
		sh_mem_hdr_address[1]++;
	}

	UnmapViewOfFile(sh_mem_header);
	CloseHandle(sh_mem_object);

	sh_mem_header = (MemoryHeader*) address;
	sh_mem_length_mapped = new_length;
	sh_mem_object = file_obj;

	if (!sh_mem_length_mapped)
	{
		error(statusVector, "sh_mem_length_mapped is 0", 0);
		return false;
	}

	return address != nullptr;
}
#endif


#ifndef ISC_REMAP_FILE_DEFINED
bool SharedMemoryBase::remapFile(CheckStatusWrapper* statusVector, ULONG, bool)
{
/**************************************
 *
 *	I S C _ r e m a p _ f i l e		( G E N E R I C )
 *
 **************************************
 *
 * Functional description
 *	Try to re-map a given file.
 *
 **************************************/

	(Arg::Gds(isc_unavailable) <<
		Arg::Gds(isc_random) << "SharedMemory::remapFile").copyTo(statusVector);

	return NULL;
}
#endif


#ifdef WIN_NT
static bool make_object_name(TEXT* buffer, size_t bufsize,
							 const TEXT* object_name,
							 const TEXT* object_type)
{
/**************************************
 *
 *	m a k e _ o b j e c t _ n a m e		( W I N _ N T )
 *
 **************************************
 *
 * Functional description
 *	Create an object name from a name and type.
 *	Also replace the file separator with "_".
 *
 **************************************/
	char hostname[64];
	const int rc = snprintf(buffer, bufsize, object_name, ISC_get_host(hostname, sizeof(hostname)));
	if (size_t(rc) == bufsize || rc <= 0)
	{
		SetLastError(ERROR_FILENAME_EXCED_RANGE);
		return false;
	}

	char& limit = buffer[bufsize - 1];
	limit = 0;

	char* p;
	char c;
	for (p = buffer; c = *p; p++)
	{
		if (c == '/' || c == '\\' || c == ':')
			*p = '_';
	}

	// We either append the full object type or produce failure.
	if (p >= &limit || p + strlen(object_type) > &limit)
	{
		SetLastError(ERROR_FILENAME_EXCED_RANGE);
		return false;
	}

	strcpy(p, object_type);

	// hvlad: windows file systems use case-insensitive file names
	// while kernel objects such as events use case-sensitive names.
	// Since we use root directory as part of kernel objects names
	// we must use lower (or upper) register for object name to avoid
	// misunderstanding between processes
	strlwr(buffer);

	// CVC: I'm not convinced that if this call has no space to put the prefix,
	// we can ignore that fact, hence I changed that signature, too.
	if (!fb_utils::private_kernel_object_name(buffer, bufsize))
	{
		SetLastError(ERROR_FILENAME_EXCED_RANGE);
		return false;
	}
	return true;
}
#endif // WIN_NT


void SharedMemoryBase::mutexLock()
{
#if defined(WIN_NT)

	int state = ISC_mutex_lock(sh_mem_mutex);

#else // POSIX SHARED MUTEX

	int state = pthread_mutex_lock(sh_mem_mutex->mtx_mutex);
#ifdef USE_ROBUST_MUTEX
	if (state == EOWNERDEAD)
	{
		// We always perform check for dead process
		// Therefore may safely mark mutex as recovered
		LOG_PTHREAD_ERROR(pthread_mutex_consistent(sh_mem_mutex->mtx_mutex));
		state = 0;
	}
#endif

#endif // os-dependent choice

	if (state != 0)
	{
		sh_mem_callback->mutexBug(state, "mutexLock");
	}
}


bool SharedMemoryBase::mutexLockCond()
{
#if defined(WIN_NT)

	return ISC_mutex_lock_cond(sh_mem_mutex) == 0;

#else // POSIX SHARED MUTEX

	int state = pthread_mutex_trylock(sh_mem_mutex->mtx_mutex);
#ifdef USE_ROBUST_MUTEX
	if (state == EOWNERDEAD)
	{
		// We always perform check for dead process
		// Therefore may safely mark mutex as recovered
		LOG_PTHREAD_ERROR(pthread_mutex_consistent(sh_mem_mutex->mtx_mutex));
		state = 0;
	}
#endif
	return state == 0;

#endif // os-dependent choice

}


void SharedMemoryBase::mutexUnlock()
{
#if defined(WIN_NT)

	int state = ISC_mutex_unlock(sh_mem_mutex);

#else // POSIX SHARED MUTEX

	int state = pthread_mutex_unlock(sh_mem_mutex->mtx_mutex);

#endif // os-dependent choice

	if (state != 0)
	{
		sh_mem_callback->mutexBug(state, "mutexUnlock");
	}
}


SharedMemoryBase::~SharedMemoryBase()
{
/**************************************
 *
 *	I S C _ u n m a p _ f i l e
 *
 **************************************
 *
 * Functional description
 *	Unmap a given file.
 *
 **************************************/


#if defined(HAVE_SHARED_MUTEX_SECTION) && defined(USE_MUTEX_MAP)
	LocalStatus ls;
	CheckStatusWrapper statusVector(&ls);
	unmapObject(&statusVector, (UCHAR**) &sh_mem_mutex, sizeof(mtx));
	if (statusVector.hasData())
	{
		iscLogStatus("unmapObject failed", &statusVector);
	}
#endif

	internalUnmap();
}

void SharedMemoryBase::logError(const char* text, const CheckStatusWrapper* status)
{
	iscLogStatus(text, status);
}


static bool event_blocked(const event_t* event, const SLONG value)
{
/**************************************
 *
 *	e v e n t _ b l o c k e d
 *
 **************************************
 *
 * Functional description
 *	If a wait would block, return true.
 *
 **************************************/

	if (event->event_count >= value)
	{
#ifdef DEBUG_ISC_SYNC
		printf("event_blocked: FALSE (eg something to report)\n");
		fflush(stdout);
#endif
		return false;
	}

#ifdef DEBUG_ISC_SYNC
	printf("event_blocked: TRUE (eg nothing happened yet)\n");
	fflush(stdout);
#endif
	return true;
}


static void error(CheckStatusWrapper* statusVector, const TEXT* string, ISC_STATUS status)
{
/**************************************
 *
 *	e r r o r
 *
 **************************************
 *
 * Functional description
 *	We've encountered an error, report it.
 *
 **************************************/
	(Arg::StatusVector(statusVector) <<
		Arg::Gds(isc_sys_request) << string << SYS_ERR(status)).copyTo(statusVector);
}
