/*
 *	PROGRAM:	Firebird Trace Services
 *	MODULE:		TraceConfigStorage.h
 *	DESCRIPTION:	Trace API shared configurations storage
 *
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
 *  The Original Code was created by Khorsun Vladyslav
 *  for the Firebird Open Source RDBMS project.
 *
 *  Copyright (c) 2008 Khorsun Vladyslav <hvlad@users.sourceforge.net>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 *
 */

#ifndef JRD_TRACECONFIGSTORAGE_H
#define JRD_TRACECONFIGSTORAGE_H

#include "../../common/classes/array.h"
#include "../../common/classes/fb_string.h"
#include "../../common/classes/init.h"
#include "../../common/isc_s_proto.h"
#include "../../common/ThreadStart.h"
#include "../../jrd/trace/TraceSession.h"
#include "../../common/classes/RefCounted.h"
#include <atomic>

namespace Jrd {

/*
  Session ID and flags are stored within slot itself.
  Length of session data could be less than or equal of slot size.
  Slots are sorted by session id.
  Slot for new session is always last slot.
  When session is removed its slot is marked as unused.
  Unused slot could be reused: slot itself moved at last position in slots array,
  higher slots are moved down on its former place, slot data is not moved.
  Slot is reused with best-fit algorithm.
*/

class StorageGuard;

struct TraceCSHeader : public Firebird::MemoryHeader
{
	static const USHORT TRACE_STORAGE_VERSION = 2;
	static const USHORT TRACE_STORAGE_MAX_SLOTS = 1000;
	static const ULONG TRACE_STORAGE_MIN_SIZE = 64 * 1024;
	static const ULONG TRACE_STORAGE_MAX_SIZE = 16 * 1024 * 1024;

	struct Slot
	{
		ULONG offset;
		ULONG size;
		ULONG used;
		ULONG ses_id;
		ULONG ses_flags;
		ULONG ses_pid;
	};

	std::atomic<ULONG> change_number;
	std::atomic<ULONG> session_number;
	ULONG cnt_uses;
	ULONG mem_max_size;			// maximum allowed mapping size
	ULONG mem_allocated;		// currently mapped memory
	ULONG mem_used;				// used memory
	ULONG mem_offset;			// never used free space begins here
	ULONG slots_free;			// number of unused slots
	ULONG slots_cnt;			// number of slots in use
	Slot slots[TRACE_STORAGE_MAX_SLOTS];
};

static_assert(sizeof(TraceCSHeader) < TraceCSHeader::TRACE_STORAGE_MIN_SIZE,
			  "TraceCSHeader not fits TRACE_STORAGE_MIN_SIZE");


class ConfigStorage final : public Firebird::GlobalStorage, public Firebird::IpcObject, public Firebird::Reasons
{
public:
	enum GET_FLAGS {ALL, FLAGS, AUTH};

	ConfigStorage();
	~ConfigStorage();

	void addSession(Firebird::TraceSession& session);
	void updateFlags(Firebird::TraceSession& session);
	void removeSession(ULONG id);

	// get session by sesion id
	bool getSession(Firebird::TraceSession& session, GET_FLAGS getFlag);

	ULONG getChangeNumber() const
	{ return m_sharedMemory && m_sharedMemory->getHeader() ? m_sharedMemory->getHeader()->change_number.load() : 0; }

	void acquire();
	void release();

	void shutdown();

	Firebird::Mutex m_localMutex;

	class Accessor
	{
	public:
		// Use when storage is not locked by caller
		explicit Accessor(ConfigStorage* storage) :
			m_storage(storage),
			m_guard(nullptr)
		{}

		// Use when storage is locked by caller
		explicit Accessor(StorageGuard* guard);

		void restart()
		{
			m_change_number = 0;
			m_sesId = 0;
			m_nextIdx = 0;
		}

		bool getNext(Firebird::TraceSession& session, GET_FLAGS getFlag);

	private:
		ConfigStorage* const m_storage;
		StorageGuard* const m_guard;
		ULONG m_change_number = 0;
		ULONG m_sesId = 0;					// last seen session ID
		ULONG m_nextIdx = 0;				// slot index next after last seen one
	};

private:
	void mutexBug(int osErrorCode, const char* text) override;
	bool initialize(Firebird::SharedMemoryBase*, bool) override;

	void initSharedFile();

	USHORT getType() const override { return Firebird::SharedMemoryBase::SRAM_TRACE_CONFIG; }
	USHORT getVersion() const override { return TraceCSHeader::TRACE_STORAGE_VERSION; }
	const char* getName() const override { return "TraceConfigStorage"; }

	void checkAudit();

	class TouchFile final :
		public Firebird::RefCntIface<Firebird::ITimerImpl<TouchFile, Firebird::CheckStatusWrapper> >
	{
	public:
		TouchFile() :
			fileName(*getDefaultMemoryPool())
		{}

		void handler();
		void start(const char* fName);
		void stop();

	private:
		Firebird::PathName fileName;
	};
	Firebird::RefPtr<TouchFile> m_timer;

	void checkDirty()
	{
		m_dirty = false;
	}

	void setDirty()
	{
		if (!m_dirty)
		{
			if (m_sharedMemory && m_sharedMemory->getHeader())
				m_sharedMemory->getHeader()->change_number++;
			m_dirty = true;
		}
	}

	// items in every session record at session data
	enum ITEM
	{
		tagName = 1,		// session Name
		tagAuthBlock,		// with which creator logged in
		tagUserName,		// creator user name
		tagConfig,			// configuration
		tagStartTS,			// date+time when started
		tagLogFile,			// log file name, if any
		tagRole,			// SQL role name, if any
		tagEnd
	};

	// allocate slot with size >= of given slotLen, returns slot index
	// could remap shared memory
	ULONG allocSlot(ULONG slotSize);
	void markDeleted(TraceCSHeader::Slot * slot);

	// remove unused space between slots data
	void compact();
	bool validate();

	ULONG getSessionSize(const Firebird::TraceSession& session);

	bool findSession(ULONG sesId, ULONG& idx);
	bool readSession(TraceCSHeader::Slot* slot, Firebird::TraceSession& session, GET_FLAGS getFlag);

	// Search for used slot starting from nextIdx and increments nextIdx to point to the next slot
	// returns false, if used slot was not found
	bool getNextSession(Firebird::TraceSession& session, GET_FLAGS getFlag, ULONG& nextIdx);

	class Reader
	{
	public:
		Reader(const void* memory, ULONG size) :
			m_mem(reinterpret_cast<const char*>(memory)),
			m_end(m_mem + size)
		{}

		// fill tag and len, returns pointer to data or NULL if data can't be read
		const void* read(ITEM& tag, ULONG& len);

	private:
		const char* m_mem;
		const char* const m_end;
	};

	class Writer
	{
	public:
		Writer(void* memory, ULONG size) :
			m_mem(reinterpret_cast<char*>(memory)),
			m_end(m_mem + size)
		{}

		void write(ITEM tag, ULONG len, const void* data);

	private:
		char* m_mem;
		const char* const m_end;
	};

	Firebird::AutoPtr<Firebird::SharedMemory<TraceCSHeader> > m_sharedMemory;
	Firebird::PathName m_filename;
	int m_recursive;
	ThreadId m_mutexTID;
	bool m_dirty;
};


class StorageInstance
{
private:
	Firebird::Mutex initMtx;
	ConfigStorage* storage;

public:
	explicit StorageInstance(Firebird::MemoryPool&) :
		storage(NULL)
	{}

	~StorageInstance()
	{
		delete storage;
	}

	ConfigStorage* getStorage()
	{
		if (!storage)
		{
			Firebird::MutexLockGuard guard(initMtx, FB_FUNCTION);
			if (!storage)
			{
				storage = FB_NEW ConfigStorage;
			}
		}
		return storage;
	}
};


class StorageGuard : public Firebird::MutexLockGuard
{
public:
	explicit StorageGuard(ConfigStorage* storage) :
		Firebird::MutexLockGuard(storage->m_localMutex, FB_FUNCTION), m_storage(storage)
	{
		m_storage->acquire();
	}

	~StorageGuard()
	{
		m_storage->release();
	}

	ConfigStorage* getStorage()
	{
		return m_storage;
	}

private:
	ConfigStorage* m_storage;
};


inline ConfigStorage::Accessor::Accessor(StorageGuard* guard) :
	m_storage(guard->getStorage()),
	m_guard(guard)
{}

}

#endif
