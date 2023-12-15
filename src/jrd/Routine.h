/*
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
 * Adriano dos Santos Fernandes
 */

#ifndef JRD_ROUTINE_H
#define JRD_ROUTINE_H

#include "../common/classes/array.h"
#include "../common/classes/alloc.h"
#include "../common/classes/BlrReader.h"
#include "../jrd/MetaName.h"
#include "../jrd/QualifiedName.h"
#include "../jrd/HazardPtr.h"
#include "../common/classes/NestConst.h"
#include "../common/MsgMetadata.h"
#include "../common/classes/auto.h"
#include "../common/classes/Nullable.h"
#include "../common/ThreadStart.h"

#include <condition_variable>

namespace Jrd
{
	class thread_db;
	class CompilerScratch;
	class Statement;
	class Lock;
	class Format;
	class Parameter;
	class UserId;
	class StartupBarrier
	{
	public:
		StartupBarrier()
			: thd(Thread::getId()), flg(false)
		{ }

		void open()
		{
			// only creator thread may open barrier
			fb_assert(thd == Thread::getId());

			// no need opening barrier twice
			if (flg)
				return;

			std::unique_lock<std::mutex> g(mtx);
			if (flg)
				return;

			flg = true;
			cond.notify_all();
		}

		void wait()
		{
			// if barrier is already opened nothing to be done
			// also enable recursive use by creator thread
			if (flg || (thd == Thread::getId()))
				return;

			std::unique_lock<std::mutex> g(mtx);
			if (flg)
				return;

			cond.wait(g, [this]{return flg;});
		}

	private:
		std::condition_variable cond;
		std::mutex mtx;
		const ThreadId thd;
		bool flg;
	};

	class RoutinePermanent : public Firebird::PermanentStorage
	{
	public:
		explicit RoutinePermanent(MemoryPool& p, MetaId metaId, Lock* existence);

		explicit RoutinePermanent(MemoryPool& p)
			: PermanentStorage(p),
			  id(~0),
			  name(p),
			  securityName(p),
			  subRoutine(true),
			  flags(0),
			  alterCount(0),
			  existenceLock(NULL)
		{ }

		USHORT getId() const
		{
			fb_assert(!subRoutine);
			return id;
		}

		void setId(USHORT value) { id = value; }

		const QualifiedName& getName() const { return name; }
		void setName(const QualifiedName& value) { name = value; }
		const char* c_name() const { return name.c_str(); }

		const MetaName& getSecurityName() const { return securityName; }
		void setSecurityName(const MetaName& value) { securityName = value; }

		bool hasData() const { return name.hasData(); }

		bool isSubRoutine() const { return subRoutine; }
		void setSubRoutine(bool value) { subRoutine = value; }

		int getObjectType() const;

	private:
		USHORT id;							// routine ID
		QualifiedName name;					// routine name
		MetaName securityName;				// security class name
		bool subRoutine;                    // Is this a subroutine?
		USHORT flags;
		USHORT alterCount;      			// No. of times the routine was altered

	public:
		Lock* existenceLock;				// existence lock, if any
		MetaName owner;
	};

	class Routine : public CacheObject
	{
	protected:
		explicit Routine(RoutinePermanent* perm)
			: permanent(perm),
			  statement(NULL),
			  implemented(true),
			  defined(true),
			  defaultCount(0),
			  inputFormat(NULL),
			  outputFormat(NULL),
			  inputFields(permanent->getPool()),
			  outputFields(permanent->getPool()),
			  flags(0),
			  invoker(NULL)
		{
		}

	public:
		virtual ~Routine()
		{
		}

	public:
		static const USHORT MAX_ALTER_COUNT = 64;	// Number of times an in-cache routine can be altered ?????????

		static Firebird::MsgMetadata* createMetadata(
			const Firebird::Array<NestConst<Parameter> >& parameters, bool isExtern);
		static Format* createFormat(MemoryPool& pool, Firebird::IMessageMetadata* params, bool addEof);

	public:
		static void destroy(Routine* routine)
		{
			delete routine;
		}

		const QualifiedName& getName() const { return permanent->getName(); }
		USHORT getId() const { return permanent->getId(); }
		const char* c_name() const override { return permanent->c_name(); }

		/*const*/ Statement* getStatement() const { return statement; }
		void setStatement(Statement* value);

		bool isImplemented() const { return implemented; }
		void setImplemented(bool value) { implemented = value; }

		bool isDefined() const { return defined; }
		void setDefined(bool value) { defined = value; }

		void checkReload(thread_db* tdbb);

		USHORT getDefaultCount() const { return defaultCount; }
		void setDefaultCount(USHORT value) { defaultCount = value; }

		const Format* getInputFormat() const { return inputFormat; }
		void setInputFormat(const Format* value) { inputFormat = value; }

		const Format* getOutputFormat() const { return outputFormat; }
		void setOutputFormat(const Format* value) { outputFormat = value; }

		const Firebird::Array<NestConst<Parameter> >& getInputFields() const { return inputFields; }
		Firebird::Array<NestConst<Parameter> >& getInputFields() { return inputFields; }

		const Firebird::Array<NestConst<Parameter> >& getOutputFields() const { return outputFields; }
		Firebird::Array<NestConst<Parameter> >& getOutputFields() { return outputFields; }

		void parseBlr(thread_db* tdbb, CompilerScratch* csb, bid* blob_id, bid* blobDbg);
		void parseMessages(thread_db* tdbb, CompilerScratch* csb, Firebird::BlrReader blrReader);

		virtual void releaseFormat()
		{
		}

		void afterDecrement(Jrd::thread_db*);
		void afterUnlock(thread_db* tdbb, unsigned fl) override;
		void releaseStatement(thread_db* tdbb);
		//void remove(thread_db* tdbb);
		virtual void releaseExternal()
		{
		}

		void sharedCheckUnlock(thread_db* tdbb);
		void releaseLocks(thread_db* tdbb);

	public:
		virtual int getObjectType() const = 0;
		virtual SLONG getSclType() const = 0;
		virtual bool checkCache(thread_db* tdbb) const = 0;

	public:
		RoutinePermanent* permanent;		// Permanent part of data

	private:
		Statement* statement;				// compiled routine statement
		bool implemented;					// Is the packaged routine missing the body/entrypoint?
		bool defined;						// UDF has its implementation module available
		USHORT defaultCount;				// default input arguments
		const Format* inputFormat;			// input format
		const Format* outputFormat;			// output format
		Firebird::Array<NestConst<Parameter> > inputFields;		// array of field blocks
		Firebird::Array<NestConst<Parameter> > outputFields;	// array of field blocks

	protected:
		virtual bool reload(thread_db* tdbb) = 0;

	public:
		USHORT flags;
		StartupBarrier startup;

	public:
		Jrd::UserId* invoker;		// Invoker ID
	};
}

#endif // JRD_ROUTINE_H
