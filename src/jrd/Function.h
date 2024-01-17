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
 */

#ifndef JRD_FUNCTION_H
#define JRD_FUNCTION_H

#include "../jrd/Routine.h"
#include "../common/classes/array.h"
#include "../common/dsc.h"
#include "../common/classes/NestConst.h"
#include "../jrd/val.h"
#include "../dsql/Nodes.h"
#include "../jrd/HazardPtr.h"
#include "../jrd/lck.h"

namespace Jrd
{
	class ValueListNode;
	class QualifiedName;

	class Function : public Routine
	{
		static const char* const EXCEPTION_MESSAGE;

	public:
		static Lock* makeLock(thread_db* tdbb, MemoryPool& p);
		static int blockingAst(void* ast_object);

		static Function* lookup(thread_db* tdbb, MetaId id, CacheObject::Flag flags);
		static Function* lookup(thread_db* tdbb, const QualifiedName& name);

	private:
		explicit Function(RoutinePermanent* perm)
			: Routine(perm),
			  fun_entrypoint(NULL),
			  fun_inputs(0),
			  fun_return_arg(0),
			  fun_temp_length(0),
			  fun_exception_message(perm->getPool()),
			  fun_deterministic(false),
			  fun_external(NULL)
		{
		}
/* ????????????????
private:
		Function(MemoryPool& p, MetaId id)
			: Routine(p, id),
			  fun_entrypoint(NULL),
			  fun_inputs(0),
			  fun_return_arg(0),
			  fun_temp_length(0),
			  fun_exception_message(p),
			  fun_deterministic(false),
			  fun_external(NULL)
		{
		}
 */
	public:
		static Function* create(thread_db* tdbb, MemoryPool& pool, RoutinePermanent* perm);
		void scan(thread_db* tdbb, CacheObject::Flag flags);

	public:
		virtual int getObjectType() const
		{
			return obj_udf;
		}

		virtual SLONG getSclType() const
		{
			return obj_functions;
		}

		virtual bool checkCache(thread_db* tdbb) const;

	private:
		virtual ~Function()
		{
			delete fun_external;
		}

	public:
		virtual void releaseExternal()
		{
			delete fun_external;
			fun_external = NULL;
		}

	public:
		int (*fun_entrypoint)();				// function entrypoint
		USHORT fun_inputs;						// input arguments
		USHORT fun_return_arg;					// return argument
		ULONG fun_temp_length;					// temporary space required

		Firebird::string fun_exception_message;	// message containing the exception error message

		bool fun_deterministic;
		const ExtEngineManager::Function* fun_external;

	protected:
		virtual bool reload(thread_db* tdbb);
	};
}

#endif // JRD_FUNCTION_H
