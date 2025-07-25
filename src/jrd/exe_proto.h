/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		exe_proto.h
 *	DESCRIPTION:	Prototype header file for exe.cpp
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

#ifndef JRD_EXE_PROTO_H
#define JRD_EXE_PROTO_H

#include "../jrd/cmp_proto.h"
#include <atomic>

namespace Jrd {
	class Request;
	class jrd_tra;
	class AssignmentNode;
}

void EXE_assignment(Jrd::thread_db*, const Jrd::AssignmentNode*);
void EXE_assignment(Jrd::thread_db*, const Jrd::ValueExprNode*, const Jrd::ValueExprNode*);
void EXE_assignment(Jrd::thread_db* tdbb, const Jrd::ValueExprNode* to, dsc* from_desc, bool from_null,
	const Jrd::ValueExprNode* missing_node, const Jrd::ValueExprNode* missing2_node);

void EXE_execute_db_triggers(Jrd::thread_db*, Jrd::jrd_tra*, enum TriggerAction);
void EXE_execute_ddl_triggers(Jrd::thread_db* tdbb, Jrd::jrd_tra* transaction,
	bool preTriggers, int action);
void EXE_execute_function(Jrd::thread_db* tdbb, Jrd::Request* request, Jrd::jrd_tra* transaction,
	ULONG inMsgLength, UCHAR* inMsg, ULONG outMsgLength, UCHAR* outMsg);
bool EXE_get_stack_trace(const Jrd::Request* request, Firebird::string& sTrace);

const Jrd::StmtNode* EXE_looper(Jrd::thread_db* tdbb, Jrd::Request* request,
	const Jrd::StmtNode* in_node);

void EXE_execute_triggers(Jrd::thread_db*, Jrd::TrigVector**, Jrd::record_param*, Jrd::record_param*,
	enum TriggerAction, Jrd::StmtNode::WhichTrigger, int = 0);

void EXE_receive(Jrd::thread_db*, Jrd::Request*, USHORT, ULONG, void*, bool = false);
void EXE_release(Jrd::thread_db*, Jrd::Request*);
void EXE_send(Jrd::thread_db*, Jrd::Request*, USHORT, ULONG, const void*);
void EXE_start(Jrd::thread_db*, Jrd::Request*, Jrd::jrd_tra*);
void EXE_unwind(Jrd::thread_db*, Jrd::Request*);

namespace Jrd
{
	class CachedRequestId
	{
	public:
		CachedRequestId()
			: id(generator++)
		{
			fb_assert(id <= MAX_USHORT);
		}

		CachedRequestId(const CachedRequestId&) = delete;
		CachedRequestId& operator=(const CachedRequestId&) = delete;

	public:
		USHORT getId() const noexcept
		{
			return id;
		}

	private:
		unsigned id;
		static inline std::atomic_uint generator;
	};

	// ASF: To make this class MT-safe in SS for v3, it should be AutoCacheRequest::release job to
	// inform CMP that the request is available for subsequent usage.
	class AutoCacheRequest
	{
	public:
		AutoCacheRequest(thread_db* tdbb, USHORT aId, USHORT aWhich)
			: id(aId),
			  which(aWhich),
			  request(tdbb->getAttachment()->findSystemRequest(tdbb, id, which))
		{
		}

		AutoCacheRequest(thread_db* tdbb, const CachedRequestId& cachedRequestId)
			: id(cachedRequestId.getId()),
			  which(CACHED_REQUESTS),
			  request(tdbb->getAttachment()->findSystemRequest(tdbb, id, which))
		{
		}

		AutoCacheRequest() noexcept
			: id(0),
			  which(0),
			  request(NULL)
		{
		}

		~AutoCacheRequest()
		{
			release();
		}

	public:
		void reset(thread_db* tdbb, USHORT aId, USHORT aWhich)
		{
			release();

			id = aId;
			which = aWhich;
			request = tdbb->getAttachment()->findSystemRequest(tdbb, id, which);
		}

		void reset(thread_db* tdbb, const CachedRequestId& cachedRequestId)
		{
			release();

			id = cachedRequestId.getId();
			which = CACHED_REQUESTS;
			request = tdbb->getAttachment()->findSystemRequest(tdbb, id, which);
		}

		void compile(thread_db* tdbb, const UCHAR* blr, ULONG blrLength)
		{
			if (request)
				return;

			request = CMP_compile_request(tdbb, blr, blrLength, true);
			cacheRequest();
		}

		Request* operator ->() noexcept
		{
			return request;
		}

		operator Request*() noexcept
		{
			return request;
		}

		bool operator !() const noexcept
		{
			return !request;
		}

	private:
		inline void release()
		{
			if (request)
			{
				EXE_unwind(JRD_get_thread_data(), request);
				request = NULL;
			}
		}

		void cacheRequest();

	private:
		USHORT id;
		USHORT which;
		Request* request;
	};

	class AutoRequest
	{
	public:
		AutoRequest() noexcept
			: request(NULL)
		{
		}

		~AutoRequest()
		{
			release();
		}

	public:
		void reset()
		{
			release();
		}

		void compile(thread_db* tdbb, const UCHAR* blr, ULONG blrLength)
		{
			if (request)
				return;

			request = CMP_compile_request(tdbb, blr, blrLength, true);
		}

		Request* operator ->() noexcept
		{
			return request;
		}

		operator Request*() noexcept
		{
			return request;
		}

		bool operator !() const noexcept
		{
			return !request;
		}

	private:
		inline void release()
		{
			if (request)
			{
				CMP_release(JRD_get_thread_data(), request);
				request = NULL;
			}
		}

	private:
		Request* request;
	};
}

#endif // JRD_EXE_PROTO_H
