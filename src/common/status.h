/*
 *	PROGRAM:		Firebird exceptions classes
 *	MODULE:			status.h
 *	DESCRIPTION:	Status vector filling and parsing.
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
 *  The Original Code was created by Mike Nordell
 *  for the Firebird Open Source RDBMS project.
 *
 *  Copyright (c) 2001 Mike Nordell <tamlin at algonet.se>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 *
 */


#ifndef COMMON_STATUS_H
#define COMMON_STATUS_H

#include "fb_exception.h"
#include "../common/isc_proto.h"
#include "../common/StatusHolder.h"
#include "../common/utils_proto.h"
#include <utility>

inline constexpr int MAX_ERRMSG_LEN = 128;
inline constexpr int MAX_ERRSTR_LEN = 1024;

namespace Firebird
{
	template <class SW>
	class LocalStatusWrapper
	{
	public:
		template <typename... Args>
		LocalStatusWrapper(Args&&... args)
			: localStatusVector(&localStatus, std::forward<Args>(args)...)
		{ }

		template <typename... Args>
		explicit LocalStatusWrapper(Firebird::MemoryPool& p, Args&&... args)
			: localStatus(p), localStatusVector(&localStatus, std::forward<Args>(args)...)
		{ }

		SW* operator->() noexcept
		{
			return &localStatusVector;
		}

		SW* operator&() noexcept
		{
			return &localStatusVector;
		}

		ISC_STATUS operator[](unsigned n) const
		{
			fb_assert(n < fb_utils::statusLength(localStatusVector.getErrors()));
			return localStatusVector.getErrors()[n];
		}

		const SW* operator->() const noexcept
		{
			return &localStatusVector;
		}

		const SW* operator&() const noexcept
		{
			return &localStatusVector;
		}

		void check() const
		{
			if (localStatusVector.isDirty())
			{
				if (localStatus.getState() & Firebird::IStatus::STATE_ERRORS)
					raise();
			}
		}

		void copyTo(SW* to) const noexcept
		{
			fb_utils::copyStatus(to, &localStatusVector);
		}

		void loadFrom(const SW* to) noexcept
		{
			fb_utils::copyStatus(&localStatusVector, to);
		}

		[[noreturn]] void raise() const
		{
			Firebird::status_exception::raise(&localStatus);
		}

		bool isEmpty() const
		{
			return localStatusVector.isEmpty();
		}

		bool isSuccess() const
		{
			return localStatusVector.isEmpty();
		}

	private:
		Firebird::LocalStatus localStatus;
		SW localStatusVector;
	};

	typedef LocalStatusWrapper<CheckStatusWrapper> FbLocalStatus;

	class LogWrapper : public BaseStatusWrapper<LogWrapper>
	{
	public:
		LogWrapper(IStatus* aStatus, const char* aText = nullptr)
			: BaseStatusWrapper(aStatus),
			  text(aText)
		{
		}

	public:
		static void checkException(const LogWrapper* status)
		{
			if (status->dirty && (status->getState() & IStatus::STATE_ERRORS))
				iscLogStatus(status->text, status->status);
		}

	private:
		const char* text;
	};

	typedef LocalStatusWrapper<LogWrapper> LogLocalStatus;

	class ThrowWrapper : public BaseStatusWrapper<ThrowWrapper>
	{
	public:
		ThrowWrapper(IStatus* aStatus)
			: BaseStatusWrapper(aStatus)
		{
		}

	public:
		static void checkException(const ThrowWrapper* status)
		{
			if (status->dirty && (status->getState() & IStatus::STATE_ERRORS))
				status_exception::raise(status->status);
		}
	};

	typedef LocalStatusWrapper<ThrowWrapper> ThrowLocalStatus;

	class ThrowStatusExceptionWrapper : public ThrowStatusWrapper
	{
	public:
		ThrowStatusExceptionWrapper(IStatus* aStatus)
			: ThrowStatusWrapper(aStatus)
		{
		}

	public:
		static void catchException(IStatus* status)
		{
			if (!status)
				return;

			try
			{
				throw;
			}
			catch (const FbException& e)
			{
				status->setErrors(e.getStatus()->getErrors());
			}
			catch (const status_exception& e)
			{
				status->setErrors(e.value());
			}
			catch (...)
			{
				const ISC_STATUS statusVector[] = {
					isc_arg_gds, isc_random,
					isc_arg_string, (ISC_STATUS) "Unrecognized C++ exception",
					isc_arg_end};
				status->setErrors(statusVector);
			}
		}
	};
}

#endif // COMMON_STATUS_H
