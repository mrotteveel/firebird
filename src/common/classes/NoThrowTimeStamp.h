/*
 *	PROGRAM:	Client/Server Common Code
 *	MODULE:		NoThrowTimeStamp.h
 *	DESCRIPTION:	Date/time handling class
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
 *  The Original Code was created by Dmitry Yemanov
 *  for the Firebird Open Source RDBMS project.
 *
 *  Copyright (c) 2004 Dmitry Yemanov <dimitr@users.sourceforge.net>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 */

#ifndef CLASSES_NOTHROW_TIMESTAMP_H
#define CLASSES_NOTHROW_TIMESTAMP_H

#include "firebird/impl/dsc_pub.h"
#include "../common/gdsassert.h"

// struct tm declaration
#if defined(TIME_WITH_SYS_TIME)
#include <sys/time.h>
#include <time.h>
#else
#if defined(HAVE_SYS_TIME_H)
#include <sys/time.h>
#else
#include <time.h>
#endif
#endif

namespace Firebird {

// Wrapper class for ISC_TIMESTAMP supposed to implement date/time conversions
// and arithmetic. Small and not platform-specific methods are implemented
// inline.
//
// This class is used in UDF, in the rest of our code TimeStamp should be used.
// Usage of this class normally should involve zero overhead.
//
// Note: default "shallow-copy" constructor and assignment operators
// are just fine for our purposes

class NoThrowTimeStamp
{
public:
	static constexpr ISC_DATE MIN_DATE = -678575;	// 01.01.0001
	static constexpr ISC_DATE MAX_DATE = 2973483;	// 31.12.9999
	static constexpr ISC_DATE UNIX_DATE = 40587;	// 01.01.1970

	static constexpr SINT64 SECONDS_PER_DAY = 24 * 60 * 60;
	static constexpr SINT64 ISC_TICKS_PER_DAY = SECONDS_PER_DAY * ISC_TIME_SECONDS_PRECISION;

	static const ISC_TIMESTAMP MIN_TIMESTAMP;
	static const ISC_TIMESTAMP MAX_TIMESTAMP;

private:
	static constexpr ISC_DATE BAD_DATE = MAX_SLONG;
	static constexpr ISC_TIME BAD_TIME = MAX_ULONG;

public:
	// Number of the first day of UNIX epoch in GDS counting
	enum { GDS_EPOCH_START = 40617 };

	static const ISC_TIME POW_10_TABLE[];

	// Constructors
	NoThrowTimeStamp() noexcept
	{
		invalidate();
	}

	NoThrowTimeStamp(const ISC_TIMESTAMP& from) noexcept
		: mValue(from)
	{}

	NoThrowTimeStamp(ISC_DATE date, ISC_TIME time) noexcept
	{
		mValue.timestamp_date = date;
		mValue.timestamp_time = time;
	}

	explicit NoThrowTimeStamp(const struct tm& times, int fractions = 0) noexcept
	{
		encode(&times, fractions);
	}

	bool isValid() const noexcept
	{
		return isValidTimeStamp(mValue);
	}

	// Check if timestamp contains a non-existing value
	bool isEmpty() const noexcept
	{
		return (mValue.timestamp_date == BAD_DATE && mValue.timestamp_time == BAD_TIME);
	}

	// Set value of timestamp to a non-existing value
	void invalidate() noexcept
	{
		mValue.timestamp_date = BAD_DATE;
		mValue.timestamp_time = BAD_TIME;
	}

	// Assign current date/time to the timestamp
	void validate() noexcept
	{
		if (isEmpty())
		{
			*this = getCurrentTimeStamp(NULL);
		}
	}

	// Encode timestamp from UNIX datetime structure
	void encode(const struct tm* times, int fractions = 0) noexcept;

	// Decode timestamp into UNIX datetime structure
	void decode(struct tm* times, int* fractions = NULL) const noexcept;

	// Write access to timestamp structure we wrap
	ISC_TIMESTAMP& value() noexcept { return mValue; }

	// Read access to timestamp structure we wrap
	const ISC_TIMESTAMP& value() const noexcept { return mValue; }

	// Return current timestamp value
	static NoThrowTimeStamp getCurrentTimeStamp(const char** error) noexcept;

	// Validation routines
	static constexpr bool isValidDate(const ISC_DATE ndate) noexcept
	{
		return (ndate >= MIN_DATE && ndate <= MAX_DATE);
	}

	static constexpr bool isValidTime(const ISC_TIME ntime) noexcept
	{
		return (ntime < 24 * 3600 * ISC_TIME_SECONDS_PRECISION);
	}

	static constexpr bool isValidTimeStamp(const ISC_TIMESTAMP ts) noexcept
	{
		return (isValidDate(ts.timestamp_date) && isValidTime(ts.timestamp_time));
	}

	// ISC date/time helper routines
	static ISC_DATE encode_date(const struct tm* times) noexcept;
	static ISC_TIME encode_time(int hours, int minutes, int seconds, int fractions = 0) noexcept;
	static ISC_TIMESTAMP encode_timestamp(const struct tm* times, const int fractions = 0) noexcept;

	static void decode_date(ISC_DATE nday, struct tm* times) noexcept;
	static void decode_time(ISC_TIME ntime, int* hours, int* minutes, int* seconds, int* fractions = NULL) noexcept;
	static void decode_timestamp(const ISC_TIMESTAMP ntimestamp, struct tm* times, int* fractions = NULL) noexcept;

	static void add10msec(ISC_TIMESTAMP* v, SINT64 msec, SINT64 multiplier) noexcept;
	static void round_time(ISC_TIME& ntime, const int precision) noexcept;

	static int convertGregorianDateToWeekDate(const struct tm& times) noexcept;
	static int convertGregorianDateToJulianDate(int year, int month, int day) noexcept;
	static void convertJulianDateToGregorianDate(int jdn, int& outYear, int& outMonth, int& outDay) noexcept;

	static constexpr bool isLeapYear(const int year) noexcept
	{
		return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
	}

	static constexpr SINT64 timeStampToTicks(ISC_TIMESTAMP ts) noexcept
	{
		return (static_cast<SINT64>(ts.timestamp_date) - MIN_DATE) * ISC_TICKS_PER_DAY + ts.timestamp_time;
	}

	static constexpr ISC_TIMESTAMP ticksToTimeStamp(SINT64 ticks) noexcept
	{
		ISC_TIMESTAMP ts;
		ts.timestamp_date = (ticks / ISC_TICKS_PER_DAY) + MIN_DATE;
		ts.timestamp_time = ticks % ISC_TICKS_PER_DAY;

		return ts;
	}

private:
	ISC_TIMESTAMP mValue;

	static int yday(const struct tm* times) noexcept;
};

}	// namespace Firebird

#endif // CLASSES_NOTHROW_TIMESTAMP_H
