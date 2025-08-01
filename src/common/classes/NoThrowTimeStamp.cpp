/*
 *	PROGRAM:	Client/Server Common Code
 *	MODULE:		NoThrowTimeStamp.cpp
 *	DESCRIPTION:	Date/time handling class
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
 * NS: The code now contains much of the logic from original gds.c
 *     this is why we need to use IPL license for it
 */

#include "firebird.h"
#include "../common/gdsassert.h"

#ifdef HAVE_SYS_TIMES_H
#include <sys/times.h>
#endif
#ifdef HAVE_SYS_TIMEB_H
#include <sys/timeb.h>
#endif

#ifdef WIN_NT
#include <windows.h>
#endif

#include "../common/classes/NoThrowTimeStamp.h"

namespace Firebird {

const ISC_TIMESTAMP NoThrowTimeStamp::MIN_TIMESTAMP = {NoThrowTimeStamp::MIN_DATE, 0};
const ISC_TIMESTAMP NoThrowTimeStamp::MAX_TIMESTAMP =
	{NoThrowTimeStamp::MAX_DATE, NoThrowTimeStamp::ISC_TICKS_PER_DAY - 1};

const ISC_TIME NoThrowTimeStamp::POW_10_TABLE[] =
	{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

NoThrowTimeStamp NoThrowTimeStamp::getCurrentTimeStamp(const char** error) noexcept
{
	if (error)
		*error = NULL;
	NoThrowTimeStamp result;

	// NS: We round generated timestamps to whole millisecond.
	// Not many applications can deal with fractional milliseconds properly and
	// we do not use high resolution timers either so actual time granularity
	// is going to to be somewhere in range between 1 ms (like on UNIX/Risc)
	// and 53 ms (such as Win9X)

	int milliseconds;

#ifdef WIN_NT
	FILETIME ftUtc, ftLocal;
	SYSTEMTIME stLocal;

	GetSystemTimeAsFileTime(&ftUtc);
	if (!FileTimeToLocalFileTime(&ftUtc, &ftLocal))
	{
		if (error)
			*error = "FileTimeToLocalFileTime";
		return result;
	}
	if (!FileTimeToSystemTime(&ftLocal, &stLocal))
	{
		if (error)
			*error = "FileTimeToSystemTime";
		return result;
	}

	milliseconds = stLocal.wMilliseconds;
#else
	time_t seconds; // UTC time

#ifdef HAVE_GETTIMEOFDAY
	struct timeval tp;
	GETTIMEOFDAY(&tp);
	seconds = tp.tv_sec;
	milliseconds = tp.tv_usec / 1000;
#else
	struct timeb time_buffer;
	ftime(&time_buffer);
	seconds = time_buffer.time;
	milliseconds = time_buffer.millitm;
#endif
#endif // WIN_NT

	// NS: Current FB behavior of using server time zone is not appropriate for
	// distributed applications. We should be storing UTC times everywhere and
	// convert timestamps to client timezone as necessary. Replace localtime stuff
	// with these lines as soon as the appropriate functionality is implemented
	//
	// mValue.timestamp_date = seconds / 86400 + GDS_EPOCH_START;
	// mValue.timestamp_time = (seconds % 86400) * ISC_TIME_SECONDS_PRECISION;

	const int fractions = milliseconds * ISC_TIME_SECONDS_PRECISION / 1000;

#ifdef WIN_NT
	// Manually convert SYSTEMTIME to "struct tm" used below

	struct tm times, *ptimes = &times;

	times.tm_sec = stLocal.wSecond;			// seconds after the minute - [0,59]
	times.tm_min = stLocal.wMinute;			// minutes after the hour - [0,59]
	times.tm_hour = stLocal.wHour;			// hours since midnight - [0,23]
	times.tm_mday = stLocal.wDay;			// day of the month - [1,31]
	times.tm_mon = stLocal.wMonth - 1;		// months since January - [0,11]
	times.tm_year = stLocal.wYear - 1900;	// years since 1900
	times.tm_wday = stLocal.wDayOfWeek;		// days since Sunday - [0,6]

	// --- no used for encoding below
	times.tm_yday = 0;						// days since January 1 - [0,365]
	times.tm_isdst = -1;					// daylight savings time flag
#else
#ifdef HAVE_LOCALTIME_R
	struct tm times, *ptimes = &times;
	if (!localtime_r(&seconds, &times))
	{
		if (error)
			*error = "localtime_r";
		return result;
	}
#else
	struct tm *ptimes = localtime(&seconds);
	if (!ptimes)
	{
		if (error)
			*error = "localtime";
		return result;
	}
#endif
#endif // WIN_NT

	result.encode(ptimes, fractions);
	return result;
}

int NoThrowTimeStamp::yday(const struct tm* times) noexcept
{
	// Convert a calendar date to the day-of-year.
	//
	// The unix time structure considers January 1 to be Year day 0, although it
	// is day 1 of the month. (Note that QLI, when printing Year days takes the other view).

	int day = times->tm_mday;
	const int month = times->tm_mon;
	const int year = times->tm_year + 1900;

	--day;

	day += (214 * month + 3) / 7;

	if (month < 2)
		return day;

	if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)
		--day;
	else
		day -= 2;

	return day;
}


void NoThrowTimeStamp::decode_date(ISC_DATE nday, struct tm* times) noexcept
{
	// Convert a numeric day to [day, month, year].
	//
	// Calenders are divided into 4 year cycles: 3 non-leap years, and 1 leap year.
	// Each cycle takes 365*4 + 1 == 1461 days.
	// There is a further cycle of 100 4 year cycles.
	// Every 100 years, the normally expected leap year is not present. Every 400 years it is.
	// This cycle takes 100 * 1461 - 3 == 146097 days.
	// The difference between Julian date (January 1, 4713 BC proleptic Julian calendar)
	//   and Modified Julian date (November 17, 1858 -- used as a base date in Firebird)
	//   is 2400001 days.
	// The origin of the constant 1721119 is unknown.
	// The difference between 2400001 and 1721119 == 678882 is the
	//   number of days from date 0/0/0000 to our base date of November 17, 1858
	// The origin of the constant 153 is unknown.
	//
	// This whole routine has problems with ndates less than -678882 (Approx 2/1/0000).

	// struct tm may include arbitrary number of additional members.
	// zero-initialize them.
	memset(times, 0, sizeof(struct tm));

	if ((times->tm_wday = (nday + 3) % 7) < 0)
		times->tm_wday += 7;

	nday += 2400001 - 1721119;
	const int century = (4 * nday - 1) / 146097;
	nday = 4 * nday - 1 - 146097 * century;
	int day = nday / 4;

	nday = (4 * day + 3) / 1461;
	day = 4 * day + 3 - 1461 * nday;
	day = (day + 4) / 4;

	int month = (5 * day - 3) / 153;
	day = 5 * day - 3 - 153 * month;
	day = (day + 5) / 5;

	int year = 100 * century + nday;

	if (month < 10)
		month += 3;
	else
	{
		month -= 9;
		year += 1;
	}

	times->tm_mday = day;
	times->tm_mon = month - 1;
	times->tm_year = year - 1900;

	times->tm_yday = yday(times);
}


ISC_DATE NoThrowTimeStamp::encode_date(const struct tm* times) noexcept
{
	// Convert a calendar date to a numeric day
	// (the number of days since the base date)

	const int day = times->tm_mday;
	int month = times->tm_mon + 1;
	int year = times->tm_year + 1900;

	if (month > 2)
		month -= 3;
	else
	{
		month += 9;
		year -= 1;
	}

	const int c = year / 100;
	const int ya = year - 100 * c;

	return (ISC_DATE) (((SINT64) 146097 * c) / 4 +
					   (1461 * ya) / 4 +
					   (153 * month + 2) / 5 + day + 1721119 - 2400001);
}

void NoThrowTimeStamp::decode_time(ISC_TIME ntime, int* hours, int* minutes, int* seconds, int* fractions) noexcept
{
	fb_assert(hours);
	fb_assert(minutes);
	fb_assert(seconds);

	*hours = ntime / (3600 * ISC_TIME_SECONDS_PRECISION);
	ntime %= 3600 * ISC_TIME_SECONDS_PRECISION;
	*minutes = ntime / (60 * ISC_TIME_SECONDS_PRECISION);
	ntime %= 60 * ISC_TIME_SECONDS_PRECISION;
	*seconds = ntime / ISC_TIME_SECONDS_PRECISION;

	if (fractions)
	{
		*fractions = ntime % ISC_TIME_SECONDS_PRECISION;
	}
}

ISC_TIME NoThrowTimeStamp::encode_time(int hours, int minutes, int seconds, int fractions) noexcept
{
	fb_assert(fractions	>= 0 && fractions < ISC_TIME_SECONDS_PRECISION);

	return ((hours * 60 + minutes) * 60 + seconds) * ISC_TIME_SECONDS_PRECISION + fractions;
}

void NoThrowTimeStamp::decode_timestamp(const ISC_TIMESTAMP ts, struct tm* times, int* fractions) noexcept
{
	decode_date(ts.timestamp_date, times);
	decode_time(ts.timestamp_time, &times->tm_hour, &times->tm_min, &times->tm_sec, fractions);
}

ISC_TIMESTAMP NoThrowTimeStamp::encode_timestamp(const struct tm* times, const int fractions) noexcept
{
	fb_assert(fractions >= 0 && fractions < ISC_TIME_SECONDS_PRECISION);

	ISC_TIMESTAMP ts;
	ts.timestamp_date = encode_date(times);
	ts.timestamp_time = encode_time(times->tm_hour, times->tm_min, times->tm_sec, fractions);

	return ts;
}

void NoThrowTimeStamp::add10msec(ISC_TIMESTAMP* v, SINT64 msec, SINT64 multiplier) noexcept
{
	const SINT64 full = msec * multiplier;
	const int days = full / (SECONDS_PER_DAY * ISC_TIME_SECONDS_PRECISION);
	const int secs = full % (SECONDS_PER_DAY * ISC_TIME_SECONDS_PRECISION);

	v->timestamp_date += days;

	// Time portion is unsigned, so we avoid unsigned rolling over negative values
	// that only produce a new unsigned number with the wrong result.
	if (secs < 0 && ISC_TIME(-secs) > v->timestamp_time)
	{
		v->timestamp_date--;
		v->timestamp_time += (SECONDS_PER_DAY * ISC_TIME_SECONDS_PRECISION) + secs;
	}
	else if ((v->timestamp_time += secs) >= (SECONDS_PER_DAY * ISC_TIME_SECONDS_PRECISION))
	{
		v->timestamp_date++;
		v->timestamp_time -= (SECONDS_PER_DAY * ISC_TIME_SECONDS_PRECISION);
	}
}

void NoThrowTimeStamp::round_time(ISC_TIME &ntime, const int precision) noexcept
{
	const int scale = -ISC_TIME_SECONDS_PRECISION_SCALE - precision;

	// for the moment, if greater precision was requested than we can provide,
	// return what we have.
	if (scale <= 0)
		return;

	fb_assert(static_cast<FB_SIZE_T>(scale) < FB_NELEM(POW_10_TABLE));

	const ISC_TIME period = POW_10_TABLE[scale];

	ntime -= (ntime % period);
}

int NoThrowTimeStamp::convertGregorianDateToWeekDate(const struct tm& times) noexcept
{
	// Algorithm for Converting Gregorian Dates to ISO 8601 Week Date by Rick McCarty, 1999
	// http://personal.ecu.edu/mccartyr/ISOwdALG.txt

	const int y = times.tm_year + 1900;
	const int dayOfYearNumber = times.tm_yday + 1;

	// Find the jan1Weekday for y (Monday=1, Sunday=7)
	const int yy = (y - 1) % 100;
	const int c = (y - 1) - yy;
	const int g = yy + yy / 4;
	const int jan1Weekday = 1 + (((((c / 100) % 4) * 5) + g) % 7);

	// Find the weekday for y m d
	const int h = dayOfYearNumber + (jan1Weekday - 1);
	const int weekday = 1 + ((h - 1) % 7);

	// Find if y m d falls in yearNumber y-1, weekNumber 52 or 53
	int yearNumber, weekNumber;

	if ((dayOfYearNumber <= (8 - jan1Weekday)) && (jan1Weekday > 4))
	{
		yearNumber = y - 1;
		weekNumber = ((jan1Weekday == 5) || ((jan1Weekday == 6) &&
			isLeapYear(yearNumber))) ? 53 : 52;
	}
	else
	{
		yearNumber = y;

		// Find if y m d falls in yearNumber y+1, weekNumber 1
		const int i = isLeapYear(y) ? 366 : 365;

		if ((i - dayOfYearNumber) < (4 - weekday))
		{
			yearNumber = y + 1;
			weekNumber = 1;
		}
	}

	// Find if y m d falls in yearNumber y, weekNumber 1 through 53
	if (yearNumber == y)
	{
		const int j = dayOfYearNumber + (7 - weekday) + (jan1Weekday - 1);
		weekNumber = j / 7;
		if (jan1Weekday > 4)
			weekNumber--;
	}

	return weekNumber;
}

int NoThrowTimeStamp::convertGregorianDateToJulianDate(int year, int month, int day) noexcept
{
	return (1461 * (year + 4800 + (month - 14)/12))/4 + (367 * (month - 2 - 12 * ((month - 14)/12)))
		/ 12 - (3 * ((year + 4900 + (month - 14)/12)/100))/4 + day - 32075;
}

void NoThrowTimeStamp::convertJulianDateToGregorianDate(int jdn, int& outYear, int& outMonth, int& outDay) noexcept
{
	const int a = jdn + 32044;
	const int b = (4 * a +3 ) / 146097;
	const int c = a - (146097 * b) / 4;
	const int d = (4 * c + 3) / 1461;
	const int e = c - (1461 * d) / 4;
	const int m = (5 * e + 2) / 153;

	outDay = e - (153 * m + 2) / 5 + 1;
	outMonth  = m + 3 - 12 * (m / 10);
	outYear = 100 * b + d - 4800 + (m / 10);
}

// Encode timestamp from UNIX datetime structure
void NoThrowTimeStamp::encode(const struct tm* times, int fractions) noexcept
{
	mValue = encode_timestamp(times, fractions);
}

// Decode timestamp into UNIX datetime structure
void NoThrowTimeStamp::decode(struct tm* times, int* fractions) const noexcept
{
	fb_assert(mValue.timestamp_date != BAD_DATE);
	fb_assert(mValue.timestamp_time != BAD_TIME);

	decode_timestamp(mValue, times, fractions);
}

} // namespace
