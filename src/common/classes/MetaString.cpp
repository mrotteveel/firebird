/*
 *	PROGRAM:	Client/Server Common Code
 *	MODULE:		MetaString.cpp
 *	DESCRIPTION:	metadata name holder
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
 *  The Original Code was created by Alexander Peshkov
 *  for the Firebird Open Source RDBMS project.
 *
 *  Copyright (c) 2005 Alexander Peshkov <peshkoff@mail.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 *
 *
 */

#include "firebird.h"

#include <algorithm>
#include <stdarg.h>

#include "../common/classes/MetaString.h"

namespace Firebird {

MetaString& MetaString::assign(const char* s, FB_SIZE_T l) noexcept
{
	init();
	if (s)
	{
		adjustLength(s, l);
		count = l;
		memcpy(data, s, l);
	}
	else {
		count = 0;
	}
	return *this;
}

char* MetaString::getBuffer(const FB_SIZE_T l) noexcept
{
	fb_assert(l < MAX_SQL_IDENTIFIER_SIZE);
	init();
	count = l;
	return data;
}

int MetaString::compare(const char* s, FB_SIZE_T l) const noexcept
{
	if (s)
	{
		adjustLength(s, l);
		const FB_SIZE_T x = std::min(length(), l);
		int rc = memcmp(c_str(), s, x);
		if (rc)
		{
			return rc;
		}
	}
	return length() - l;
}

void MetaString::adjustLength(const char* const s, FB_SIZE_T& l) noexcept
{
	fb_assert(s);
	if (l > MAX_SQL_IDENTIFIER_LEN)
	{
#ifdef DEV_BUILD
		for (FB_SIZE_T i = MAX_SQL_IDENTIFIER_LEN; i < l; ++i)
			fb_assert(s[i] == '\0' || s[i] == ' ');
#endif
		l = MAX_SQL_IDENTIFIER_LEN;
	}
	while (l)
	{
		if (s[l - 1] != ' ')
		{
			break;
		}
		--l;
	}
}

void MetaString::printf(const char* format, ...)
{
	init();
	va_list params;
	va_start(params, format);
	int l = vsnprintf(data, MAX_SQL_IDENTIFIER_LEN, format, params);
	if (l < 0 || FB_SIZE_T(l) > MAX_SQL_IDENTIFIER_LEN)
	{
		l = MAX_SQL_IDENTIFIER_LEN;
	}
	data[l] = 0;
	count = l;
	va_end(params);
}

FB_SIZE_T MetaString::copyTo(char* to, FB_SIZE_T toSize) const
{
	fb_assert(to);
	fb_assert(toSize);
	toSize = std::min(toSize - 1, length());
	memcpy(to, c_str(), toSize);
	to[toSize] = 0;
	return toSize;
}

} // namespace Firebird
