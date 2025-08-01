/*
 *	PROGRAM:	JRD access method
 *	MODULE:		dsc.h
 *	DESCRIPTION:	Definitions associated with descriptors
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
 * 2002.04.16  Paul Beach - HP10 Define changed from -4 to (-4) to make it
 *             compatible with the HP Compiler
 * Adriano dos Santos Fernandes
 */

#ifndef COMMON_DSC_H
#define COMMON_DSC_H

#include "firebird/impl/dsc_pub.h"
#include "firebird/impl/consts_pub.h"
#include "../jrd/ods.h"
#include "../intl/charsets.h"
#include "../common/DecFloat.h"
#include "../common/Int128.h"

// Data type information

inline constexpr bool DTYPE_IS_TEXT(UCHAR d) noexcept
{
	return d >= dtype_text && d <= dtype_varying;
}

inline constexpr bool DTYPE_IS_DATE(UCHAR t) noexcept
{
	return (t >= dtype_sql_date && t <= dtype_timestamp) || (t >= dtype_sql_time_tz && t <= dtype_ex_timestamp_tz);
}

// DTYPE_IS_BLOB includes both BLOB and ARRAY since array's are implemented over blobs.
inline constexpr bool DTYPE_IS_BLOB(UCHAR d) noexcept
{
	return d == dtype_blob || d == dtype_array;
}

// DTYPE_IS_BLOB_OR_QUAD includes both BLOB, QUAD and ARRAY since array's are implemented over blobs.
inline constexpr bool DTYPE_IS_BLOB_OR_QUAD(UCHAR d) noexcept
{
	return d == dtype_blob || d == dtype_quad || d == dtype_array;
}

// Exact numeric?
inline constexpr bool DTYPE_IS_EXACT(UCHAR d) noexcept
{
	return d == dtype_int64 || d == dtype_long || d == dtype_short || d == dtype_int128;
}

inline constexpr bool DTYPE_IS_APPROX(UCHAR d) noexcept
{
	return d == dtype_double || d == dtype_real;
}

inline constexpr bool DTYPE_IS_DECFLOAT(UCHAR d) noexcept
{
	return d == dtype_dec128 || d == dtype_dec64;
}

inline constexpr bool DTYPE_IS_NUMERIC(UCHAR d) noexcept
{
	return (d >= dtype_byte && d <= dtype_d_float) || d == dtype_int64 ||
			d == dtype_int128 || DTYPE_IS_DECFLOAT(d);
}

// Descriptor format

typedef struct dsc
{
	dsc() = default;

	// These Ods::Descriptor constructor and operator were added to have
	// interoperability between Ods::Descriptor and struct dsc
	dsc(const Ods::Descriptor& od) noexcept
		: dsc_dtype(od.dsc_dtype),
		  dsc_scale(od.dsc_scale),
		  dsc_length(od.dsc_length),
		  dsc_sub_type(od.dsc_sub_type),
		  dsc_flags(od.dsc_flags),
		  dsc_address((UCHAR*)(IPTR)(od.dsc_offset))
	{}

	UCHAR	dsc_dtype = 0;
	SCHAR	dsc_scale = 0;
	USHORT	dsc_length = 0;
	SSHORT	dsc_sub_type = 0;
	USHORT	dsc_flags = 0;
	UCHAR*	dsc_address = nullptr; // Used either as offset in a message or as a pointer

#ifdef __cplusplus
	SSHORT dsc_blob_ttype() const noexcept { return dsc_scale | (dsc_flags & 0xFF00);}
	SSHORT& dsc_ttype() noexcept { return dsc_sub_type;}
	SSHORT dsc_ttype() const noexcept { return dsc_sub_type;}

	bool isNullable() const noexcept
	{
		return dsc_flags & DSC_nullable;
	}

	void setNullable(bool nullable) noexcept
	{
		if (nullable)
			dsc_flags |= DSC_nullable;
		else
			dsc_flags &= ~(DSC_nullable | DSC_null);
	}

	bool isNull() const noexcept
	{
		return dsc_flags & DSC_null;
	}

	void setNull() noexcept
	{
		dsc_flags |= DSC_null | DSC_nullable;
	}

	void clearNull() noexcept
	{
		dsc_flags &= ~DSC_null;
	}

	bool isBlob() const noexcept
	{
		return dsc_dtype == dtype_blob || dsc_dtype == dtype_quad;
	}

	bool isBoolean() const noexcept
	{
		return dsc_dtype == dtype_boolean;
	}

	bool isExact() const noexcept
	{
		return dsc_dtype == dtype_int128 || dsc_dtype == dtype_int64 ||
			   dsc_dtype == dtype_long || dsc_dtype == dtype_short;
	}

	bool isNumeric() const noexcept
	{
		return DTYPE_IS_NUMERIC(dsc_dtype);
	}

	bool isText() const noexcept
	{
		return DTYPE_IS_TEXT(dsc_dtype);
	}

	bool isDbKey() const noexcept
	{
		return dsc_dtype == dtype_dbkey;
	}

	bool isDateTime() const noexcept
	{
		return DTYPE_IS_DATE(dsc_dtype);
	}

	bool isDateTimeTz() const noexcept
	{
		return dsc_dtype >= dtype_sql_time_tz && dsc_dtype <= dtype_ex_timestamp_tz;
	}

	bool isDate() const noexcept
	{
		return dsc_dtype == dtype_sql_date;
	}

	bool isTime() const noexcept
	{
		return dsc_dtype == dtype_sql_time || dsc_dtype == dtype_sql_time_tz || dsc_dtype == dtype_ex_time_tz;
	}

	bool isTimeStamp() const noexcept
	{
		return dsc_dtype == dtype_timestamp || dsc_dtype == dtype_timestamp_tz || dsc_dtype == dtype_ex_timestamp_tz;
	}

	bool isDecFloat() const noexcept
	{
		return DTYPE_IS_DECFLOAT(dsc_dtype);
	}

	bool isInt128() const noexcept
	{
		return dsc_dtype == dtype_int128;
	}

	bool isDecOrInt() const noexcept
	{
		return isDecFloat() || isExact();
	}

	bool isDecOrInt128() const noexcept
	{
		return isDecFloat() || isInt128();
	}

	bool is128() const noexcept
	{
		return dsc_dtype == dtype_dec128 || dsc_dtype == dtype_int128;
	}

	bool isApprox() const noexcept
	{
		return DTYPE_IS_APPROX(dsc_dtype);
	}

	bool isUnknown() const noexcept
	{
		return dsc_dtype == dtype_unknown;
	}

	SSHORT getBlobSubType() const noexcept
	{
		if (isBlob())
			return dsc_sub_type;

		return isc_blob_text;
	}

	SSHORT getSubType() const noexcept
	{
		if (isBlob() || isExact())
			return dsc_sub_type;

		return 0;
	}

	void setBlobSubType(SSHORT subType) noexcept
	{
		if (isBlob())
			dsc_sub_type = subType;
	}

	UCHAR getCharSet() const noexcept
	{
		if (isText())
			return dsc_sub_type & 0xFF;

		if (isBlob())
		{
			if (dsc_sub_type == isc_blob_text)
				return dsc_scale;

			return CS_BINARY;
		}

		if (isDbKey())
			return CS_BINARY;

		return CS_NONE;
	}

	USHORT getTextType() const noexcept
	{
		if (isText())
			return dsc_sub_type;

		if (isBlob())
		{
			if (dsc_sub_type == isc_blob_text)
				return dsc_scale | (dsc_flags & 0xFF00);

			return CS_BINARY;
		}

		if (isDbKey())
			return CS_BINARY;

		return CS_NONE;
	}

	void setTextType(USHORT ttype) noexcept
	{
		if (isText())
			dsc_sub_type = ttype;
		else if (isBlob() && dsc_sub_type == isc_blob_text)
		{
			dsc_scale = ttype & 0xFF;
			dsc_flags = (dsc_flags & 0xFF) | (ttype & 0xFF00);
		}
	}

	USHORT getCollation() const noexcept
	{
		return getTextType() >> 8;
	}

	void clear() noexcept
	{
		memset(this, 0, sizeof(*this));
	}

	void clearFlags() noexcept
	{
		if (isBlob() && dsc_sub_type == isc_blob_text)
			dsc_flags &= 0xFF00;
		else
			dsc_flags = 0;
	}

	void makeBlob(SSHORT subType, USHORT ttype, ISC_QUAD* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_blob;
		dsc_length = sizeof(ISC_QUAD);
		setBlobSubType(subType);
		setTextType(ttype);
		dsc_address = (UCHAR*) address;
	}

	void makeDate(GDS_DATE* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_sql_date;
		dsc_length = sizeof(GDS_DATE);
		dsc_address = (UCHAR*) address;
	}

	void makeDbkey(void* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_dbkey;
		dsc_length = sizeof(ISC_QUAD);
		dsc_address = (UCHAR*) address;
	}

	void makeDouble(double* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_double;
		dsc_length = sizeof(double);
		dsc_address = (UCHAR*) address;
	}

	void makeDecimal64(Firebird::Decimal64* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_dec64;
		dsc_length = sizeof(Firebird::Decimal64);
		dsc_address = (UCHAR*) address;
	}

	void makeDecimal128(Firebird::Decimal128* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_dec128;
		dsc_length = sizeof(Firebird::Decimal128);
		dsc_address = (UCHAR*) address;
	}

	void makeInt64(SCHAR scale, SINT64* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_int64;
		dsc_length = sizeof(SINT64);
		dsc_scale = scale;
		dsc_address = (UCHAR*) address;
	}

	void makeInt128(SCHAR scale, Firebird::Int128* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_int128;
		dsc_length = sizeof(Firebird::Int128);
		dsc_scale = scale;
		dsc_address = (UCHAR*) address;
	}

	void makeLong(SCHAR scale, SLONG* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_long;
		dsc_length = sizeof(SLONG);
		dsc_scale = scale;
		dsc_address = (UCHAR*) address;
	}

	void makeBoolean(UCHAR* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_boolean;
		dsc_length = sizeof(UCHAR);
		dsc_address = address;
	}

	void makeNullString() noexcept
	{
		clear();

		// CHAR(1) CHARACTER SET NONE
		dsc_dtype = dtype_text;
		setTextType(CS_NONE);
		dsc_length = 1;
		dsc_flags = DSC_nullable | DSC_null;
	}

	void makeShort(SCHAR scale, SSHORT* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_short;
		dsc_length = sizeof(SSHORT);
		dsc_scale = scale;
		dsc_address = (UCHAR*) address;
	}

	void makeText(USHORT length, USHORT ttype, UCHAR* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_text;
		dsc_length = length;
		setTextType(ttype);
		dsc_address = address;
	}

	void makeTime(GDS_TIME* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_sql_time;
		dsc_length = sizeof(GDS_TIME);
		dsc_scale = 0;
		dsc_address = (UCHAR*) address;
	}

	void makeTimeTz(ISC_TIME_TZ* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_sql_time_tz;
		dsc_length = sizeof(ISC_TIME_TZ);
		dsc_scale = 0;
		dsc_address = (UCHAR*) address;
	}

	void makeTimeTzEx(ISC_TIME_TZ_EX* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_ex_time_tz;
		dsc_length = sizeof(ISC_TIME_TZ_EX);
		dsc_scale = 0;
		dsc_address = (UCHAR*) address;
	}

	void makeTimestamp(GDS_TIMESTAMP* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_timestamp;
		dsc_length = sizeof(GDS_TIMESTAMP);
		dsc_scale = 0;
		dsc_address = (UCHAR*) address;
	}

	void makeTimestampTz(ISC_TIMESTAMP_TZ* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_timestamp_tz;
		dsc_length = sizeof(ISC_TIMESTAMP_TZ);
		dsc_scale = 0;
		dsc_address = (UCHAR*) address;
	}

	void makeTimestampTzEx(ISC_TIMESTAMP_TZ_EX* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_ex_timestamp_tz;
		dsc_length = sizeof(ISC_TIMESTAMP_TZ_EX);
		dsc_scale = 0;
		dsc_address = (UCHAR*) address;
	}

	void makeVarying(USHORT length, USHORT ttype, UCHAR* address = NULL) noexcept
	{
		clear();
		dsc_dtype = dtype_varying;
		dsc_length = sizeof(USHORT) + length;
		if (dsc_length < length)
		{
			// overflow - avoid segfault
			dsc_length = MAX_USHORT;
		}
		setTextType(ttype);
		dsc_address = address;
	}

	USHORT getStringLength() const noexcept;

	operator Ods::Descriptor() const
	{
#ifdef DEV_BUILD
		address32bit();
#endif
		Ods::Descriptor d;
		d.dsc_dtype = dsc_dtype;
		d.dsc_scale = dsc_scale;
		d.dsc_length = dsc_length;
		d.dsc_sub_type = dsc_sub_type;
		d.dsc_flags = dsc_flags;
		d.dsc_offset = (ULONG)(IPTR) dsc_address;
		return d;
	}

	operator paramdsc() const noexcept
	{
		paramdsc d;
		d.dsc_dtype = dsc_dtype;
		d.dsc_scale = dsc_scale;
		d.dsc_length = dsc_length;
		d.dsc_sub_type = dsc_sub_type;
		d.dsc_flags = dsc_flags;
		d.dsc_address = dsc_address;
		return d;
	}

#ifdef DEV_BUILD
	void address32bit() const;
#endif

	const char* typeToText() const noexcept;
	void getSqlInfo(SLONG* sqlLength, SLONG* sqlSubType, SLONG* sqlScale, SLONG* sqlType) const;
#endif	// __cpluplus
} DSC;

inline SSHORT DSC_GET_CHARSET(const dsc* desc) noexcept
{
	return (desc->dsc_sub_type & 0x00FF);
}

inline SSHORT DSC_GET_COLLATE(const dsc* desc) noexcept
{
	return (desc->dsc_sub_type >> 8);
}

struct alt_dsc
{
	SLONG dsc_combined_type;
	SSHORT dsc_sub_type;
	USHORT dsc_flags;			// Not currently used
};

inline bool DSC_EQUIV(const dsc* d1, const dsc* d2, bool check_collate) noexcept
{
	if (((alt_dsc*) d1)->dsc_combined_type == ((alt_dsc*) d2)->dsc_combined_type)
	{
		if ((d1->dsc_dtype >= dtype_text && d1->dsc_dtype <= dtype_varying) ||
			d1->dsc_dtype == dtype_blob)
		{
			if (d1->getCharSet() == d2->getCharSet())
			{
				if (check_collate)
					return d1->getCollation() == d2->getCollation();

				return true;
			}

			return false;
		}

		return true;
	}

	return false;
}

// In DSC_*_result tables, DTYPE_CANNOT means that the two operands
// cannot participate together in the requested operation.

inline constexpr UCHAR DTYPE_CANNOT	= 127;

// Historical alias definition
inline constexpr UCHAR dtype_date		= dtype_timestamp;

inline constexpr UCHAR dtype_aligned	= dtype_varying;
inline constexpr UCHAR dtype_any_text	= dtype_varying;
inline constexpr UCHAR dtype_min_comp	= dtype_packed;
inline constexpr UCHAR dtype_max_comp	= dtype_d_float;

// NOTE: For types <= dtype_any_text the dsc_sub_type field defines the text type

inline USHORT TEXT_LEN(const dsc* desc) noexcept
{
	return ((desc->dsc_dtype == dtype_text) ?
		desc->dsc_length :
		(desc->dsc_dtype == dtype_cstring) ?
			desc->dsc_length - 1u : desc->dsc_length - sizeof(USHORT));
}


// Text Sub types, distinct from character sets & collations

inline constexpr SSHORT dsc_text_type_none		= 0;	// Normal text
inline constexpr SSHORT dsc_text_type_fixed		= 1;	// strings can contain null bytes
inline constexpr SSHORT dsc_text_type_ascii		= 2;	// string contains only ASCII characters
inline constexpr SSHORT dsc_text_type_metadata	= 3;	// string represents system metadata


// Exact numeric subtypes: with ODS >= 10, these apply when dtype
// is short, long, or quad.

inline constexpr SSHORT dsc_num_type_none		= 0;	// defined as SMALLINT or INTEGER
inline constexpr SSHORT dsc_num_type_numeric	= 1;	// defined as NUMERIC(n,m)
inline constexpr SSHORT dsc_num_type_decimal	= 2;	// defined as DECIMAL(n,m)

// Date type information

inline constexpr SCHAR NUMERIC_SCALE(const dsc desc) noexcept
{
	return ((DTYPE_IS_TEXT(desc.dsc_dtype)) ? 0 : desc.dsc_scale);
}

inline constexpr UCHAR DEFAULT_DOUBLE  = dtype_double;

#endif // COMMON_DSC_H
