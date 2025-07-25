/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		blb.h
 *	DESCRIPTION:	Blob handling definitions
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
 * 2002.10.28 Sean Leyne - Code cleanup, removed obsolete "DecOSF" port
 *
 */

#ifndef JRD_BLB_H
#define JRD_BLB_H

#include "../include/fb_blk.h"

#include "../jrd/RecordNumber.h"
#include "../jrd/EngineInterface.h"
#include "../common/classes/array.h"
#include "../common/classes/File.h"
#include "../common/classes/auto.h"

#include "firebird/Interface.h"
#include "../common/classes/ImplementHelper.h"
#include "../common/dsc.h"

namespace Ods
{
	struct blob_page;
	struct blh;
}

namespace Jrd
{

class Attachment;
class BlobControl;
class jrd_rel;
class Request;
class jrd_tra;
class vcl;
class thread_db;
struct win;
class Record;
class ArrayField;
struct impure_value;


// Your basic blob block.

class blb : public pool_alloc<type_blb>
{
public:
	blb(MemoryPool& pool, USHORT page_size)
		: blb_buffer(pool, page_size / sizeof(SLONG)),
		  blb_has_buffer(true)
	{
	}

	jrd_rel* blb_relation = nullptr; // Relation, if known
	JBlob* blb_interface = nullptr;

	FB_UINT64 blb_length = 0;	// Blob's total length (in bytes)
	USHORT blb_flags = 0;		// Interesting stuff (see below)

	SSHORT blb_sub_type = isc_blob_untyped; // Blob's declared sub-type
	UCHAR blb_charset = 0;		// Blob's charset

	// inline functions
	bool hasBuffer() const;
	UCHAR* getBuffer();
	void freeBuffer();
	bool isSegmented() const;
	Attachment* getAttachment();
	jrd_tra* getTransaction();
	USHORT getLevel() const;
	ULONG getMaxSequence() const;
	ULONG getTempId() const;
	ULONG getSegmentCount() const;
	USHORT getFragmentSize() const;
	USHORT getMaxSegment() const;
	// end inline

	void	BLB_cancel(thread_db* tdbb);
	void	BLB_cancel();
	void	BLB_check_well_formed(thread_db*, const dsc* desc);
	bool	BLB_close(thread_db*);
	static blb*	create(thread_db*, jrd_tra*, bid*);
	static blb*	create2(thread_db*, jrd_tra*, bid*, USHORT, const UCHAR*, bool = false);
	static Jrd::blb* get_array(Jrd::thread_db*, Jrd::jrd_tra*, const Jrd::bid*, Ods::InternalArrayDesc*);
	ULONG	BLB_get_data(thread_db*, UCHAR*, SLONG, bool = true);
	USHORT	BLB_get_segment(thread_db*, void*, USHORT);
	static SLONG get_slice(Jrd::thread_db*, Jrd::jrd_tra*, const Jrd::bid*, const UCHAR*, USHORT,
					const UCHAR*, SLONG, UCHAR*);
	SLONG	BLB_lseek(USHORT, SLONG);
	static void	move(thread_db* tdbb, dsc* from_desc, dsc* to_desc, jrd_rel* relation = nullptr, Record* record = nullptr, USHORT fieldId = 0, bool bulk = false);
	static blb* open(thread_db*, jrd_tra*, const bid*);
	static blb* open2(thread_db*, jrd_tra*, const bid*, USHORT, const UCHAR*, bool = false);
	void	BLB_put_data(thread_db*, const UCHAR*, SLONG);
	void	BLB_put_segment(thread_db*, const void*, USHORT);
	static void	put_slice(thread_db*, jrd_tra*, bid*, const UCHAR*, USHORT, const UCHAR*, SLONG, UCHAR*);
	static void release_array(Jrd::ArrayField*);
	static void scalar(Jrd::thread_db*, Jrd::jrd_tra*, const Jrd::bid*, USHORT, const SLONG*, Jrd::impure_value*);

	static void delete_blob_id(thread_db*, const bid*, ULONG, jrd_rel*);
	void fromPageHeader(const Ods::blh* header);
	void toPageHeader(Ods::blh* header) const;
	void getFromPage(USHORT length, const UCHAR* data);
	void storeToPage(USHORT* length, Firebird::Array<UCHAR>& buffer, const UCHAR** data, void* stack);

	static bid copy(thread_db* tdbb, const bid* source)
	{
		bid destination;
		copy_blob(tdbb, source, &destination, 0, nullptr, 0);
		return destination;
	}

private:
	static blb* allocate_blob(thread_db*, jrd_tra*);
	static blb* copy_blob(thread_db* tdbb, const bid* source, bid* destination,
					USHORT bpb_length, const UCHAR* bpb, USHORT destPageSpaceID);
	void delete_blob(thread_db*, ULONG);
	Ods::blob_page* get_next_page(thread_db*, win*);
	void insert_page(thread_db*);
	void destroy(const bool purge_flag);

	FB_SIZE_T blb_temp_size = 0;	// size stored in transaction temp space
	offset_t blb_temp_offset = 0;	// offset in transaction temp space
	Attachment* blb_attachment = nullptr; // database attachment
	jrd_tra* blb_transaction = nullptr; // Parent transaction block
	UCHAR* blb_segment = nullptr;	// Next segment to be addressed
	BlobControl* blb_filter = nullptr; // Blob filter control block, if any
	bid blb_blob_id{};				// Id of materialized blob
	vcl* blb_pages = nullptr;		// Vector of pages

	Firebird::Array<SLONG> blb_buffer; // buffer used in opened blobs - must be longword aligned

	ULONG blb_temp_id = 0;			// ID of newly created blob in transaction
	ULONG blb_sequence = 0;			// Blob page sequence
	ULONG blb_lead_page = 0;		// First page number
	FB_UINT64 blb_seek = 0;			// Seek location
	ULONG blb_max_sequence = 0;		// Number of data pages
	ULONG blb_count = 0;			// Number of segments

	USHORT blb_pointers = 0;		// Max pointer on a page
	USHORT blb_clump_size = 0;		// Size of data clump
	USHORT blb_space_remaining = 0;	// Data space left
	USHORT blb_max_pages = 0;		// Max pages in vector
	USHORT blb_level = 0;			// Storage type
	USHORT blb_pg_space_id = 0;		// page space
	USHORT blb_fragment_size = 0;	// Residual fragment size
	USHORT blb_max_segment = 0;		// Longest segment
#ifdef CHECK_BLOB_FIELD_ACCESS_FOR_SELECT
	USHORT blb_fld_id = 0;			// Field ID
#endif
	bool blb_has_buffer;
};

inline constexpr int BLB_temporary		= 1;		// Newly created blob
inline constexpr int BLB_eof			= 2;		// This blob is exhausted
inline constexpr int BLB_stream			= 4;		// Stream style blob
inline constexpr int BLB_closed			= 8;		// Temporary blob has been closed
inline constexpr int BLB_damaged		= 16;		// Blob is busted
inline constexpr int BLB_seek			= 32;		// Seek is pending
inline constexpr int BLB_large_scan		= 64;		// Blob is larger than page buffer cache
inline constexpr int BLB_close_on_read	= 128;		// Temporary blob is not closed until read
inline constexpr int BLB_bulk			= 256;		// Blob created by bulk insert operation
inline constexpr int BLB_user			= 512;		// User-defined blob

/* Blob levels are:

	0	small blob -- blob "record" is actual data
	1	medium blob -- blob "record" is pointer to pages
	2	large blob -- blob "record" is pointer to pages of pointers
*/


inline bool blb::hasBuffer() const
{
	return blb_has_buffer;
}

inline UCHAR* blb::getBuffer()
{
	fb_assert(blb_has_buffer);
	return (UCHAR*) blb_buffer.getBuffer(blb_buffer.getCapacity());
}

inline void blb::freeBuffer()
{
	fb_assert(blb_has_buffer);
	blb_buffer.free();
	blb_has_buffer = false;
}

inline bool blb::isSegmented() const
{
	return !(blb_flags & BLB_stream);
}

inline Attachment* blb::getAttachment()
{
	return blb_attachment;
}

inline jrd_tra* blb::getTransaction()
{
	return blb_transaction;
}

inline USHORT blb::getLevel() const
{
	return blb_level;
}

inline ULONG blb::getMaxSequence() const
{
	return blb_max_sequence;
}

inline ULONG blb::getTempId() const
{
	return blb_temp_id;
}

inline ULONG blb::getSegmentCount() const
{
	return blb_count;
}

inline USHORT blb::getFragmentSize() const
{
	return blb_fragment_size;
}

inline USHORT blb::getMaxSegment() const
{
	return blb_max_segment;
}


} //namespace Jrd


namespace Firebird {

template <>
inline void SimpleDelete<Jrd::blb>::clear(Jrd::blb* b)
{
	if (b)
		b->BLB_cancel();
}

} //namespace Firebird


#endif // JRD_BLB_H
