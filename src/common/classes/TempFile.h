/*
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
 *  Copyright (c) 2006 Dmitry Yemanov <dimitr@users.sf.net>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 */

#ifndef CLASSES_TEMP_FILE_H
#define CLASSES_TEMP_FILE_H

#include "firebird.h"
#include "../common/classes/fb_string.h"
#include "../common/classes/File.h"
#include "firebird/Interface.h"

namespace Firebird {

class TempFile : public File
{
public:
	TempFile(MemoryPool& pool, const PathName& prefix, const PathName& directory,
			 bool do_unlink = true)
		: filename(pool), position(0), size(0), doUnlink(do_unlink)
	{
		init(directory, prefix);
	}

	TempFile(const PathName& prefix, bool do_unlink = true)
		: position(0), size(0), doUnlink(do_unlink)
	{
		init("", prefix);
	}

	virtual ~TempFile();

	FB_SIZE_T read(offset_t, void*, FB_SIZE_T) override;
	FB_SIZE_T write(offset_t, const void*, FB_SIZE_T) override;

	void unlink() noexcept override;

	offset_t getSize() const noexcept override
	{
		return size;
	}

	void extend(offset_t);

	const PathName& getName() const noexcept
	{
		return filename;
	}

	static PathName getTempPath();
	static PathName create(const PathName& prefix, const PathName& directory = "");
	static PathName create(CheckStatusWrapper* status, const PathName& prefix,
		const PathName& directory = "");

private:
	void init(const PathName&, const PathName&);
	void seek(const offset_t);

#if defined(WIN_NT)
	HANDLE handle;
#else
	int handle;
#endif

	PathName filename;
	offset_t position;
	offset_t size;
	bool doUnlink;
};

}	// namespace Firebird

#endif // CLASSES_TEMP_FILE_H
