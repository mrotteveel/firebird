/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		CharSetContainer.h
 *	DESCRIPTION:	Container for character set and it's collations
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
 * Alex Peshkoff <peshkoff@mail.ru>
 */

#ifndef JRD_CHARSETCONTAINER_H
#define JRD_CHARSETCONTAINER_H

#include "../jrd/HazardPtr.h"
#include "../jrd/Collation.h"
#include "../common/classes/alloc.h"

struct SubtypeInfo;

namespace Jrd {

typedef UCHAR CollId;

class CharSetContainer : public Firebird::PermanentStorage
{
public:
	CharSetContainer(thread_db* tdbb, MemoryPool& p, MetaId cs_id, /*const SubtypeInfo* info*/Lock* lock);

	void destroy()
	{
		cs->destroy();
	}

	static void destroy(CharSetContainer* container)
	{
		container->destroy();
		delete container;
	}

	static CharSetContainer* create(thread_db* tdbb, MetaId id);
	static Lock* makeLock(thread_db* tdbb, MemoryPool& p);

	CharSet* getCharSet()
	{
		return cs;
	}

	CsConvert lookupConverter(thread_db* tdbb, CHARSET_ID to_cs);

	static CharSetContainer* lookupCharset(thread_db* tdbb, USHORT ttype);
	static Lock* createCollationLock(thread_db* tdbb, USHORT ttype, void* object = NULL);

	bool hasData() const
	{
		return cs != nullptr;
	}

	const char* c_name() const
	{
		return cs->getName();
	}

	MetaId getId();

	Lock* getLock()
	{
		return cs_lock;
	}

private:
	static bool lookupInternalCharSet(USHORT id, SubtypeInfo* info);

private:
	CharSet* cs;
	Lock* cs_lock;
};

class CharSetVers final : public ObjectBase
{
public:
	CharSetVers(CharSetContainer* parent)
		: perm(parent), charset_collations(perm->getPool())
	{ }

	const char* c_name() const override
	{
		return perm->c_name();
	}

	static const char* objectFamily(void*)
	{
		return "character set";
	}

	void release(thread_db* tdbb)
	{
		for (auto coll : charset_collations)
		{
			if (coll)
				coll->release(tdbb);
		}
	}

	static void destroy(CharSetVers* csv);
	static CharSetVers* create(thread_db* tdbb, MemoryPool& p, CharSetContainer* perm);
	void scan(thread_db* tdbb, ObjectBase::Flag flags);
	static Lock* makeLock(thread_db*, MemoryPool&);

	Collation* lookupCollation(thread_db* tdbb, MetaId tt_id);
	Collation* lookupCollation(thread_db* tdbb, MetaName name);
	//void unloadCollation(thread_db* tdbb, USHORT tt_id);
	Collation* getCollation(CollId collId);

private:
	CharSetContainer* perm;
	Firebird::HalfStaticArray<Collation*, 16> charset_collations;
};

namespace Rsc
{

class Coll
{
public:
	Coll(CSet* cs, CollId id)
		: cSet(cs), collId(id)
	{ }

	Collation* operator()(const VersionedObjects* runTime) const
	{
		return (*cSet)(runTime)->getCollation(collId);
	}

	Collation* operator()(thread_db* tdbb) const
	{
		return (*cSet)(tdbb)->getCollation(collId);
	}

private:
	CSet* cSet;
	CollId collId;
};

} // namespace Rsc

} // namespace Jrd

#endif // JRD_CHARSETCONTAINER_H

