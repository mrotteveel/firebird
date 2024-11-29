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

#include "../jrd/MetaName.h"
#include "../jrd/HazardPtr.h"
#include "../jrd/Collation.h"
#include "../jrd/Resources.h"
#include "../jrd/met_proto.h"
#include "../common/classes/alloc.h"


namespace Jrd {

struct SubtypeInfo;

class CharSetContainer : public Firebird::PermanentStorage
{
public:
	CharSetContainer(thread_db* tdbb, MemoryPool& p, MetaId cs_id, MakeLock* makeLock, NoData);

	static bool destroy(thread_db* tdbb, CharSetContainer* container)
	{
		container->cs->destroy();
		return false;
	}

	static CharSetContainer* create(thread_db* tdbb, MetaId id);
	static int blockingAst(void* ast_object);

	CharSet* getCharSet()
	{
		return cs;
	}

	CsConvert lookupConverter(thread_db* tdbb, CSetId to_cs);

	static CharSetContainer* lookupCharset(thread_db* tdbb, TTypeId ttype);

	bool hasData() const
	{
		return cs != nullptr;
	}

	const char* c_name() const
	{
		return cs->getName();
	}

	MetaName getName() const
	{
		return cs->getName();
	}

	MetaId getId();

	Lock* getLock()
	{
		return cs_lock;
	}

	void releaseLocks(thread_db* tdbb);

private:
	static bool lookupInternalCharSet(CSetId id, SubtypeInfo* info);

public:
	CharsetVariants names;

private:
	CharSet* cs;
	Lock* cs_lock;
};

class CharSetVers final : public ObjectBase
{
public:
	CharSetVers(Cached::CharSet* parent)
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

	MetaId getId()
	{
		return perm->getId();
	}

	MetaName getName() const
	{
		return perm->getName();
	}

	static void destroy(thread_db* tdbb, CharSetVers* csv);
	static CharSetVers* create(thread_db* tdbb, MemoryPool& p, Cached::CharSet* perm);
	static Lock* makeLock(thread_db* tdbb, MemoryPool& p);
	bool scan(thread_db* tdbb, ObjectBase::Flag flags);

	bool reload(thread_db* tdbb, ObjectBase::Flag flags)
	{
		return scan(tdbb, flags);
	}

	Collation* getCollation(CollId id);
	Collation* getCollation(MetaName name);
	Cached::CharSet* getContainer() const
	{
		return perm;
	}

	static int objectType();

private:
	Cached::CharSet* perm;
	Firebird::HalfStaticArray<Collation*, 16> charset_collations;

public:
	decltype(perm) getPermanent() const
	{
		return perm;
	}
};

} // namespace Jrd

#endif // JRD_CHARSETCONTAINER_H

