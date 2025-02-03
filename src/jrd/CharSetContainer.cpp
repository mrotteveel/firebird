/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		CharSetContainer.cpp
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

#include "firebird.h"
#include "../jrd/CharSetContainer.h"
#include "../jrd/jrd.h"
#include "../jrd/obj.h"

using namespace Jrd;

CharSetVers* CharSetVers::create(thread_db* tdbb, MemoryPool& pool, Cached::CharSet* csp)
{
	return FB_NEW_POOL(pool) CharSetVers(csp);
}

void CharSetVers::destroy(thread_db*, CharSetVers* csv)
{
	delete csv;
}

MetaId CharSetContainer::getId()
{
	return cs->getId();
}

Lock* CharSetVers::makeLock(thread_db* tdbb, MemoryPool& p)
{
	return FB_NEW_RPT(p, 0) Lock(tdbb, sizeof(SLONG), LCK_cs_exist, nullptr, CharSetContainer::blockingAst);
}

int CharSetContainer::blockingAst(void* ast_object)
{
	auto* const charSet = static_cast<Cached::CharSet*>(ast_object);

	try
	{
		Database* const dbb = charSet->cs_lock->lck_dbb;

		AsyncContextHolder tdbb(dbb, FB_FUNCTION, charSet->cs_lock);

		LCK_release(tdbb, charSet->cs_lock);
		charSet->resetDependentObject(tdbb, ElementBase::ResetType::Mark);
	}
	catch (const Firebird::Exception&)
	{} // no-op

	return 0;
}

void CharSetContainer::releaseLocks(thread_db* tdbb)
{
	LCK_release(tdbb, cs_lock);
}

bool CharSetContainer::destroy(thread_db* tdbb, CharSetContainer* container)
{
	container->cs->destroy();
	return false;
}

int CharSetVers::objectType()
{
	return obj_charset;
}

const char* CharSetContainer::c_name() const
{
	return cs->getName();
}

MetaName CharSetContainer::getName() const
{
	return cs->getName();
}
