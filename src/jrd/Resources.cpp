/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		Resources.cpp
 *	DESCRIPTION:	Resource used by request / transaction
 *
 *
 * All Rights Reserved.
 * Contributor(s): ______________________________________.
 */

#include "firebird.h"
#include "../jrd/Resources.h"

#include "../jrd/Relation.h"
#include "../jrd/CharSetContainer.h"
#include "../jrd/Function.h"
#include "../jrd/met.h"

using namespace Firebird;
using namespace Jrd;


void Resources::transfer(thread_db* tdbb, VersionedObjects* to, bool internal)
{
	charSets.transfer(tdbb, to, internal);
	relations.transfer(tdbb, to, internal);
	procedures.transfer(tdbb, to, internal);
	functions.transfer(tdbb, to, internal);
	triggers.transfer(tdbb, to, internal);
	indices.transfer(tdbb, to, internal);
}

Resources::~Resources()
{ }

