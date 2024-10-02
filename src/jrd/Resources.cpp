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


void Resources::transfer(thread_db* tdbb, VersionedObjects* to)
{
	charSets.transfer(tdbb, to);
	relations.transfer(tdbb, to);
	procedures.transfer(tdbb, to);
	functions.transfer(tdbb, to);
	triggers.transfer(tdbb, to);
	indices.transfer(tdbb, to);
}

Resources::~Resources()
{ }

