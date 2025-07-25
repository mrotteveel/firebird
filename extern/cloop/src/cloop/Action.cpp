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
 *  The Original Code was created by Alexander Peshkov.
 *
 *  Copyright (c) 2021 Alexander Peshkov <peshkoff@mail.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 */

#include "Action.h"
#include "Parser.h"
#include "Generator.h"

#include <stdio.h>

using std::string;


inline void identify(const ActionParametersBlock& apb, unsigned ident)
{
	identify(apb.out, ident);
}


void IfThenElseAction::generate(const ActionParametersBlock& apb, unsigned ident)
{
	switch(apb.language)
	{
	case LANGUAGE_C:
	case LANGUAGE_CPP:
		identify(apb, ident);
		fprintf(apb.out, "if (%s) {\n", exprIf->generate(apb.language, apb.prefix).c_str());
		actThen->generate(apb, ident + 1);
		identify(apb, ident);
		fprintf(apb.out, "}\n");
		if (actElse)
		{
			identify(apb, ident);
			fprintf(apb.out, "else {\n");
			actElse->generate(apb, ident + 1);
			identify(apb, ident);
			fprintf(apb.out, "}\n");
		}
		break;

	case LANGUAGE_PASCAL:
		identify(apb, ident);
		fprintf(apb.out, "if %s then begin\n", exprIf->generate(apb.language, apb.prefix).c_str());
		actThen->generate(apb, ident + 1);
		identify(apb, ident);
		fprintf(apb.out, "end\n");
		if (actElse)
		{
			identify(apb, ident);
			fprintf(apb.out, "else begin\n");
			actElse->generate(apb, ident + 1);
			identify(apb, ident);
			fprintf(apb.out, "end\n");
		}
		break;
	}
}


void CallAction::generate(const ActionParametersBlock& apb, unsigned ident)
{
	identify(apb, ident);
	fprintf(apb.out, "%s(", (apb.language == LANGUAGE_PASCAL ? PascalGenerator::escapeName(name) : name).c_str());
	for (auto itr = parameters.begin(); itr != parameters.end(); ++itr)
	{
		string parname = *itr;
		if (apb.language == LANGUAGE_PASCAL)
			parname = PascalGenerator::escapeName(parname);
		fprintf(apb.out, "%s%s", itr == parameters.begin() ? "" : ", ", parname.c_str());
	}
	fprintf(apb.out, ");\n");
}


void DefAction::generate(const ActionParametersBlock& apb, unsigned ident)
{
	switch(defType)
	{
	case DEF_NOT_IMPLEMENTED:
		switch(apb.language)
		{
		case LANGUAGE_C:
			if (!apb.method->statusName.empty())
			{
				identify(apb, ident);
				fprintf(apb.out, "CLOOP_setVersionError(%s, \"%s%s\", cloopVTable->version, %d);\n",
					apb.method->statusName.c_str(), apb.prefix.c_str(),
					apb.interface->name.c_str(), apb.method->version);
			}
			break;

		case LANGUAGE_CPP:
			if (!apb.method->statusName.empty())
			{
				identify(apb, ident);
				fprintf(apb.out, "%s::setVersionError(%s, \"%s%s\", cloopVTable->version, %d);\n",
					apb.exceptionClass.c_str(), apb.method->statusName.c_str(), apb.prefix.c_str(),
					apb.interface->name.c_str(), apb.method->version);
				identify(apb, ident);
				fprintf(apb.out, "%s::checkException(%s);\n",
					apb.exceptionClass.c_str(), apb.method->statusName.c_str());
			}
			break;

		case LANGUAGE_PASCAL:
			if (!apb.method->statusName.empty() && !apb.exceptionClass.empty())
			{
				identify(apb, ident);
				fprintf(apb.out, "%s.setVersionError(%s, \'%s%s\', vTable.version, %d);\n",
					apb.exceptionClass.c_str(), apb.method->statusName.c_str(), apb.prefix.c_str(),
					apb.interface->name.c_str(), apb.method->version);
			}
			break;
		}
		break;

	case DEF_IGNORE:
		if (apb.method->returnTypeRef.token.type != Token::TYPE_VOID ||
			apb.method->returnTypeRef.isPointer)
		{
			identify(apb, ident);

			switch(apb.language)
			{
			case LANGUAGE_C:
			case LANGUAGE_CPP:
				fprintf(apb.out, "return 0;\n");
				break;

			case LANGUAGE_PASCAL:
				{
					const char* sResult = "nil";
					if (!apb.method->returnTypeRef.isPointer)
					{
						switch (apb.method->returnTypeRef.token.type)
						{
						case Token::TYPE_STRING:
							break;

						case Token::TYPE_BOOLEAN:
							sResult = "false";
							break;

						case Token::TYPE_IDENTIFIER:
							if (apb.method->returnTypeRef.type == BaseType::TYPE_INTERFACE)
								break;

							// fallthru
						default:
							sResult = "0";
							break;
						}
					}

					fprintf(apb.out, "Result := %s;\n", sResult);
				}
				break;
			}
		}
		break;
	}
}


