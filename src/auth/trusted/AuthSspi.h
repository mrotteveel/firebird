/*
 *	PROGRAM:		Firebird authentication
 *	MODULE:			AuthSspi.h
 *	DESCRIPTION:	Windows trusted authentication
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
 *  The Original Code was created by Alex Peshkov
 *  for the Firebird Open Source RDBMS project.
 *
 *  Copyright (c) 2006 Alex Peshkov <peshkoff at mail.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 *
 *
 */
#ifndef AUTH_SSPI_H
#define AUTH_SSPI_H

#include <firebird.h>

// This is old versions backward compatibility
#define FB_PREDEFINED_GROUP "Predefined_Group"
#define FB_DOMAIN_ANY_RID_ADMINS "DOMAIN_ANY_RID_ADMINS"

#ifdef TRUSTED_AUTH

#include <../common/classes/fb_string.h>
#include <../common/classes/array.h>
#include "../common/classes/ImplementHelper.h"
#include <ibase.h>
#include "firebird/Interface.h"
#include "../common/classes/objects_array.h"

#define SECURITY_WIN32

#include <windows.h>
#include <Security.h>
#include <stdio.h>

namespace Auth {

class AuthSspi
{
public:
	typedef Firebird::ObjectsArray<Firebird::string> GroupsList;
	typedef Firebird::UCharBuffer Key;

private:
	static constexpr size_t BUFSIZE = 4096;

	SecHandle secHndl;
	bool hasCredentials;
	CtxtHandle ctxtHndl;
	bool hasContext;
	Firebird::string ctName;
	bool wheel;
	GroupsList groupNames;
	Key sessionKey;

	// Handle of library
	static HINSTANCE library;

	// declare entries, required from secur32.dll
	ACQUIRE_CREDENTIALS_HANDLE_FN_A fAcquireCredentialsHandle;
	DELETE_SECURITY_CONTEXT_FN fDeleteSecurityContext;
	FREE_CREDENTIALS_HANDLE_FN fFreeCredentialsHandle;
	QUERY_CONTEXT_ATTRIBUTES_FN_A fQueryContextAttributes;
	FREE_CONTEXT_BUFFER_FN fFreeContextBuffer;
	INITIALIZE_SECURITY_CONTEXT_FN_A fInitializeSecurityContext;
	ACCEPT_SECURITY_CONTEXT_FN fAcceptSecurityContext;

	bool checkAdminPrivilege();
	bool initEntries() noexcept;

public:
	typedef Firebird::Array<unsigned char> DataHolder;

	AuthSspi();
	~AuthSspi();

	// true when has non-empty security context,
	// ready to be sent to the other side
	bool isActive() const noexcept
	{
		return hasContext;
	}

	// prepare security context to be sent to the server (used by client)
	bool request(DataHolder& data);

	// accept security context from the client (used by server)
	bool accept(DataHolder& data);

	// returns Windows user/group names, matching accepted security context
	bool getLogin(Firebird::string& login, bool& wh, GroupsList& grNames);

	// returns session key for wire encryption
	const Key* getKey() const noexcept;
};

class WinSspiServer :
	public Firebird::StdPlugin<Firebird::IServerImpl<WinSspiServer, Firebird::CheckStatusWrapper> >
{
public:
	// IServer implementation
	int authenticate(Firebird::CheckStatusWrapper* status, Firebird::IServerBlock* sBlock,
		Firebird::IWriter* writerInterface);
	void setDbCryptCallback(Firebird::CheckStatusWrapper* status, Firebird::ICryptKeyCallback* callback) noexcept {}; // do nothing

	WinSspiServer(Firebird::IPluginConfig*);

private:
	AuthSspi::DataHolder sspiData;
	AuthSspi sspi;
	bool done;
};

class WinSspiClient :
	public Firebird::StdPlugin<Firebird::IClientImpl<WinSspiClient, Firebird::CheckStatusWrapper> >
{
public:
	// IClient implementation
	int authenticate(Firebird::CheckStatusWrapper* status, Firebird::IClientBlock* sBlock);

	WinSspiClient(Firebird::IPluginConfig*);

private:
	AuthSspi::DataHolder sspiData;
	AuthSspi sspi;
	bool keySet;
};

void registerTrustedClient(Firebird::IPluginManager* iPlugin);
void registerTrustedServer(Firebird::IPluginManager* iPlugin);

// Set per-thread flag that specify which security package should be used by
// newly created plugin instances: true - use NTLM, false - use Negotiate.
void setLegacySSP(bool value) noexcept;

} // namespace Auth

#endif // TRUSTED_AUTH
#endif // AUTH_SSPI_H
