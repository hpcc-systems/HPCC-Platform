/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */
#pragma once

// Small, LDAP-connection-free helper functions that build the escaped filter/DN fragments
// used at several security-sensitive call sites in ldapconnection.cpp and aci.cpp. Extracted
// out so the same code can be exercised directly by unittests/querybuilderstests.cpp
// (rather than the tests reconstructing the fragment separately, which would not catch a
// future edit that drops the escaping call from the production call site).

#include "jstring.hpp"

// Appends "sAMAccountName=<escaped value>" to filter. Used by getGroups() (Active Directory
// branch) and enableUser() when searching for a user by sAMAccountName.
void appendSAMAccountNameFilter(const char *value, StringBuffer &filter);

// Given a filter buffer already containing a base filter (e.g. "objectClass=*"), wraps it as
// "(&(<filter>)(|(uNCName=*<escaped searchstr>*)))" when searchstr is non-empty and not "*",
// leaving filter unchanged otherwise. Matches getResources()'s and countResources()'s
// search-string handling.
void appendResourceNameSearchFilter(const char *searchstr, StringBuffer &filter);

// Appends "cn=<escaped groupname>," to dn. Used by addGroup() and getGroupDN() as the common
// prefix before the group base DN is appended.
void appendGroupCnPrefix(const char *groupname, StringBuffer &dn);

// Appends "uid=<escaped username>,<userBasedn>" to userdn. Used by getUserDN()'s non-Active-
// Directory branch, addUser()'s non-Active-Directory branch (when adding a new user's DN),
// appendAciUserdnClause(), and CAciList::getPermissions() (aci.cpp) when reconstructing a
// candidate user's userdn to compare against a parsed ACI's stored userdn clause.
void appendUserUidDn(const char *username, const char *userBasedn, StringBuffer &userdn);

// Appends '(userdn = "ldap:///uid=<escaped username>,<userBasedn>");)' to sd. Used by
// CIPlanetAciProcessor::createDefaultSD() to build the per-user default ACI.
void appendAciUserdnClause(const char *username, const char *userBasedn, StringBuffer &sd);

// Returns true if, for a non-Active-Directory server, username contains a character forbidden
// in the LDAP URL embedded in an ACI userdn clause ('"' would break out of the ACI string
// enabling clause injection; '?' would truncate the URL's DN component). Used by addUser() to
// reject such usernames before they are ever embedded, unescaped, into an ACI.
bool shouldRejectAddUserUsername(bool isActiveDirectory, const char *username);
