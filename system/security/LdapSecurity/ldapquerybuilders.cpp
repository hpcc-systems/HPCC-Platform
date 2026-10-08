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

#include "ldapquerybuilders.hpp"
#include "ldapsanitization.hpp"

void appendSAMAccountNameFilter(const char *value, StringBuffer &filter)
{
    filter.append("sAMAccountName=");
    appendEscapedLdapFilter(value, filter);
}

void appendResourceNameSearchFilter(const char *searchstr, StringBuffer &filter)
{
    if (searchstr && *searchstr && strcmp(searchstr, "*") != 0)
    {
        filter.insert(0, "(&(");
        StringBuffer escapedSearch;
        appendEscapedLdapFilter(searchstr, escapedSearch);
        filter.appendf(")(|(%s=*%s*)))", "uNCName", escapedSearch.str());
    }
}

void appendGroupCnPrefix(const char *groupname, StringBuffer &dn)
{
    dn.append("cn=");
    escapeLdapDistinguishedName(groupname, dn);
    dn.append(",");
}

void appendUserUidDn(const char *username, const char *userBasedn, StringBuffer &userdn)
{
    userdn.append("uid=");
    escapeLdapDistinguishedName(username, userdn);
    userdn.append(",").append(userBasedn);
}

void appendAciUserdnClause(const char *username, const char *userBasedn, StringBuffer &sd)
{
    sd.append("(userdn = \"ldap:///");
    appendUserUidDn(username, userBasedn, sd);
    sd.append("\");)");
}

bool shouldRejectAddUserUsername(bool isActiveDirectory, const char *username)
{
    return !isActiveDirectory && usernameContainsLdapUrlForbiddenChars(username);
}
