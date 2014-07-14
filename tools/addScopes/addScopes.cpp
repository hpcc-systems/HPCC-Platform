/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "seclib.hpp"
#include "ldapsecurity.hpp"
#include "jliball.hpp"

#ifndef _WIN32
#include <unistd.h>
#endif

int main(int argc, char* argv[])
{
    if(argc != 2)
    {
        printf("usage: addScopes daliconf.xml\n");
        printf("\n\tCreates all user-specific LDAP private file scopes 'hpccinternal::<user>'\n\tand grants users access to their scope. The configuration file\n\tdaliconf.xml is the dali configuration file, typically\n\tfound in /var/lib/HPCCSystems/mydali\n\n");
        return -1;
    }

    InitModuleObjects();

    try
    {
        Owned<IPropertyTree> cfg = createPTreeFromXMLFile(argv[1]);
        Owned<IPropertyTree> seccfg = cfg->getPropTree(".//ldapSecurity");
        if(seccfg == NULL)
        {
            printf("ldapSecurity not found\n");
            return -1;
        }
#ifdef _NO_LDAP
        printf("System was built with _NO_LDAP\n");
        return -1;
#else
        Owned<ISecManager> secmgr = newLdapSecManager("addScopes", *LINK(seccfg));
        if(secmgr == NULL)
        {
            printf("Security manager can't be created\n");
            return -1;
        }
        bool ok = secmgr->createUserScopes();
        printf(ok ? "User scopes added\n" : "Some scopes not added\n");
#endif
    }
    catch(IException* e)
    {
        StringBuffer errmsg;
        e->errorMessage(errmsg);
        printf("%s\n", errmsg.str());
    }
    catch(...)
    {
        printf("Unknown exception\n");
    }

    releaseAtoms();
    return 0;
}
