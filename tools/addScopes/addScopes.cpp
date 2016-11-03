/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include "dasess.hpp"

#ifndef _WIN32
#include <unistd.h>
#endif

int main(int argc, char* argv[])
{
    if(argc < 2  || argc > 3)
    {
        printf("usage: addScopes daliconf.xml [-c]\n");
        printf("\n\tCreates all user-specific LDAP private file scopes 'hpccinternal::<user>'\n\tand grants users access to their scope. The configuration file\n\tdaliconf.xml is the dali configuration file, typically\n\tfound in /var/lib/HPCCSystems/mydali\n\tSpecify -c to make changes immediately visible by clearing permission caches\n\n");
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
            releaseAtoms();
            return -1;
        }
        bool ok = secmgr->createUserScopes();
        printf(ok ? "User scopes added\n" : "Some scopes not added\n");

        //Clear permission caches?
        if (argc == 3 && 0==stricmp(argv[2], "-c"))
        {
            //Clear ESP Cache
            StringBuffer sysuser;
            StringBuffer passbuf;
            seccfg->getProp(".//@systemUser", sysuser);
            seccfg->getProp(".//@systemPassword", passbuf);

            if (0 == sysuser.length())
            {
                printf("Error in configuration file %s - systemUser not specified", argv[1]);
                releaseAtoms();
                return -1;
            }

            if (0 == passbuf.length())
            {
                printf("Error in configuration file %s - systemPassword not specified", argv[1]);
                releaseAtoms();
                return -1;
            }

            StringBuffer decPwd;
            decrypt(decPwd, passbuf.str());

            //Clear Dali cache
            Owned<IUserDescriptor> userdesc(createUserDescriptor());
            userdesc->set(sysuser, decPwd);
            ok = querySessionManager().clearPermissionsCache(userdesc);
            printf(ok ? "Dali Cache cleared\n" : "Error clearing Dali Cache\n");
        }
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
