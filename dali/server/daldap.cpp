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

#include "platform.h"
#include "jlib.hpp"
#include "jiface.hpp"
#include "jencrypt.hpp"
#include "thirdparty.h"

#include "dasds.hpp"
#include "daldap.hpp"
#include "mpbase.hpp"
#include "dautils.hpp"

#ifndef _NO_LDAP
#include "seclib.hpp"
#include "ldapsecurity.hpp"

static void ignoreSigPipe()
{
#ifndef _WIN32
    struct sigaction act;
    sigset_t blockset;
    sigemptyset(&blockset);
    act.sa_mask = blockset;
    act.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &act, NULL);
#endif
}

class CDaliLdapConnection: public CInterface, implements IDaliLdapConnection
{
    Owned<ISecManager>      ldapsecurity;
    StringAttr              filesdefaultuser;
    StringAttr              filesdefaultpassword;
    unsigned                ldapflags;

    void createDefaultScopes()
    {
        try {
            ISecUser* user = NULL;
            if (ldapsecurity->addResourceEx(RT_FILE_SCOPE, *user, "file",PT_ADMINISTRATORS_ONLY, NULL))
                PROGLOG("LDAP: Created default 'file' scope");
            else
                throw MakeStringException(-1, "Error adding LDAP resource 'file'");

            StringBuffer userTempFileScope(queryDfsXmlBranchName(DXB_Internal));
            if (ldapsecurity->addResourceEx(RT_FILE_SCOPE, *user, userTempFileScope.str(),PT_ADMINISTRATORS_ONLY, NULL))
                PROGLOG("LDAP: Created default '%s' scope", userTempFileScope.str());
            else
                throw MakeStringException(-1, "Error adding LDAP resource '%s'",userTempFileScope.str());
        }
        catch (IException *e) {
            EXCLOG(e,"LDAP createDefaultScopes");
            throw;
        }   
    }


public:
    IMPLEMENT_IINTERFACE;

    CDaliLdapConnection(IPropertyTree *ldapprops)
    {
        ldapflags = 0;
        if (ldapprops) {
            if (ldapprops->getPropBool("@checkScopeScans",true))
                ldapflags |= DLF_SCOPESCANS;
            if (ldapprops->getPropBool("@safeLookup",true))
                ldapflags |= DLF_SAFE;
            const char *addr = ldapprops->queryProp("@ldapAddress");
            if (!addr || !*addr)
            {
                /* Do not give an error if blank ldap server provided (for backward compat of old configuration

                const char* pszErrMsg = "Invalid LDAP server address!";
                ERRLOG(pszErrMsg);
                throw MakeStringException(-1, pszErrMsg);
                */
            }
            else
            {
                filesdefaultuser.set(ldapprops->queryProp("@filesDefaultUser"));
                filesdefaultpassword.set(ldapprops->queryProp("@filesDefaultPassword"));

                try {
                    ignoreSigPipe(); // LDAP can generate
                    ldapprops->Link();
                    ISecManager *mgr=newLdapSecManager("", *ldapprops);
                    ldapsecurity.setown(mgr);
                    if (mgr)
                        ldapflags |= DLF_ENABLED;

                }
                catch (IException *e) {
                    EXCLOG(e,"LDAP server");
                    throw;
                }   
                createDefaultScopes();
            }
        }
    }


    int getPermissions(const char *key,const char *obj,IUserDescriptor *udesc,unsigned auditflags)
    {
        if (!ldapsecurity||((getLDAPflags()&DLF_ENABLED)==0)) 
            return 255;
        bool filescope = stricmp(key,"Scope")==0;
        bool wuscope = stricmp(key,"workunit")==0;
        if (filescope||wuscope) {
            StringBuffer username;
            StringBuffer password;
            int perm = 0;
            if (udesc) {
                udesc->getUserName(username);
                udesc->getPassword(password);
            }
            if (username.length()==0)  {
#ifndef _NO_DALIUSER_STACKTRACE
                DBGLOG("UNEXPECTED USER (NULL) in daldap.cpp getPermissions() line %d", __LINE__);
                //following debug code to be removed
                PrintStackReport();
#endif
                username.append(filesdefaultuser);
                decrypt(password, filesdefaultpassword);
            }
            unsigned start = msTick();
            Owned<ISecUser> user = ldapsecurity->createUser(username);
            if (user) {
                user->credentials().setPassword(password);
                if (filescope)
                    perm=ldapsecurity->authorizeFileScope(*user, obj);
                else if (wuscope)
                    perm=ldapsecurity->authorizeWorkunitScope(*user, obj);
                if (perm==-1)
                    perm = 0;
            }
            unsigned taken = msTick()-start;
#ifndef _DEBUG
            if (taken>100) 
#endif
            {
                PROGLOG("LDAP: getPermissions(%s) scope=%s user=%s returns %d in %d ms",key?key:"NULL",obj?obj:"NULL",username.str(),perm,taken);
            }
            if (auditflags&DALI_LDAP_AUDIT_REPORT) {
                StringBuffer auditstr;
                if ((auditflags&DALI_LDAP_READ_WANTED)&&!HASREADPERMISSION(perm)) 
                    auditstr.append("Lookup Access Denied");
                else if ((auditflags&DALI_LDAP_WRITE_WANTED)&&!HASWRITEPERMISSION(perm)) 
                    auditstr.append("Create Access Denied");
                if (auditstr.length()) {
                    auditstr.append(":\n\tProcess:\tdaserver");
                    auditstr.appendf("\n\tUser:\t%s",username.str());
                    auditstr.appendf("\n\tScope:\t%s\n",obj?obj:"");
                    SYSLOG(AUDIT_TYPE_ACCESS_FAILURE,auditstr.str());
                }
            }
            return perm;
        }
        return 255;
    }

    bool clearPermissionsCache(IUserDescriptor *udesc)
    {
        if (!ldapsecurity || ((getLDAPflags() & DLF_ENABLED) == 0))
            return true;
        StringBuffer username;
        StringBuffer password;
        udesc->getUserName(username);
        udesc->getPassword(password);
        Owned<ISecUser> user = ldapsecurity->createUser(username);
        user->credentials().setPassword(password);
        return ldapsecurity->clearPermissionsCache(*user);
    }

    bool enableScopeScans(IUserDescriptor *udesc, bool enable, int * err)
    {
        bool superUser;
        StringBuffer username;
        StringBuffer password;
        udesc->getUserName(username);
        udesc->getPassword(password);
        Owned<ISecUser> user = ldapsecurity->createUser(username);
        user->credentials().setPassword(password);
        if (!ldapsecurity->authenticateUser(*user,superUser) || !superUser)
        {
            *err = -1;
            return false;
        }
        unsigned flags = getLDAPflags();
        if (enable)
        {
            DBGLOG("Scope Scans Enabled by user %s",username.str());
            flags |= (unsigned)DLF_SCOPESCANS;
        }
        else
        {
            DBGLOG("Scope Scans Disabled by user %s",username.str());
            flags &= ~(unsigned)DLF_SCOPESCANS;
        }
        setLDAPflags(flags);
        *err = 0;
        return true;
    }

    bool checkScopeScans()
    {
        return (ldapflags&DLF_SCOPESCANS)!=0;
    }

    virtual unsigned getLDAPflags()
    {
        return ldapflags;
    }
    
    void setLDAPflags(unsigned flags)
    {
        ldapflags = flags;
    }


};


IDaliLdapConnection *createDaliLdapConnection(IPropertyTree *proptree)
{
    return new CDaliLdapConnection(proptree);
}
#else
IDaliLdapConnection *createDaliLdapConnection(IPropertyTree *proptree)
{
    return NULL;
}
#endif
