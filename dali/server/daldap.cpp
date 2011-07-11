/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#include "platform.h"
#include "jlib.hpp"
#include "jiface.hpp"
#include "jencrypt.hpp"
#include "thirdparty.h"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/dali/server/daldap.cpp $ $Id: daldap.cpp 62376 2011-02-04 21:59:58Z sort $");

#include "dasds.hpp"
#include "daldap.hpp"

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
