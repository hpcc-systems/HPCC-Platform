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

#include "platform.h"
#include "jlib.hpp"
#include "jiface.hpp"
#include "jencrypt.hpp"
#include "thirdparty.h"

#include "dasds.hpp"
#include "daldap.hpp"
#include "mpbase.hpp"
#include "dautils.hpp"
#include "digisign.hpp"

using namespace cryptohelper;

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
    act.sa_flags = 0;
    sigaction(SIGPIPE, &act, NULL);
#endif
}

class CDaliLdapConnection: implements IDaliLdapConnection, public CInterface
{
    Owned<ISecManager>      ldapsecurity;
    StringAttr              filesdefaultuser;
    StringAttr              filesdefaultpassword;
    unsigned                ldapflags;
    unsigned                requestSignatureExpiryMinutes;//Age at which a dali permissions request signature becomes invalid
    IDigitalSignatureManager * pDSM = nullptr;

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
            requestSignatureExpiryMinutes = ldapprops->getPropInt("@reqSignatureExpiry", 10);
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


    SecAccessFlags getPermissions(const char *key,const char *obj,IUserDescriptor *udesc,unsigned auditflags,const char * reqSignature, CDateTime & reqUTCTimestamp)
    {
        if (!ldapsecurity||((getLDAPflags()&DLF_ENABLED)==0)) 
            return SecAccess_Full;
        StringBuffer username;
        StringBuffer password;
        if (udesc) 
        {
            udesc->getUserName(username);
            udesc->getPassword(password);
        }
        else
        {
            WARNLOG("NULL UserDescriptor in daldap.cpp getPermissions('%s')",key ? key : "NULL");
        }

        if (0 == username.length())
        {
            username.append(filesdefaultuser);
            decrypt(password, filesdefaultpassword);
            WARNLOG("Missing credentials, injecting deprecated filesdefaultuser");
            reqSignature = nullptr;
        }

        Owned<ISecUser> user = ldapsecurity->createUser(username);
        user->credentials().setPassword(password);

        //Check that the digital signature provided by the caller (signature of
        //caller's "scope;username;timeStamp") matches what we expect it to be
        if (!isEmptyString(reqSignature))
        {
            if (nullptr == pDSM)
                pDSM = queryDigitalSignatureManagerInstanceFromEnv();
            if (pDSM && pDSM->isDigiVerifierConfigured())
            {
                StringBuffer requestTimestamp;
                reqUTCTimestamp.getString(requestTimestamp, false);//extract timestamp string from Dali request

                CDateTime now;
                now.setNow();
                if (now.compare(reqUTCTimestamp) < 0)//timestamp from the future?
                {
                    ERRLOG("LDAP: getPermissions(%s) scope=%s user=%s Request digital signature timestamp %s from the future",key?key:"NULL",obj?obj:"NULL",username.str(), requestTimestamp.str());
                    return SecAccess_None;//deny
                }

                CDateTime expiry;
                expiry.set(now);
                expiry.adjustTime(requestSignatureExpiryMinutes);//compute expiration timestamp

                if (expiry.compare(reqUTCTimestamp) < 0)//timestamp too far in the past?
                {
                    ERRLOG("LDAP: getPermissions(%s) scope=%s user=%s Expired request digital signature timestamp %s",key?key:"NULL",obj?obj:"NULL",username.str(), requestTimestamp.str());
                    return SecAccess_None;//deny
                }

                VStringBuffer expectedStr("%s;%s;%s", obj, username.str(), requestTimestamp.str());
                StringBuffer b64Signature(reqSignature);// signature of scope;user;timestamp

                if (!pDSM->digiVerify(b64Signature, expectedStr))//does the digital signature match what we expect?
                {
                    ERRLOG("LDAP: getPermissions(%s) scope=%s user=%s fails digital signature verification",key?key:"NULL",obj?obj:"NULL",username.str());
                    return SecAccess_None;//deny
                }

                //Mark user as authenticated. The call below to authenticateUser
                //will add this user to the LDAP cache
                user->setAuthenticateStatus(AS_AUTHENTICATED);
            }
            else
                ERRLOG("LDAP: getPermissions(%s) scope=%s user=%s digital signature support not available",key?key:"NULL",obj?obj:"NULL",username.str());
        }

        if (!isEmptyString(user->credentials().getPassword()))
        {
            if (!ldapsecurity->authenticateUser(*user, NULL))
            {
                const char * extra = "";
                if (isEmptyString(reqSignature))
                    extra = " (Password or Dali Signature not provided)";
                ERRLOG("LDAP: getPermissions(%s) scope=%s user=%s fails LDAP authentication%s",key?key:"NULL",obj?obj:"NULL",username.str(), extra);
                return SecAccess_None;//deny
            }
        }
        else
            user->setAuthenticateStatus(AS_AUTHENTICATED);

        bool filescope = stricmp(key,"Scope")==0;
        bool wuscope = stricmp(key,"workunit")==0;

        if (filescope || wuscope) {
            SecAccessFlags perm = SecAccess_None;
            unsigned start = msTick();
            if (filescope)
                perm=ldapsecurity->authorizeFileScope(*user, obj);
            else if (wuscope)
                perm=ldapsecurity->authorizeWorkunitScope(*user, obj);
            if (perm == SecAccess_Unavailable)
                perm = SecAccess_None;

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
        return SecAccess_Full;
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

        //Check user's digital signature, if present
        bool authenticated = false;
        if (!isEmptyString(udesc->querySignature()))
        {
            if (nullptr == pDSM)
                pDSM = queryDigitalSignatureManagerInstanceFromEnv();
            if (pDSM && pDSM->isDigiVerifierConfigured())
            {
                StringBuffer b64Signature(udesc->querySignature());
                if (!pDSM->digiVerify(b64Signature, username))//digital signature valid?
                {
                    ERRLOG("LDAP: enableScopeScans(%s) : Invalid user digital signature", username.str());
                    *err = -1;
                    return false;
                }
                else
                    authenticated = true;
            }
        }

        if (!authenticated)
        {
            user->credentials().setPassword(password);
            if (!ldapsecurity->authenticateUser(*user, &superUser) || !superUser)
            {
                *err = -1;
                return false;
            }
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
