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

#ifndef DEFAULTSECURITY_INCL
#define DEFAULTSECURITY_INCL

#include "basesecurity.hpp"

class CDefaultSecurityManager : public CBaseSecurityManager
{
protected:

public:
    CDefaultSecurityManager(const char *serviceName, const char *config);
    CDefaultSecurityManager(const char *serviceName, IPropertyTree *config);
    virtual ~CDefaultSecurityManager(){};
    virtual bool dbauthenticate(ISecUser& User, StringBuffer& SQLQuery){return true;}
    virtual bool dbValidateResource(ISecResource& res,int usernum,const char* realm)
    {
        CSecurityResource * tmpResource =  (CSecurityResource*)(&res);
        if(tmpResource)
            tmpResource->setAccessFlags(SecAccess_Full);
        return true;
    }

    virtual int getAccessFlagsEx(SecResourceType rtype, ISecUser& sec_user, const char* resourcename)
    {
        return SecAccess_Full;
    }
    virtual int authorizeFileScope(ISecUser & user, const char * filescope)
    {
        return SecAccess_Full;
    }
    virtual bool authorizeFileScope(ISecUser & user, ISecResourceList * resources)
    {
        if(resources)
        {
            int cnt = resources->count();
            for(int i = 0; i < cnt; i++)
            {
                ISecResource* r = resources->queryResource(i);
                if(r)
                    r->setAccessFlags(SecAccess_Full);
            }
        }

        return true;
    }
    virtual int authorizeWorkunitScope(ISecUser & user, const char * filescope)
    {
        return SecAccess_Full;
    }
    virtual bool authorizeWorkunitScope(ISecUser & user, ISecResourceList * resources)
    {
        if(resources)
        {
            int cnt = resources->count();
            for(int i = 0; i < cnt; i++)
            {
                ISecResource* r = resources->queryResource(i);
                if(r)
                    r->setAccessFlags(SecAccess_Full);
            }
        }

        return true;
    }
    virtual bool createUserScopes() { return false; }
    virtual aindex_t getManagedFileScopes(IArrayOf<ISecResource>& scopes) { return 0; }
    virtual int queryDefaultPermission(ISecUser& user) { return SecAccess_Full; }
    virtual secManagerType querySecMgrType() { return SMT_Default; }
    inline virtual const char* querySecMgrTypeName() { return "Default"; }
};

class CLocalSecurityManager : public CDefaultSecurityManager
{
public:
    CLocalSecurityManager(const char *serviceName, const char *config);
    CLocalSecurityManager(const char *serviceName, IPropertyTree *config);
    virtual ~CLocalSecurityManager();
    IAuthMap * createAuthMap(IPropertyTree * authconfig);
protected:
    virtual bool IsPasswordValid(ISecUser& sec_user);
    virtual secManagerType querySecMgrType() { return SMT_Local; }
    inline virtual const char* querySecMgrTypeName() { return "Local"; }
};


#endif // DEFAULTSECURITY_INCL
//end


