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
};


#endif // DEFAULTSECURITY_INCL
//end


