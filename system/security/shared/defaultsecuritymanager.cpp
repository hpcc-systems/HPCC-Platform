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

#pragma warning( disable : 4786 )

#include "defaultsecuritymanager.hpp"
#include "authmap.ipp"


//#ifdef _WIN32
CDefaultSecurityManager::CDefaultSecurityManager(const char *serviceName, const char *config) : CBaseSecurityManager(serviceName,config)
{
}

CDefaultSecurityManager::CDefaultSecurityManager(const char *serviceName, IPropertyTree *config) : CBaseSecurityManager(serviceName,config)
{
}


//========Local Secuirty Manager==========
CLocalSecurityManager::CLocalSecurityManager(const char *serviceName, const char *config) : CDefaultSecurityManager(serviceName, config)
{
}

CLocalSecurityManager::CLocalSecurityManager(const char *serviceName, IPropertyTree *config) : CDefaultSecurityManager(serviceName, config)
{
}

CLocalSecurityManager::~CLocalSecurityManager()
{
}

bool CLocalSecurityManager::IsPasswordValid(ISecUser& sec_user)
{
    IAuthenticatedUser* au = createAuthenticatedUser();
    StringBuffer userbuf;
#ifdef _WIN32
    const char* realm = sec_user.getRealm();
    if(realm&&*realm)
        userbuf.append(realm).append("\\");
#endif
    userbuf.append(sec_user.getName());
    return au->login(userbuf.str(), sec_user.credentials().getPassword());
}


IAuthMap * CLocalSecurityManager::createAuthMap(IPropertyTree * authconfig)
{
    CAuthMap* authmap = new CAuthMap(this);

    IPropertyTreeIterator *loc_iter = NULL;
    loc_iter = authconfig->getElements(".//Location");
    if (loc_iter != NULL)
    {
        IPropertyTree *location = NULL;
        loc_iter->first();
        while(loc_iter->isValid())
        {
            location = &loc_iter->query();
            if (location)
            {
                StringBuffer pathstr, rstr, required, description;
                location->getProp("@path", pathstr);
                location->getProp("@resource", rstr);
                location->getProp("@required", required);
                location->getProp("@description", description);
                
                if(pathstr.length() == 0)
                    throw MakeStringException(-1, "path empty in Authenticate/Location");
                if(rstr.length() == 0)
                    throw MakeStringException(-1, "resource empty in Authenticate/Location");

                ISecResourceList* rlist = authmap->queryResourceList(pathstr.str());
                if(rlist == NULL)
                {
                    rlist = createResourceList("localsecurity");                        
                    authmap->add(pathstr.str(), rlist);
                }
                ISecResource* rs = rlist->addResource(rstr.str());
                unsigned requiredaccess = str2perm(required.str());
                rs->setRequiredAccessFlags(requiredaccess);
                rs->setDescription(description.str());
            }
            loc_iter->next();
        }
        loc_iter->Release();
        loc_iter = NULL;
    }

    return authmap;
}

