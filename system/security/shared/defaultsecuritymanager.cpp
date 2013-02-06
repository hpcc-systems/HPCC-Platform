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
