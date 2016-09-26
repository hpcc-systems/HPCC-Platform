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

#pragma warning( disable : 4786)
#ifndef _SECHANDLER_HPP__
#define _SECHANDLER_HPP__

#include "jliball.hpp"
#include "seclib.hpp"
#include "esp.hpp"
#include "sechandler.hpp"
#include "espsecurecontext.hpp"

class SecHandler : public CInterface
{
    Owned<ISecManager> m_secmgr;
    Owned<ISecResourceList> m_resources;
    Owned<ISecUser> m_user;
    Owned<IAuthMap> m_feature_authmap;
    Owned<IEspSecureContext> m_secureContext;
private:
    bool authorizeTrial(ISecUser& user,const char* pszFeatureUrl, SecAccessFlags & required_access);
    void AuditMessage(AuditType type, const char *filterType, const char *title, const char *parms, ...) __attribute__((format(printf, 5, 6)));
    void AuditMessage(AuditType type, const char *filterType, const char *title);

public:
    IMPLEMENT_IINTERFACE;
    SecHandler();
    virtual ~SecHandler();
    bool authorizeSecFeature(const char* pszFeatureUrl, SecAccessFlags& access);
    bool authorizeSecFeatures(StringArray & features, IEspStringIntMap & pmap);
    bool validateSecFeatureAccess(const char* pszFeatureUrl, unsigned required, bool throwExcpt);
    bool validateSecFeaturesAccess(MapStringTo<SecAccessFlags> & accessmap, bool throwExcpt);
    bool authorizeSecReqFeatures(StringArray & features, IEspStringIntMap & pmap, unsigned *required);
    bool authorizeSecFeature(const char * pszFeatureUrl, const char* UserID, const char* CompanyID, SecAccessFlags & access,bool bCheckTrial,int DebitUnits, SecUserStatus & user_status);

    void setSecManger(ISecManager* mgr);
    void setResources(ISecResourceList* rlist);
    void setUser(ISecUser* user);
    void setFeatureAuthMap(IAuthMap * map);
    void setSecureContext(IEspSecureContext* secureContext);
};


#endif // !defined(_SECHANDLER_HPP__)
