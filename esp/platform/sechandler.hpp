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

#pragma warning( disable : 4786)
#ifndef _SECHANDLER_HPP__
#define _SECHANDLER_HPP__

#include "jliball.hpp"
#include "seclib.hpp"
#include "esp.hpp"
#include "sechandler.hpp"


class SecHandler : public CInterface
{
    Owned<ISecManager> m_secmgr;
    Owned<ISecResourceList> m_resources;
    Owned<ISecUser> m_user;
    Owned<IAuthMap> m_feature_authmap;
private:
    bool authorizeTrial(ISecUser& user,const char* pszFeatureUrl, SecAccessFlags & required_access);
    void AuditMessage(AuditType type, const char *filterType, const char *title, const char *parms, ...);

public:
    IMPLEMENT_IINTERFACE;
    SecHandler();
    virtual ~SecHandler();
    bool authorizeSecFeature(const char* pszFeatureUrl, SecAccessFlags& access);
    bool authorizeSecFeatures(StringArray & features, IEspStringIntMap & pmap);
    bool validateSecFeatureAccess(const char* pszFeatureUrl, unsigned required, bool throwExcpt);
    bool authorizeSecReqFeatures(StringArray & features, IEspStringIntMap & pmap, unsigned *required);


    bool authorizeSecFeature(const char * pszFeatureUrl, const char* UserID, const char* CompanyID, SecAccessFlags & access,bool bCheckTrial,int DebitUnits, SecUserStatus & user_status);

    void setSecManger(ISecManager* mgr);
    void setResources(ISecResourceList* rlist);
    void setUser(ISecUser* user);
    void setFeatureAuthMap(IAuthMap * map);
};


#endif // !defined(_SECHANDLER_HPP__)
