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

// SecHandler.cpp: implementation of the CSecHandler class.
//
//////////////////////////////////////////////////////////////////////

#include "sechandler.hpp"
#include "bindutil.hpp"
#include <map>
#include <string>
#include "espcontext.hpp"
//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

SecHandler::SecHandler() 
{
}



SecHandler::~SecHandler()
{

}

void SecHandler::setSecManger(ISecManager* mgr)
{
    m_secmgr.set(mgr);
}


void SecHandler::setResources(ISecResourceList* rlist)
{
    m_resources.set(rlist);
}

void SecHandler::setUser(ISecUser* user)
{
    m_user.set(user);
}

void SecHandler::setFeatureAuthMap(IAuthMap * map)
{
    if(map != NULL)
        m_feature_authmap.set(map);
}

bool SecHandler::authorizeSecFeature(const char * pszFeatureUrl, const char* UserID, const char* CompanyID, SecAccessFlags & required_access,bool bCheckTrial,int DebitUnits, SecUserStatus & user_status)
{
    if(m_user.get()==0)
        throw MakeStringException(500,"No user defined in SecHandler::authorizeSecFeature");

    user_status = SecUserStatus_Unknown;
    //lets see is our primary user allowed to access the resource.
    bool bPrimaryAccessAllowed = authorizeSecFeature(pszFeatureUrl,required_access);
    if(bPrimaryAccessAllowed==false)
        return false;

    //by now the prime ISecUser will have been fully initialized. We should be able
    //..to tell its status. 
    if(m_user->getStatus() == SecUserStatus_FreeTrial || m_user->getStatus() == SecUserStatus_Rollover)
    {
        bool bReturn = true;
        if(bCheckTrial==true)
            bReturn =  authorizeTrial(*m_user.get(),pszFeatureUrl,required_access);
        user_status = m_user->getStatus();
        return bReturn;
    }
    
    //m_user should be the user who logs in. This may not be the user whose resource we are looking for.
    //if userid and companyid are blank then we must be authenticating a normal user.. so continue on..
    if(UserID==0 || *UserID=='\0' || CompanyID==0 || *CompanyID=='\0')
        return bPrimaryAccessAllowed;

    //see do we have a cached version of our secondary user.....
    //.. if we do then check it...
    Owned<ISecUser> pSecondaryUser;
    pSecondaryUser.set(m_secmgr->findUser(UserID));
    if(pSecondaryUser.get()== NULL)
    {
        pSecondaryUser.setown(m_secmgr->createUser(UserID));
        if (!pSecondaryUser)
            return false;
        pSecondaryUser->setRealm(CompanyID);
        bool bSecondaryAccessAllowed = m_secmgr->initUser(*pSecondaryUser.get());
        if(bSecondaryAccessAllowed==false)
            return false;
        m_secmgr->addUser(*pSecondaryUser.get());
    }

    // currently not the responsibility of this service to authenticate the secondary user.
    //we just need to chech and see if on a free trial whether they should be allowed continue
    if(pSecondaryUser->getStatus() == SecUserStatus_FreeTrial || pSecondaryUser->getStatus() == SecUserStatus_Rollover)
    {
        //if the primary user is inhouse then we only want to check for a free trial
        // if a debit units value has been passed in.
        if(m_user->getStatus() == SecUserStatus_Inhouse && DebitUnits == 0)
        {
            if (getEspLogLevel() >= LogNormal)
                DBGLOG("Inhouse primary user and DebitUtits are 0 so not decrementing free trial");
            return true;
        }
        if (DebitUnits > 0)
            pSecondaryUser->setPropertyInt("debitunits",DebitUnits);

        bool bReturn = true;
        if(bCheckTrial==true)
            bReturn = authorizeTrial(*pSecondaryUser,pszFeatureUrl,required_access);
        user_status = pSecondaryUser->getStatus();
        return bReturn;
    }
    return true;
}

bool SecHandler::authorizeTrial(ISecUser& user,const char* pszFeatureUrl, SecAccessFlags & required_access)
{
    int trial_access = m_secmgr->authorizeEx(RT_TRIAL,user,pszFeatureUrl);
    if(trial_access < required_access)
        throw MakeStringException(201,"Your company has used up all of their free transaction credits");
    return true;
}

bool SecHandler::authorizeSecFeature(const char* pszFeatureUrl, SecAccessFlags& access)
{
    StringArray features;
    features.append(pszFeatureUrl);
    Owned<IEspStringIntMap> pmap=createStringIntMap();
    bool rc = authorizeSecFeatures(features, *pmap);
    if (rc)
    {
        int accessAllowed = pmap->queryValue(pszFeatureUrl);
        if (accessAllowed == -1)
            rc = false;
        else
            access = (SecAccessFlags) accessAllowed;
    }
    return rc;
}

bool SecHandler::authorizeSecFeatures(StringArray & features, IEspStringIntMap & pmap)
{
    return authorizeSecReqFeatures(features, pmap, NULL);
}


bool SecHandler::authorizeSecReqFeatures(StringArray & features, IEspStringIntMap & pmap, unsigned *required)
{
    if(features.length() == 0)
        return false;
    
    if(m_secmgr.get() == NULL)
    {
        for(unsigned i = 0; i < features.length(); i++)
        {
            const char* feature = features.item(i);
            if(feature != NULL && feature[0] != 0)
                pmap.setValue(feature, SecAccess_Full);
        }
        return true;
    }

    if(m_user.get() == NULL)
    {
        AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authorization", "Access Denied: No username provided");
        return false;
    }


    Owned<ISecResourceList> plist = m_secmgr->createResourceList("FeatureMap");

    std::map<std::string, std::string> namemap;

    unsigned i;
    for(i = 0; i < features.length(); i++)
    {
        const char* feature = features.item(i);
        if(feature == NULL || feature[0] == 0)
            continue;
        
        if(m_feature_authmap.get() == NULL)
        {
            plist->addResource(feature);
            namemap[feature] = feature;
        }
        else
        {
            ISecResourceList* rlist = m_feature_authmap->queryResourceList(feature);
            ISecResource* resource = NULL;
            if(rlist != NULL && (resource = rlist->queryResource((unsigned)0)) != NULL)
            {
                plist->addResource(resource->clone());
                namemap[resource->getName()] = feature;
            }
            else
            {
                // Use the feature name as the resource name if no authmap was found
                ISecResource* res = plist->addResource(feature);
                res->setRequiredAccessFlags(SecAccess_Unknown);
                namemap[feature] = feature;
            }
        }
    }
    
    bool auth_ok = false;
    try
    {
        auth_ok = m_secmgr->authorize(*m_user.get(), plist);
    }
    catch(IException* e)
    {
        StringBuffer errmsg;
        e->errorMessage(errmsg);
        ERRLOG("Exception authorizing, error=%s\n", errmsg.str());
        return false;
    }
    catch(...)
    {
        ERRLOG("Unknown exception authorizing\n");
        return false;
    }
    if(auth_ok)
    {
        for(i = 0; i < (unsigned)plist->count(); i++)
        {
            ISecResource* resource = plist->queryResource(i);
            if(resource != NULL)
            {
                std::string feature = namemap[resource->getName()];
                if(feature.size() == 0)
                    continue;
                pmap.setValue(feature.c_str(), resource->getAccessFlags());
                if (required && required[i]>0 && resource->getAccessFlags()<required[i])
                {
                    AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authorization", "Access Denied: Not enough access rights for resource", "Resource: %s [%s]", resource->getName(), resource->getDescription());
                }
            }
        }
    }
    return auth_ok;
}


bool SecHandler::validateSecFeatureAccess(const char* pszFeatureUrl, unsigned required, bool throwExcpt)
{

    
    StringArray features;
    features.append(pszFeatureUrl);
    unsigned reqarray[1];
    reqarray[0] = required;

    Owned<IEspStringIntMap> pmap=createStringIntMap();

    if (authorizeSecReqFeatures(features, *pmap, reqarray))
    {
        int accessAllowed = pmap->queryValue(pszFeatureUrl);
        if ((accessAllowed == -1) || (required && (accessAllowed < required)))
        {
            if (throwExcpt)
                throw MakeStringException(-1, "Access Denied!");
            return false;
        }
        else
            return true;
    }
    if (throwExcpt)
        throw MakeStringException(-1, "Access Denied!");
    
    return false;
}


void SecHandler::AuditMessage(AuditType type, const char *filterType, const char *title, const char *parms, ...)
{
    va_list args;
    va_start(args, parms);

    StringBuffer msg(title);
    msg.appendf("\n\tProcess: esp\n\tUser: %s",  m_user->getName());
    if (parms)
        msg.append("\n\t").valist_appendf(parms, args);
    
    va_end(args);
    AUDIT(type, msg.str());
}

void SecHandler::AuditMessage(AuditType type, const char *filterType, const char *title)
{
    VStringBuffer msg("%s\n\tProcess: esp\n\tUser: %s", title, m_user->getName());
    AUDIT(type, msg.str());
}
