/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

#ifdef _USE_OPENLDAP
#include "ldapsecurity.ipp"
#endif

#include "ws_espcontrolservice.hpp"
#include "jlib.hpp"
#include "exception_util.hpp"
#include "dasds.hpp"

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5 mins

const char* CWSESPControlEx::readSessionTimeStamp(int t, StringBuffer& str)
{
    CDateTime time;
    time.set(t);
    return time.getString(str).str();
}

IEspSession* CWSESPControlEx::setSessionInfo(IPropertyTree* espSessionTree, unsigned port, IEspSession* session)
{
    if (espSessionTree == nullptr)
        return nullptr;

    StringBuffer createTimeStr, lastAccessedStr, TimeoutAtStr;
    int lastAccessed = espSessionTree->getPropInt(PropSessionLastAccessed, 0);

    session->setPort(port);
    session->setID(espSessionTree->queryProp(PropSessionExternalID));
    session->setUserID(espSessionTree->queryProp(PropSessionUserID));
    session->setNetworkAddress(espSessionTree->queryProp(PropSessionNetworkAddress));
    session->setCreateTime(readSessionTimeStamp(espSessionTree->getPropInt(PropSessionCreateTime, 0), createTimeStr));
    session->setLastAccessed(readSessionTimeStamp(espSessionTree->getPropInt(PropSessionLastAccessed, 0), lastAccessedStr));
    session->setTimeoutAt(readSessionTimeStamp(espSessionTree->getPropInt(PropSessionTimeoutAt, 0), TimeoutAtStr));
    session->setTimeoutByAdmin(espSessionTree->getPropBool(PropSessionTimeoutByAdmin, false));
    return session;
}

void CWSESPControlEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if(cfg == NULL)
        throw MakeStringException(-1, "Can't initialize CWSESPControlEx, cfg is NULL");

    espProcess.set(process);

    VStringBuffer xpath("Software/EspProcess[@name=\"%s\"]", process);
    IPropertyTree* espCFG = cfg->queryPropTree(xpath.str());
    if (!espCFG)
        throw MakeStringException(-1, "Can't find EspBinding for %s", process);

    loggingLevelSetting = cfg->getPropInt("Software/EspProcess/@logLevel", 1);
    logRequestsSetting = readLogRequest(cfg->queryProp("Software/EspProcess/@logRequests"));
    logResponsesSetting = cfg->getPropBool("Software/EspProcess/@logResponses", false);

    Owned<IPropertyTreeIterator> it = espCFG->getElements("AuthDomains/AuthDomain");
    ForEach(*it)
    {
        IPropertyTree& authDomain = it->query();
        StringBuffer name(authDomain.queryProp("@domainName"));
        if (name.isEmpty())
            name.set("default");
        sessionTimeoutMinutesMap.setValue(name.str(), authDomain.getPropInt("@sessionTimeoutMinutes", 0));
    }
}

bool CWSESPControlEx::handleDaliAttachmentRequest(bool attach, bool force, StringBuffer & message)
{
    bool success = true;
    CEspServer * thisESPServer = static_cast<CEspServer *>(m_container);
    if (thisESPServer)
    {
        if (attach)
        {
            message.setf("Request to ATTACH ESP Process '%s' to Dali has been issued - ", espProcess.get());
            success = thisESPServer->attachESPToDali();
        }
        else
        {
            message.setf("Request to DETACH ESP Process '%s' from Dali has been issued - ", espProcess.get());
            success = thisESPServer->detachESPFromDali(force);
        }

        if (success)
            message.append("and success reported.");
        else
            message.append("but failure reported.");
    }
    else
    {
        message.append("ESP/DALI Attachment request could not be issued due to internal error");
        success = false;
    }

    return success;
}

bool CWSESPControlEx::onDetachFromDali(IEspContext& context, IEspDetachFromDaliRequest& req, IEspDetachFromDaliResponse& resp)
{
    StringBuffer message;
    bool status = handleDaliAttachmentRequest(false, req.getForce_isNull() ? false : req.getForce(), message);
    resp.setMessage(message.str());
    resp.setStatus(status ? 0 : -1);
    return status;
}

bool CWSESPControlEx::onAttachToDali(IEspContext& context, IEspAttachToDaliRequest& req, IEspAttachToDaliResponse& resp)
{
    StringBuffer message;
    bool status = handleDaliAttachmentRequest(true, false, message);
    resp.setMessage(message.str());
    resp.setStatus(status ? 0 : -1);
    return status;
}

bool CWSESPControlEx::handleDaliSubscriptionRequest(bool enable, StringBuffer & message)
{
    bool success = true;
    CEspServer * thisESPServer = static_cast<CEspServer *>(m_container);
    if (thisESPServer)
    {
        if (enable)
        {
            message.setf("Request to enable all DALI subscriptions on ESP Process '%s' issued - ", this->espProcess.get());
            success = thisESPServer->reSubscribeESPToDali();
        }
        else
        {
            message.setf("Request to cancel all DALI subscriptions on ESP Process '%s' issued - ", this->espProcess.get());
            success = thisESPServer->unsubscribeESPFromDali();
        }

        if (success)
            message.append("and success reported.");
        else
            message.append("but failure reported.");
    }
    else
    {
        message.append("Dali subscription request could not be issued due to internal error");
        success = false;
    }

    return success;
}
bool CWSESPControlEx::onDisableDaliSubscriptions(IEspContext& context, IEspDisableDaliSubscriptionsRequest& req, IEspDisableDaliSubscriptionsResponse& resp)
{
    StringBuffer message;
    bool status = handleDaliSubscriptionRequest(false, message);
    resp.setMessage(message.str());
    resp.setStatus(status ? 0 : -1);
    return status;
}

bool CWSESPControlEx::onEnableDaliSubscriptions(IEspContext& context, IEspEnableDaliSubscriptionsRequest& req, IEspEnableDaliSubscriptionsResponse& resp)
{
    StringBuffer message;
    bool status = handleDaliSubscriptionRequest(true, message);
    resp.setMessage(message.str());
    resp.setStatus(status ? 0 : -1);
    return status;
}

bool CWSESPControlEx::onSetLogging(IEspContext& context, IEspSetLoggingRequest& req, IEspSetLoggingResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(secmgr && !secmgr->isSuperUser(context.queryUser()))
        {
            context.setAuthStatus(AUTH_STATUS_NOACCESS);
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to change log settings. Permission denied.");
        }
#endif

        if (!m_container)
            throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed to access container.");

        if (!req.getLoggingLevel_isNull())
            m_container->setLogLevel(req.getLoggingLevel());
        m_container->setLogRequests(readLogRequest(req.getLogRequests()));
        if (!req.getLogResponses_isNull())
            m_container->setLogResponses(req.getLogResponses());
        resp.setStatus(0);
        resp.setMessage("Logging settings are updated.");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSESPControlEx::onGetLoggingSettings(IEspContext& context, IEspGetLoggingSettingsRequest& req, IEspGetLoggingSettingsResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(secmgr && !secmgr->isSuperUser(context.queryUser()))
        {
            context.setAuthStatus(AUTH_STATUS_NOACCESS);
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to get log settings. Permission denied.");
        }
#endif
        if (!m_container)
            throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed to access container.");

        StringBuffer logRequests;
        resp.setLoggingLevel(m_container->getLogLevel());
        resp.setLogRequests(getLogRequestString(m_container->getLogRequests(), logRequests));
        resp.setLogResponses(m_container->getLogResponses());
        resp.setLoggingLevelSetting(loggingLevelSetting);
        resp.setLogRequestsSetting(getLogRequestString(logRequestsSetting, logRequests.clear()));
        resp.setLogResponsesSetting(logResponsesSetting);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

IRemoteConnection* CWSESPControlEx::getSDSConnection(const char* xpath, unsigned mode, unsigned timeout)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect(xpath, myProcessSession(), mode, timeout);
    if (!globalLock)
        throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Unable to connect to ESP Session information in dali %s", xpath);
    return globalLock.getClear();
}

IRemoteConnection* CWSESPControlEx::getSDSConnectionForESPSession(unsigned mode, unsigned timeout)
{
    VStringBuffer xpath("/%s/%s[@name='%s']", PathSessionRoot, PathSessionProcess, espProcess.get());
    return getSDSConnection(xpath.str(), mode, timeout);
}

bool CWSESPControlEx::onSessionQuery(IEspContext& context, IEspSessionQueryRequest& req, IEspSessionQueryResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(secmgr && !secmgr->isSuperUser(context.queryUser()))
        {
            context.setAuthStatus(AUTH_STATUS_NOACCESS);
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to query session. Permission denied.");
        }
#endif

        StringBuffer xpath;
        setSessionXPath(false, nullptr, req.getUserID(), req.getFromIP(), xpath);

        IArrayOf<IEspSession> sessions;
        Owned<IRemoteConnection> globalLock = getSDSConnectionForESPSession(RTM_LOCK_READ, SESSION_SDS_LOCK_TIMEOUT);
        Owned<IPropertyTreeIterator> iter = globalLock->queryRoot()->getElements("*");
        ForEach(*iter)
        {
            IPropertyTree& appSessionTree = iter->query();
            unsigned port = appSessionTree.getPropInt("@port");
            Owned<IPropertyTreeIterator> iter1 = appSessionTree.getElements(xpath.str());
            ForEach(*iter1)
            {
                IPropertyTree& sessionTree = iter1->query();
                Owned<IEspSession> s = createSession();
                setSessionInfo(&sessionTree, port, s);
                sessions.append(*s.getLink());
            }
        }
        resp.setSessions(sessions);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSESPControlEx::onSessionInfo(IEspContext& context, IEspSessionInfoRequest& req, IEspSessionInfoResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(secmgr && !secmgr->isSuperUser(context.queryUser()))
        {
            context.setAuthStatus(AUTH_STATUS_NOACCESS);
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to get session information. Permission denied.");
        }
#endif

        StringBuffer id(req.getID());
        if (id.trim().isEmpty())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "ID not specified.");

        unsigned port = 8010;
        if (!req.getPort_isNull())
            port = req.getPort();

        Owned<IRemoteConnection> globalLock;
        VStringBuffer xpath("/%s/%s[@name='%s']/%s[@port='%d']/%s*[%s='%s']", PathSessionRoot, PathSessionProcess, espProcess.get(),
            PathSessionApplication, port, PathSessionSession, PropSessionExternalID, id.str());
        try
        {
            globalLock.setown(getSDSConnection(xpath.str(), RTM_LOCK_READ, SESSION_SDS_LOCK_TIMEOUT));
        }
        catch(IException* e)
        {
            VStringBuffer msg("Failed to get session info for id %s on port %u: ", id.str(), port);
            e->errorMessage(msg);
            e->Release();
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "%s", msg.str());
        }
        setSessionInfo(globalLock->queryRoot(), port, &resp.updateSession());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSESPControlEx::onCleanSession(IEspContext& context, IEspCleanSessionRequest& req, IEspCleanSessionResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(secmgr && !secmgr->isSuperUser(context.queryUser()))
        {
            context.setAuthStatus(AUTH_STATUS_NOACCESS);
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to clean session. Permission denied.");
        }
#endif

        StringBuffer id, userID, fromIP;
        bool allSessions = req.getAllSessions();
        if (!allSessions)
        {
            id.set(req.getID());
            userID.set(req.getUserID());
            fromIP.set(req.getFromIP());
            if ((id.trim().isEmpty()) && (userID.trim().isEmpty()) && (fromIP.trim().isEmpty()))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "ID, userID or FromIP has to be specified.");
        }
        cleanSessions(allSessions, id.str(), userID.str(), fromIP.str());

        resp.setStatus(0);
        resp.setMessage("Session is cleaned.");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSESPControlEx::onSetSessionTimeout(IEspContext& context, IEspSetSessionTimeoutRequest& req, IEspSetSessionTimeoutResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(secmgr && !secmgr->isSuperUser(context.queryUser()))
        {
            context.setAuthStatus(AUTH_STATUS_NOACCESS);
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to set session timeout. Permission denied.");
        }
#endif

        StringBuffer id, userID, fromIP;
        bool allSessions = req.getAllSessions();
        if (!allSessions)
        {
            id.set(req.getID());
            userID.set(req.getUserID());
            fromIP.set(req.getFromIP());
            if ((id.trim().isEmpty()) && (userID.trim().isEmpty()) && (fromIP.trim().isEmpty()))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "ID, userID or FromIP has to be specified.");
        }

        int timeoutMinutes = req.getTimeoutMinutes_isNull() ? 0 : req.getTimeoutMinutes();
        if (timeoutMinutes <= 0)
            cleanSessions(allSessions, id.str(), userID.str(), fromIP.str());
        else
        {
            StringBuffer searchPath;
            setSessionXPath(allSessions, id.str(), userID.str(), fromIP.str(), searchPath);

            Owned<IRemoteConnection> globalLock = getSDSConnectionForESPSession(RTM_LOCK_WRITE, SESSION_SDS_LOCK_TIMEOUT);
            Owned<IPropertyTreeIterator> iter = globalLock->queryRoot()->getElements("*");
            ForEach(*iter)
            {
                Owned<IPropertyTreeIterator> iter1 = iter->query().getElements(searchPath.str());
                ForEach(*iter1)
                    setSessionTimeout(timeoutMinutes, iter1->query());
            }
        }

        resp.setStatus(0);
        resp.setMessage("Session timeout is updated.");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

const char* CWSESPControlEx::setSessionXPath(bool allSessions, const char* id, const char* userID, const char* fromIP, StringBuffer& xPath)
{
    if (allSessions)
    {
        xPath.set("*");
        return xPath.str();
    }

    if (!isEmptyString(userID))
        xPath.setf("%s*[%s='%s']", PathSessionSession, PropSessionUserID, userID);
    else if (!isEmptyString(fromIP))
        xPath.setf("%s*[%s='%s']", PathSessionSession, PropSessionNetworkAddress, fromIP);
    else if (!isEmptyString(id))
        xPath.setf("%s*[%s='%s']", PathSessionSession, PropSessionExternalID, id);
    else
        xPath.set("*");
    return xPath.str();
}

void CWSESPControlEx::cleanSessions(bool allSessions, const char* _id, const char* _userID, const char* _fromIP)
{
    StringBuffer searchPath;
    setSessionXPath(allSessions, _id, _userID, _fromIP, searchPath);

    Owned<IRemoteConnection> globalLock = getSDSConnectionForESPSession(RTM_LOCK_WRITE, SESSION_SDS_LOCK_TIMEOUT);
    Owned<IPropertyTreeIterator> iter = globalLock->queryRoot()->getElements("*");
    ForEach(*iter)
    {
        IArrayOf<IPropertyTree> toRemove;
        Owned<IPropertyTreeIterator> iter1 = iter->query().getElements(searchPath.str());
        ForEach(*iter1)
            toRemove.append(*LINK(&iter1->query()));

        ForEachItemIn(i, toRemove)
            iter->query().removeTree(&toRemove.item(i));
    }
}

void CWSESPControlEx::setSessionTimeout(int timeoutMinutes, IPropertyTree& session)
{
    CDateTime timeNow;
    timeNow.setNow();
    time_t simple = timeNow.getSimple() + timeoutMinutes*60;
    session.setPropInt64(PropSessionTimeoutAt, simple);
    session.setPropBool(PropSessionTimeoutByAdmin, true);
}
