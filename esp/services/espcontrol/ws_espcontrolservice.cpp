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

IEspSession* CWSESPControlEx::setSessionInfo(const char* id, IPropertyTree* espSessionTree, unsigned port, IEspSession* session)
{
    if (espSessionTree == nullptr)
        return nullptr;

    StringBuffer createTimeStr, lastAccessedStr, TimeoutAtStr;
    int lastAccessed = espSessionTree->getPropInt(PropSessionLastAccessed, 0);

    session->setPort(port);
    session->setID(id);
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

    Owned<IPropertyTreeIterator> it = espCFG->getElements("AuthDomains/AuthDomain");
    ForEach(*it)
    {
        IPropertyTree& authDomain = it->query();
        StringBuffer name = authDomain.queryProp("@domainName");
        if (name.isEmpty())
            name.set("default");
        sessionTimeoutMinutesMap.setValue(name.str(), authDomain.getPropInt("@sessionTimeoutMinutes", 0));
    }
}


bool CWSESPControlEx::onSetLogging(IEspContext& context, IEspSetLoggingRequest& req, IEspSetLoggingResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(secmgr && !secmgr->isSuperUser(context.queryUser()))
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to change log settings. Permission denied.");
#endif

        if (!m_container)
            throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed to access container.");

        if (!req.getLoggingLevel_isNull())
            m_container->setLogLevel(req.getLoggingLevel());
        if (!req.getLogRequests_isNull())
            m_container->setLogRequests(req.getLogRequests());
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

IRemoteConnection* CWSESPControlEx::querySDSConnection(const char* xpath, unsigned mode, unsigned timeout)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect(xpath, myProcessSession(), RTM_LOCK_READ, SESSION_SDS_LOCK_TIMEOUT);
    if (!globalLock)
        throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Unable to connect to ESP Session information in dali %s", xpath);
    return globalLock.getClear();
}

bool CWSESPControlEx::onSessionQuery(IEspContext& context, IEspSessionQueryRequest& req, IEspSessionQueryResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(secmgr && !secmgr->isSuperUser(context.queryUser()))
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to query session. Permission denied.");
#endif

        StringBuffer fromIP = req.getFromIP();
        unsigned port = 8010;
        if (!req.getPort_isNull())
            port = req.getPort();

        VStringBuffer xpath("/%s/%s[@name='%s']/%s[@port='%d']", PathSessionRoot, PathSessionProcess,
            espProcess.get(), PathSessionApplication, port);
        Owned<IRemoteConnection> globalLock = querySDSConnection(xpath.str(), RTM_LOCK_READ, SESSION_SDS_LOCK_TIMEOUT);

        IArrayOf<IEspSession> sessions;
        if (!fromIP.trim().isEmpty())
            xpath.setf("%s[%s='%s']", PathSessionSession, PropSessionNetworkAddress, fromIP.str());
        else
            xpath.set("*");
        Owned<IPropertyTreeIterator> iter = globalLock->queryRoot()->getElements(xpath.str());
        ForEach(*iter)
        {
            IPropertyTree& sessionTree = iter->query();
            Owned<IEspSession> s = createSession();
            setSessionInfo(sessionTree.queryProp(PropSessionExternalID), &sessionTree, port, s);
            sessions.append(*s.getLink());
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
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to get session information. Permission denied.");
#endif

        StringBuffer id = req.getID();
        if (id.trim().isEmpty())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "ID not specified.");

        unsigned port = 8010;
        if (!req.getPort_isNull())
            port = req.getPort();

        VStringBuffer xpath("/%s/%s[@name='%s']/%s[@port='%d']/%s[%s='%s']", PathSessionRoot, PathSessionProcess, espProcess.get(),
            PathSessionApplication, port, PathSessionSession, PropSessionExternalID, id.str());
        Owned<IRemoteConnection> globalLock = querySDSConnection(xpath.str(), RTM_LOCK_READ, SESSION_SDS_LOCK_TIMEOUT);
        setSessionInfo(id.str(), globalLock->queryRoot(), port, &resp.updateSession());
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
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to clean session. Permission denied.");
#endif

        StringBuffer id = req.getID();
        StringBuffer fromIP = req.getFromIP();
        if ((id.trim().isEmpty()) && (fromIP.trim().isEmpty()))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "ID or FromIP has to be specified.");

        unsigned port = 8010;
        if (!req.getPort_isNull())
            port = req.getPort();

        VStringBuffer xpath("/%s/%s[@name='%s']/%s[@port='%d']", PathSessionRoot, PathSessionProcess,
            espProcess.get(), PathSessionApplication, port);
        Owned<IRemoteConnection> globalLock = querySDSConnection(xpath.str(), RTM_LOCK_WRITE, SESSION_SDS_LOCK_TIMEOUT);

        IArrayOf<IPropertyTree> toRemove;
        if (!id.isEmpty())
            xpath.setf("%s[%s='%s']", PathSessionSession, PropSessionExternalID, id.str());
        else
            xpath.setf("%s[%s='%s']", PathSessionSession, PropSessionNetworkAddress, fromIP.str());
        Owned<IPropertyTreeIterator> iter = globalLock->queryRoot()->getElements(xpath.str());
        ForEach(*iter)
            toRemove.append(*LINK(&iter->query()));
        ForEachItemIn(i, toRemove)
            globalLock->queryRoot()->removeTree(&toRemove.item(i));

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
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to set session timeout. Permission denied.");
#endif

        StringBuffer id = req.getID();
        StringBuffer fromIP = req.getFromIP();
        if (id.trim().isEmpty() && fromIP.trim().isEmpty())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "ID or FromIP has to be specified.");

        unsigned port = 8010;
        if (!req.getPort_isNull())
            port = req.getPort();

        VStringBuffer xpath("/%s/%s[@name='%s']/%s[@port='%d']", PathSessionRoot, PathSessionProcess,
            espProcess.get(), PathSessionApplication, port);
        Owned<IRemoteConnection> globalLock = querySDSConnection(xpath.str(), RTM_LOCK_WRITE, SESSION_SDS_LOCK_TIMEOUT);

        IArrayOf<IPropertyTree> toRemove;
        int timeoutMinutes = req.getTimeoutMinutes_isNull() ? 0 : req.getTimeoutMinutes();
        if (!id.isEmpty())
            xpath.setf("%s[%s='%s']", PathSessionSession, PropSessionExternalID, id.str());
        else
            xpath.setf("%s[%s='%s']", PathSessionSession, PropSessionNetworkAddress, fromIP.str());
        Owned<IPropertyTreeIterator> iter = globalLock->queryRoot()->getElements(xpath.str());
        ForEach(*iter)
        {
            IPropertyTree& item = iter->query();
            if (timeoutMinutes <= 0)
                toRemove.append(*LINK(&item));
            else
            {
                CDateTime timeNow;
                timeNow.setNow();
                time_t simple = timeNow.getSimple() + timeoutMinutes*60;
                item.setPropInt64(PropSessionTimeoutAt, simple);
                item.setPropBool(PropSessionTimeoutByAdmin, true);
            }
        }
        ForEachItemIn(i, toRemove)
            globalLock->queryRoot()->removeTree(&toRemove.item(i));

        resp.setStatus(0);
        resp.setMessage("Session timeout is updated.");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}
