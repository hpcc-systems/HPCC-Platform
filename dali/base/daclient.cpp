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

#define da_decl __declspec(dllexport)
#include "platform.h"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jtime.hpp"

#include "mpcomm.hpp"
#include "mplog.hpp"
#include "dasess.hpp"
#include "daserver.hpp"
#include "dacsds.ipp"
#include "dautils.hpp"

#include "daclient.hpp"

extern bool registerClientProcess(ICommunicator *comm, IGroup *& retcoven,unsigned timeout,DaliClientRole role);
extern void stopClientProcess();

static bool daliClientIsActive = false;
static INode * daliClientLoggingParent = 0;

static IArrayOf<IDaliClientShutdown> shutdownHooks;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    shutdownHooks.kill();
}


class CDaliClientException: public CInterface, public IDaliClient_Exception
{
public:
    IMPLEMENT_IINTERFACE;

    CDaliClientException(DaliClientError err, const char *_msg) : error(err), msg(_msg)
    {
    }

    StringBuffer &  errorMessage(StringBuffer &str) const
    { 
        switch (error) {
        case DCERR_ok:                          str.append("OK"); break;
        case DCERR_server_closed:               str.append("Connection to Dali server lost or server closed"); break;
        case DCERR_version_incompatibility:     str.append("Client/Server version incompatibility"); break;
        }
        if (msg.length()) str.append(": ").append(msg);
        return str;
    }
    int             errorCode() const { return error; }
    MessageAudience errorAudience() const 
    { 
        switch (error) {
        case DCERR_server_closed:         
            return MSGAUD_operator; 
        }
        return MSGAUD_user; 
    }
private:
    DaliClientError error;
    StringAttr msg;
};

IDaliClient_Exception *createClientException(DaliClientError err, const char *msg)
{
    return new CDaliClientException(err, msg);
}



bool initClientProcess(IGroup *servergrp, DaliClientRole role, unsigned mpport, const char *clientVersion, const char *minServerVersion, unsigned timeout)
{
    assertex(servergrp);
    daliClientIsActive = true;
    startMPServer(mpport);
    Owned<ICommunicator> comm(createCommunicator(servergrp,true));
    IGroup * covengrp;
    if (!registerClientProcess(comm.get(),covengrp,timeout,role))
    {
        daliClientIsActive = false;
        return false;
    }
    initCoven(covengrp,NULL,clientVersion, minServerVersion);
    covengrp->Release();
    queryLogMsgManager()->setSession(myProcessSession());
    return true;
}

void addShutdownHook(IDaliClientShutdown &shutdown)
{
    shutdownHooks.append(*LINK(&shutdown));
}

void removeShutdownHook(IDaliClientShutdown &shutdown)
{
    shutdownHooks.zap(shutdown);
}

void closedownClientProcess()
{
    if (!daliClientIsActive)
        return;
    while (shutdownHooks.ordinality())
    {
        Owned<IDaliClientShutdown> c = &shutdownHooks.popGet();
        c->clientShutdown();
    }
    clearPagedElementsCache(); // has connections
    closeSDS();
    closeSubscriptionManager();
    stopClientProcess();
    closeCoven();
    stopMPServer();
    daliClientIsActive = false;
}

bool reinitClientProcess(IGroup *servergrp, DaliClientRole role, const char *clientVersion, const char *minServerVersion, unsigned timeout)
{
    if (!daliClientIsActive)
        return false;
    while (shutdownHooks.ordinality())
    {
        Owned<IDaliClientShutdown> c = &shutdownHooks.popGet();
        c->clientShutdown();
    }
    stopClientProcess();
    closeSDS();
    closeSubscriptionManager();
    closeCoven();
    Owned<ICommunicator> comm(createCommunicator(servergrp,true));
    IGroup * covengrp;
    if (!registerClientProcess(comm.get(),covengrp,timeout,role))   // should be save as before TBD
        return false;
    initCoven(covengrp,NULL,clientVersion,minServerVersion);
    covengrp->Release();
    return true;
}

bool daliClientActive()
{
    return daliClientIsActive;
}

// Server status

CSDSServerStatus::CSDSServerStatus(const char *servername)
{
    conn = querySDS().connect("Status/Servers/Server", myProcessSession(), RTM_CREATE_ADD | RTM_LOCK_READ | RTM_DELETE_ON_DISCONNECT, 5*60*1000);
    if (conn) {
        IPropertyTree &root = *conn->queryRoot();
        root.setProp("@name",servername);
        StringBuffer node;
        queryMyNode()->endpoint().getIpText(node);
        root.setProp("@node",node.str());
        root.setPropInt("@mpport",queryMyNode()->endpoint().port);
        CDateTime dt;
        dt.setNow();
        StringBuffer str;
        root.setProp("@started",dt.getString(str).str());
        conn->commit();
    }
}


void CSDSServerStatus::stop()
{
    try {
        ::Release(conn);
    }
    catch (IException *e)   // Dali server may have stopped
    {
        EXCLOG(e,"CSDSServerStatus::stop");
        e->Release();
    }
    conn = NULL;
};



void connectLogMsgManagerToDali()
{
    IGroup & servers = queryCoven().queryGroup();
    unsigned parentRank = getRandom() % servers.ordinality();    // PG: Not sure if logging to random parent is best?
    daliClientLoggingParent = &servers.queryNode(parentRank);
    connectLogMsgManagerToParent(LINK(daliClientLoggingParent));   // PG: This may be nasty if node chosen is down
}

void disconnectLogMsgManagerFromDali()
{
    disconnectLogMsgManagerFromParentOwn(daliClientLoggingParent);
    daliClientLoggingParent = 0;
}

void connectLogMsgListenerToDali()
{
    // MORE: may be better to make daservers share messages, in which case only need to listen to one member of coven
    IGroup & servers = queryCoven().queryGroup();
    unsigned idx;
    unsigned max = servers.ordinality();
    for(idx = 0; idx < max; idx++)
        connectLogMsgListenerToChild(&servers.queryNode(idx));
}

void disconnectLogMsgListenerFromDali()
{
    // MORE: may be better to make daservers share messages, in which case only need to listen to one member of coven
    IGroup & servers = queryCoven().queryGroup();
    unsigned idx;
    unsigned max = servers.ordinality();
    for(idx = 0; idx < max; idx++)
        disconnectLogMsgListenerFromChild(&servers.queryNode(idx));
}

IPropertyTree *findDaliProcess(IPropertyTree *env, const SocketEndpoint &dali)
{
    Owned<IPropertyTreeIterator> dalis = env->getElements("Software/DaliServerProcess");
    ForEach(*dalis)
    {
        IPropertyTree *cur = &dalis->query();
        Owned<IPropertyTreeIterator> instances = cur->getElements("Instance");
        ForEach(*instances)
        {
            IPropertyTree *inst = &instances->query();
            const char *ps = inst->queryProp("@port");
            unsigned port = ps?atoi(ps):0;
            if (!port)
                port = DALI_SERVER_PORT;
            SocketEndpoint daliep(inst->queryProp("@netAddress"),port);
            if (dali.equals(daliep))
                return cur;;
        }
    }
    return NULL;
}

bool updateDaliEnv(IPropertyTree *env, bool forceGroupUpdate, const char *daliIp)
{
    Owned<IPropertyTreeIterator> dalis = env->getElements("Software/DaliServerProcess/Instance");
    if (!dalis||!dalis->first()) {
        fprintf(stderr,"Could not find DaliServerProcess\n");
        return false;
    }
    SocketEndpoint daliep;
    loop {
        const char *ps = dalis->query().queryProp("@port");
        unsigned port = ps?atoi(ps):0;
        if (!port)
            port = DALI_SERVER_PORT;
        daliep.set(dalis->query().queryProp("@netAddress"),port);
        if (daliIp && *daliIp) {
            SocketEndpoint testep;
            testep.set(daliIp,DALI_SERVER_PORT);
            if (testep.equals(daliep))
                break;
            daliep.set(NULL,0);
        }
        if (!dalis->next())
            break;
        if (!daliep.isNull()) {
            fprintf(stderr,"Ambiguous DaliServerProcess instance\n");
            return false;
        }
    }
    if (daliep.isNull()) {
        fprintf(stderr,"Could not find DaliServerProcess instance\n");
        return false;
    }
    SocketEndpointArray epa;
    epa.append(daliep);
    Owned<IGroup> group = createIGroup(epa);

    bool ret = true;
    initClientProcess(group, DCR_Util);
    StringBuffer response;
    if (querySDS().updateEnvironment(env, forceGroupUpdate, response))
    {
        StringBuffer tmp;
        PROGLOG("Environment and node groups updated in dali at %s",daliep.getUrlStr(tmp).str());
    }
    else
        ret = false;
    if (response.length())
        WARNLOG("%s", response.str());

    closedownClientProcess();
    return ret;
}
