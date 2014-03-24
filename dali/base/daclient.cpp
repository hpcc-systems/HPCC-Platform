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

class CDaliClient : public CInterfaceOf<IDaliClient>
{
public:
    CDaliClient()
    {
        daliClientLoggingParent = 0;
    }
    ~CDaliClient()
    {
        while (shutdownHooks.ordinality())
        {
            Owned<IDaliClientShutdown> c = &shutdownHooks.popGet();
            c->clientShutdown();
        }
        clearPagedElementsCache(); // has connections
        closeSDS();
        closeSubscriptionManager();
        stopClientProcess();
        closeCoven(coven);
        stopMPServer();
    }

    virtual void addShutdownHook(IDaliClientShutdown &shutdown)
    {
        shutdownHooks.append(*LINK(&shutdown));
    }

    virtual void removeShutdownHook(IDaliClientShutdown &shutdown)
    {
        shutdownHooks.zap(shutdown);
    }

    //MOE: These should be in a different interface
    void connectLogMsgManagerToDali()
    {
        IGroup & servers = queryDefaultDali()->queryCoven().queryGroup();
        unsigned parentRank = getRandom() % servers.ordinality();    // PG: Not sure if logging to random parent is best?
        daliClientLoggingParent = &servers.queryNode(parentRank);
        connectLogMsgManagerToParent(LINK(daliClientLoggingParent));   // PG: This may be nasty if node chosen is down
    }

    void disconnectLogMsgManagerFromDali()
    {
        disconnectLogMsgManagerFromParentOwn(daliClientLoggingParent);
        daliClientLoggingParent = 0;
    }


    bool initClientProcess(IGroup *servergrp, DaliClientRole role, unsigned mpport, const char *clientVersion, const char *minServerVersion, unsigned timeout)
    {
        assertex(servergrp);
        startMPServer(mpport);
        Owned<ICommunicator> comm(createCommunicator(servergrp,true));
        IGroup * covengrp;
        if (!registerClientProcess(comm.get(),covengrp,timeout,role))
        {
            return false;
        }
        coven.setown(initCoven(covengrp,NULL,clientVersion, minServerVersion));
        covengrp->Release();
        queryLogMsgManager()->setSession(myProcessSession());
        return true;
    }

    virtual ICoven &queryCoven()
    {
        //MORE: This doesn't work
        if (!coven)
        {
            Owned<IException> e = MakeStringException(-1, "No access to Dali - this normally means a plugin call is being called from a thorslave");
            EXCLOG(e, NULL);
            throw e.getClear();
        }
        return *coven;
    }

    virtual bool verifyCovenConnection(unsigned timeout)
    {
        return coven->verifyAll(true, timeout);
    }

    int compareDaliServerVersion(const char * version) const
    {
        return coven->queryDaliServerVersion().compare(version);
    }

    const CDaliVersion &queryDaliServerVersion() const
    {
        return coven->queryDaliServerVersion();
    }

protected:
    Owned<ICoven> coven;
    INode * daliClientLoggingParent;
    IArrayOf<IDaliClientShutdown> shutdownHooks;
};

//Use a pointer instead of an Owned<IDaliClient> - to prevent the system crashing if closedownClientProcess not called.
static IDaliClient * defaultDaliClient;

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



void closedownClientProcess()
{
    ::Release(defaultDaliClient);
    defaultDaliClient = NULL;
}

IDaliClient * createDaliClient(IGroup *servergrp, DaliClientRole role, unsigned mpport, const char *clientVersion, const char *minServerVersion, unsigned timeout)
{
    Owned<CDaliClient> client = new CDaliClient;
    if (client->initClientProcess(servergrp, role, mpport, clientVersion, minServerVersion, timeout))
        return client.getClear();
    return NULL;
}

bool initClientProcess(IGroup *servergrp, DaliClientRole role, unsigned mpport, const char *clientVersion, const char *minServerVersion, unsigned timeout)
{
    closedownClientProcess();
    defaultDaliClient = createDaliClient(servergrp, role, mpport, clientVersion, minServerVersion, timeout);
    return defaultDaliClient != NULL;
}

IDaliClient * queryDefaultDali()
{
    return defaultDaliClient;
}

bool daliClientActive()
{
    return defaultDaliClient != NULL;
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



void connectLogMsgListenerToDali()
{
    // MORE: may be better to make daservers share messages, in which case only need to listen to one member of coven
    IGroup & servers = queryDefaultDali()->queryCoven().queryGroup();
    unsigned idx;
    unsigned max = servers.ordinality();
    for(idx = 0; idx < max; idx++)
        connectLogMsgListenerToChild(&servers.queryNode(idx));
}

void disconnectLogMsgListenerFromDali()
{
    // MORE: may be better to make daservers share messages, in which case only need to listen to one member of coven
    IGroup & servers = queryDefaultDali()->queryCoven().queryGroup();
    unsigned idx;
    unsigned max = servers.ordinality();
    for(idx = 0; idx < max; idx++)
        disconnectLogMsgListenerFromChild(&servers.queryNode(idx));
}

const CDaliVersion &queryDaliServerVersion()
{
    return queryDefaultDali()->queryDaliServerVersion();
}

int compareDaliServerVersion(const char * version)
{
    return queryDefaultDali()->compareDaliServerVersion(version);
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
        const char *ps = dalis->get().queryProp("@port");
        unsigned port = ps?atoi(ps):0;
        if (!port)
            port = DALI_SERVER_PORT;
        daliep.set(dalis->get().queryProp("@netAddress"),port);
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
