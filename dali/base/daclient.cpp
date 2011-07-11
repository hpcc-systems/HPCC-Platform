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

#define da_decl __declspec(dllexport)
#include "platform.h"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jtime.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/dali/base/daclient.cpp $ $Id: daclient.cpp 62376 2011-02-04 21:59:58Z sort $");

#include "mpcomm.hpp"
#include "mplog.hpp"
#include "dasess.hpp"
#include "daserver.hpp"
#include "dacsds.ipp"
#include "dautils.hpp"

#include "daclient.hpp"

extern bool registerClientProcess(ICommunicator *comm, IGroup *& retcoven,unsigned timeout,DaliClientRole role);
extern void stopClientProcess();

static bool restoreSEH=false;
static bool daliClientIsActive = false;
static INode * daliClientLoggingParent = 0;
static ILogMsgHandler * fileMsgHandler = 0;

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



bool initClientProcess(IGroup *servergrp, DaliClientRole role, unsigned short mpport, const char *clientVersion, const char *minServerVersion, unsigned timeout, bool enableSEH)
{
    assertex(servergrp);
    daliClientIsActive = true;
    restoreSEH = enableSEH;
    if (enableSEH)
      EnableSEHtoExceptionMapping();
    startMPServer(mpport);
    Owned<ICommunicator> comm(createCommunicator(servergrp,true));
    IGroup * covengrp;
    if (!registerClientProcess(comm.get(),covengrp,timeout,role)) {
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
    if (restoreSEH) {
        DisableSEHtoExceptionMapping();
        restoreSEH = false;
    }
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

