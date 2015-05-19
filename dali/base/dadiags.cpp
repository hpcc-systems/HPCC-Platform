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
#include "dacoven.hpp"
#include "daclient.hpp"
#include "mpbuff.hpp"
#include "mpcomm.hpp"
#include "mputil.hpp"
#include "mputil.hpp"
#include "daserver.hpp"
#include "dasds.hpp"
#include "dasubs.ipp"
#include "dautils.hpp"
#include "dadiags.hpp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif

enum MDiagnosticsRequestKind { 
    MDR_GET_VALUE
};


class CDaliDiagnosticsServer: public IDaliServer, public Thread
{  // Coven size
    
    bool stopped;
public:

    IMPLEMENT_IINTERFACE;

    CDaliDiagnosticsServer()
        : Thread("CDaliDiagnosticsServer")
    {
        stopped = true;
    }

    ~CDaliDiagnosticsServer()
    {
    }

    void start()
    {
        Thread::start();
    }

    void ready()
    {
    }
    
    void suspend()
    {
    }

    void stop()
    {
        if (!stopped) {
            stopped = true;
            queryCoven().cancel(RANK_ALL,MPTAG_DALI_DIAGNOSTICS_REQUEST);
        }
        join();
    }

    int run()
    {
        ICoven &coven=queryCoven();
        CMessageBuffer mb;
        stopped = false;
        CMessageHandler<CDaliDiagnosticsServer> handler("CDaliDiagnosticsServer",this,&CDaliDiagnosticsServer::processMessage);
        while (!stopped) {
            try {
                mb.clear();
                if (coven.recv(mb,RANK_ALL,MPTAG_DALI_DIAGNOSTICS_REQUEST,NULL)) {
                    handler.handleMessage(mb);
                }   
                else
                    stopped = true;
            }
            catch (IException *e)
            {
                EXCLOG(e, "CDaliDiagnosticsServer");
                e->Release();
            }
        }
        return 0;
    }


    void processMessage(CMessageBuffer &mb)
    {
        ICoven &coven=queryCoven();
        MemoryBuffer params;
        params.swapWith(mb);
        int fn;
        params.read(fn);
        switch (fn) {
        case MDR_GET_VALUE: {
                StringAttr id;
                StringBuffer buf;
                params.read(id);
                if (0 == stricmp(id,"threads")) {
                    mb.append(getThreadList(buf).str());
                }
                else if (0 == stricmp(id, "mpqueue")) {
                    mb.append(getReceiveQueueDetails(buf).str());
                }
                else if (0 == stricmp(id, "locks")) { // Legacy - newer diag clients should use querySDS().getLocks() directly
                    Owned<ILockInfoCollection> lockInfoCollection = querySDS().getLocks();
                    mb.append(lockInfoCollection->toString(buf).str());
                }
                else if (0 == stricmp(id, "sdsstats")) { // Legacy - newer diag clients should use querySDS().getUsageStats() directly
                    mb.append(querySDS().getUsageStats(buf).str());
                }
                else if (0 == stricmp(id, "connections")) { // Legacy - newer diag clients should use querySDS().getConnections() directly
                    mb.append(querySDS().getConnections(buf).str());
                }
                else if (0 == stricmp(id, "sdssubscribers")) { // Legacy - newer diag clients should use querySDS().getSubscribers() directly
                    mb.append(querySDS().getSubscribers(buf).str());
                }
                else if (0 == stricmp(id, "clients")) {
                    mb.append(querySessionManager().getClientProcessList(buf).str());
                }
                else if (0 == stricmp(id, "subscriptions")) {
                    mb.append(getSubscriptionList(buf).str());
                }
                else if (0 == stricmp(id, "mpverify")) {
                    queryWorldCommunicator().verifyAll(buf);
                    mb.append(buf.str());
                }
                else if (0 == stricmp(id, "extconsistency")) {
                    mb.append(querySDS().getExternalReport(buf).str());
                }
                else if (0 == stricmp(id, "build")) {
                    mb.append("$Id: dadiags.cpp 62376 2011-02-04 21:59:58Z sort $");
                }
                else if (0 == stricmp(id, "sdsfetch")) {
                    StringAttr branchpath;
                    params.read(branchpath);
                    Linked<IPropertyTree> sroot = querySDSServer().lockStoreRead();
                    try { sroot->queryPropTree(branchpath)->serialize(mb); }
                    catch (...) { querySDSServer().unlockStoreRead(); throw; }
                    querySDSServer().unlockStoreRead();
                }
                else if (0 == stricmp(id, "perf")) {
                    getSystemTraceInfo(buf,PerfMonStandard);
                    mb.append(buf.str());
                }
                else if (0 == stricmp(id, "sdssize")) {
                    StringAttr branchpath;
                    params.read(branchpath);
                    Linked<IPropertyTree> sroot = querySDSServer().lockStoreRead();
                    StringBuffer sbuf;
                    try { 
                        toXML(sroot->queryPropTree(branchpath),sbuf); 
                        DBGLOG("sdssize '%s' = %d",branchpath.get(),sbuf.length());
                    }
                    catch (...) { 
                        querySDSServer().unlockStoreRead(); 
                        throw; 
                    }
                    querySDSServer().unlockStoreRead();
                    mb.append(sbuf.length());
                }
                else if (0 == stricmp(id, "disconnect")) {
                    StringAttr client;
                    params.read(client);
                    SocketEndpoint ep(client);
                    PROGLOG("Dalidiag request to close client connection: %s", client.get());
                    Owned<INode> node = createINode(ep);
                    queryCoven().disconnect(node);
                }
                else if (0 == stricmp(id, "unlock")) {
                    __int64 connectionId;
                    bool disconnect;
                    params.read(connectionId);
                    params.read(disconnect);
                    PROGLOG("Dalidiag request to unlock connection id: %" I64F "x", connectionId);
                    StringBuffer connectionInfo;
                    bool success = querySDSServer().unlock(connectionId, disconnect, connectionInfo);
                    mb.append(success);
                    if (success)
                        mb.append(connectionInfo);
                }
                else if (0 == stricmp(id, "save")) {
                    PROGLOG("Dalidiag requests SDS save");
                    querySDSServer().saveRequest();
                }
                else if (0 == stricmp(id, "settracetransactions")) {
                    PROGLOG("Dalidiag requests Trace Transactions");
                    if(traceAllTransactions(true))
                        mb.append("OK - no change");
                    else
                        mb.append("OK - transaction tracing enabled");
                }
                else if (0 == stricmp(id, "cleartracetransactions")) {
                    PROGLOG("Dalidiag requests Trace Transactions stopped");
                    if(traceAllTransactions(false))
                        mb.append("OK - transaction tracing disabled");
                    else
                        mb.append("OK - no change");
                }
                else if (0 == stricmp(id, "setldapflags")) {
                    unsigned f;
                    params.read(f);
                    PROGLOG("Dalidiag requests setldapflags %d",f);
                    querySessionManager().setLDAPflags(f);

                }
                else if (0 == stricmp(id, "getldapflags")) {
                    unsigned f=querySessionManager().getLDAPflags();;
                    mb.append(f);
                }
                else if (0 == stricmp(id, "setsdsdebug")) {
                    PROGLOG("Dalidiag setsdsdebug");
                    unsigned p;
                    params.read(p);
                    StringArray arr;
                    while (p--)
                    {
                        StringAttr s;
                        params.read(s);
                        arr.append(s);
                    }
                    StringBuffer reply;
                    bool success = querySDSServer().setSDSDebug(arr, reply);
                    mb.append(success).append(reply);
                }
                else
                    mb.append(StringBuffer("UNKNOWN OPTION: ").append(id).str());
            }
            break;
        }
        coven.reply(mb);            
    }   


    void nodeDown(rank_t rank)
    {
        assertex(!"TBD");
    }

} *daliDiagnosticsServer = NULL;


MemoryBuffer & getDaliDiagnosticValue(MemoryBuffer &m)
{
    CMessageBuffer mb;
    mb.append((int)MDR_GET_VALUE).append(m);
    queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_DIAGNOSTICS_REQUEST);
    m.swapWith(mb);
    return m;
}

StringBuffer & getDaliDiagnosticValue(const char *name,StringBuffer &ret)
{
    CMessageBuffer mb;
    mb.append((int)MDR_GET_VALUE).append(name);
    queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_DIAGNOSTICS_REQUEST);
    StringAttr str;
    mb.read(str);
    ret.append(str);
    return ret;
}

IDaliServer *createDaliDiagnosticsServer()
{
    assertex(!daliDiagnosticsServer); // initialization problem
    daliDiagnosticsServer = new CDaliDiagnosticsServer();
    return daliDiagnosticsServer;
}


