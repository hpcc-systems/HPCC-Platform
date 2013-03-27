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

#include "build-config.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jdebug.hpp"
#include "jptree.hpp"
#include "jprop.hpp"
#include "jfile.hpp"
#include "jsocket.hpp"
#include "jregexp.hpp"
#include "mplog.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "eclagent.ipp"
#include "deftype.hpp"
#include "hthor.hpp"
#include "portlist.h"
#include "dalienv.hpp"
#include "daaudit.hpp"

#include "hqlplugins.hpp"
#include "eclrtl_imp.hpp"
#include "rtlds_imp.hpp"
#include "workunit.hpp"
#include "eventqueue.hpp"
#include "schedulectrl.hpp"

#include "mpbase.hpp"
#include "daclient.hpp"
#include "dacoven.hpp"
#include "thorxmlread.hpp"
#include "rmtfile.hpp"
#include "roxiedebug.hpp"
#include "roxiehelper.hpp"
#include "jlzw.hpp"

using roxiemem::OwnedRoxieString;

#include <new>

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#endif


//#define LEAK_FILE         "c:\\leaks.txt"

#define MONITOR_ECLAGENT_STATUS     
 
static const char XMLHEADER[] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

//#define ROOT_DRIVE      "c:"

//#define DEFAULT_REALTHOR_HOST "localhost"
#define PERSIST_LOCK_TIMEOUT 10000

#define ABORT_CHECK_INTERVAL 30     // seconds
#define ABORT_DEADMAN_INTERVAL (60*5)  // seconds

typedef IEclProcess* (* EclProcessFactory)();

const LogMsgCategory MCsetresult = MCprogress(100);     // Category used to inform when setting result
const LogMsgCategory MCgetresult = MCprogress(200);     // Category used to inform when getting result
const LogMsgCategory MCresolve = MCprogress(100);       // Category used to inform during name resolution
const LogMsgCategory MCrunlock = MCprogress(100);      // Category used to inform about run lock progress

Owned<IPropertyTree> agentTopology;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    agentTopology.clear();
}

//-----------------------------------------------------------------------------------------------------

inline const char * nullText(const char * text)
{
    if (text) return text;
    return "(null)";
}

inline const char * ensureText(const char * text)
{
    if (text) return text;
    return "";
}

static const char * getResultText(StringBuffer & s, const char * stepname, unsigned sequence)
{
    switch ((int)sequence)
    {
    case -1: return s.append("STORED('").append(stepname).append("')");
    case -2: return s.append("PERSIST('").append(stepname).append("')");
    case -3: return s.append("global('").append(stepname).append("')");
    default:
        if (stepname)
            return s.append(stepname);
        return s.append('#').append(sequence);
    }
}

void logGetResult(const char * name, const char * stepname, unsigned sequence)
{
    LOG(MCgetresult, unknownJob, "getResult%s(%s,%d)", name, nullText(stepname), sequence);
}

//=======================================================================================

interface IHThorDebugSocketListener : extends IInterface
{
    virtual void start() = 0;
    virtual bool stop(unsigned timeout) = 0;
    virtual void stopListening() = 0;
    virtual unsigned queryPort() const = 0;
    virtual bool suspend(bool suspendIt) = 0;
};

class CHThorDebugSocketListener : public Thread, implements IHThorDebugSocketListener, implements IThreadFactory
{
    unsigned port;
    bool running;
    bool suspended;
    Semaphore started;
    Owned<ISocket> socket;
    Owned<IThreadPool> pool;
    Owned<CHThorDebugContext> debugContext;
    SocketEndpoint ep;

    CriticalSection activeCrit;

public:
    IMPLEMENT_IINTERFACE;
    CHThorDebugSocketListener(CHThorDebugContext * _debugContext) 
        : Thread("CHThorDebugSocketListener"), debugContext(_debugContext)
    {
        running = false;
        suspended = false;
        unsigned poolSize = 10;//MORE : What is a good threadcoutn?

        // Note we allow a few additional threads than requested - these are the threads that return "Too many active queries" responses
        pool.setown(createThreadPool("HThorSocketWorkerPool", this, NULL, poolSize+5, INFINITE));
    }

    virtual void start()
    {
        assertex(!running);
        Thread::start();
        started.wait();
    }

    virtual bool stop(unsigned timeout)
    {
        if (running)
        {
            running = false;
            socket->cancel_accept();
            join();
            Release();
        }
        return pool->joinAll(false, timeout);
    }

    virtual void stopListening()
    {
        // Not threadsafe, but we only call this when generating a core file... what's the worst that can happen?
        try
        {
            DBGLOG("Closing listening socket %d", port);
            socket.clear();
            DBGLOG("Closed listening socket %d", port);
        }
        catch(...)
        {
        }
    }

    virtual int run()
    {
        port = HTHOR_DEBUG_BASE_PORT;
        loop 
        {
            try 
            {
                DBGLOG("CHThorDebugSocketListener trying port %d", port);
                socket.setown( ISocket::create(port) );
                break;
            }
            catch (IJSOCK_Exception *e)
            {
                if (e->errorCode()!=JSOCKERR_port_in_use)
                    throw;
                e->Release();
            }
            if (port < (HTHOR_DEBUG_BASE_PORT + HTHOR_DEBUG_PORT_RANGE))
                ++port;
            else
            {
                DBGLOG("No DEBUG ports currently available in range %d - %d",HTHOR_DEBUG_BASE_PORT, HTHOR_DEBUG_BASE_PORT + HTHOR_DEBUG_PORT_RANGE); // MORE - or has terminated?
                MilliSleep(5000);
                port = HTHOR_DEBUG_BASE_PORT;
            }
        }
        DBGLOG("CHThorDebugSocketListener listening to socket on port %d", port);
        ep.set(port, queryHostIP());

        running = true;
        started.signal();
        while (running)
        {
            ISocket *client = socket->accept(true);
            if (client)
            {
                client->set_linger(-1);
                pool->start(client);
            }
        }
        DBGLOG("CHThorDebugSocketListener closed query socket");
        return 0;
    }

    virtual IPooledThread *createNew();

    virtual unsigned queryPort() const
    {
        return port;
    }

    virtual bool suspend(bool suspendIt)
    {
        CriticalBlock b(activeCrit);
        bool ret = suspended;
        suspended = suspendIt;
        return ret;
    }
};

//=======================================================================================

class CHThorDebugSocketWorker : public CInterface, implements IPooledThread
{
    Owned<SafeSocket> client;
    Owned<CHThorDebugContext> debugContext;
    Owned<CDebugCommandHandler> debugCmdHandler;
    SocketEndpoint ep;

public:
    IMPLEMENT_IINTERFACE;

    CHThorDebugSocketWorker(CHThorDebugContext * _debugContext, SocketEndpoint &_ep) :debugContext(_debugContext), ep(_ep)
    {
    }

    void init(void *_r)
    {
        client.setown(new CSafeSocket((ISocket *) _r));
    }


    bool sanitizeQuery(Owned<IPropertyTree> &queryXML, StringAttr &queryName, StringBuffer &saniText, bool isHTTP, const char *&uid, bool &isRequest, bool &isRequestArray)
    {
        if (queryXML)
        {
            queryName.set(queryXML->queryName());
            isRequest = false;
            isRequestArray = false;
            if (isHTTP)
            {
                if (stricmp(queryName, "envelope") == 0)
                {
                    queryXML.setown(queryXML->getPropTree("Body/*"));
                    if (!queryXML)
                        throw MakeStringException(0, "Malformed SOAP request (missing Body)");
                    String reqName(queryXML->queryName());
                    queryXML->removeProp("@xmlns:m");

                    // following code is moved from main() - should be no performance hit
                    String requestString("Request");
                    String requestArrayString("RequestArray");

                    if (reqName.endsWith(requestArrayString))
                    {
                        isRequestArray = true;
                        queryName.set(reqName.toCharArray(), reqName.length() - requestArrayString.length());
                    }
                    else if (reqName.endsWith(requestString))
                    {
                        isRequest = true;
                        queryName.set(reqName.toCharArray(), reqName.length() - requestString.length());
                    }
                    else
                        queryName.set(reqName.toCharArray());

                    queryXML->renameProp("/", queryName.get());  // reset the name of the tree
                }
                else
                    throw MakeStringException(0, "Malformed SOAP request");
            }
            bool isBlind = queryXML->getPropBool("@blind", false) || queryXML->getPropBool("_blind", false);

            // convert to XML with attribute values in single quotes - makes replaying queries easier
            toXML(queryXML, saniText, 0, isBlind ? (XML_SingleQuoteAttributeValues | XML_Sanitize) : XML_SingleQuoteAttributeValues);
            uid = queryXML->queryProp("@uid");
            if (!uid)
                uid = "-";
            return isBlind;
        }
        else
            throw MakeStringException(0, "Malformed request");
    }

    void main()
    {
        StringBuffer rawText;
        unsigned priority = (unsigned) -2;
        unsigned memused = 0;
        IpAddress peer;
        bool continuationNeeded;
        bool isStatus;

        Owned<IDebuggerContext> debuggerContext;
        unsigned slavesReplyLen = 0;
        HttpHelper httpHelper;
        try
        {
            client->querySocket()->getPeerAddress(peer);
            DBGLOG("Reading debug command from socket...");
            if (!client->readBlock(rawText, WAIT_FOREVER, &httpHelper, continuationNeeded, isStatus, 1024*1024))
            {
                if (traceLevel > 8)
                {
                    StringBuffer b;
                    DBGLOG("No data reading query from socket");
                }
                client.clear();
                return;
            }
            assertex(!continuationNeeded);
        }
        catch (IException * E)
        {
            if (traceLevel > 0)
            {
                StringBuffer b;
                DBGLOG("Error reading query from socket: %s", E->errorMessage(b).str());
            }
            E->Release();
            client.clear();
            return;
        }

        bool isRaw = false;
        bool isHTTP = false;
        Owned<IPropertyTree> queryXml;
        StringBuffer sanitizedText;
        StringAttr queryName;
        StringBuffer peerStr;
        peer.getIpText(peerStr);
        const char *uid = "-";
        StringBuffer ctxstr;
        try
        {

            try
            {
                queryXml.setown(createPTreeFromXMLString(rawText.str(), ipt_caseInsensitive, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_ignoreNameSpaces)));
            }
            catch (IException *E)
            {
                StringBuffer s;
                DBGLOG("ERROR: Invalid XML received from %s:%s", E->errorMessage(s).str(), rawText.str());
                throw;
            }
            bool isRequest = false;
            bool isRequestArray = false;
            bool isBlind = false;
            bool isDebug = false;

            sanitizeQuery(queryXml, queryName, sanitizedText, isHTTP, uid, isRequest, isRequestArray);
            DBGLOG("Received debug query %s", sanitizedText.str());

            FlushingStringBuffer response(client, false, MarkupFmt_XML, false, false, queryDummyContextLogger());
            response.startDataset("Debug", NULL, (unsigned) -1);

            if (!debugCmdHandler.get())
                debugCmdHandler.setown(new CDebugCommandHandler);
            debugCmdHandler->doDebugCommand(queryXml, debugContext, response);
            response.flush(true);
        }
        catch (IException *E)
        {
            StringBuffer s;
            client->sendException("EclAgent", E->errorCode(), E->errorMessage(s), false, queryDummyContextLogger());
        }
        unsigned replyLen = 0;
        client->write(&replyLen, sizeof(replyLen));
    }

    bool canReuse()
    {
        return true;
    }

    bool stop()
    {
        ERRLOG("CHThorDebugSocketWorker stopped with queries active");
        return true; 
    }

};

IPooledThread *CHThorDebugSocketListener::createNew()
{
    return new CHThorDebugSocketWorker(debugContext, ep);
}

//=======================================================================================

CHThorDebugContext::CHThorDebugContext(const IContextLogger &_logctx, IPropertyTree *_queryXGMML, EclAgent *_eclAgent) 
    : CBaseServerDebugContext(_logctx, _queryXGMML, client), eclAgent(_eclAgent)
{
}

inline unsigned CHThorDebugContext::queryPort()
{
    return listener->queryPort();
}

void CHThorDebugContext::debugInitialize(const char *_id, const char *_queryName, bool _breakAtStart)
{
    CBaseServerDebugContext::debugInitialize(_id, _queryName, _breakAtStart);

    //Start debug socket listener thread
    listener.setown(new CHThorDebugSocketListener(this));
    listener->start();
}

void CHThorDebugContext::debugInterrupt(IXmlWriter *output)
{
    CriticalBlock b(debugCrit);
    if (!running)
        return; // Query will already be paused in ECLAGENT !
    detached = false;
    nextActivity.clear();
    watchState = WatchStateStep;
    debuggerActive++;
    {
        CriticalUnblock b(debugCrit);
        debuggerSem.wait(); // MORE - should this be inside critsec? Should it be there at all?
    }
    doStandardResult(output);
}

void CHThorDebugContext::debugPrintVariable(IXmlWriter *output, const char *name, const char *type) const
{
    UNIMPLEMENTED;
}

void CHThorDebugContext::waitForDebugger(DebugState state, IActivityDebugContext *probe)
{
    if (eclAgent->queryWorkUnit()->aborting())
        return;
    eclAgent->setDebugPaused();
    CBaseServerDebugContext::waitForDebugger(state, probe);
    eclAgent->setDebugRunning();
}

bool CHThorDebugContext::onDebuggerTimeout()
{
    return eclAgent->queryWorkUnit()->aborting();
}

//=======================================================================================

class EclAgentPluginCtx : public SimplePluginCtx
{
public:
    virtual int ctxGetPropInt(const char *propName, int defaultValue) const
    {
        return agentTopology->getPropInt(propName, defaultValue);
    }
    virtual const char *ctxQueryProp(const char *propName) const
    {
        return agentTopology->queryProp(propName);
    }
} PluginCtx;

//=======================================================================================

EclAgent::EclAgent(IConstWorkUnit *wu, const char *_wuid, bool _checkVersion, bool _resetWorkflow, bool _noRetry, char const * _logname, const char *_allowedPipeProgs, IPropertyTree *_queryXML, IProperties *_globals, IPropertyTree *_config, ILogMsgHandler * _logMsgHandler)
    : wuRead(wu), wuid(_wuid), checkVersion(_checkVersion), resetWorkflow(_resetWorkflow), noRetry(_noRetry), allowedPipeProgs(_allowedPipeProgs), globals(_globals), config(_config), logMsgHandler(_logMsgHandler)
{
    isAborting = false;
    isStandAloneExe = false;
    isRemoteWorkunit = true;
    resolveFilesLocally = false;
    writeResultsToStdout = false;

    wuRead->getUser(StringAttrAdaptor(userid));
    useProductionLibraries = wuRead->getDebugValueBool("useProductionLibraries", false);
    SCMStringBuffer clusterName;
    wuRead->getClusterName(clusterName);
    clusterNames.append(clusterName.str());
    clusterWidth = -1;
    abortmonitor = new cAbortMonitor(*this);
    abortmonitor->start();
    EnableSEHtoExceptionMapping();
    setSEHtoExceptionHandler(abortmonitor);

    StringAttrAdaptor adaptor(clusterType);
    wuRead->getDebugValue("targetClusterType", adaptor);
    rltCache.setown(createRecordLayoutTranslatorCache());
    pluginMap = NULL;
    stopAfter = globals->getPropInt("-limit",-1);

    RemoteFilename logfile;
    logfile.setLocalPath(_logname);
    logfile.getRemotePath(logname);
    if (wu->getDebugValueBool("Debug", false))
    {
        Owned<IPropertyTree> destTree = createPTree("Query");
        Owned<IConstWUGraphIterator> graphs = &wu->getGraphs(GraphTypeActivities);
        SCMStringBuffer graphName;
        ForEach(*graphs)
        {
            graphs->query().getName(graphName);
            Owned<IPropertyTree> graphTree = createPTree("Graph");
            graphTree->addProp("@id", graphName.str());
            Owned<IPropertyTree> xgmmlTree = createPTree("xgmml");
            Owned<IPropertyTree> graphXgmml = graphs->query().getXGMMLTree(false);
            xgmmlTree->addPropTree("graph", graphXgmml.getClear());
            graphTree->addPropTree("xgmml", xgmmlTree.getClear());
            destTree->addPropTree("Graph", graphTree.getClear());
        }
        debugContext.setown(new CHThorDebugContext(queryDummyContextLogger(), destTree.getClear(), this ));
        SCMStringBuffer jobName;
        debugContext->debugInitialize(wuid, wu->getJobName(jobName).str(), true);
    }
    if (_queryXML)
    {
        Owned<IWorkUnit> w = updateWorkUnit();
        w->setXmlParams(_queryXML);
    }
    Owned<const IPropertyTree> xmlParams = wuRead->getXmlParams();
    if (xmlParams)
        processXmlParams(xmlParams);
}

EclAgent::~EclAgent()
{
    ::Release(pluginMap);
    abortmonitor->stop();
    ::Release(abortmonitor); // no nead to join
    deleteTempFiles();
    setSEHtoExceptionHandler(NULL); // clear handler
    // MORE - could delete local DLL at this point but I may prefer not to
}

void EclAgent::setStandAloneOptions(bool _isStandAloneExe, bool _isRemoteWorkunit, bool _resolveFilesLocally, bool _writeResultsToStdout, outputFmts _outputFmt)
{
    isStandAloneExe = _isStandAloneExe;
    isRemoteWorkunit = _isRemoteWorkunit;
    resolveFilesLocally = _resolveFilesLocally;

    writeResultsToStdout = _writeResultsToStdout;
    outputFmt = _outputFmt;

    if (writeResultsToStdout)
        outputSerializer.setown(createOrderedOutputSerializer(stdout));
    if (isRemoteWorkunit)
        wuRead->subscribe(SubscribeOptionAbort);
}

void EclAgent::processXmlParams(const IPropertyTree *params)
{
    Owned<IPropertyTreeIterator> elems = params->getElements("*");
    ForEach(*elems)
    {
        IPropertyTree & curVal = elems->query();
        const char *name = curVal.queryName();
        Owned<IWUResult> r = updateResult(name, -1);
        if (r)
        {
            StringBuffer s;
            if (r->isResultScalar() && !curVal.hasChildren())
            {
                curVal.getProp(".", s);
                r->setResultXML(s.str());
                r->setResultStatus(ResultStatusSupplied);
            }
            else
            {
                toXML(&curVal, s);
                bool isSet = (curVal.hasProp("Item") || curVal.hasProp("string"));
                r->setResultRaw(s.length(), s.str(), isSet ? ResultFormatXmlSet : ResultFormatXml);
            }
        }
        else
            DBGLOG("WARNING: no matching variable in workunit for input parameter %s", name);
    }
}

ICodeContext *EclAgent::queryCodeContext()
{
    return this;    //called by helper
}

const char *EclAgent::queryTempfilePath()
{
    if (agentTempDir.isEmpty()) 
    {
        StringBuffer dir;
        if (!getConfigurationDirectory(agentTopology->queryPropTree("Directories"),"temp","eclagent",agentTopology->queryProp("@name"),dir))
        {
            dir.clear();
#ifdef _WIN32
            char path[_MAX_PATH+1];
            DWORD len = GetEnvironmentVariable("TEMP",path,sizeof(path));
            if (len)
                dir.append(path);
            else
                dir.append("c:");
            dir.append("\\HPCCSystems\\hthortemp");
#else
            dir.append("/tmp/HPCCSystems/hthortemp");
#endif
        }
        recursiveCreateDirectory(dir.str());
        agentTempDir.set(dir.str());
    }
    return agentTempDir.sget();
}

StringBuffer & EclAgent::getTempfileBase(StringBuffer & buff)
{
    return buff.append(queryTempfilePath()).append(PATHSEPCHAR).append(wuid);
}

const char *EclAgent::queryTemporaryFile(const char *fname)
{
    StringBuffer tempfilename;
    getTempfileBase(tempfilename).append('.').append(fname);
    CriticalBlock crit(tfsect);
    ForEachItemIn(idx, tempFiles)
    {
        if (strcmp(tempFiles.item(idx), tempfilename.str())==0)
            return tempFiles.item(idx);
    }
    StringBuffer errmsg;
    errmsg.append("Attempt to read temp file that has not yet been registered: ").append(tempfilename);
    fail(0, errmsg.str());
    return 0;
}

const char *EclAgent::noteTemporaryFile(const char *fname)
{
    StringBuffer tempfilename;
    getTempfileBase(tempfilename).append('.').append(fname);
    CriticalBlock crit(tfsect);
    tempFiles.append(tempfilename.str());
    return tempFiles.item(tempFiles.length()-1);
}

const char *EclAgent::noteTemporaryFilespec(const char *fspec)
{
    CriticalBlock crit(tfsect);
    tempFiles.append(fspec);
    return tempFiles.item(tempFiles.length()-1);
}

void EclAgent::deleteTempFiles()
{
    CriticalBlock crit(tfsect);
    ForEachItemIn(idx, tempFiles)
    {
        remove(tempFiles.item(idx));
    }
    tempFiles.kill();
}

const char *EclAgent::loadResource(unsigned id)
{
    return reinterpret_cast<const char *>(dll->getResource(id));  // stays loaded as long as dll stays loaded
}

IWorkUnit *EclAgent::updateWorkUnit()
{
    CriticalBlock block(wusect);
    if (!wuWrite)
    {
        wuWrite.setown(&wuRead->lock());
    }
    return wuWrite.getLink();
}

void EclAgent::unlockWorkUnit()
{
    CriticalBlock block(wusect);
    if (wuWrite)
    {
        IWorkUnit *w = wuWrite.getClear();
        if (!w->Release()) 
            ERRLOG("EclAgent::unlockWorkUnit workunit not released");
    }
}

void EclAgent::reloadWorkUnit()
{
    unlockWorkUnit();
    wuRead->forceReload();
}

void EclAgent::abort()
{
    if (activeGraph)
        activeGraph->abort();
}

IWUResult *EclAgent::updateResult(const char *name, unsigned sequence)
{
    WorkunitUpdate w = updateWorkUnit();
    return updateWorkUnitResult(w, name, sequence);
}

IConstWUResult *EclAgent::getResult(const char *name, unsigned sequence)
{
    IConstWorkUnit *w = queryWorkUnit();
    return getWorkUnitResult(w, name, sequence);
}

IConstWUResult *EclAgent::getResultForGet(const char *name, unsigned sequence)
{
    Owned<IConstWUResult> r = getResult(name, sequence);
    if (!r || (r->getResultStatus() == ResultStatusUndefined))
    {
        StringBuffer s;
        failv(0, "value %s in workunit is undefined", getResultText(s,name,sequence));
    }
    return r.getClear();
}

IConstWUResult *EclAgent::getExternalResult(const char * wuid, const char *name, unsigned sequence)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> externalWU = factory->openWorkUnit(wuid, false);
    externalWU->remoteCheckAccess(queryUserDescriptor(), false);
    return getWorkUnitResult(externalWU, name, sequence);
}

void EclAgent::outputFormattedResult(const char * name, unsigned sequence, bool close)
{
    SCMStringBuffer buff;
    Owned<IConstWUResult> res = getResult(name, sequence);
    if (!res)
        fail(0, "Internal error: No result found in outputFormattedResult()");

    switch (outputFmt)
    {
    case ofXML:
        {
            res->getResultXml(buff);
            outputSerializer->fwrite(sequence, (const void*)buff.str(), 1, buff.length());
            break;
        }
    case ofRAW:
        {
            MemoryBuffer temp;
            MemoryBuffer2IDataVal adaptor(temp);
            res->getResultRaw(adaptor, NULL, NULL);
            outputSerializer->fwrite(sequence, (const void*)temp.bufferBase(), 1, temp.length());
            break;
        }
    default:
        {
            fail(0, "Internal error: Unexpected outputFmts type in outputFormattedResult()");
            break;
        }
    }
    if (close)
        outputSerializer->close(sequence, false);
}

void EclAgent::setResultInt(const char * name, unsigned sequence, __int64 val)
{
    LOG(MCsetresult, unknownJob, "setResultInt(%s,%d,%"I64F"d)", nullText(name), sequence, val);
    Owned<IWUResult> r = updateResult(name, sequence);
    if (r)
    {
        r->setResultInt(val);   
        r->setResultStatus(ResultStatusCalculated);
    }
    else
        fail(0, "Unexpected parameters to setResultInt");
    if (writeResultsToStdout && (int) sequence >= 0)
    {
        if (outputFmt == ofSTD)
        {
            outputSerializer->printf(sequence, "%"I64F"d", val);
            outputSerializer->close(sequence, true);
        }
        else
            outputFormattedResult(name, sequence);
    }
}

void EclAgent::setResultUInt(const char * name, unsigned sequence, unsigned __int64 val)
{
    LOG(MCsetresult, unknownJob, "setResultUInt(%s,%d,%"I64F"u)", nullText(name), sequence, val);
    Owned<IWUResult> r = updateResult(name, sequence);
    if (r)
    {
        r->setResultUInt(val);  
        r->setResultStatus(ResultStatusCalculated);
    }
    else
        fail(0, "Unexpected parameters to setResultUInt");
    if (writeResultsToStdout && (int) sequence >= 0)
    {
        if (outputFmt == ofSTD)
        {
            outputSerializer->printf(sequence, "%"I64F"u", val);
            outputSerializer->close(sequence, true);
        }
        else
            outputFormattedResult(name, sequence);
    }
}

void EclAgent::setResultReal(const char *name, unsigned sequence, double val)
{
    // Still a bit of a mess - variables vs results
    LOG(MCsetresult, unknownJob, "setResultReal(%s,%d,%6f)", nullText(name), sequence, val);
    Owned<IWUResult> r = updateResult(name, sequence);
    if (r)
    {
        r->setResultReal(val);  
        r->setResultStatus(ResultStatusCalculated);
    }
    else
        fail(0, "Unexpected parameters to setResultReal");
    if (writeResultsToStdout && (int) sequence >= 0)
    {
        if (outputFmt == ofSTD)
        {
            StringBuffer out;
            out.append(val);
            outputSerializer->printf(sequence, "%s", out.str());
            outputSerializer->close(sequence, true);
        }
        else
            outputFormattedResult(name, sequence);
    }
}

bool EclAgent::isResult(const char *name, unsigned sequence)
{
    Owned<IConstWUResult> r = getResult(name, sequence);
    return r != NULL && r->getResultStatus() != ResultStatusUndefined;
}

unsigned EclAgent::getResultHash(const char * name, unsigned sequence)
{
    logGetResult("Hash", name, sequence);
    Owned<IConstWUResult> r = getResult(name, sequence);
    if (!r)
        failv(0, "Failed to retrieve hash value %s from workunit", name);
    return r->getResultHash();
}


//---------------------------------------------------------------------------

#define PROTECTED_GETRESULT(STEPNAME, SEQUENCE, KIND, KINDTEXT, ACTION) \
    logGetResult(KIND, STEPNAME, SEQUENCE); \
    Owned<IConstWUResult> r = getResultForGet(STEPNAME, SEQUENCE); \
    try \
    { \
        ACTION \
    } \
    catch (IException * e) { \
        StringBuffer s, text; e->errorMessage(text); e->Release(); \
        throw MakeStringException(0, "result %s in workunit contains an invalid " KINDTEXT " value [%s]", getResultText(s, STEPNAME, SEQUENCE), text.str()); \
    } \
    catch (...) { StringBuffer s; throw MakeStringException(0, "value %s in workunit contains an invalid " KINDTEXT " value", getResultText(s, STEPNAME, SEQUENCE)); }


__int64 EclAgent::getResultInt(const char * stepname, unsigned sequence)
{
    PROTECTED_GETRESULT(stepname, sequence, "Int", "integer",
        return r->getResultInt();
    );
}

double EclAgent::getResultReal(const char * stepname, unsigned sequence)
{
    PROTECTED_GETRESULT(stepname, sequence, "Real", "real",
        return r->getResultReal();
    );
}

char *EclAgent::getResultVarString(const char * stepname, unsigned sequence)
{
    PROTECTED_GETRESULT(stepname, sequence, "VarString", "string",
        SCMStringBuffer result;
        r->getResultString(result);
        return result.s.detach();
    );
}

UChar *EclAgent::getResultVarUnicode(const char * stepname, unsigned sequence)
{
    PROTECTED_GETRESULT(stepname, sequence, "VarUnicode", "unicode",
        MemoryBuffer result;
        r->getResultUnicode(MemoryBuffer2IDataVal(result));
        unsigned tlen = result.length()/2;
        result.append((UChar)0);
        return (UChar *)result.detach();
    );
}

void EclAgent::getResultString(unsigned & tlen, char * & tgt, const char * stepname, unsigned sequence)
{
    PROTECTED_GETRESULT(stepname, sequence, "String", "string",
        SCMStringBuffer result;
        r->getResultString(result);
        tlen = result.length();
        tgt = (char *)result.s.detach();
    );
}

void EclAgent::getResultStringF(unsigned tlen, char * tgt, const char * stepname, unsigned sequence)
{
    PROTECTED_GETRESULT(stepname, sequence, "String", "string",
        //MORE: Could used a fixed size IStringVal implementation to save a memory allocation, but hardly worth it.
        SCMStringBuffer result;
        r->getResultString(result);
        rtlStrToStr(tlen, tgt, result.length(), result.s.str());
    );
}

void EclAgent::getResultData(unsigned & tlen, void * & tgt, const char * stepname, unsigned sequence)
{
    PROTECTED_GETRESULT(stepname, sequence, "Data", "data",
        SCMStringBuffer result;
        r->getResultString(result);
        tlen = result.length();
        tgt = (char *)result.s.detach();
    );
}

void EclAgent::getResultRaw(unsigned & tlen, void * & tgt, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
{
    tgt = NULL;
    PROTECTED_GETRESULT(stepname, sequence, "Raw", "raw",
        Variable2IDataVal result(&tlen, &tgt);
        Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
        Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
        r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
    );
}

void EclAgent::getResultSet(bool & tisAll, size32_t & tlen, void * & tgt, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
{
    tgt = NULL;
    PROTECTED_GETRESULT(stepname, sequence, "Raw", "raw",
        Variable2IDataVal result(&tlen, &tgt);
        Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
        Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
        tisAll = r->getResultIsAll();
        r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
    );
}

void EclAgent::getResultUnicode(unsigned & tlen, UChar * & tgt, const char * stepname, unsigned sequence)
{
    PROTECTED_GETRESULT(stepname, sequence, "Unicode", "unicode",
        MemoryBuffer result;
        r->getResultUnicode(MemoryBuffer2IDataVal(result));
        tlen = result.length()/2;
        tgt = (UChar *)result.detach();
    );
}

void EclAgent::getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence)
{
    PROTECTED_GETRESULT(stepname, sequence, "Decimal", "decimal",
        r->getResultDecimal(tgt, tlen, precision, isSigned);
    );
}

bool EclAgent::getResultBool(const char * stepname, unsigned sequence)
{
    PROTECTED_GETRESULT(stepname, sequence, "Bool", "bool",
        return r->getResultBool();
    );
}

void EclAgent::getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
{
    tgt = NULL;
    logGetResult("ExternalRaw", stepname, sequence);
    try
    {
        Owned<IConstWUResult> r = getExternalResult(wuid, stepname, sequence);
        if (!r) failv(0, "Failed to find raw value %s:%d in workunit %s", nullText(stepname),sequence, wuid);
        
        Variable2IDataVal result(&tlen, &tgt);
        Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
        Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
        r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
    }
    catch (IException * e) 
    {
        StringBuffer text; 
        e->errorMessage(text); 
        e->Release();
        failv(0, "value %s:%d in workunit %s contains an invalid raw value [%s]", nullText(stepname), sequence, wuid, text.str());
    }
    catch (...)
    {
        failv(0, "value %s:%d in workunit %s contains an invalid raw value", nullText(stepname), sequence, wuid);
    }
}

void EclAgent::getResultRowset(size32_t & tcount, byte * * & tgt, const char * stepname, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
{
    tgt = NULL;
    PROTECTED_GETRESULT(stepname, sequence, "Rowset", "rowset",
        MemoryBuffer datasetBuffer;
        MemoryBuffer2IDataVal result(datasetBuffer);
        Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
        Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
        r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
        Owned<IOutputRowDeserializer> deserializer = _rowAllocator->createDiskDeserializer(queryCodeContext());
        rtlDataset2RowsetX(tcount, tgt, _rowAllocator, deserializer, datasetBuffer.length(), datasetBuffer.toByteArray(), isGrouped);
    );
}

void EclAgent::getResultDictionary(size32_t & tcount, byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher)
{
    tcount = 0;
    tgt = NULL;
    PROTECTED_GETRESULT(stepname, sequence, "Rowset", "rowset",
        MemoryBuffer datasetBuffer;
        MemoryBuffer2IDataVal result(datasetBuffer);
        Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
        Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
        r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
        Owned<IOutputRowDeserializer> deserializer = _rowAllocator->createDiskDeserializer(queryCodeContext());
        rtlDeserializeDictionary(tcount, tgt, _rowAllocator, deserializer, datasetBuffer.length(), datasetBuffer.toByteArray());
    );
}

const void * EclAgent::fromXml(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
{
    return createRowFromXml(rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
}


bool EclAgent::getWorkunitResultFilename(StringBuffer & diskFilename, const char * wuid, const char * stepname, int sequence)
{
    try
    {
        Owned<IConstWUResult> result;
        if (wuid)
            result.setown(getExternalResult(wuid, stepname, sequence));
        else
            result.setown(getResultForGet(stepname, sequence));
        if (!result)    failv(0, "Failed to find value %s:%d in workunit %s", nullText(stepname),sequence, nullText(wuid));

        SCMStringBuffer tempFilename;
        result->getResultFilename(tempFilename);
        if (tempFilename.length() == 0)
            return false;

        diskFilename.append("~").append(tempFilename.str());
        return true;
    }
    catch (IException * e) 
    {
        StringBuffer text; 
        e->errorMessage(text); 
        e->Release();
        failv(0, "Failed to find value %s:%d in workunit %s [%s]", nullText(stepname),sequence, nullText(wuid), text.str());
    }
    catch (...)
    {
        failv(0, "Failed to find value %s:%d in workunit %s", nullText(stepname),sequence, nullText(wuid));
    }
    return false;
}

//---------------------------------------------------------------------------

void EclAgent::setResultVarString(const char * stepname, unsigned sequence, const char *val)
{
    setResultString(stepname, sequence, strlen(val), val);
}

void EclAgent::setResultVarUnicode(const char * stepname, unsigned sequence, UChar const *val)
{
    setResultUnicode(stepname, sequence, rtlUnicodeStrlen(val), val);
}

void EclAgent::setResultString(const char * stepname, unsigned sequence, int len, const char *val)
{
    doSetResultString(type_string, stepname, sequence, len, val);
}

void EclAgent::setResultData(const char * stepname, unsigned sequence, int len, const void *val)
{
    doSetResultString(type_data, stepname, sequence, len, (const char *)val);
}

void EclAgent::doSetResultString(type_t type, const char *name, unsigned sequence, int len, const char *val)
{
    LOG(MCsetresult, unknownJob, "setResultString(%s,%d,'%.*s')", nullText(name), sequence, len, val);
    Owned<IWUResult> r = updateResult(name, sequence);
    if (r)
    {
        r->setResultString(val, len);   
        r->setResultStatus(ResultStatusCalculated);
    }
    else
        fail(0, "Unexpected parameters to setResultString");
    if (writeResultsToStdout && (int) sequence >= 0)
    {
        if (outputFmt == ofSTD || outputFmt==ofRAW)
        {
            outputSerializer->fwrite(sequence, (const void*)val, 1, len);
            outputSerializer->close(sequence, outputFmt!=ofRAW);
        }
        else
            outputFormattedResult(name, sequence);
    }
}

//used to output a row
void EclAgent::setResultRaw(const char * name, unsigned sequence, int len, const void *val)
{
    LOG(MCsetresult, unknownJob, "setResultRaw(%s,%d,(%d bytes))", nullText(name), sequence, len);
    Owned<IWUResult> r = updateResult(name, sequence);
    if (r)
    {
        r->setResultRow(len, val);
        r->setResultStatus(ResultStatusCalculated);
    }
    else
        fail(0, "Unexpected parameters to setResultRaw");
    if (writeResultsToStdout && (int) sequence >= 0)
    {
        if (outputFmt == ofSTD || outputFmt==ofRAW)
        {
            outputSerializer->fwrite(sequence, (const void*)val, 1, len);
            outputSerializer->close(sequence, outputFmt!=ofRAW);
        }
        else
            outputFormattedResult(name, sequence);
    }
}

void EclAgent::setResultSet(const char * name, unsigned sequence, bool isAll, size32_t len, const void *val, ISetToXmlTransformer *xform)
{
    LOG(MCsetresult, unknownJob, "setResultSet(%s,%d)", nullText(name), sequence);
    Owned<IWUResult> r = updateResult(name, sequence);
    if (r)
    {
        r->setResultIsAll(isAll);
        r->setResultRaw(len, val, ResultFormatRaw); 
        r->setResultStatus(ResultStatusCalculated);
    }
    else
        fail(0, "Unexpected parameters to setResultSet");
    if (writeResultsToStdout && (int) sequence >= 0)
    {
        if (outputFmt == ofSTD)
        {
            outputSerializer->printf(sequence, "[");
            if (isAll)
                outputSerializer->printf(sequence, "*]");
            else if (xform)
            {
                SimpleOutputWriter x;
                xform->toXML(isAll, len, (const byte *) val, x);
                outputSerializer->printf(sequence, "%s]", x.str()); 
            }
            else
                outputSerializer->printf(sequence, "?]");
            outputSerializer->close(sequence, true);
        }
        else if (outputFmt == ofXML)
        {
            assertex(xform);
            StringBuffer datasetName, resultName;
            if (name)
            {
                datasetName.append(name);
                resultName.append(name);
            }
            else
            {
                datasetName.appendf("Result %d", sequence+1);
                resultName.appendf("Result_%d", sequence+1);
            }
            CommonXmlWriter xmlwrite(0,1);
            xmlwrite.outputBeginNested("Row", false);
            xmlwrite.outputBeginNested(resultName.str(), false);
            xform->toXML(isAll, len, (const byte *)val, xmlwrite);
            xmlwrite.outputEndNested(resultName.str());
            xmlwrite.outputEndNested("Row");
            outputSerializer->printf(sequence, "<Dataset name=\'%s'>\n%s</Dataset>\n",
                    datasetName.str(),
                    xmlwrite.str());
            outputSerializer->close(sequence, false);
        }
        else
            outputFormattedResult(name, sequence);
    }
}

void EclAgent::setResultUnicode(const char * name, unsigned sequence, int len, UChar const * val)
{
    LOG(MCsetresult, unknownJob, "setResultUnicode(%s,%d)", nullText(name), sequence);

    Owned<IWUResult> r = updateResult(name, sequence);
    if (r)
    {
        r->setResultUnicode((char const *)val, len);
        r->setResultStatus(ResultStatusCalculated);
    }
    else
        fail(0, "Unexpected parameters to setResultUnicode");
    if (writeResultsToStdout && (int) sequence >= 0)
    {
        if (outputFmt == ofSTD)
        {
            char * buff = 0;
            unsigned bufflen = 0;
            rtlUnicodeToCodepageX(bufflen, buff, len, val, "utf-8");
            outputSerializer->fwrite(sequence, (const void*)buff, 1, bufflen);
            outputSerializer->close(sequence, true);
        }
        else
            outputFormattedResult(name, sequence);
    }
}

void EclAgent::setResultBool(const char *name, unsigned sequence, bool val)
{
    LOG(MCsetresult, unknownJob, "setResultBool(%s,%d,%s)", nullText(name), sequence, val ? "true" : "false");

    Owned<IWUResult> r = updateResult(name, sequence);
    if (r)
    {
        r->setResultBool(val);
        r->setResultStatus(ResultStatusCalculated);
    }
    else
        fail(0, "Unexpected parameters to setResultBool");
    if (writeResultsToStdout && (int) sequence >= 0)
    {
        if (outputFmt == ofSTD)
        {
            outputSerializer->printf(sequence, val ? "true" : "false");
            outputSerializer->close(sequence, true);
        }
        else
            outputFormattedResult(name, sequence);
    }
}

void EclAgent::setResultDecimal(const char *name, unsigned sequence, int len, int precision, bool isSigned, const void *val)
{
    LOG(MCsetresult, unknownJob, "setResultDecimal(%s,%d)", nullText(name), sequence);
    Owned<IWUResult> r = updateResult(name, sequence);
    if (r)
    {
        r->setResultDecimal(val, len);
        r->setResultStatus(ResultStatusCalculated);
    }
    else
        fail(0, "Unexpected parameters to setResultDecimal");
    if (writeResultsToStdout && (int) sequence >= 0)
    {
        if (outputFmt == ofSTD)
        {
            StringBuffer out;
            if (isSigned)
                outputXmlDecimal(val, len, precision, NULL, out);
            else
                outputXmlUDecimal(val, len, precision, NULL, out);
            outputSerializer->printf(sequence, "%s", out.str());
            outputSerializer->close(sequence, true);
        }
        else
            outputFormattedResult(name, sequence);
    }
}

void EclAgent::setResultDataset(const char * name, unsigned sequence, size32_t len, const void *val, unsigned numRows, bool extend)
{
    LOG(MCsetresult, unknownJob, "setResultDataset(%s,%d)", nullText(name), sequence);
    Owned<IWUResult> result = updateResult(name, sequence);
    if (!result)
        fail(0, "Unexpected parameters to setResultDataset");

    __int64 rows = numRows;
    if (extend)
    {
        rows += result->getResultRowCount();
        result->addResultRaw(len, val, ResultFormatRaw);
    }
    else
        result->setResultRaw(len, val, ResultFormatRaw);

    result->setResultStatus(ResultStatusCalculated);
    result->setResultRowCount(rows);
    result->setResultTotalRowCount(rows);
}

char * EclAgent::resolveName(const char *in, char *out, unsigned outlen)
{
    assertex(!"resolveName() not implemented");
    // is it used???
    return NULL;
}

void EclAgent::logFileAccess(IDistributedFile * file, char const * component, char const * type)
{
    const char * cluster = clusterNames.item(clusterNames.length()-1);
    LOG(daliAuditLogCat,
        ",FileAccess,%s,%s,%s,%s,%s,%s,%s",
        component,
        type,
        ensureText(cluster),
        ensureText(userid.get()),
        file->queryLogicalName(),
        wuid.get(),
        activeGraph ? activeGraph->queryGraphName() : "");
}

bool EclAgent::expandLogicalName(StringBuffer & fullname, const char * logicalName)
{
    bool useScope = true;
    if (logicalName[0]=='~')
    {
        logicalName++;
        useScope = false;
    }
    else if (isAbsolutePath(logicalName)) 
    {
        fullname.append(logicalName);
        return false;
    }
    else
    {
        SCMStringBuffer nsScope;
        queryWorkUnit()->getScope(nsScope);
        if (nsScope.length())
            fullname.append(nsScope.s).append("::");
    }
    fullname.append(logicalName);
    fullname.toLowerCase();
    return useScope;
}

ILocalOrDistributedFile *EclAgent::resolveLFN(const char *fname, const char *errorTxt, bool optional, bool noteRead, bool isWrite, StringBuffer * expandedlfn)
{
    StringBuffer lfn;
    expandLogicalFilename(lfn, fname, queryWorkUnit(), resolveFilesLocally);
    if (resolveFilesLocally && *fname != '~')
    {
        StringBuffer name;
        if (strstr(lfn,"::"))
        {
            bool wasDFS;
            makeSinglePhysicalPartName(lfn, name, true, wasDFS);
        }
        else
        {
            makeAbsolutePath(lfn.str(), name);  
        }
        lfn.clear().append(name);  
    }
    if (expandedlfn)
        *expandedlfn = lfn;
    Owned<ILocalOrDistributedFile> ldFile = createLocalOrDistributedFile(lfn.str(), queryUserDescriptor(), resolveFilesLocally, !resolveFilesLocally, isWrite);
    if (ldFile)
    {
        IDistributedFile * dFile = ldFile->queryDistributedFile();
        if (dFile)
        {
            IDistributedSuperFile *super = dFile->querySuperFile();
            if (super && (0 == super->numSubFiles(true)))
            {
                if (optional) return NULL;
                if (!errorTxt) return NULL;
                StringBuffer errorMsg(errorTxt);
                throw MakeStringException(0, "%s", errorMsg.append(": Superkey '").append(lfn).append("' is empty").str());
            }

            if (noteRead)
            {
                WorkunitUpdate wu = updateWorkUnit();
                wu->noteFileRead(dFile);
            }
        }
    }
    if (!ldFile)
    {
        if (optional) return NULL;
        if (!errorTxt) return NULL;
        StringBuffer errorMsg(errorTxt);
        throw MakeStringException(0, "%s", errorMsg.append(": Logical file name '").append(lfn).append("' could not be resolved").str());
    }
    return ldFile.getClear();
}

static void getFileSize(IFile * file, unsigned __int64 & gotSize)
{
    gotSize = file->size();
}

static unsigned __int64 getFileSize(IFile * file)
{
    unsigned __int64 size;
    try
    {
        getFileSize(file, size);
    }
    catch (IException * e)
    {
        e->Release();
        size = (unsigned __int64)-1;
    }
    return size;
}

bool EclAgent::fileExists(const char *name)
{
    unsigned __int64 size = 0;
    StringBuffer lfn;
    expandLogicalName(lfn, name);

    Owned<IDistributedFile> f = queryDistributedFileDirectory().lookup(lfn.str(),queryUserDescriptor());
    if (f) 
        return true;
    return false;
}

void EclAgent::deleteFile(const char * logicalName)
{
    queryDistributedFileDirectory().removeEntry(logicalName, queryUserDescriptor());
}

char * EclAgent::getExpandLogicalName(const char * logicalName)
{
    StringBuffer lfn;
    expandLogicalName(lfn, logicalName);
    return lfn.detach();
}

void EclAgent::addWuException(const char * text, unsigned code, unsigned severity)
{
    addException((WUExceptionSeverity)severity, "user", code, text, NULL, 0, 0, false, false);
}

void EclAgent::addWuException(const char * text, unsigned code, unsigned severity, char const * source)
{
    addException((WUExceptionSeverity)severity, source, code, text, NULL, 0, 0, false, false);
}

void EclAgent::addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort)
{
    addException(ExceptionSeverityError, "user", code, text, filename, lineno, column, false, false);
    if (isAbort)
        rtlFailOnAssert();      // minimal implementation
}

IUserDescriptor *EclAgent::queryUserDescriptor()
{
    if (isRemoteWorkunit)
        return wuRead->queryUserDescriptor();
    else
        return NULL;
}

void EclAgent::fail(int code, const char * str)
{
    DBGLOG("EclAgent::fail %d: %s", code, str);
    throw MakeStringException(code, "%s", str);
}

void EclAgent::failv(int code, const char * fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    StringBuffer msg;
    msg.valist_appendf(fmt, args);
    va_end(args);
    va_start(args, fmt);
    IException * e = MakeStringExceptionVA(code, fmt, args);
    va_end(args);
    DBGLOG("EclAgent::fail %d: %s", code, msg.str());
    throw e;
}

int EclAgent::queryLastFailCode()
{
    if(!workflow)
        return 0; //not sure how this could happen
    return workflow->queryLastFailCode();
}

void EclAgent::getLastFailMessage(size32_t & outLen, char * & outStr, const char * tag)
{
    const char * text = "";
    if(workflow)
        text = workflow->queryLastFailMessage();
    rtlExceptionExtract(outLen, outStr, text, tag);
}

void EclAgent::getEventName(size32_t & outLen, char * & outStr)
{
    const char * text = "";
    if(workflow)
        text = workflow->queryEventName();
    rtlExtractTag(outLen, outStr, text, NULL, "Event");
}

void EclAgent::getEventExtra(size32_t & outLen, char * & outStr, const char * tag)
{
    const char * text = "";
    if(workflow)
        text = workflow->queryEventExtra();
    rtlExtractTag(outLen, outStr, text, tag, "Event");
}

char *EclAgent::getPlatform()
{
    if (!isStandAloneExe)
    {
        const char * cluster = clusterNames.tos();
        Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster);
        if (!clusterInfo)
            throw MakeStringException(-1, "Unknown Cluster '%s'", cluster);
        return strdup(clusterTypeString(clusterInfo->getPlatform(), false));
    }
    else
        return strdup("standalone");
}

char *EclAgent::getEnv(const char *name, const char *defaultValue) const 
{
    const char *val = globals->queryProp(name);
    if (!val)
        val = getenv(name);
    if (val)
        return strdup(val);
    else if (defaultValue)
        return strdup(defaultValue);
    else
        return strdup("");
}

void EclAgent::selectCluster(const char * cluster)
{
    SCMStringBuffer clusterName;
    queryWorkUnit()->getClusterName(clusterName);
    clusterNames.append(clusterName.str());
    WorkunitUpdate wu = updateWorkUnit();
    wu->setClusterName(cluster);
    clusterWidth = -1;
}

void EclAgent::restoreCluster()
{
    WorkunitUpdate wu = updateWorkUnit();
    wu->setClusterName(clusterNames.item(clusterNames.length()-1));
    clusterNames.pop();
    clusterWidth = -1;
}

unsigned EclAgent::getNodes()//retrieve node count for current cluster
{
    if (clusterWidth == -1)
    {
        if (!isStandAloneExe)
        {
            const char * cluster = clusterNames.tos();
            Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster);
            if (!clusterInfo) 
                throw MakeStringException(-1, "Unknown cluster '%s'", cluster);
            clusterWidth = clusterInfo->getSize();
            assertex(clusterWidth != 0);
        }
        else
            clusterWidth = 1;
    }
    return clusterWidth;
}

void EclAgent::setBlocked()
{
    WorkunitUpdate w = updateWorkUnit();
    w->setState(WUStateBlocked);
}

void EclAgent::setRunning()
{
    WorkunitUpdate w = updateWorkUnit();
    w->setState(WUStateRunning);
}

void EclAgent::setDebugPaused()
{
    WorkunitUpdate w = updateWorkUnit();
    w->setState(WUStateDebugPaused);
}

void EclAgent::setDebugRunning()
{
    WorkunitUpdate w = updateWorkUnit();
    w->setState(WUStateDebugRunning);
}


IHThorGraphResults * EclAgent::createGraphLoopResults()
{
    return new GraphResults;
}

//---------------------------------------------------------------------------

void addException(IWorkUnit *w, WUExceptionSeverity severity, const char * source, unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool failOnError)
{
    PrintLog("%s", text);
    if ((severity == ExceptionSeverityError) && (w->getState()!=WUStateAborting) && failOnError)
        w->setState(WUStateFailed);
    addExceptionToWorkunit(w, severity, source, code, text, filename, lineno, column);
}


EclAgentQueryLibrary * EclAgent::queryEclLibrary(const char * libraryName, unsigned expectedInterfaceHash)
{
    ForEachItemIn(i, queryLibraries)
    {
        EclAgentQueryLibrary & cur = queryLibraries.item(i);
        if (stricmp(libraryName, cur.name) == 0)
        {
            //MORE: Check interface crc is consistent
            return &cur;
        }
    }
    return NULL;
}

EclAgentQueryLibrary * EclAgent::loadEclLibrary(const char * libraryName, unsigned expectedInterfaceHash, bool embedded)
{
    Linked<EclAgentQueryLibrary> library = queryEclLibrary(libraryName, expectedInterfaceHash);
    if (!library)
    {
        const char * graphName = "graph1";
        if (embedded)
        {
            library.setown(new EclAgentQueryLibrary);
            library->wu.set(queryWorkUnit());
            library->dll.set(dll);
            library->name.set(libraryName);
            queryLibraries.append(*LINK(library));

            graphName = libraryName;
        }
        else
        {
            Owned<IConstWorkUnit> libraryWu = resolveLibrary(libraryName, expectedInterfaceHash);
            library.set(loadQueryLibrary(libraryName, libraryWu));
        }

        library->graph.setown(loadGraph(graphName, library->wu, library->dll, true));
    }
    return library;
}


IConstWorkUnit * EclAgent::resolveLibrary(const char * libraryName, unsigned expectedInterfaceHash)
{
    StringAttr cluster;
    queryWorkUnit()->getClusterName(StringAttrAdaptor(cluster));
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(cluster, false);
    Owned<IPropertyTree> resolved = queryRegistry ? resolveQueryAlias(queryRegistry, libraryName) : NULL;
    if (!resolved)
        throw MakeStringException(0, "No current implementation of library %s", libraryName);
    
    const char * libraryWuid = resolved->queryProp("@wuid");

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> wu = factory->openWorkUnit(libraryWuid, false);
    if (!wu)
        throw MakeStringException(0, "Could not open workunit %s implementing library %s", libraryWuid, libraryName);

    unsigned interfaceHash = wu->getApplicationValueInt("LibraryModule", "interfaceHash", 0);
    if (interfaceHash != expectedInterfaceHash)
        throw MakeStringException(0, "Library Query %s[%s] did not match expected interface (%u,%u)", libraryName, libraryWuid, expectedInterfaceHash, interfaceHash);
    return wu.getClear();
}


EclAgentQueryLibrary * EclAgent::loadQueryLibrary(const char * name, IConstWorkUnit * wu)
{
    Owned<EclAgentQueryLibrary> entry = new EclAgentQueryLibrary;
    loadDependencies(wu);
    entry->wu.set(wu);
    entry->dll.setown(loadWorkUnitDll(wu));
    entry->name.set(name);
    //more load graph information

    queryLibraries.append(*LINK(entry));
    return entry;
}

void EclAgent::loadDependencies(IConstWorkUnit * wu)
{
#define ECLAGENT_FILE_OPEN_FAIL 100
#define ECLAGENT_MISMATCH       101

    Owned<IConstWULibraryIterator> libraries = &wu->getLibraries();
    StringBuffer pluginsList, pluginDirectory;
    agentTopology->getProp("@pluginDirectory", pluginDirectory);
    if (pluginDirectory.length())
        addPathSepChar(pluginDirectory);

    Owned<IConstWUPluginIterator> plugins = &wu->getPlugins();
    for (plugins->first();plugins->isValid();plugins->next())
    {
        IConstWUPlugin &thisplugin = plugins->query();
        SCMStringBuffer name, version;
        thisplugin.getPluginName(name);
        thisplugin.getPluginVersion(version);

        StringBuffer plugIn;
        plugIn.append(pluginDirectory).append(name);
        if (pluginsList.length())
            pluginsList.append(ENVSEPCHAR);
        pluginsList.append(plugIn);
    }

    if (pluginsList.length())
    {
        pluginMap = new SafePluginMap(&PluginCtx, traceLevel > 0);
        pluginMap->loadFromList(pluginsList.str());
    }
}

//----------------------------------------------------------------

ILoadedDllEntry * EclAgent::loadWorkUnitDll(IConstWorkUnit * wu)
{
    Owned<IConstWUQuery> q = wu->getQuery();
    SCMStringBuffer dllname;
    q->getQueryDllName(dllname);

    bool isLibrary = wu->getApplicationValueInt("LibraryModule", "major", -1) != -1;
    bool forceLocal = wu->getIsQueryService() || isLibrary; // more don't do if sandboxed library
    if (isStandAloneExe)
        return createExeDllEntry(dllname.str());
    return queryDllServer().loadDll(dllname.str(), forceLocal ? DllLocationLocal : DllLocationAnywhere);
}

IEclProcess *EclAgent::loadProcess()
{
    loadDependencies(queryWorkUnit());
    if (!isStandAloneExe)
    {
        dll.setown(loadWorkUnitDll(queryWorkUnit()));
    }
    else
    {
        dll.setown(createExeDllEntry("StandAloneHThor")); // meaning current executable (should we use argv[0] for name?)
    }


    EclProcessFactory factory = (EclProcessFactory) dll->getEntry("createProcess");
    if (!factory)
    {
        SCMStringBuffer dllname;
        Owned<IConstWUQuery> q = queryWorkUnit()->getQuery();
        q->getQueryDllName(dllname);
        failv(0, "createProcess() entrypoint not found in %s", dllname.str());
    }
    return factory();
}

void EclAgent::doProcess()
{
#ifdef _DEBUG
    PrintLog ("Entering doProcess ()");
#endif
    bool failed = true;
    try
    {
        LOG(MCrunlock, unknownJob, "Waiting for workunit lock");
        {
            WorkunitUpdate w = updateWorkUnit();
            LOG(MCrunlock, unknownJob, "Obtained workunit lock");
            if (w->hasDebugValue("traceLevel"))
                traceLevel = w->getDebugValueInt("traceLevel", 10);
            w->setTracingValue("EclAgentBuild", BUILD_TAG);
            if (agentTopology->hasProp("@name"))
                w->addProcess("EclAgent", agentTopology->queryProp("@name"), logname.str());
            if (checkVersion && ((w->getCodeVersion() > ACTIVITY_INTERFACE_VERSION) || (w->getCodeVersion() < MIN_ACTIVITY_INTERFACE_VERSION)))
                failv(0, "Workunit was compiled for eclagent interface version %d, this eclagent requires version %d..%d", w->getCodeVersion(), MIN_ACTIVITY_INTERFACE_VERSION, ACTIVITY_INTERFACE_VERSION);
            if(noRetry && (w->getState() == WUStateFailed))
                throw MakeStringException(0, "Ecl agent started in 'no retry' mode for failed workunit, so failing");
            w->setState(WUStateRunning);
            w->addTimeStamp("EclAgent", GetCachedHostName(), "Started");
            w->setAgentPID(GetCurrentProcessId());
            if (isRemoteWorkunit)
            {
                w->setAgentSession(myProcessSession());
                w->clearGraphProgress();
            }
            if (debugContext)   
            {
                w->setDebugAgentListenerPort(debugContext->queryPort());

                StringBuffer sb;
                queryHostIP().getIpText(sb);
                w->setDebugAgentListenerIP(sb);
            }
            if(resetWorkflow)
                w->resetWorkflow();
        }
        {
            MTIME_SECTION(timer, "Process");
            Owned<IEclProcess> process = loadProcess();
            PrintLog("Starting process");
            runProcess(process);
            failed = false;
        }
    }
    catch (WorkflowException * e)
    {
        if (debugContext)
            debugContext->checkBreakpoint(DebugStateException, NULL, static_cast<IException *>(e));
        logException(e);
        e->Release();
    }
    catch (IException * e)
    {
        if (debugContext)
            debugContext->checkBreakpoint(DebugStateException, NULL, e);
        logException(e);
        e->Release();
    }
    catch (std::exception & e)
    {
        if (debugContext)
            debugContext->checkBreakpoint(DebugStateException, NULL, NULL);
        logException(e);
    }
    catch (RELEASE_CATCH_ALL)
    {
        if (debugContext)
            debugContext->checkBreakpoint(DebugStateFailed, NULL, NULL);
        logException((IException *) NULL);
    }

    PrintLog("Process complete");
    // Add some timing stats
    bool deleteJobTemps = true;
    try
    {
        WorkunitUpdate w = updateWorkUnit();

        w->addTimeStamp("EclAgent", GetCachedHostName(), "Finished");
        addTimings();

        switch (w->getState())
        {
        case WUStateFailed:
        case WUStateAborted:
        case WUStateCompleted:
        {
            WUAction action = w->getAction();
            switch (action)
            {
                case WUActionPause:
                case WUActionPauseNow:
                case WUActionResume:
                    w->setAction(WUActionUnknown);
            }
            break;
        }
        case WUStateAborting:
            w->setState(WUStateAborted);
            break;
        default:
            if(failed)
                w->setState(WUStateFailed);
            if(workflow && workflow->hasItemsWaiting())
            {
                w->setState(WUStateWait);
                deleteJobTemps = false;
            }
            else
                w->setState(WUStateCompleted);
            break;
        }

        if(w->queryEventScheduledCount() > 0)
            switch(w->getState())
            {
            case WUStateFailed:
            case WUStateCompleted:
                try
                {
                    w->deschedule();
                }
                catch(IException * e)
                {
                    int code = e->errorCode();
                    SCMStringBuffer wuid;
                    queryWorkUnit()->getWuid(wuid);
                    StringBuffer msg;
                    msg.append("Failed to deschedule workunit ").append(wuid.str()).append(": ");
                    e->errorMessage(msg);
                    logException(ExceptionSeverityWarning, code, msg.str(), false);
                    e->Release();
                    WARNLOG("%s (%d)", msg.str(), code);
                }
            }

        while (clusterNames.ordinality())
            restoreCluster();
        if (!queryResolveFilesLocally())
        {
            w->deleteTempFiles(NULL, false, deleteJobTemps);
            if (deleteJobTemps)
                w->deleteTemporaries();
        }

        wuRead.clear(); // have a write lock still, but don't want to leave dangling unlocked wuRead after releasing write lock
                        // or else something can delete whilst still referenced (e.g. on complete signal)
        w.clear();
    }
    catch (IException *e)
    {
        logException(e);
        e->Release();
    }
    catch (std::exception & e)
    {
        logException(e);
    }
    catch (RELEASE_CATCH_ALL)
    {
        logException((IException *) NULL);
    }
    try {
        unlockWorkUnit(); 
    }
    catch (IException *e)
    {
        try
        {
            // a final attempt to commit the original error (only) to the workunit
            // since unlockWorkUnit( which commits ) can error due to the nature of the transaction.
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
            Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid.get(), false);
            Owned<IWorkUnit> w = &wu->lock();
            StringBuffer m("System error ");
            m.append(e->errorCode()).append(": ");
            e->errorMessage(m);
            ::addException(w, ExceptionSeverityError, "eclagent", e->errorCode(), m.str(), NULL, 0, 0, true);
        }
        catch (IException *e2)
        {
            EXCLOG(e, "Failed to report exception to workunit");
            EXCLOG(e2, NULL);
            e2->Release();
        }
        e->Release();
    }
    PrintLog("Workunit written complete");
}

void EclAgent::runProcess(IEclProcess *process)
{
    assertex(rowManager==NULL);
    allocatorMetaCache.setown(createRowAllocatorCache(this));
    rowManager.setown(roxiemem::createRowManager(0, NULL, queryDummyContextLogger(), allocatorMetaCache, false));
    setHThorRowManager(rowManager.get());

    //Get memory limit. Workunit specified value takes precedence over config file
    int memLimitMB = agentTopology->getPropInt("@defaultMemoryLimitMB", DEFAULT_MEM_LIMIT);
    memLimitMB = globals->getPropInt("defaultMemoryLimitMB", memLimitMB);
    memLimitMB = queryWorkUnit()->getDebugValueInt("hthorMemoryLimit", memLimitMB);

    bool allowHugePages = agentTopology->getPropBool("@heapUseHugePages", false);
    allowHugePages = globals->getPropBool("heapUseHugePages", allowHugePages);

#ifndef __64BIT__
    if (memLimitMB > 4096)
    {
        StringBuffer errmsg;
        errmsg.append("EclAgent (32 bit) specified memory limit (").append(memLimitMB).append("MB) greater than allowed limit (4096MB)");
        fail(0, errmsg.str());
    }
#endif
    memsize_t memLimitBytes = (memsize_t)memLimitMB * 1024 * 1024;
    roxiemem::setTotalMemoryLimit(allowHugePages, memLimitBytes, 0, NULL);

    if (debugContext)
        debugContext->checkBreakpoint(DebugStateReady, NULL, NULL);

    if(queryWorkUnit()->hasWorkflow())
    {
        workflow.setown(new EclAgentWorkflowMachine(*this));
        workflow->perform(this, process);
    }
    else
        process->perform(this, 0);

    ForEachItemIn(i, queryLibraries)
        queryLibraries.item(i).updateProgress();

    if (rowManager)
        rowManager->getMemoryUsage();//Causes statistics to be written to logfile

#ifdef _DEBUG_LEAKS
    rowManager.clear();//Early release of rowManager, so activity IDs of leaked blocks are available
#endif
    allocatorMetaCache.clear(); //release meta before libraries unloaded
    queryLibraries.kill();

    if (debugContext)
    {
        debugContext->checkBreakpoint(DebugStateFinished, NULL, NULL);
        debugContext->debugTerminate();
    }
    LOG(MCrunlock, unknownJob, "Releasing persist read locks");
    persistReadLocks.kill();
    LOG(MCrunlock, unknownJob, "Released persist read locks");
}

unsigned EclAgent::getWorkflowId()
{
    return workflow->queryCurrentWfid(); 
}

//----------------------------------------------------------------

EclAgentWorkflowMachine::EclAgentWorkflowMachine(EclAgent &_agent)
    : WorkflowMachine(), agent(_agent), persistsPrelocked(false)
{
    wfconn.setown(getWorkflowScheduleConnection(agent.wuid.get()));
}

void EclAgentWorkflowMachine::begin()
{
    if (agent.needToLockWorkunit())
        obtainRunlock();
    workflow.setown(agent.queryWorkUnit()->getWorkflowClone());
    if(agent.queryWorkUnit()->getDebugValueBool("prelockpersists", false))
        prelockPersists();
}

int strptrcmp(char const ** l, char const ** r) { return strcmp(*l, *r); }

void EclAgentWorkflowMachine::prelockPersists()
{
    unsigned count = workflow->count();
    StringArray names;
    for(unsigned wfid=1; wfid<=count; wfid++)
    {
        IRuntimeWorkflowItem & item = workflow->queryWfid(wfid);
        if(item.queryMode() == WFModePersist)
        {
            SCMStringBuffer name;
            item.getPersistName(name);
            names.append(name.str());
        }
    }
    names.sort(strptrcmp);
    ForEachItemIn(idx, names)
    {
        char const * name = names.item(idx);
        agent.startPersist(name);
        agent.cachePersist(name);
    }
    persistsPrelocked = true;
}

void EclAgentWorkflowMachine::end()
{
    syncWorkflow();
    workflow.clear();
    releaseRunlock();
}

void EclAgentWorkflowMachine::schedulingStart()
{
    wfconn->lock();
    wfconn->setActive();
    wfconn->pull(workflow);
    wfconn->unlock();
}

bool EclAgentWorkflowMachine::schedulingPull()
{
    wfconn->lock();
    bool more = wfconn->pull(workflow);
    wfconn->unlock();
    return more;
}

bool EclAgentWorkflowMachine::schedulingPullStop()
{
    wfconn->lock();
    bool more = wfconn->pull(workflow);
    if(!more) wfconn->resetActive();
    wfconn->unlock();
    return more;
}

void EclAgentWorkflowMachine::obtainRunlock()
{
    StringBuffer xpath;
    xpath.append("/WorkUnitRunLocks/").append(agent.wuid.get());
    LOG(MCrunlock, unknownJob, "Waiting for run lock");
    runlock.setown(querySDS().connect(xpath.str(), myProcessSession(), RTM_CREATE | RTM_LOCK_WRITE | RTM_DELETE_ON_DISCONNECT, INFINITE));
    LOG(MCrunlock, unknownJob, "Obtained run lock");
    if(!runlock)
        agent.fail(0, "EclAgent could not get a lock to run the workunit");
}

void EclAgentWorkflowMachine::releaseRunlock()
{
    LOG(MCrunlock, unknownJob, "Releasing run lock");
    if(runlock && queryDaliServerVersion().compare("1.3") < 0)
        runlock->close(true);
    runlock.clear();
}

void EclAgentWorkflowMachine::syncWorkflow()
{
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Updating workunit's workflow data");
#endif
    WorkunitUpdate w = agent.updateWorkUnit();
    w->syncRuntimeWorkflow(workflow);
}

void EclAgentWorkflowMachine::reportContingencyFailure(char const * type, IException * e)
{
    StringBuffer msg;
    msg.append(type).append(" clause failed (execution will continue): ").append(e->errorCode()).append(": ");
    e->errorMessage(msg);
    agent.logException(ExceptionSeverityWarning, e->errorCode(), msg.str(), false);
}

void EclAgentWorkflowMachine::checkForAbort(unsigned wfid, IException * handling)
{
    if(agent.queryWorkUnit()->aborting())
    {
        if(handling)
        {
            StringBuffer msg;
            msg.append("Abort takes precedence over error: ").append(handling->errorCode()).append(": ");
            handling->errorMessage(msg);
            msg.append(" (in item ").append(wfid).append(")");
            agent.logException(ExceptionSeverityWarning, handling->errorCode(), msg.str(), false);
            handling->Release();
        }
        throw new WorkflowException(0, "Workunit abort request received", wfid, WorkflowException::ABORT, MSGAUD_user);
    }
}

void EclAgentWorkflowMachine::doExecutePersistItem(IRuntimeWorkflowItem & item)
{
    if (agent.isStandAloneExe)
    {
        throw MakeStringException(0, "PERSIST not supported when running standalone");
    }
    unsigned wfid = item.queryWfid();
    doExecuteItemDependencies(item, wfid);
    SCMStringBuffer name;
    item.getPersistName(name);
    if(persistsPrelocked)
        agent.decachePersist(name.str());
    else
        agent.startPersist(name.str());
    doExecuteItemDependency(item, item.queryPersistWfid(), wfid);
    if(!persist)
    {
        StringBuffer errmsg;
        errmsg.append("Internal error in generated code: for wfid ").append(wfid).append(", persist CRC wfid ").append(item.queryPersistWfid()).append(" did not call returnPersistVersion");
        throw MakeStringException(0, "%s", errmsg.str());
    }
    if(strcmp(name.str(), persist->logicalName.get()) != 0)
    {
        StringBuffer errmsg;
        errmsg.append("Failed workflow/persist consistency check: wfid ").append(wfid).append(", WU persist name ").append(name.str()).append(", runtime persist name ").append(persist->logicalName.get());
        throw MakeStringException(0, "%s", errmsg.str());
    }
    if(agent.arePersistsFrozen())
    {
        agent.checkPersistMatches(name.str(), persist->eclCRC);
    }
    else if(!agent.isPersistUptoDate(name.str(), persist->eclCRC, persist->allCRC, persist->isFile))
    {
        agent.clearPersist(name.str());
        doExecuteItem(item, wfid);
        agent.updatePersist(name.str(), persist->eclCRC, persist->allCRC);
    }
    persist.clear();
    agent.finishPersist();
}

//----------------------------------------------------------------

void EclAgent::doNotify(char const * name, char const * text)
{
    Owned<IScheduleEventPusher> pusher(getScheduleEventPusher());
    pusher->push(name, text, NULL);
}

void EclAgent::doNotify(char const * name, char const * text, const char * target)
{
    Owned<IScheduleEventPusher> pusher(getScheduleEventPusher());
    pusher->push(name, text, target);
}

void EclAgent::logException(WUExceptionSeverity severity, unsigned code, const char * text, bool isAbort)
{
    addException(severity, "eclagent", code, text, NULL, 0, 0, true, isAbort);
    if (severity == ExceptionSeverityError)
        ERRLOG(code, "%s", text);
}

void EclAgent::addException(WUExceptionSeverity severity, const char * source, unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool failOnError, bool isAbort)
{
    if (writeResultsToStdout && (severity != ExceptionSeverityInformation))
    {
        StringBuffer location;
        if (filename)
        {
            location.append(filename);
            if (lineno)
                location.append('(').append(lineno).append(")");
            location.append(": ");
        }
        const char * kind = (severity == ExceptionSeverityError) ? "error" : "warning";
        fprintf(stderr, "%s%s: C%04u %s\n", location.str(), kind, code, text);
    }
    try
    {
        WorkunitUpdate w = updateWorkUnit();
        if(isAbort)
            w->setState(WUStateAborting);
        ::addException(w, severity, source, code, text, filename, lineno, column, failOnError);
    }
    catch (IException *E)
    {
        StringBuffer m;
        E->errorMessage(m);
        PrintLog("Unable to record exception in workunit: %s", m.str());
        E->Release();
    }
    catch (std::bad_alloc &)
    {
        PrintLog("Unable to record exception in workunit: out of memory (std::bad_alloc)");
    }
    catch (std::exception & e)
    {
        PrintLog("Unable to record exception in workunit: standard library exception (std::exception %s)", e.what());
    }
    catch (...)
    {
        PrintLog("Unable to record exception in workunit: unknown exception");
    }
}

void EclAgent::logException(WorkflowException *e)
{
    StringBuffer m;
    unsigned code = 0;
    bool isAbort = false;
    if(e)
    {
        switch(e->queryType())
        {
        case WorkflowException::USER:
            m.append("Error: ");
            break;
        case WorkflowException::SYSTEM:
            m.append("System error: ");
            break;
        case WorkflowException::ABORT:
            m.append("Abort: ");
            isAbort = true;
            break;
        default:
            throwUnexpected();
        }
        m.append(e->errorCode()).append(": ");
        e->errorMessage(m);
        code = e->errorCode();
    }
    else    
        m.append("Unknown error");

    logException(ExceptionSeverityError, code, m.str(), isAbort);
}

void EclAgent::logException(IException *e)
{
    StringBuffer m;
    unsigned code = 0;
    if (e)
    {
        if(dynamic_cast<IUserException *>(e))
            m.append("Error: ");
        else
            m.append("System error: ");
        m.append(e->errorCode()).append(": ");
        e->errorMessage(m);
        code = e->errorCode();
    }
    else    
        m.append("Unknown error");

    logException(ExceptionSeverityError, code, m.str(), false);
}

void EclAgent::logException(std::exception & e)
{
    StringBuffer m;
    if(dynamic_cast<std::bad_alloc *>(&e))
        m.append("out of memory (std::bad_alloc)");
    else
        m.append("standard library exception (std::exception ").append(e.what()).append(")");
    logException(ExceptionSeverityError, 0, m.str(), false);
}

static unsigned __int64 crcLogicalFileTime(IDistributedFile * file, unsigned __int64 crc, const char * filename)
{
    CDateTime dt;
    StringBuffer dtstr;
    file->getModificationTime(dt);
    unsigned __int64 modifiedTime = dt.getSimple();
    PrintLog("getDatasetHash adding crc %"I64F"u for file %s", modifiedTime, filename);
    return rtlHash64Data(sizeof(modifiedTime), &modifiedTime, crc);
}

unsigned __int64 EclAgent::getDatasetHash(const char * logicalName, unsigned __int64 crc)
{
    PrintLog("getDatasetHash initial crc %"I64F"x", crc);

    StringBuffer fullname;
    expandLogicalName(fullname, logicalName);

    if (resolveFilesLocally)
    {
        //MORE: This needs to correctly resolve the file.
        Owned<IFile> file = createIFile(fullname);
        CDateTime modified;
        file->getTime(NULL, &modified, NULL);
        unsigned __int64 pseudoCrc = (unsigned __int64)modified.getSimple();
        if (pseudoCrc > crc)
            crc = pseudoCrc;
        return crc;
    }

    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(fullname.str(),queryUserDescriptor());
    if (file)
    {
        WorkunitUpdate wu = updateWorkUnit();
        wu->noteFileRead(file);
        IDistributedSuperFile * super = file->querySuperFile();
        if (super)
        {
            Owned<IDistributedFileIterator> iter = super->getSubFileIterator(true);
            ForEach(*iter)
            {
                IDistributedFile & cur = iter->query();
                const char * name = cur.queryLogicalName();
                crc = rtlHash64Data(strlen(name), name, crc);
                crc = crcLogicalFileTime(&cur, crc, name);
            }
        }
        else
            crc = crcLogicalFileTime(file, crc, fullname.str());
    }
    else
        PrintLog("getDatasetHash did not find file %s", fullname.str());

    PrintLog("getDatasetHash final crc %"I64F"x", crc);
    return crc;
}

//---------------------------------------------------------------------------

bool EclAgent::checkPersistUptoDate(const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile, StringBuffer &errText)
{
    StringBuffer lfn, crcName, eclName;
    expandLogicalName(lfn, logicalName);
    crcName.append(lfn).append("$crc");
    eclName.append(lfn).append("$eclcrc");

    if (!isResult(lfn, (unsigned)-2))
        errText.appendf("Building PERSIST('%s'): It hasn't been calculated before", logicalName);
    else if (!isResult(crcName, (unsigned)-2))
        errText.appendf("Rebuilding PERSIST('%s'): Saved CRC isn't present", logicalName);
    else if (isFile && !fileExists(logicalName))
        errText.appendf("Rebuilding PERSIST('%s'): Persistent file does not exist", logicalName);
    else
    {
        unsigned savedEclCRC = (unsigned)getResultInt(eclName, (unsigned)-2);
        unsigned __int64 savedCRC = (unsigned __int64)getResultInt(crcName, (unsigned)-2);
        if (savedEclCRC != eclCRC)
            errText.appendf("Rebuilding PERSIST('%s'): ECL has changed", logicalName);
        else if (savedCRC != allCRC)
            errText.appendf("Rebuilding PERSIST('%s'): Input files have changed", logicalName);
        else
            return true;
    }

    return false;
}

bool EclAgent::changePersistLockMode(unsigned mode, const char * name, bool repeat)
{
    LOG(MCrunlock, unknownJob, "Waiting to change persist lock to %s for %s", (mode == RTM_LOCK_WRITE) ? "write" : "read", name);
    loop
    {
        try
        {
            persistLock->changeMode(mode, PERSIST_LOCK_TIMEOUT);
            reportProgress("Changed persist lock");
            return true;
        }
        catch(ISDSException *E)
        {
            if (SDSExcpt_LockTimeout != E->errorCode())
                throw E;
            E->Release();
        }
        if (!repeat)
        {
            reportProgress("Failed to convert persist lock"); // gives a chance to abort
            return false;
        }
        reportProgress("Waiting to convert persist lock"); // gives a chance to abort
    }
}

void EclAgent::getPersistReadLock(const char * logicalName)
{
    StringBuffer lfn;
    expandLogicalName(lfn, logicalName);
    if (!lfn.length())
        throw MakeStringException(0, "Invalid persist name used : '%s'", logicalName);

    const char * name = lfn;

    StringBuffer xpath;
    xpath.append("/PersistRunLocks/");
    if (isdigit(*name))
        xpath.append("_");
    for (const char * cur = name;*cur;cur++)
        xpath.append(isalnum(*cur) ? *cur : '_');

    LOG(MCrunlock, unknownJob, "Waiting for persist read lock for %s", name);
    loop
    {
        try
        {
            unsigned mode = RTM_CREATE_QUERY | RTM_LOCK_READ;
            if (queryDaliServerVersion().compare("1.4") >= 0)
                mode |= RTM_DELETE_ON_DISCONNECT;
            persistLock.setown(querySDS().connect(xpath.str(), myProcessSession(), mode, PERSIST_LOCK_TIMEOUT));
        }
        catch(ISDSException *E)
        {
            if (SDSExcpt_LockTimeout != E->errorCode())
                throw E;
            E->Release();
        }
        if (persistLock)
            break;
        reportProgress("Waiting for persist read lock"); // gives a chance to abort
    }

    reportProgress("Obtained persist read lock");
}

void EclAgent::setBlockedOnPersist(const char * logicalName)
{
    StringBuffer s;
    s.append("Waiting for persist ").append(logicalName);
    WorkunitUpdate w = updateWorkUnit();
    w->setState(WUStateBlocked);
    w->setStateEx(s.str());
}

bool EclAgent::isPersistUptoDate(const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile)
{
    //Loop trying to get a write lock - if it fails, then release the read lock, otherwise
    //you can get a deadlock with several things waiting to read, and none being able to write.
    loop
    {
        StringBuffer dummy;
        if (checkPersistUptoDate(logicalName, eclCRC, allCRC, isFile, dummy))
        {
            StringBuffer msg;
            msg.append("PERSIST('").append(logicalName).append("') is up to date");
            logException(ExceptionSeverityInformation, 0, msg.str(), false);
            return true;
        }

        //Get a write lock
        setBlockedOnPersist(logicalName);
        unlockWorkUnit();
        if (changePersistLockMode(RTM_LOCK_WRITE, logicalName, false))
            break;

        //failed to get a write lock, so release our read lock
        persistLock.clear();
        MilliSleep(getRandom()%2000);
        getPersistReadLock(logicalName);
    }
    setRunning();

    //Check again whether up to date, someone else might have updated it!
    StringBuffer errText;
    if (checkPersistUptoDate(logicalName, eclCRC, allCRC, isFile, errText))
    {
        StringBuffer msg;
        msg.append("PERSIST('").append(logicalName).append("') is up to date (after being calculated by another job)");
        logException(ExceptionSeverityInformation, 0, msg.str(), false);
        changePersistLockMode(RTM_LOCK_READ, logicalName, true);
        return true;
    }
    if (errText.length())
        logException(ExceptionSeverityInformation, 0, errText.str(), false);
    return false;
}

void EclAgent::clearPersist(const char * logicalName)
{
    StringBuffer lfn, crcName, eclName;
    expandLogicalName(lfn, logicalName);
    crcName.append(lfn).append("$crc");
    eclName.append(lfn).append("$eclcrc");

    setResultInt(crcName,(unsigned)-2,0);
    setResultInt(eclName,(unsigned)-2,0);
    LOG(MCrunlock, unknownJob, "Recalculate persistent value %s", logicalName);
}

void EclAgent::updatePersist(const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC)
{
    StringBuffer lfn, crcName, eclName;
    expandLogicalName(lfn, logicalName);
    crcName.append(lfn).append("$crc");
    eclName.append(lfn).append("$eclcrc");

    setResultInt(crcName,(unsigned)-2,allCRC);
    setResultInt(eclName,(unsigned)-2,eclCRC);

    reportProgress("Convert persist write lock to read lock");
    changePersistLockMode(RTM_LOCK_READ, logicalName, true);
}

void EclAgent::startPersist(const char * logicalName)
{
    setBlockedOnPersist(logicalName);
    unlockWorkUnit();
    getPersistReadLock(logicalName);
    setRunning();
}

void EclAgent::cachePersist(const char * logicalName)
{
    persistCache.setValue(logicalName, persistLock.getClear());
    LOG(MCrunlock, unknownJob, "Cached persist read lock for %s", logicalName);
}

void EclAgent::decachePersist(const char * logicalName)
{
    persistLock.setown(persistCache.getValue(logicalName));
    persistCache.setValue(logicalName, NULL);
    LOG(MCrunlock, unknownJob, "Decached persist read lock for %s", logicalName);
}

void EclAgent::finishPersist()
{
    LOG(MCrunlock, unknownJob, "Finished persists - add to read lock list");
    persistReadLocks.append(*persistLock.getClear());
}

void EclAgent::checkPersistMatches(const char * logicalName, unsigned eclCRC)
{
    StringBuffer lfn, eclName;
    expandLogicalName(lfn, logicalName);
    eclName.append(lfn).append("$eclcrc");

    if (!isResult(lfn, (unsigned)-2))
        failv(0, "Frozen PERSIST('%s') hasn't been calculated ", logicalName);
    if (isResult(eclName, (unsigned)-2) && (getResultInt(eclName, (unsigned)-2) != eclCRC))
        failv(0, "Frozen PERSIST('%s') ECL has changed", logicalName);

    StringBuffer msg;
    msg.append("Frozen PERSIST('").append(logicalName).append("') is up to date");
    logException(ExceptionSeverityInformation, 0, msg.str(), false);
}

//---------------------------------------------------------------------------

char *EclAgent::getWuid()
{
    return strdup(wuid);
}

const char *EclAgent::queryWuid()
{
    return wuid.get();
}

char * EclAgent::getJobName()
{
    SCMStringBuffer out;
    queryWorkUnit()->getJobName(out);
    return out.s.detach();
}

char * EclAgent::getJobOwner()
{
    SCMStringBuffer out;
    queryWorkUnit()->getUser(out);
    return out.s.detach();
}

char * EclAgent::getClusterName()
{
    SCMStringBuffer out;
    queryWorkUnit()->getClusterName(out);
    return out.s.detach();
}

char * EclAgent::getGroupName()
{
    StringBuffer groupName;
    if (!isStandAloneExe)
    {
        const char * cluster = clusterNames.tos();
        Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster);
        if (!clusterInfo)
            throw MakeStringException(-1, "Unknown cluster '%s'", cluster);
        const StringArray &thors = clusterInfo->getThorProcesses();
        if (thors.length())
        {
            StringArray envClusters, envGroups, envTargets, envQueues;
            getEnvironmentThorClusterNames(envClusters, envGroups, envTargets, envQueues);
            ForEachItemIn(i, thors)
            {
                const char *thorName = thors.item(i);
                ForEachItemIn(j, envClusters)
                {
                    if (strieq(thorName, envClusters.item(j)))
                    {
                        const char *envGroup = envGroups.item(j);
                        if (groupName.length())
                        {
                            if (!strieq(groupName, envGroup))
                                throw MakeStringException(-1, "getGroupName(): ambiguous groups %s, %s", groupName.str(), envGroup);
                        }
                        else
                            groupName.append(envGroup);
                        break;
                    }
                }
            }

        }
        else
        {
            // eclagent group name not stored in cluster info so reverse lookup in dali (bit of kludge)
            SocketEndpoint ep = queryMyNode()->endpoint();
            ep.port = 0;
            Owned<IGroup> grp = createIGroup(1,&ep);
            queryNamedGroupStore().find(grp, groupName);
        }
    }
    return groupName.detach();
}

char * EclAgent::queryIndexMetaData(char const * lfn, char const * xpath)
{
    Owned<ILocalOrDistributedFile> ldFile = resolveLFN(lfn, "IndexMetaData");
    IDistributedFile * dFile = ldFile->queryDistributedFile();
    if (!dFile)
        return NULL;

    Owned<IDistributedFilePart> part = dFile->getPart(dFile->numParts()-1);
    unsigned numCopies = part->numCopies();
    Owned<IException> exc;
    Owned<IKeyIndex> key;
    for(unsigned copy=0; copy<numCopies; ++copy)
    {
        RemoteFilename rfn;
        try
        {
            OwnedIFile file = createIFile(part->getFilename(rfn, copy));
            unsigned __int64 thissize = file->size();
            if(thissize != -1)
            {
                StringBuffer remotePath;
                rfn.getRemotePath(remotePath);
                unsigned crc;
                part->getCrc(crc);
                key.setown(createKeyIndex(remotePath.str(), crc, false, false));
                break;
            }
        }
        catch(IException * e)
        {
            EXCLOG(e, "While opening index file");
            if(exc)
                e->Release();
            else
                exc.setown(e);
        }
    }

    if(!key)
    {
        if(exc)
        {
            throw exc.getClear();
        }
        else
        {
            StringBuffer url;
            RemoteFilename rfn;
            part->getFilename(rfn).getRemotePath(url);
            throw MakeStringException(1001, "Could not open key file at %s%s", url.str(), (numCopies > 1) ? " or any alternate location." : ".");
        }
    }

    Owned<IPropertyTree> metadata = key->getMetadata();
    if(!metadata)
        return NULL;
    StringBuffer out;
    if(!metadata->getProp(xpath, out))
        return NULL;
    return out.detach();
}

IConstWorkUnit *EclAgent::queryWorkUnit()
{
    return wuRead;
}

char *EclAgent::getFilePart(const char *lfn, bool create)
{
    if (create)
    {
        StringBuffer full_lfn;
        expandLogicalName(full_lfn, lfn);

        StringBuffer physical;
        makePhysicalPartName(full_lfn.str(), 1, 1, physical, false);//MORE: What do we do if local ?

        StringBuffer dir,base;
        splitFilename(physical.str(), &dir, &dir, &base, &base);

        Owned<IFileDescriptor> desc = createFileDescriptor();
        desc->setDefaultDir(dir.str());
        desc->setPart(0, queryMyNode(), base.str(), NULL);
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(desc);
        file->attach(full_lfn.str(),queryUserDescriptor());
        return physical.detach();
    }
    else
    {
        Owned<ILocalOrDistributedFile> ldFile = resolveLFN(lfn, "l2p", false, false);
        if (!ldFile)
            return NULL;
        unsigned numParts = ldFile->numParts();
        if (numParts != 1)
            failv(0, "l2p: number of parts in file %s does not match cluster size 1", lfn);
        for (unsigned copyno=0; copyno < ldFile->numPartCopies(0); copyno++)
        {
            RemoteFilename rfn;
            try
            {
                OwnedIFile file = ldFile->getPartFile(0,copyno);
                if (file->exists())
                {
                    StringBuffer p(file->queryFilename()); 
                    return p.detach();
                }
            }
            catch (IException *E)
            {
                EXCLOG(E, "While checking file part existence");
                E->Release();
            }
        }
    }

    failv(0, "l2p: no matching physical file found for file %s", lfn);
    return 0;
}


void EclAgent::reportProgress(const char *progress, unsigned flags)
{
    if (isAborting) {
        CriticalBlock block(wusect);
        WorkunitUpdate w = updateWorkUnit();
        w->setState(WUStateAborting);
        throw new WorkflowException(0, "Workunit abort request received", (workflow ? workflow->queryCurrentWfid() : 0), WorkflowException::ABORT, MSGAUD_user);
    }
    if (progress)
    {
        // MORE - think about how to best do this
        PrintLog("%s", progress);
//      WorkunitUpdate wu = updateWorkUnit();
//      wu->reportProgress(progress, flags);
    }
}

IDistributedFileTransaction *EclAgent::querySuperFileTransaction()
{
    if (!superfiletransaction.get())
        superfiletransaction.setown(createDistributedFileTransaction(queryUserDescriptor()));
    return superfiletransaction.get();
}



void EclAgent::addTimings()
{
    StringBuffer str;
    WorkunitUpdate w = updateWorkUnit();
    for (unsigned i = 0; i < timer->numSections(); i++)
    {
        timer->getSection(i, str.clear());
        w->setTimerInfo(str.str(), NULL, (unsigned)(timer->getTime(i)/1000000), timer->getCount(i), (unsigned)timer->getMaxTime(i));
    }
}

// eclagent abort monitoring
void EclAgent::abortMonitor() 
{
    StringBuffer errorText;
    unsigned guillotineleft = 0;
    loop {
        unsigned waittime = ABORT_CHECK_INTERVAL;   
        if (abortmonitor->guillotinetimeout) {
            if (guillotineleft==0) {
                guillotineleft = abortmonitor->guillotinetimeout;
                DBGLOG("Guillotine set to %ds",guillotineleft);
            }
            if (guillotineleft<waittime)
                waittime = guillotineleft;
        }
        if (abortmonitor->sem.wait(waittime*1000) && abortmonitor->stopping) {
            return;
        }
        if (guillotineleft) {
            if (abortmonitor->guillotinetimeout) {
                guillotineleft-=waittime;
                if (guillotineleft==0) {
                    DBGLOG("Guillotine triggered");
                    errorText.appendf("Workunit time limit (%d seconds) exceeded", abortmonitor->guillotinetimeout);
                    break;
                }
            }
            else
                guillotineleft = 0; // reset
        }
        {
            CriticalBlock block(wusect);
            if (queryWorkUnit()) // could be exiting
                isAborting = queryWorkUnit()->aborting();
        }
        if (isAborting) {
            DBGLOG("Abort detected");
            while (abortmonitor->sem.wait(ABORT_DEADMAN_INTERVAL*1000))
                if (abortmonitor->stopping)
                    return; // stopped in time
            ERRLOG("EclAgent failed to abort within %ds - killing process",ABORT_DEADMAN_INTERVAL);
            break;
        }
    }
    fatalAbort(isAborting,errorText.str());
}

void EclAgent::fatalAbort(bool userabort,const char *excepttext)
{
    try {
        CriticalBlock block(wusect); 
        WorkunitUpdate w = updateWorkUnit();
        if (userabort) 
            w->setState(WUStateAborted);
        if (excepttext&&*excepttext)
            addException(ExceptionSeverityError, "ECLAGENT", 1000, excepttext, NULL, 0, 0, true, false);
        w->deleteTempFiles(NULL, false, true);
        wuRead.clear(); 
        w->commit();        // needed because we can't unlock the workunit in this thread
        w.clear();
        deleteTempFiles();
    }
    catch (IException *e) {
        EXCLOG(e,"EclAgent exit");
        e->Release();
    }
    queryLogMsgManager()->flushQueue(10*1000);
#ifdef _WIN32
    TerminateProcess(GetCurrentProcess(), 1);
#else
    kill(getpid(), SIGKILL);
#endif
}

IGroup *EclAgent::getHThorGroup(StringBuffer &out)
{
    // convoluted as can be multiple eclagents with same name
    StringBuffer mygroupname("hthor__");
    agentTopology->getProp("@name",mygroupname);
    size32_t l = mygroupname.length();
    unsigned ins = 0;
    SocketEndpoint ep(0,queryMyNode()->endpoint());
    Owned<IGroup> mygrp = createIGroup(1,&ep);
    loop
    {
        Owned<IGroup> grp = queryNamedGroupStore().lookup(mygroupname.str());
        if (!grp)
            break;
        if (grp->equals(mygrp))
        {
            out.append(mygroupname);
            return grp.getClear();
        }
        ins++;
        mygroupname.setLength(l);
        mygroupname.append('_').append(ins);
    }
    // this shouldn't happen but..
    WARNLOG("Adding group %s",mygroupname.str());
    queryNamedGroupStore().add(mygroupname.str(),mygrp,true);
    out.append(mygroupname);
    return queryNamedGroupStore().lookup(mygroupname.str());
}

//======================================================================================================================

void printStart(int argc, const char *argv[])
{
    StringBuffer cmd;
    for (int argno = 0; argno < argc; argno++)
    {
        if (argno)
            cmd.append(' ');
        cmd.append('"').append(argv[argno]).append('"');
    }
    PrintLog("Starting %s", cmd.toCharArray());
}

//--------------------------------------------------------------

bool ControlHandler()
{
    LOG(MCevent,"ControlHandler Stop signalled");
    return true;
}

//--------------------------------------------------------------
#ifdef _WIN32
int myhook(int alloctype, void *, size_t nSize, int p1, long allocSeq, const unsigned char *file, int line)
{
    // Handy place to put breakpoints when tracking down obscure memory leaks...
    if (nSize==32 && !file && allocSeq==1553)
    {
        int a = 1;
    }
    return true;
}
#endif
//--------------------------------------------------------------
extern int HTHOR_API eclagent_main(int argc, const char *argv[], StringBuffer * wuXML, bool standAloneExe)
{
#ifdef _DEBUG
#ifdef _WIN32
    _CrtSetAllocHook(myhook);
#endif
#endif
    addAbortHandler(ControlHandler);
    Owned<IProperties> globals = createProperties(true); // cmdline props only
    for (int i = 1; i < argc; i++) 
        globals->loadProp(argv[i], true);

    //get logfile location from agentexec.xml config file
    if (!standAloneExe)
    {
        try
        {
            agentTopology.setown(createPTreeFromXMLFile("agentexec.xml", ipt_caseInsensitive));
        }
        catch (IException *) 
        {
            agentTopology.setown(createPTree("AGENTEXEC"));
        }
    }
    else
        agentTopology.setown(createPTree("AGENTEXEC"));

    //Build log file specification
    StringBuffer logfilespec;
    ILogMsgHandler * logMsgHandler = NULL;
    if (!standAloneExe)
    {
        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(agentTopology, "eclagent");
        lf->setMsgFields(MSGFIELD_timeDate | MSGFIELD_msgID | MSGFIELD_process | MSGFIELD_thread | MSGFIELD_code);
        lf->setCreateAliasFile(false);
        logMsgHandler = lf->beginLogging();
        PROGLOG("Logging to %s", lf->queryLogFileSpec());
        logfilespec.set(lf->queryLogFileSpec());
    }
    else
    {
        StringBuffer exeName;
        splitFilename(argv[0], NULL, NULL, &exeName, NULL);
        openLogFile(logfilespec, exeName.append(".log"));
        PROGLOG("Logging to %s", logfilespec.str());
    }

    if (wuXML && wuXML->length())
    {
        if (const char * fn = globals->queryProp("-wu"))
        {
            //Write workunit to file and exit
            if (0==strcmp(fn,"1"))
                fn = "stdout:";
            Owned<IFile> file = createIFile(fn);
            OwnedIFileIO io;
            try
            {
                io.setown(file->open(IFOcreate));
            }
            catch(IException * e)
            {
                StringBuffer sb;
                e->errorMessage(sb);
                throw MakeStringException(errno, "Failed to create WU XML file %s : %s", fn, sb.str());
            }

            Owned<IFileIOStream> out = createIOStream(io);
            
            StringBuffer str;
            str.appendf("%s\n",XMLHEADER);// "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
            out->write(str.length(), str.str());

            out->write(wuXML->length(), wuXML->str());
            return 0;
        }
    }

#ifdef _DEBUG
    traceLevel = 10;
#ifdef _WIN32
    if (globals->getPropInt("ALLOCBREAK"))
        _CrtSetBreakAlloc(globals->getPropInt("ALLOCBREAK"));
    if (globals->getPropInt("BREAKATSTART"))
        DebugBreak();
#endif
#else
    traceLevel = 0;
#endif
    traceLevel = agentTopology->getPropInt("@traceLevel", traceLevel);
    traceLevel = globals->getPropInt("TRACELEVEL", traceLevel);

    if (traceLevel)
    {
        printStart(argc, argv);
        DBGLOG("Build %s", BUILD_TAG);
    }

    // Extract any params into stored - primarily for standalone case but handy for debugging eclagent sometimes too
    Owned<IPropertyTree> query;
    try
    {
        const char *queryXML = globals->queryProp("query");
        if (queryXML)
        {
            if (queryXML[0]=='@')
                query.setown(createPTreeFromXMLFile(queryXML+1));
            else
                query.setown(createPTreeFromXMLString(queryXML));
        }
        Owned<IPropertyIterator> it = globals->getIterator();
        ForEach(*it)
        {
            const char * key = it->getPropKey();
            if (key && key[0] == '/')
            {
                if (!query)
                    query.setown(createPTree("Query"));
                const char *val = globals->queryProp(key);
                if (val[0]=='<')
                {
                    Owned<IPropertyTree> valtree = createPTreeFromXMLString(val);
                    query->setPropTree(key+1, valtree.getClear());
                }
                else 
                    query->setProp(key+1, val);
            }
        }
    }
    catch (IException *E)
    {
        StringBuffer msg;
        E->errorMessage(msg);
        E->Release();
        throw MakeStringException(0, "Invalid xml: %s", msg.str());
    }

    SCMStringBuffer wuid;
    StringBuffer daliServers;
    if (!globals->getProp("DALISERVERS", daliServers))
        daliServers.append(agentTopology->queryProp("@daliServers"));

#ifdef LEAK_FILE
    enableMemLeakChecking(true);
    logLeaks(LEAK_FILE);
#endif
    if (globals->getPropInt("DAFILESRVCACHE", 1))
        setDaliServixSocketCaching(true);

    try
    {
#ifdef MONITOR_ECLAGENT_STATUS  
        CSDSServerStatus * serverstatus = NULL;
#endif
        Owned<ILocalWorkUnit> standAloneWorkUnit;
        if (wuXML)
        {
            //Create workunit from XML
            standAloneWorkUnit.setown(createLocalWorkUnit());
            standAloneWorkUnit->loadXML(wuXML->str());
            wuXML->kill();  // free up text as soon as possible.
        }

        if (daliServers.length())
        {
            {
                MTIME_SECTION(timer, "SDS_Initialize");
                Owned<IGroup> serverGroup = createIGroup(daliServers.str(), DALI_SERVER_PORT);
                initClientProcess(serverGroup, DCR_EclAgent, 0, NULL, NULL, MP_WAIT_FOREVER);
            }
#ifdef MONITOR_ECLAGENT_STATUS  
            serverstatus = new CSDSServerStatus("ECLagent");
            serverstatus->queryProperties()->setPropInt("Pid", GetCurrentProcessId());
            serverstatus->commitProperties();
#endif

            {
                MTIME_SECTION(timer, "Environment_Initialize");
                setPasswordsFromSDS();
            }
            PrintLog("ECLAGENT build %s", BUILD_TAG);
            startLogMsgParentReceiver();    
            connectLogMsgManagerToDali();

            StringBuffer baseDir;
            if (getConfigurationDirectory(agentTopology->queryPropTree("Directories"),"data","eclagent",agentTopology->queryProp("@name"),baseDir.clear()))
                setBaseDirectory(baseDir.str(), false);
            if (getConfigurationDirectory(agentTopology->queryPropTree("Directories"),"mirror","eclagent",agentTopology->queryProp("@name"),baseDir.clear()))
                setBaseDirectory(baseDir.str(), true);

            if (agentTopology->getPropBool("@useNASTranslation", true))
                envInstallNASHooks();

            if (standAloneWorkUnit)
            {
                //Stand alone program, but dali is specified => create a workunit in dali, and store the results there....
                Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                Owned<IWorkUnit> daliWu = factory->createWorkUnit(NULL, "eclagent", "eclagent");
                IExtendedWUInterface * extendedWu = queryExtendedWU(daliWu);
                extendedWu->copyWorkUnit(standAloneWorkUnit, true);
                daliWu->getWuid(wuid);
                globals->setProp("WUID", wuid.str());
                standAloneWorkUnit.clear();
            }
        }

        if (!standAloneWorkUnit)
        {
            if (!globals->hasProp("WUID"))
                throw MakeStringException(0, "WUID not specified");
            wuid.set(globals->queryProp("WUID"));
        }
        else
        {
            //Need to create a unique wu name, otherwise multiple queries will step on each other.
            StringBuffer uid;
            uid.append("WLOCAL_").append((unsigned)GetCurrentProcessId());
            wuid.set(uid);
        }

#ifdef MONITOR_ECLAGENT_STATUS  
        if (serverstatus)
        {
            serverstatus->queryProperties()->setProp("WorkUnit",wuid.str());
            serverstatus->commitProperties();
        }
#endif

        // MORE - this is a bit messed up - opening for read when we want to write first...
        try
        {
            IConstWorkUnit *w = NULL;
            if (!standAloneWorkUnit)
            {
                Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
#ifdef _DEBUG
                factory->setTracingLevel(10);
#endif
                w = factory->openWorkUnit(wuid.str(), false);
            }
            else
                w = standAloneWorkUnit.getClear();

            if (w)
            {
                EclAgent agent(w, wuid.str(), globals->getPropInt("IGNOREVERSION", 0)==0, globals->getPropBool("WFRESET", false), globals->getPropBool("NORETRY", false), logfilespec.str(), globals->queryProp("allowedPipePrograms"), query.getClear(), globals, agentTopology, logMsgHandler);
                const bool isRemoteWorkunit = (daliServers.length() != 0);
                const bool resolveFilesLocally = !isRemoteWorkunit || globals->getPropBool("USELOCALFILES", false);
                const bool writeResultsToStdout = !isRemoteWorkunit || globals->getPropBool("RESULTSTOSTDOUT", false);

                outputFmts outputFmt = ofSTD;
                if (globals->getPropBool("-xml", false))
                    outputFmt = ofXML;
                else if (globals->getPropBool("-raw", false))
                    outputFmt = ofRAW;
                else if (globals->getPropBool("-csv", false))
                {
                    fprintf(stdout,"\nCSV output format not supported\n");
                    return false;
                }

                agent.setStandAloneOptions(standAloneExe, isRemoteWorkunit, resolveFilesLocally, writeResultsToStdout, outputFmt);
                agent.doProcess();
            }
            else
            {
                LOG(MCerror, "Unknown workunit %s", wuid.str());
                try
                {
                    descheduleNonexistentWorkUnit(wuid.str());
                }
                catch(IException * e)
                {
                    int code = e->errorCode();
                    StringBuffer msg;
                    msg.append("Failed to deschedule unknown workunit ").append(wuid.str()).append(": ");
                    e->errorMessage(msg);
                    e->Release();
                    WARNLOG("%s (%d)", msg.str(), code);
                }
            }
        }
        catch (IException * e)
        {
            EXCLOG(e, "EclAgent");
            e->Release();
        }
        catch (RELEASE_CATCH_ALL)
        {
        }

        if (serverstatus)
            delete serverstatus;
    }
    catch (IException * e)
    {
        EXCLOG(e, "EclAgent");
        e->Release();
    }

    setDaliServixSocketCaching(false);
    closeDllServer();
    closeEnvironment();
    ::closedownClientProcess(); // dali client closedown
    if (traceLevel)
        PrintLog("exiting");

    return 0;
}

//=======================================================================================
void usage(const char * exeName)
{
    fprintf(stdout,"\nUsage:\n"
           "    %s <options>\n"
           "\nGeneral options:\n"
           "    -wu=<file>          Write XML formatted workunit to given filespec and exit\n"
           "    -xml                Display output as XML\n"
           "    -raw                Display output as binary\n"
           "    -limit=x            Limit number of output rows\n"
           "    --help              Display this message\n",
          exeName
    );
}
//=======================================================================================


int STARTQUERY_API start_query(int argc, const char *argv[])
{
    EnableSEHtoExceptionMapping();
    InitModuleObjects();

    for (int idx = 1; idx < argc; idx++)
    {
        if (strstr(argv[idx], "-help" ))
        {
            const char * p = strrchr(argv[0],PATHSEPCHAR);
            usage(p ? ++p : argv[0]);
            return false;
        }
    }

    /* Change logging at start, so no logging to stdout, and errors without any prefix go to stderr */
    removeLog();
    Owned<ILogMsgFilter> filter = getCategoryLogMsgFilter(MSGAUD_user, MSGCLS_error);
    queryStderrLogMsgHandler()->setMessageFields(0);
    queryLogMsgManager()->addMonitor(queryStderrLogMsgHandler(), filter);

    int ret;
    try
    {
        Owned<ILoadedDllEntry> exeEntry = createExeDllEntry(argv[0]);
        StringBuffer wuXML;
        if (!getEmbeddedWorkUnitXML(exeEntry, wuXML))
            throw MakeStringException(0, "Could not locate workunit resource");

        ret = eclagent_main(argc, argv, &wuXML, true);
    }
    catch (IException *E)
    {
        StringBuffer msg;
        fprintf(stderr, "Error: %s\n", E->errorMessage(msg).str());
        E->Release();
        ret = 2;
    }
    catch (...)
    {
        fprintf(stderr, "Error: Unknown exception\n");
        ret = 2;
    }
    ExitModuleObjects();
#ifdef _DEBUG
    releaseAtoms();
#endif
    fflush(stdout);
    fflush(stderr);
    return ret;
}


//=======================================================================================
//copied/modified from ccdserver
class InputProbe : public CInterface, implements IHThorInput // base class for the edge probes used for tracing and debugging....
{
protected:
    IHThorInput *in;
    unsigned sourceId;
    unsigned sourceIdx;
    unsigned targetId;
    unsigned targetIdx;
    unsigned iteration;
    unsigned channel;
    unsigned totalTime;

    IOutputMetaData *inMeta;
    unsigned rowCount;
    size32_t maxRowSize;
    bool hasStarted;
    bool hasStopped;
    bool everStarted;
        
public:
    InputProbe(IHThorInput *_in, unsigned _sourceId, unsigned _sourceIdx, unsigned _targetId, unsigned _targetIdx, unsigned _iteration, unsigned _channel)
        : in(_in), sourceId(_sourceId), sourceIdx(_sourceIdx), targetId(_targetId), targetIdx(_targetIdx), iteration(_iteration), channel(_channel)
    {
        hasStarted = false;
        hasStopped = false;
        everStarted = false;
        rowCount = 0;
        maxRowSize = 0;
        inMeta = NULL;
    }

    virtual IInputSteppingMeta * querySteppingMeta() 
    {
        return in->querySteppingMeta();
    }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) 
    {
        return in->gatherConjunctions(collector); 
    }
    virtual void resetEOF() 
    { 
        in->resetEOF(); 
    }

    virtual IOutputMetaData * queryOutputMeta() const 
    { 
        return in->queryOutputMeta(); 
    }

    virtual IOutputMetaData * queryOutputMeta() 
    { 
        return in->queryOutputMeta();
    }

    virtual IHThorInput *queryInput(unsigned idx) const
    {
        if (!idx)
            return in;
        else
            return NULL;
    }

    virtual const void * nextGE(const void * seek, unsigned numFields)
    {
        return in->nextGE(seek, numFields);
    }

    virtual const void *nextInGroup()
    {
        const void *ret = in->nextInGroup();
        if (ret)
        {
            size32_t size = in->queryOutputMeta()->getRecordSize(ret);
            if (size > maxRowSize)
                maxRowSize = size;
            rowCount++;
        }
        return ret;
    }

    virtual bool isGrouped() 
    { 
        return in->isGrouped(); 
    }

    virtual void ready() 
    { 
        // NOTE: rowCount/maxRowSize not reset, as we want them cumulative when working in a child query.
        hasStarted = true;
        hasStopped = false;
        everStarted = true;
        in->ready(); 
    }

    virtual void done() 
    { 
        hasStopped = true;
        in->done(); 
    }
};

//=======================================================================================
//copied/modified from ccdserver
class DebugProbe : public InputProbe, implements IActivityDebugContext
{
    IDebuggableContext *debugContext;
    ICopyArrayOf<IBreakpointInfo> breakpoints;
    HistoryRow *history;
    unsigned lastSequence;
    unsigned historySize;
    unsigned historyCapacity;
    unsigned nextHistorySlot;
    unsigned childGraphId;
    
    mutable memsize_t proxyId; // MORE - do we need a critsec to protect too?

    DebugActivityRecord *sourceAct;
    DebugActivityRecord *targetAct;

    StringAttr edgeId;
    bool forceEOF;
    bool EOGseen;
    bool EOGsent;

    static void putAttributeUInt(IXmlWriter *output, const char *name, unsigned value)
    {
        output->outputBeginNested("att", false);
        output->outputCString(name, "@name");
        output->outputInt(value, "@value");
        output->outputEndNested("att");
    }

    void rowToXML(IXmlWriter *output, const void *row, unsigned sequence, unsigned rowCount, bool skipped, bool limited, bool eof, bool eog) const
    {
        output->outputBeginNested("Row", true);
        output->outputInt(sequence, "@seq");
        if (skipped)
            output->outputBool(true, "@skip");
        if (limited)
            output->outputBool(true, "@limit");
        if (eof)
            output->outputBool(true, "@eof");
        if (eog)
            output->outputBool(true, "@eog");
        if (row)
        {
            output->outputInt(rowCount, "@count");
            IOutputMetaData *meta = queryOutputMeta();
            output->outputInt(meta->getRecordSize(row), "@size");
            meta->toXML((const byte *) row, *output);
        }
        output->outputEndNested("Row");
    }

public:
    DebugProbe(IHThorInput *_in, unsigned _sourceId, unsigned _sourceIdx, DebugActivityRecord *_sourceAct, unsigned _targetId, unsigned _targetIdx, DebugActivityRecord *_targetAct, unsigned _iteration, unsigned _channel, IDebuggableContext *_debugContext)
        : InputProbe(_in, _sourceId, _sourceIdx, _targetId, _targetIdx, _iteration, _channel),
          sourceAct(_sourceAct), targetAct(_targetAct), debugContext(_debugContext)
    {
        historyCapacity = debugContext->getDefaultHistoryCapacity();
        nextHistorySlot = 0;
        if (historyCapacity)
            history = new HistoryRow [historyCapacity];
        else
            history = NULL;
        historySize = 0;
        lastSequence = 0;
        StringBuffer idText;
        idText.appendf("%d_%d", sourceId, sourceIdx);
        if (iteration)
            idText.appendf(".%d", iteration);
        if (channel)
            idText.appendf("#%d", channel);
        edgeId.set(idText);
        debugContext->checkDelayedBreakpoints(this);
        forceEOF = false;
        EOGseen = false;
        EOGsent = false;
        proxyId = 0;
    }

    ~DebugProbe()
    {
        if (history)
        {
            for (unsigned idx = 0; idx < historyCapacity; idx++)
                ReleaseRoxieRow(history[idx].row);
            delete [] history;
        }
        ForEachItemIn(bpIdx, breakpoints)
        {
            breakpoints.item(bpIdx).removeEdge(*this);
        }
    }

    virtual void Link() const
    {
        CInterface::Link(); 
    }

    virtual bool Release() const
    {
        return CInterface::Release();
    }


    virtual memsize_t queryProxyId() const
    {
        return proxyId;
    }

    virtual unsigned queryChildGraphId() const
    {
        return childGraphId;
    }

    virtual void resetEOF() 
    { 
        forceEOF = false;
        EOGseen = false;
        EOGsent = false;
        InputProbe::resetEOF(); 
    }
#if 0
    virtual unsigned queryId() const
    {
        return sourceId;
    }
#endif
    virtual const char *queryEdgeId() const
    {
        return edgeId.get();
    }

    virtual const char *querySourceId() const
    {
        UNIMPLEMENTED;
    }

    virtual void printEdge(IXmlWriter *output, unsigned startRow, unsigned numRows) const
    {
        output->outputBeginNested("edge", true);
        output->outputString(edgeId.length(), edgeId.get(), "@edgeId");
        if (startRow < historySize)
        {
            if (numRows > historySize - startRow)
                numRows = historySize - startRow;
            while (numRows)
            {
                IHistoryRow *rowData = queryHistoryRow(startRow+numRows-1);
                assertex(rowData);
                rowToXML(output, rowData->queryRow(), rowData->querySequence(), rowData->queryRowCount(), rowData->wasSkipped(), rowData->wasLimited(), rowData->wasEof(), rowData->wasEog());
                numRows--;
            }
        }
        output->outputEndNested("edge");
    }

    virtual void searchHistories(IXmlWriter *output, IRowMatcher *matcher, bool fullRows)
    {
        IOutputMetaData *meta = queryOutputMeta();
        bool anyMatchedYet = false;
        if (matcher->canMatchAny(meta))
        {
            for (unsigned i = 0; i < historySize; i++)
            {
                IHistoryRow *rowData = queryHistoryRow(i);
                assertex(rowData);
                const void *row = rowData->queryRow();
                if (row)
                {
                    matcher->reset();
                    meta->toXML((const byte *) rowData->queryRow(), *matcher);
                    if (matcher->matched())
                    {
                        if (!anyMatchedYet)
                        {
                            output->outputBeginNested("edge", true);
                            output->outputString(edgeId.length(), edgeId.get(), "@edgeId");
                            anyMatchedYet = true;
                        }
                        if (fullRows)
                            rowToXML(output, rowData->queryRow(), rowData->querySequence(), rowData->queryRowCount(), rowData->wasSkipped(), rowData->wasLimited(), rowData->wasEof(), rowData->wasEog());
                        else
                        {
                            output->outputBeginNested("Row", true);
                            output->outputInt(rowData->querySequence(), "@sequence");
                            output->outputInt(rowData->queryRowCount(), "@count");
                            output->outputEndNested("Row");
                        }
                    }
                }
            }
            if (anyMatchedYet)
                output->outputEndNested("edge");
        }
    }

    virtual void getXGMML(IXmlWriter *output) const 
    {
        output->outputBeginNested("edge", false);
        sourceAct->outputId(output, "@source");
        targetAct->outputId(output, "@target");
        output->outputString(edgeId.length(), edgeId.get(), "@id");

        if (sourceIdx)
            putAttributeUInt(output, "_sourceIndex", sourceIdx);
        putAttributeUInt(output, "count", rowCount);
        putAttributeUInt(output, "maxRowSize", maxRowSize);
        putAttributeUInt(output, "_roxieStarted", everStarted);
        putAttributeUInt(output, "_started", hasStarted);
        putAttributeUInt(output, "_stopped", hasStopped);
        putAttributeUInt(output, "_eofSeen", forceEOF);
        if (breakpoints.ordinality())
            putAttributeUInt(output, "_breakpoints", breakpoints.ordinality());
        output->outputEndNested("edge");
    }

    virtual IOutputMetaData *queryOutputMeta() const 
    {
        return InputProbe::queryOutputMeta();
    }

    virtual IActivityDebugContext *queryInputActivity() const
    {
        return NULL;
    }

    // NOTE - these functions are threadsafe because only called when query locked by debugger.
    // Even though this thread may not yet be blocked on the debugger's critsec, because all manipulation (including setting history rows) is from 
    // within debugger it is ok.

    virtual unsigned queryHistorySize() const
    {
        return historySize;
    }

    virtual IHistoryRow *queryHistoryRow(unsigned idx) const
    {
        assertex(idx < historySize);
        int slotNo = nextHistorySlot - idx - 1;
        if (slotNo < 0)
            slotNo += historyCapacity;
        return &history[slotNo];
    }

    virtual unsigned queryHistoryCapacity() const
    {
        return historyCapacity;
    }

    virtual unsigned queryLastSequence() const
    {
        return lastSequence;
    }

    virtual IBreakpointInfo *debuggerCallback(unsigned sequence, const void *row)
    {
        // First put the row into the history buffer...
        lastSequence = sequence;
        if (historyCapacity)
        {
            ReleaseClearRoxieRow(history[nextHistorySlot].row);
            if (row) LinkRoxieRow(row);
            history[nextHistorySlot].sequence = sequence; // MORE - timing might be interesting too, but would need to exclude debug wait time somehow...
            history[nextHistorySlot].row = row;
            history[nextHistorySlot].rowCount = rowCount;
            if (!row)
            {
                if (forceEOF)
                    history[nextHistorySlot].setEof();
                else
                    history[nextHistorySlot].setEog();
            }
            if (historySize < historyCapacity)
                historySize++;
            nextHistorySlot++;
            if (nextHistorySlot==historyCapacity)
                nextHistorySlot = 0;
        }
        // Now check breakpoints...
        ForEachItemIn(idx, breakpoints)
        {
            IBreakpointInfo &bp = breakpoints.item(idx);
            if (bp.matches(row, forceEOF, rowCount, queryOutputMeta())) // should optimize to only call queryOutputMeta once - but not that common to have multiple breakpoints
                return &bp;
        }
        return NULL;
    }

    virtual void setHistoryCapacity(unsigned newCapacity)
    {
        if (newCapacity != historyCapacity)
        {
            HistoryRow *newHistory;
            if (newCapacity)
            {
                unsigned copyCount = historySize;
                if (copyCount > newCapacity)
                    copyCount = newCapacity;
                newHistory = new HistoryRow [newCapacity];
                unsigned slot = 0;
                while (copyCount--)
                {
                    IHistoryRow *oldrow = queryHistoryRow(copyCount);
                    newHistory[slot].sequence = oldrow->querySequence();
                    newHistory[slot].row = oldrow->queryRow();
                    newHistory[slot].rowCount = oldrow->queryRowCount();
                    if (newHistory[slot].row)
                        LinkRoxieRow(newHistory[slot].row);
                    slot++;
                }
                historySize = slot;
                nextHistorySlot = slot;
                if (nextHistorySlot==historyCapacity)
                    nextHistorySlot = 0;
            }
            else
            {
                newHistory = NULL;
                historySize = 0;
                nextHistorySlot = 0;
            }
            for (unsigned idx = 0; idx < historyCapacity; idx++)
                ReleaseRoxieRow(history[idx].row);
            delete [] history;
            history = newHistory;
            historyCapacity = newCapacity;
        }
    }

    virtual void clearHistory()
    {
        for (unsigned idx = 0; idx < historyCapacity; idx++)
            ReleaseClearRoxieRow(history[idx].row);
        historySize = 0;
        nextHistorySlot = 0;
    }

    virtual void ready()
    {
        forceEOF = false;
        EOGseen = false;
        EOGsent = false;
        InputProbe::ready();
    }

    virtual const void * nextGE(const void * seek, unsigned numFields)
    {
        return in->nextGE(seek, numFields);
    }

    virtual const void *nextInGroup()
    {
        // Code is a little complex to avoid interpreting a skip on all rows in a group as EOF
        try
        {
            if (forceEOF)
                return NULL;
            loop
            {
                const void *ret = InputProbe::nextInGroup();
                if (!ret)
                {
                    if (EOGseen)
                        forceEOF = true;
                    else
                        EOGseen = true;
                }
                else
                    EOGseen = false;
                BreakpointActionMode action = debugContext->checkBreakpoint(DebugStateEdge, this, ret);
                if (((CHThorDebugContext*)debugContext)->getEclAgent()->queryWorkUnit()->aborting())
                {
                    if (ret)
                        ReleaseClearRoxieRow(ret);
                    ((CHThorDebugContext*)debugContext)->getEclAgent()->abort();
                    forceEOF = true;
                    return NULL;
                }
                if (action == BreakpointActionSkip && !forceEOF)
                {
                    if (historyCapacity)
                        queryHistoryRow(0)->setSkipped();
                    if (ret)
                    {
                        ReleaseClearRoxieRow(ret);
                        rowCount--;
                    }
                    continue;
                }
                else if (action == BreakpointActionLimit)
                {
                    // This return value implies that we should not return the current row NOR should we return any more...
                    forceEOF = true;
                    ReleaseClearRoxieRow(ret);
                    if (historyCapacity)
                        queryHistoryRow(0)->setLimited();
                    rowCount--;
                }
                if (forceEOF || ret || !EOGsent)
                {
                    EOGsent = (ret == NULL);
                    return ret;
                }
            }
        }
        catch (IException *E)
        {
            debugContext->checkBreakpoint(DebugStateException, this, E);
            throw;
        }
    }

    virtual void setBreakpoint(IBreakpointInfo &bp)
    {
        if (bp.canMatchAny(queryOutputMeta()))
        {
            breakpoints.append(bp);
            bp.noteEdge(*this);
        }
    }

    virtual void removeBreakpoint(IBreakpointInfo &bp)
    {
        breakpoints.zap(bp);
        bp.removeEdge(*this);
    }

    virtual void updateProgress(IWUGraphProgress &progress) const 
    {   
        if (in)
            in->updateProgress(progress);
    }

};

IDebugGraphManager *createProxyDebugGraphManager(unsigned graphId, unsigned channel, memsize_t remoteGraphId);

//=======================================================================================

class CHThorDebugGraphManager : extends CBaseDebugGraphManager
{
public:
    CHThorDebugGraphManager(IDebuggableContext *_debugContext, unsigned _id, const char *_graphName)
        : CBaseDebugGraphManager(_debugContext, _id, _graphName)
    {
    }

    IInputBase *createProbe(IInputBase *in, IActivityBase *sourceAct, IActivityBase *targetAct, unsigned sourceIdx, unsigned targetIdx, unsigned iteration)
    {
        CriticalBlock b(crit);
        unsigned channel = debugContext->queryChannel();
        unsigned sourceId = sourceAct->queryId();
        unsigned targetId = targetAct->queryId();
        DebugActivityRecord *sourceActRecord = noteActivity(sourceAct, iteration, channel, debugContext->querySequence());
        DebugActivityRecord *targetActRecord = noteActivity(targetAct, iteration, channel, debugContext->querySequence());
        DebugProbe *probe = new DebugProbe(dynamic_cast<IHThorInput*>(in), sourceId, sourceIdx, sourceActRecord, targetId, targetIdx, targetActRecord, iteration, channel, debugContext);
    #ifdef _DEBUG
        DBGLOG("Creating probe for edge id %s in graphManager %p", probe->queryEdgeId(), this);
    #endif
        assertex(!allProbes.getValue(probe->queryEdgeId()));
        allProbes.setValue(probe->queryEdgeId(), (IActivityDebugContext *) probe);

        probe->Release(); // the two maps will have linked, and are enough to ensure lifespan...
        return probe;
    }

    void deleteGraph(IArrayOf<IActivityBase> *activities, IArrayOf<IInputBase> *probes)
    {
        CriticalBlock b(crit);
        if (activities)
        {
            ForEachItemIn(idx, *activities)
            {
                IActivityBase &activity = activities->item(idx);
                if (activity.isSink())
                    sinks.zap(activity);
                Linked<DebugActivityRecord> node = allActivities.getValue(&activity);
                if (node)
                    allActivities.remove(&activity);
            }
        }
        if (probes)
        {
            IArrayOf<IInputBase>* fprobes = (IArrayOf<IInputBase>*)(probes);
            ForEachItemIn(probeIdx, *fprobes)
            {
                DebugProbe &probe = (DebugProbe &) fprobes->item(probeIdx);
#ifdef _DEBUG
                DBGLOG("removing probe for edge id %s in graphManager %p", probe.queryEdgeId(), this);
#endif
                allProbes.remove(probe.queryEdgeId());
            }
        }
    }

    IProbeManager *startChildGraph(unsigned childGraphId, IActivityBase *parent)
    {
        CriticalBlock b(crit);
        if (childGraphId || parent)
        {
            CBaseDebugGraphManager *childManager = new CHThorDebugGraphManager(debugContext, childGraphId, NULL);
            IDebugGraphManager *graph = childManager;
            childGraphs.append(*LINK(graph));
            debugContext->noteGraphChanged();
            return childManager;
        }
        else
            return LINK(this);
    }

};

IProbeManager *createDebugManager(IDebuggableContext *debugContext, const char *graphName)
{
    return new CHThorDebugGraphManager(debugContext, 0, graphName);
}
