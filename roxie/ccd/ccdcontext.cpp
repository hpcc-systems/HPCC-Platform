/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#include "platform.h"
#include "jlib.hpp"

#include "environment.hpp"
#include "workunit.hpp"
#include "wujobq.hpp"
#include "nbcd.hpp"
#include "rtlread_imp.hpp"
#include "thorplugin.hpp"
#include "thorxmlread.hpp"
#include "thorstats.hpp"
#include "roxiemem.hpp"
#include "eventqueue.hpp"

#include "ccd.hpp"
#include "ccdcontext.hpp"
#include "ccddebug.hpp"
#include "ccddali.hpp"
#include "ccdquery.hpp"
#include "ccdqueue.ipp"
#include "ccdsnmp.hpp"
#include "ccdstate.hpp"
#include "roxiehelper.hpp"
#include "enginecontext.hpp"
#include "ws_dfsclient.hpp"
#include "wfcontext.hpp"

#include <list>
#include <string>
#include <algorithm>

using roxiemem::IRowManager;

//=======================================================================================================================

#define DEBUGEE_TIMEOUT 10000
class CAgentDebugContext : public CBaseDebugContext
{
    /*

    Some thoughts on agent debugging
    1. Something like a ping can be used to get data from agent when needed
    2. Should disable IBYTI processing (always use primary) - DONE
       and server-side caching - DONE
    3. Roxie server can know what agent transactions are pending by intercepting the sends - no need for agent to call back just to indicate start of agent subgraph
    4. There is a problem when a agent hits a breakpoint in that the breakpoint cound have been deleted by the time it gets a chance to tell the Roxie server - can't
       happen in local case because of the critical block at the head of checkBreakpoint but the local copy of BPs out on agent CAN get out of date. Should we care?
       Should there be a "Sorry, your breakpoints are out of date, here's the new set" response?
       Actually what we do is recheck the BP on the server, and ensure that breakpoint indexes are persistent. DONE
    5. We need to serialize over our graph info if changed since last time.
    6. I think we need to change implementation of debugGraph to support children. Then we have a place to put a proxy for a remote one.
       - id's should probably be structured so we can use a hash table at each level

    */
    const RoxiePacketHeader &header;
    memsize_t parentActivity;
    unsigned channel;
    int debugSequence;
    CriticalSection crit;
    const IRoxieContextLogger &logctx; // hides base class definition with more derived class pointer

public:
    CAgentDebugContext(IRoxieAgentContext *_ctx, const IRoxieContextLogger &_logctx, RoxiePacketHeader &_header)
        : CBaseDebugContext(_logctx), header(_header), logctx(_logctx)
    {
        channel = header.channel;
        debugSequence = 0;
        parentActivity = 0;
    }

    void init(const IRoxieQueryPacket *_packet)
    {
        unsigned traceLength = _packet->getTraceLength();
        assertex(traceLength);
        const byte *traceInfo = _packet->queryTraceInfo();
        assertex((*traceInfo & LOGGING_DEBUGGERACTIVE) != 0);
        unsigned debugLen = *(unsigned short *) (traceInfo + 1);
        MemoryBuffer b;
        b.setBuffer(debugLen, (char *) (traceInfo + 1 + sizeof(unsigned short)), false);
        deserialize(b);
        __uint64 tmp; // can't serialize memsize_t
        b.read(tmp); // note - this is written by the RemoteAdaptor not by the serialize....
        parentActivity = (memsize_t)tmp;
    }

    virtual unsigned queryChannel() const
    {
        return channel;
    }

    virtual BreakpointActionMode checkBreakpoint(DebugState state, IActivityDebugContext *probe, const void *extra)
    {
        return CBaseDebugContext::checkBreakpoint(state, probe, extra);
    }

    virtual void waitForDebugger(DebugState state, IActivityDebugContext *probe)
    {
        StringBuffer debugIdString;
        CriticalBlock b(crit); // Make sure send sequentially - don't know if this is strictly needed...
        debugSequence++;
        debugIdString.appendf(".debug.%x", debugSequence);
        IPendingCallback *callback = ROQ->notePendingCallback(header, debugIdString.str()); // note that we register before the send to avoid a race.
        try
        {
            RoxiePacketHeader newHeader(header, ROXIE_DEBUGCALLBACK, 0);  // subchannel not relevant
            for (;;) // retry indefinitely, as more than likely Roxie server is waiting for user input ...
            {
                Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
                // These are deserialized in onDebugCallback..
                MemoryBuffer debugInfo;
                debugInfo.append(debugSequence);
                debugInfo.append((char) state);
                if (state==DebugStateGraphFinished)
                {
                    debugInfo.append(globalCounts.count());
                    HashIterator edges(globalCounts);
                    ForEach(edges)
                    {
                        IGlobalEdgeRecord *edge = globalCounts.mapToValue(&edges.query());
                        debugInfo.append((const char *) edges.query().getKey());
                        debugInfo.append(edge->queryCount());
                    }
                }
                debugInfo.append(currentBreakpointUID);
                debugInfo.append((__uint64)parentActivity);     // can't serialize memsize_t
                debugInfo.append(channel);
                assertex (currentGraph); // otherwise what am I remote debugging?
                currentGraph->serializeProxyGraphs(debugInfo);
                debugInfo.append(probe ? probe->queryEdgeId() : "");

                char *buf = (char *) output->getBuffer(debugInfo.length(), true);
                memcpy(buf, debugInfo.toByteArray(), debugInfo.length());
                output->putBuffer(buf, debugInfo.length(), true);
                output->flush();
                output.clear();
                if (callback->wait(5000))
                    break;
            }
            MemoryBuffer &serverData = callback->queryData();
            deserialize(serverData);
        }
        catch (...)
        {
            ROQ->removePendingCallback(callback);
            throw;
        }
        ROQ->removePendingCallback(callback);
    }

    virtual IRoxieQueryPacket *onDebugCallback(const RoxiePacketHeader &header, size32_t len, char *data)
    {
        // MORE - Implies a server -> agent child -> agent grandchild type situation - need to pass call on to Roxie server (rather as I do for file callback)
        UNIMPLEMENTED;
    }

    virtual bool onDebuggerTimeout()
    {
        throwUnexpected();
    }

    virtual void debugCounts(IXmlWriter *output, unsigned sinceSequence, bool reset)
    {
        // This gives info for the global view - accumulated counts for all instances, plus the graph as fetched from the workunit
        HashIterator edges(globalCounts);
        ForEach(edges)
        {
            IGlobalEdgeRecord *edge = globalCounts.mapToValue(&edges.query());
            if (edge->queryLastSequence() && (!sinceSequence || edge->queryLastSequence() > sinceSequence))
            {
                output->outputBeginNested("edge", true);
                output->outputCString((const char *) edges.query().getKey(), "@edgeId");
                output->outputUInt(edge->queryCount(), sizeof(unsigned), "@count");
                output->outputEndNested("edge");
            }
            if (reset)
                edge->reset();
        }
    }

};

//=======================================================================================================================
#define PERSIST_LOCK_TIMEOUT 10000
#define PERSIST_LOCK_SLEEP 5000

class CRoxieWorkflowMachine : public WorkflowMachine
{
public:
    CRoxieWorkflowMachine(IPropertyTree *_workflowInfo, IConstWorkUnit *_wu, bool _doOnce, bool _parallelWorkflow, unsigned _numWorkflowThreads, const IRoxieContextLogger &_logctx)
    : WorkflowMachine(_logctx)
    {
        workunit = _wu;
        workflowInfo = _workflowInfo;
        doOnce = _doOnce;
        parallelWorkflow = _parallelWorkflow;
        numWorkflowThreads = _numWorkflowThreads;
    }
    void returnPersistVersion(char const * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile)
    {
        persist.setown(new PersistVersion(logicalName, eclCRC, allCRC, isFile));
    }
protected:
    virtual void begin()
    {
        // MORE - should pre-do more of this work
        unsigned count = 0;
        if (workunit)
        {
            workflow.setown(workunit->getWorkflowClone());
        }
        else
        {
            Owned<IConstWorkflowItemIterator> iter = createWorkflowItemIterator(workflowInfo);
            for(iter->first(); iter->isValid(); iter->next())
                count++;
            workflow.setown(createWorkflowItemArray(count));
            for(iter->first(); iter->isValid(); iter->next())
            {
                IConstWorkflowItem *item = iter->query();
                bool isOnce = (item->queryMode() == WFModeOnce);
                workflow->addClone(item);
                if (isOnce != doOnce)
                    workflow->queryWfid(item->queryWfid()).setState(WFStateDone);
            }
        }
    }
    virtual bool getParallelFlag() const override
    {
        return parallelWorkflow && !doOnce;
    }
    virtual unsigned getThreadNumFlag() const override
    {
        return numWorkflowThreads;
    }
    virtual void end()
    {
        if (workunit)
        {
            WorkunitUpdate w(&workunit->lock());
            w->syncRuntimeWorkflow(workflow);
        }
        workflow.clear();
    }
    virtual void schedulingStart()
    {
        if (!workunit)
            throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "Scheduling not supported when running predeployed queries");
        if (!wfconn)
            wfconn.setown(getWorkflowScheduleConnection(workunit->queryWuid()));

        wfconn->lock();
        wfconn->setActive();
        wfconn->pull(workflow);
        wfconn->unlock();
    }
    virtual bool schedulingPull()
    {
        if (!workunit)
            throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "Scheduling not supported when running predeployed queries");
        wfconn->lock();
        bool more = wfconn->pull(workflow);
        wfconn->unlock();
        return more;
    }

    virtual bool schedulingPullStop()
    {
        if (!workunit)
            throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "Scheduling not supported when running predeployed queries");
        wfconn->lock();
        bool more = wfconn->pull(workflow);
        if(!more) wfconn->resetActive();
        wfconn->unlock();
        return more;
    }

    virtual void noteTiming(unsigned wfid, timestamp_type startTime, stat_type elapsedNs)
    {
        if (!workunit)
            return;

        WorkunitUpdate wu(&workunit->lock());
        StringBuffer scope;
        scope.append(WorkflowScopePrefix).append(wfid);
        updateWorkunitStat(wu, SSTworkflow, scope, StWhenStarted, nullptr, startTime, 0);
        updateWorkunitStat(wu, SSTworkflow, scope, StTimeElapsed, nullptr, elapsedNs, 0);
    }


    virtual void reportContingencyFailure(char const * type, IException * e)
    {
        if (workunit)
        {
            ErrorSeverity severity = workunit->getWarningSeverity(e->errorCode(), SeverityWarning);
            if (severity != SeverityIgnore)
            {
                StringBuffer msg;
                msg.append(type).append(" clause failed (execution will continue): ").append(e->errorCode()).append(": ");
                e->errorMessage(msg);
                WorkunitUpdate wu(&workunit->lock());
                addExceptionToWorkunit(wu, severity, "user", e->errorCode(), msg.str(), NULL, 0, 0, 0);
            }
        }
    }
    virtual void checkForAbort(unsigned wfid, IException * handling)
    {
        if (workunit && workunit->aborting())
        {
            if(handling)
            {
                StringBuffer msg;
                msg.append("Abort takes precedence over error: ").append(handling->errorCode()).append(": ");
                handling->errorMessage(msg);
                msg.append(" (in item ").append(wfid).append(")");
                WorkunitUpdate wu(&workunit->lock());
                addExceptionToWorkunit(wu, SeverityWarning, "user", handling->errorCode(), msg.str(), NULL, 0, 0, 0);
                handling->Release();
            }
            throw new WorkflowException(0, "Workunit abort request received", wfid, WorkflowException::ABORT, MSGAUD_user);
        }
    }
    virtual void doExecutePersistItem(IRuntimeWorkflowItem & item)
    {
        if (!workunit)
        {
            throw MakeStringException(0, "PERSIST not supported when running predeployed queries");
        }
        unsigned wfid = item.queryWfid();
        // Old persist model requires dependencies to be executed BEFORE checking if the persist is up to date
        // Defaults to old model, in case executing a WU that is created by earlier eclcc
        if (!workunit->getDebugValueBool("expandPersistInputDependencies", false))
            doExecuteItemDependencies(item, wfid);
        SCMStringBuffer name;
        const char *logicalName = item.getPersistName(name).str();
        int maxPersistCopies = item.queryPersistCopies();
        if (maxPersistCopies < 0)
            maxPersistCopies = DEFAULT_PERSIST_COPIES;
        Owned<IRemoteConnection> persistLock;
        persistLock.setown(startPersist(logicalName));
        doExecuteItemDependency(item, item.queryPersistWfid(), wfid, true);  // generated code should end up calling back to returnPersistVersion, which sets persist
        if (!persist)
        {
            StringBuffer errmsg;
            errmsg.append("Internal error in generated code: for wfid ").append(wfid).append(", persist CRC wfid ").append(item.queryPersistWfid()).append(" did not call returnPersistVersion");
            throw MakeStringExceptionDirect(0, errmsg.str());
        }
        Owned<PersistVersion> thisPersist = persist.getClear();
        if (strcmp(logicalName, thisPersist->logicalName.get()) != 0)
        {
            StringBuffer errmsg;
            errmsg.append("Failed workflow/persist consistency check: wfid ").append(wfid).append(", WU persist name ").append(logicalName).append(", runtime persist name ").append(thisPersist->logicalName.get());
            throw MakeStringExceptionDirect(0, errmsg.str());
        }
        if (workunit->getDebugValueInt("freezepersists", 0) != 0)
        {
            checkPersistMatches(logicalName, thisPersist->eclCRC);
        }
        else if(!isPersistUptoDate(persistLock, item, logicalName, thisPersist->eclCRC, thisPersist->allCRC, thisPersist->isFile))
        {
            // New persist model allows dependencies to be executed AFTER checking if the persist is up to date
            if (workunit->getDebugValueBool("expandPersistInputDependencies", false))
                doExecuteItemDependencies(item, wfid);
            if (maxPersistCopies > 0)
                deleteLRUPersists(logicalName, (unsigned) maxPersistCopies-1);
            doExecuteItem(item, wfid);
            updatePersist(persistLock, logicalName, thisPersist->eclCRC, thisPersist->allCRC);
        }
        logctx.CTXLOG("Finished persists - add to read lock list");
        persistReadLocks.append(*persistLock.getClear());
    }
    bool getPersistTime(time_t & when, IRuntimeWorkflowItem & item)
    {
        SCMStringBuffer name;
        const char *logicalName = item.getPersistName(name).str();
        StringBuffer whenName;
        expandLogicalFilename(whenName, logicalName, workunit, false, false);
        whenName.append("$when");
        if (!isResult(whenName, ResultSequencePersist))
            return false;

        when = getResultInt(whenName, ResultSequencePersist);
        return true;
    }
    virtual void doExecuteCriticalItem(IRuntimeWorkflowItem & item)
    {
        if (!workunit)
            throw MakeStringException(0, "CRITICAL not supported when running predeployed queries");

        unsigned wfid = item.queryWfid();

        SCMStringBuffer name;
        const char *criticalName = item.getCriticalName(name).str();

        Owned<IRemoteConnection> rlock = obtainCriticalLock(criticalName);
        if (!rlock.get())
            throw MakeStringException(0, "Cannot obtain Critical section lock");

        doExecuteItemDependencies(item, wfid);
        doExecuteItem(item, wfid);
        releaseCriticalLock(rlock);
    }

private:

    bool isResult(const char *name, unsigned sequence)
    {
        Owned<IConstWUResult> r = getWorkUnitResult(workunit, name, sequence);
        return r != NULL && r->getResultStatus() != ResultStatusUndefined;
    }

    unsigned getResultHash(const char * name, unsigned sequence)
    {
        Owned<IConstWUResult> r = getWorkUnitResult(workunit, name, sequence);
        if (!r)
            throw MakeStringException(ROXIE_INTERNAL_ERROR, "Failed to retrieve hash value %s from workunit", name);
        return r->getResultHash();
    }

    unsigned __int64 getResultInt(const char * name, unsigned sequence)
    {
        Owned<IConstWUResult> r = getWorkUnitResult(workunit, name, sequence);
        if (!r)
            throw MakeStringException(ROXIE_INTERNAL_ERROR, "Failed to retrieve persist value %s from workunit", name);
        return r->getResultInt();
    }

    void setResultInt(const char * name, unsigned sequence, unsigned __int64 value, unsigned size)
    {
        WorkunitUpdate w(&workunit->lock());
        w->setResultInt(name, sequence, value);
    }

    inline bool fileExists(const char *lfn)
    {
        Owned<IDistributedFile> f = wsdfs::lookup(lfn, queryUserDescriptor(), AccessMode::readMeta, false, false, nullptr, defaultPrivilegedUser, INFINITE);
        if (f)
            return true;
        return false;
    }

    inline IUserDescriptor *queryUserDescriptor()
    {
        if (workunit)
            return workunit->queryUserDescriptor();//ad-hoc mode
        else
        {
            Owned<IRoxieDaliHelper> daliHelper = connectToDali(false);
            if (daliHelper)
                return daliHelper->queryUserDescriptor();//predeployed query mode
        }
        return NULL;
    }

    bool checkPersistUptoDate(IRuntimeWorkflowItem & item, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile, StringBuffer &errText)
    {
        StringBuffer lfn, crcName, eclName;
        expandLogicalFilename(lfn, logicalName, workunit, false, false);
        crcName.append(lfn).append("$crc");
        eclName.append(lfn).append("$eclcrc");

        if (!isResult(lfn, ResultSequencePersist))
            errText.appendf("Building PERSIST('%s'): It hasn't been calculated before", logicalName);
        else if (isFile && !fileExists(lfn))
            errText.appendf("Rebuilding PERSIST('%s'): Persistent file does not exist", logicalName);
        else if (!item.queryPersistRefresh())
        {
            errText.appendf("Not rebuilding PERSIST('%s'): due to REFRESH(false)", logicalName);
            return true;
        }
        else if (!isResult(crcName, ResultSequencePersist))
            errText.appendf("Rebuilding PERSIST('%s'): Saved CRC isn't present", logicalName);
        else
        {
            unsigned savedEclCRC = (unsigned) getResultInt(eclName, ResultSequencePersist);
            unsigned __int64 savedCRC = (unsigned __int64)getResultInt(crcName, ResultSequencePersist);
            if (savedEclCRC != eclCRC)
                errText.appendf("Rebuilding PERSIST('%s'): ECL has changed", logicalName);
            else if (savedCRC != allCRC)
                errText.appendf("Rebuilding PERSIST('%s'): Input files have changed", logicalName);
            else if (isItemOlderThanInputPersists(item))
                errText.appendf("Rebuilding PERSIST('%s'): Input persists are more recent", logicalName);
            else
                return true;
        }

        return false;
    }

    bool changePersistLockMode(IRemoteConnection *persistLock, unsigned mode, const char * name, bool repeat)
    {
        logctx.CTXLOG("Waiting to change persist lock to %s for %s", (mode == RTM_LOCK_WRITE) ? "write" : "read", name);  // MORE - pass a logctx around?
        //When converting a read lock to a write lock so the persist can be rebuilt hold onto the lock as short as
        //possible.  Otherwise lots of workunits each trying to convert read locks to write locks will mean
        //that the read lock is never released by all the workunits at the same time, so no workunit can progress.
        unsigned timeout = repeat ? PERSIST_LOCK_TIMEOUT : 0;
        for (;;)
        {
            try
            {
                persistLock->changeMode(mode, timeout);
                logctx.CTXLOG("Changed persist lock");
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
                logctx.CTXLOG("Failed to convert persist lock");
                return false;
            }
            //This is only executed when converting write->read.  There is significant doubt whether the changeMode()
            //can ever fail - and whether the execution can ever get here.
            logctx.CTXLOG("Waiting to convert persist lock"); // MORE - give a chance to abort
        }
    }

    IRemoteConnection *getPersistReadLock(const char * logicalName)
    {
        StringBuffer lfn;
        expandLogicalFilename(lfn, logicalName, workunit, false, false);
        if (!lfn.length())
            throw MakeStringException(0, "Invalid persist name used : '%s'", logicalName);

        const char * name = lfn;

        StringBuffer xpath;
        xpath.append("/PersistRunLocks/");
        if (isdigit(*name))
            xpath.append("_");
        for (const char * cur = name;*cur;cur++)
            xpath.append(isalnum(*cur) ? *cur : '_');

        logctx.CTXLOG("Waiting for persist read lock for %s", name);
        Owned<IRemoteConnection> persistLock;
        for (;;)
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
            logctx.CTXLOG("Waiting for persist read lock"); // MORE - give a chance to abort
        }

        logctx.CTXLOG("Obtained persist read lock");
        return persistLock.getClear();
    }

    void setBlockedOnPersist(const char * logicalName)
    {
        StringBuffer s;
        s.append("Waiting for persist ").append(logicalName);
        WorkunitUpdate w(&workunit->lock());
        w->setState(WUStateBlocked);
        w->setStateEx(s.str());
    }

    bool isPersistUptoDate(Owned<IRemoteConnection> &persistLock, IRuntimeWorkflowItem & item, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile)
    {
        //Loop trying to get a write lock - if it fails, then release the read lock, otherwise
        //you can get a deadlock with several things waiting to read, and none being able to write.
        bool rebuildAllPersists = false;   // Useful for debugging purposes
        for (;;)
        {
            StringBuffer dummy;
            if (checkPersistUptoDate(item, logicalName, eclCRC, allCRC, isFile, dummy) && !rebuildAllPersists)
            {
                if (dummy.length())
                    logctx.CTXLOG("%s", dummy.str());
                else
                    logctx.CTXLOG("PERSIST('%s') is up to date", logicalName);
                return true;
            }

            //Get a write lock
            setBlockedOnPersist(logicalName);
            if (changePersistLockMode(persistLock, RTM_LOCK_WRITE, logicalName, false))
                break;

            //failed to get a write lock, so release our read lock
            persistLock.clear();
            MilliSleep(PERSIST_LOCK_SLEEP + (getRandom()%PERSIST_LOCK_SLEEP));
            persistLock.setown(getPersistReadLock(logicalName));
        }
        WorkunitUpdate w(&workunit->lock());
        w->setState(WUStateRunning);

        //Check again whether up to date, someone else might have updated it!
        StringBuffer errText;
        if (checkPersistUptoDate(item, logicalName, eclCRC, allCRC, isFile, errText) && !rebuildAllPersists)
        {
            if (errText.length())
            {
                errText.append(" (after being calculated by another job)");
                logctx.CTXLOG("%s", errText.str());
            }
            else
                logctx.CTXLOG("PERSIST('%s') is up to date (after being calculated by another job)", logicalName);
            changePersistLockMode(persistLock, RTM_LOCK_READ, logicalName, true);
            return true;
        }
        if (errText.length())
            logctx.CTXLOG("%s", errText.str());
        return false;
    }

    virtual void updatePersist(IRemoteConnection *persistLock, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC)
    {
        StringBuffer lfn, crcName, eclName, whenName;
        expandLogicalFilename(lfn, logicalName, workunit, false, false);
        crcName.append(lfn).append("$crc");
        eclName.append(lfn).append("$eclcrc");
        whenName.append(lfn).append("$when");

        setResultInt(crcName, ResultSequencePersist, allCRC, sizeof(int));
        setResultInt(eclName, ResultSequencePersist, eclCRC, sizeof(int));
        setResultInt(whenName, ResultSequencePersist, time(NULL), sizeof(int));

        logctx.CTXLOG("Convert persist write lock to read lock");
        changePersistLockMode(persistLock, RTM_LOCK_READ, logicalName, true);
    }

    virtual IRemoteConnection *startPersist(const char * logicalName)
    {
        setBlockedOnPersist(logicalName);
        IRemoteConnection *persistLock = getPersistReadLock(logicalName);
        WorkunitUpdate w(&workunit->lock());
        w->setState(WUStateRunning);
        return persistLock;
    }
    virtual void finishPersist(const char * persistName, IRemoteConnection *persistLock)
    {
        //this protects lock array from race conditions
        CriticalBlock block(finishPersistCritSec);
        logctx.CTXLOG("Finished persists - add to read lock list");
        persistReadLocks.append(*persistLock);
    }
    virtual bool checkFreezePersists(const char *logicalName, unsigned eclCRC)
    {
        bool freeze = (workunit->getDebugValueInt("freezepersists", 0) != 0);
        if (freeze)
            checkPersistMatches(logicalName, eclCRC);
        return freeze;
    }
    virtual void checkPersistSupported()
    {
        if (!workunit)
        {
            throw MakeStringException(0, "PERSIST not supported when running predeployed queries");
        }
    }
    virtual bool isPersistAlreadyLocked(const char * logicalName)
    {
        //Note: if workunits are restarted, then the engine should check to verify that the persist has not already been calculated
        return false;
    }
    void checkPersistMatches(const char * logicalName, unsigned eclCRC)
    {
        StringBuffer lfn, eclName;
        expandLogicalFilename(lfn, logicalName, workunit, true, false);
        eclName.append(lfn).append("$eclcrc");

        if (!isResult(lfn, ResultSequencePersist))
            throw MakeStringException(ROXIE_INTERNAL_ERROR, "Frozen PERSIST('%s') hasn't been calculated ", logicalName);
        if (isResult(eclName, ResultSequencePersist) && (getResultInt(eclName, ResultSequencePersist) != eclCRC))
            throw MakeStringException(ROXIE_INTERNAL_ERROR, "Frozen PERSIST('%s') ECL has changed", logicalName);

        StringBuffer msg;
        msg.append("Frozen PERSIST('").append(logicalName).append("') is up to date");
        logctx.CTXLOG("%s", msg.str());
    }

    static int comparePersistAccess(IInterface * const *_a, IInterface * const *_b)
    {
        IPropertyTree *a = *(IPropertyTree **)_a;
        IPropertyTree *b = *(IPropertyTree **)_b;
        const char *accessedA = a->queryProp("@accessed");
        const char *accessedB = b->queryProp("@accessed");
        if (accessedA && accessedB)
            return strcmp(accessedB, accessedA);
        else if (accessedB)
            return -1;
        else if (accessedA)
            return 1;
        else
            return 0;

    }

    void deleteLRUPersists(const char * logicalName, unsigned keep)
    {
        StringBuffer lfn;
        expandLogicalFilename(lfn, logicalName, workunit, false, false);
        logicalName = lfn.str();
        const char *tail = strrchr(logicalName, '_');     // Locate the trailing double-underbar
        assertex(tail);
        StringBuffer head(tail-logicalName+1, logicalName);
        head.append("p*");                                  // Multi-mode persist names end with __pNNNNNNN
        restart:     // If things change beneath us as we are deleting, repeat the process
        IArrayOf<IPropertyTree> persists;
        Owned<IDFAttributesIterator> iter = queryDistributedFileDirectory().getDFAttributesIterator(head,queryUserDescriptor(),false,false,NULL);
        ForEach(*iter)
        {
            IPropertyTree &pt = iter->query();
            const char *name = pt.queryProp("@name");
            if (stricmp(name, logicalName) == 0)   // Don't include the one we are intending to recreate in the LRU list (keep value does not include it)
                continue;
            if (pt.getPropBool("@persistent", false))
            {
                // Paranoia - check as far as we can that it really is another instance of this persist
                tail = strrchr(name, '_');     // Locate the trailing double-underbar
                assertex(tail);
                tail++;
                bool crcSuffix = (*tail++=='p');
                while (crcSuffix && *tail)
                {
                    if (!isdigit(*tail))
                        crcSuffix = false;
                    tail++;
                }
                if (crcSuffix)
                    persists.append(*LINK(&pt));
            }
        }
        if (persists.ordinality() > keep)
        {
            persists.sort(comparePersistAccess);
            while (persists.ordinality() > keep)
            {
                Owned<IPropertyTree> oldest = &persists.popGet();
                const char *oldAccessTime = oldest->queryProp("@accessed");
                VStringBuffer goer("~%s", oldest->queryProp("@name"));   // Make sure we don't keep adding the scope
                Owned<IRemoteConnection> persistLock = getPersistReadLock(goer);
                while (!changePersistLockMode(persistLock, RTM_LOCK_WRITE, goer, false))
                {
                    persistLock.clear();
                    MilliSleep(PERSIST_LOCK_SLEEP + (getRandom()%PERSIST_LOCK_SLEEP));
                    persistLock.setown(getPersistReadLock(goer));
                }
                Owned<IDistributedFile> f = wsdfs::lookup(goer, queryUserDescriptor(), AccessMode::writeSequential, false, false, nullptr, defaultPrivilegedUser, INFINITE);
                if (!f)
                    goto restart; // Persist has been deleted since last checked - repeat the whole process
                const char *newAccessTime = f->queryAttributes().queryProp("@accessed");
                if (oldAccessTime && newAccessTime && !streq(oldAccessTime, newAccessTime))
                    goto restart; // Persist has been accessed since last checked - repeat the whole process
                else if (newAccessTime && !oldAccessTime)
                    goto restart; // Persist has been accessed since last checked - repeat the whole process
                DBGLOG("Deleting LRU persist %s (last accessed at %s)", goer.str(), oldAccessTime);
                f->detach();
            }
        }
    }
    IRemoteConnection *obtainCriticalLock(const char *name)
    {
        StringBuffer xpath;
        xpath.append("/WorkUnitCriticalLocks/").append(name);
        return querySDS().connect(xpath.str(), myProcessSession(), RTM_CREATE | RTM_LOCK_WRITE | RTM_DELETE_ON_DISCONNECT, INFINITE);
    }
    void releaseCriticalLock(IRemoteConnection *criticalLock)
    {
        if(criticalLock && queryDaliServerVersion().compare("1.3") < 0)
            criticalLock->close(true);
    }
    IConstWorkUnit *workunit;
    IPropertyTree *workflowInfo;
    Owned<IWorkflowScheduleConnection> wfconn;
    IArray persistReadLocks;
    bool doOnce;
    bool parallelWorkflow;
    unsigned numWorkflowThreads;
};

CRoxieWorkflowMachine *createRoxieWorkflowMachine(IPropertyTree *_workflowInfo, IConstWorkUnit *_wu, bool _doOnce, bool _parallelWorkflow, unsigned _numWorkflowThreads, const IRoxieContextLogger &_logctx)
{
    return new CRoxieWorkflowMachine(_workflowInfo, _wu, _doOnce, _parallelWorkflow, _numWorkflowThreads, _logctx);
}

//=======================================================================================================================

typedef const byte *row_t;
typedef const byte ** rowset_t;

class DeserializedDataReader : implements IWorkUnitRowReader, public CInterface
{
    rowset_t data;
    size32_t count;
    unsigned idx;
public:
    IMPLEMENT_IINTERFACE;
    DeserializedDataReader(size32_t _count, rowset_t _data)
    : data(_data), count(_count)
    {
        idx = 0;
    }
    virtual const void * nextRow() override
    {
        if (idx < count)
        {
            const void *row = data[idx];
            if (row)
                LinkRoxieRow(row);
            idx++;
            return row;
        }
        return NULL;
    }
    virtual void getResultRowset(size32_t & tcount, const byte * * & tgt) override
    {
        tcount = count;
        if (data)
            rtlLinkRowset(data);
        tgt = data;
    }
};

class CDeserializedResultStore : implements IDeserializedResultStore, public CInterface
{
    PointerArrayOf<row_t> stored;
    UnsignedArray counts;
    IPointerArrayOf<IOutputMetaData> metas;
    mutable SpinLock lock;
public:
    IMPLEMENT_IINTERFACE;
    ~CDeserializedResultStore()
    {
        ForEachItemIn(idx, stored)
        {
            rowset_t rows = stored.item(idx);
            if (rows)
            {
                rtlReleaseRowset(counts.item(idx), rows);
            }
        }
    }
    virtual int addResult(size32_t count, rowset_t data, IOutputMetaData *meta) override
    {
        SpinBlock b(lock);
        stored.append(data);
        counts.append(count);
        metas.append(meta);
        return stored.ordinality()-1;
    }
    virtual void queryResult(int id, size32_t &count, rowset_t &data) const override
    {
        count = counts.item(id);
        data = stored.item(id);
    }
    virtual IWorkUnitRowReader *createDeserializedReader(int id) const override
    {
        return new DeserializedDataReader(counts.item(id), stored.item(id));
    }
    virtual void serialize(unsigned & tlen, void * & tgt, int id, ICodeContext *codectx) const override
    {
        IOutputMetaData *meta = metas.item(id);
        rowset_t data = stored.item(id);
        size32_t count = counts.item(id);

        MemoryBuffer result;
        Owned<IOutputRowSerializer> rowSerializer = meta->createDiskSerializer(codectx, 0); // NOTE - we don't have a meaningful activity id. Only used for error reporting.
        bool grouped = meta->isGrouped();
        for (size32_t idx = 0; idx<count; idx++)
        {
            const byte *row = data[idx];
            if (grouped && idx)
                result.append(row == NULL);
            if (row)
            {
                CThorDemoRowSerializer serializerTarget(result);
                rowSerializer->serialize(serializerTarget, row);
            }
        }
        tlen = result.length();
        tgt= result.detach();
    }
};

extern IDeserializedResultStore *createDeserializedResultStore()
{
    return new CDeserializedResultStore;
}

class WorkUnitRowReaderBase : implements IWorkUnitRowReader, public CInterface
{
protected:
    bool isGrouped;
    Linked<IEngineRowAllocator> rowAllocator;

public:
    IMPLEMENT_IINTERFACE;
    WorkUnitRowReaderBase(IEngineRowAllocator *_rowAllocator, bool _isGrouped)
        : isGrouped(_isGrouped), rowAllocator(_rowAllocator)
    {

    }

    virtual void getResultRowset(size32_t & tcount, const byte * * & tgt) override
    {
        bool atEOG = true;
        RtlLinkedDatasetBuilder builder(rowAllocator);
        for (;;)
        {
            const void *ret = nextRow();
            if (!ret)
            {
                if (atEOG || !isGrouped)
                    break;
                atEOG = true;
            }
            else
                atEOG = false;
            builder.appendOwn(ret);
        }
        tcount = builder.getcount();
        tgt = builder.linkrows();
    }
};

class RawDataReader : public WorkUnitRowReaderBase
{
protected:
    const IRoxieContextLogger &logctx;
    byte *bufferBase;
    MemoryBuffer blockBuffer;
    Owned<IBufferedSerialInputStream> bufferStream;
    CThorStreamDeserializerSource rowSource;
    bool eof;
    bool eogPending;
    Owned<IOutputRowDeserializer> rowDeserializer;

    virtual bool nextBlock(unsigned & tlen, void * & tgt, void * & base) = 0;

    bool reload()
    {
        free(bufferBase);
        size32_t lenData;
        bufferBase = NULL;
        void *tempData, *base;
        eof = !nextBlock(lenData, tempData, base);
        bufferBase = (byte *) base;
        blockBuffer.setBuffer(lenData, tempData, false);
        return !eof;
    }

public:
    RawDataReader(ICodeContext *codeContext, IEngineRowAllocator *_rowAllocator, bool _isGrouped, const IRoxieContextLogger &_logctx)
        : WorkUnitRowReaderBase(_rowAllocator, _isGrouped), logctx(_logctx)
    {
        eof = false;
        eogPending = false;
        bufferBase = NULL;
        rowDeserializer.setown(rowAllocator->createDiskDeserializer(codeContext));
        bufferStream.setown(createMemoryBufferSerialStream(blockBuffer));
        rowSource.setStream(bufferStream);
    }
    ~RawDataReader()
    {
        if (bufferBase)
            free(bufferBase);
    }
    virtual const void *nextRow() override
    {
        if (eof)
            return NULL;
        if (rowSource.eos() && !reload())
            return NULL;
        if (eogPending)
        {
            eogPending = false;
            return NULL;
        }
#if 0
        // MORE - think a bit about what happens on incomplete rows - I think deserializer will throw an exception?
        unsigned thisSize = meta.getRecordSize(data+cursor);
        if (thisSize > lenData-cursor)
        {
            CTXLOG("invalid stored dataset - incomplete row at end");
            throw MakeStringException(ROXIE_DATA_ERROR, "invalid stored dataset - incomplete row at end");
        }
#endif
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t size = rowDeserializer->deserialize(rowBuilder, rowSource);
        if (isGrouped)
            rowSource.read(sizeof(bool), &eogPending);
        return rowBuilder.finalizeRowClear(size);
    }
};

class InlineRawDataReader : public RawDataReader
{
    Linked<IPropertyTree> xml;
public:
    InlineRawDataReader(ICodeContext *codeContext, IEngineRowAllocator *_rowAllocator, bool _isGrouped, const IRoxieContextLogger &_logctx, IPropertyTree *_xml)
        : RawDataReader(codeContext, _rowAllocator, _isGrouped, _logctx), xml(_xml)
    {
    }

    virtual bool nextBlock(unsigned & tlen, void * & tgt, void * & base) override
    {
        base = tgt = NULL;
        if (xml)
        {
            MemoryBuffer result;
            xml->getPropBin(NULL, result);
            tlen = result.length();
            base = tgt = result.detach();
            xml.clear();
            return tlen != 0;
        }
        else
        {
            tlen = 0;
            return false;
        }
    }
};

class StreamedRawDataReader : public RawDataReader
{
    SafeSocket &client;
    StringAttr id;
    offset_t offset;
public:
    StreamedRawDataReader(ICodeContext *codeContext, IEngineRowAllocator *_rowAllocator, bool _isGrouped, const IRoxieContextLogger &_logctx, SafeSocket &_client, const char *_id)
        : RawDataReader(codeContext, _rowAllocator, _isGrouped, _logctx), client(_client), id(_id)
    {
        offset = 0;
    }

    virtual bool nextBlock(unsigned & tlen, void * & tgt, void * & base) override
    {
        try
        {
#ifdef FAKE_EXCEPTIONS
            if (offset > 0x10000)
                throw MakeStringException(ROXIE_INTERNAL_ERROR, "TEST EXCEPTION");
#endif
            // Go request from the socket
            MemoryBuffer request;
            request.reserve(sizeof(size32_t));
            request.append('D');
            offset_t loffset = offset;
            _WINREV(loffset);
            request.append(sizeof(loffset), &loffset);
            request.append(strlen(id)+1, id);
            size32_t len = request.length() - sizeof(size32_t);
            len |= 0x80000000;
            _WINREV(len);
            *(size32_t *) request.toByteArray() = len;
            client.write(request.toByteArray(), request.length());

            // Note: I am the only thread reading (we only support a single input dataset in roxiepipe mode)
            MemoryBuffer reply;
            client.readBlocktms(reply, readTimeout*1000, INFINITE);
            tlen = reply.length();
            // MORE - not very robust!
            // skip past block header
            if (tlen > 0)
            {
                tgt = base = reply.detach();
                tgt = ((char *)base) + 9;
                tgt = strchr((char *)tgt, '\0') + 1;
                tlen -= ((char *)tgt - (char *)base);
                offset += tlen;
            }
            else
                tgt = base = NULL;

            return tlen != 0;
        }
        catch (IException *E)
        {
            StringBuffer text;
            E->errorMessage(text);
            int errCode = E->errorCode();
            E->Release();
            IException *ee = MakeStringException(MSGAUD_programmer, errCode, "%s", text.str());
            logctx.logOperatorException(ee, __FILE__, __LINE__, "Exception caught in RawDataReader::nextBlock");
            throw ee;
        }
        catch (...)
        {
            logctx.logOperatorException(NULL, __FILE__, __LINE__, "Unknown exception caught in RawDataReader::nextBlock");
            throw;
        }
    }
};

class WuResultDataReader : public RawDataReader
{
    Owned<IConstWUResult> result;
    IXmlToRowTransformer *rowTransformer;
public:
    WuResultDataReader(ICodeContext *codeContext, IEngineRowAllocator *_rowAllocator, bool _isGrouped, const IRoxieContextLogger &_logctx, IConstWUResult *_result, IXmlToRowTransformer *_rowTransformer)
        : RawDataReader(codeContext, _rowAllocator, _isGrouped, _logctx), result(_result), rowTransformer(_rowTransformer)
    {
    }

    virtual bool nextBlock(unsigned & tlen, void * & tgt, void * & base) override
    {
        tgt = NULL;
        base = NULL;
        if (result)
        {
            Variable2IDataVal r(&tlen, &tgt);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(rowTransformer);
            result->getResultRaw(r, rawXmlTransformer, NULL);
            base = tgt;
            result.clear();
            return tlen != 0;
        }
        else
        {
            tlen = 0;
            return false;
        }
    }
};

class InlineXmlDataReader : public WorkUnitRowReaderBase
{
    Linked<IPropertyTree> xml;
    Owned <XmlColumnProvider> columns;
    Owned<IPropertyTreeIterator> rows;
    IXmlToRowTransformer &rowTransformer;
public:
    InlineXmlDataReader(IXmlToRowTransformer &_rowTransformer, IPropertyTree *_xml, IEngineRowAllocator *_rowAllocator, bool _isGrouped)
        : WorkUnitRowReaderBase(_rowAllocator, _isGrouped), xml(_xml), rowTransformer(_rowTransformer)
    {
        columns.setown(new XmlDatasetColumnProvider);
        rows.setown(xml->getElements("Row")); // NOTE - the 'hack for Gordon' as found in thorxmlread is not implemented here. Does it need to be ?
        rows->first();
    }

    virtual const void *nextRow() override
    {
        if (rows->isValid())
        {
            columns->setRow(&rows->query());
            rows->next();
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            NullDiskCallback callback;
            size_t outSize = rowTransformer.transform(rowBuilder, columns, &callback);
            return rowBuilder.finalizeRowClear(outSize);
        }
        return NULL;
    }
};

//---------------------------------------------------------------------------------------

static const StatisticsMapping graphStatistics({});
class CRoxieContextBase : implements IRoxieAgentContext, implements ICodeContext, implements roxiemem::ITimeLimiter, implements IRowAllocatorMetaActIdCacheCallback, public CInterface
{
protected:
    Owned<IWUGraphStats> graphStats;   // This needs to be destroyed very late (particularly, after the childgraphs)
    mutable Owned<IRowAllocatorMetaActIdCache> allocatorMetaCache;
    Owned<IRowManager> rowManager; // NOTE: the order of destruction here is significant. For leak check to work destroy this BEFORE allAllocators, but after most other things
    Owned <IDebuggableContext> debugContext;
    const IQueryFactory *factory;
    Owned<IProbeManager> probeManager; // must be destroyed after childGraphs
    MapXToMyClass<unsigned, unsigned, IActivityGraph> childGraphs;
    Owned<IActivityGraph> graph;
    StringBuffer authToken;
    unsigned lastWuAbortCheck;
    unsigned startTime;
    std::atomic<unsigned> totAgentsReplyLen = {0};
    std::atomic<unsigned> totAgentsDuplicates = {0};
    std::atomic<unsigned> totAgentsResends = {0};
    CCycleTimer elapsedTimer;

    QueryOptions options;
    Owned<IConstWorkUnit> workUnit;
    Owned<IConstWorkUnit> statsWu;
    Owned<IRoxieDaliHelper> daliHelperLink;
    Owned<IDistributedFileTransaction> superfileTransaction;
    IArrayOf<IQueryFactory> loadedLibraries;

    const IRoxieContextLogger &logctx;


protected:
    bool exceptionLogged;
    std::atomic<bool> aborted;
    CriticalSection abortLock; // NOTE: we don't bother to get lock when just reading to see whether to abort
    Owned<IException> exception;

    static void _toXML(IPropertyTree *tree, StringBuffer &xgmml, unsigned indent)
    {
        if (tree->getPropInt("att[@name='_roxieStarted']/@value", 1) == 0)
            return;
        if (0 && tree->getPropInt("att[@name='_kind']/@value", 0) == 496)
        {
            Owned<IPropertyTreeIterator> sub = tree->getElements(".//att[@name='_roxieStarted']");
            bool foundOne = false;
            ForEach(*sub)
            {
                if (sub->query().getPropInt("@value", 1)==0)
                {
                    foundOne = true;
                    break;
                }
            }
            if (!foundOne)
                return;
        }

        const char *name = tree->queryName();
        xgmml.pad(indent).append('<').append(name);
        Owned<IAttributeIterator> it = tree->getAttributes(true);
        if (it->first())
        {
            do
            {
                const char *key = it->queryName();
                xgmml.append(' ').append(key+1).append("=\"");
                encodeXML(it->queryValue(), xgmml, ENCODE_NEWLINES);
                xgmml.append("\"");
            }
            while (it->next());
        }
        Owned<IPropertyTreeIterator> sub = tree->getElements("*", iptiter_sort);
        if (!sub->first())
        {
            xgmml.append("/>\n");
        }
        else
        {
            xgmml.append(">\n");
            for(; sub->isValid(); sub->next())
                _toXML(&sub->query(), xgmml, indent+1);
            xgmml.pad(indent).append("</").append(name).append(">\n");
        }
    }

public:
    IMPLEMENT_IINTERFACE;
    CRoxieContextBase(const IQueryFactory *_factory, const IRoxieContextLogger &_logctx)
        : factory(_factory), options(factory->queryOptions()), logctx(_logctx), globalStats(graphStatistics)
    {
        startTime = lastWuAbortCheck = msTick();
        persists = NULL;
        temporaries = NULL;
        deserializedResultStore = NULL;
        rereadResults = NULL;
        xmlStoredDatasetReadFlags = ptr_none;
        aborted = false;
        exceptionLogged = false;
        totAgentsReplyLen = 0;
        totAgentsDuplicates = 0;
        totAgentsResends = 0;

        allocatorMetaCache.setown(createRowAllocatorCache(this));
        rowManager.setown(roxiemem::createRowManager(options.memoryLimit, this, logctx, allocatorMetaCache, false));
        //MORE: If checking heap required then should have
        //rowManager.setown(createCheckingHeap(rowManager)) or something similar.
    }
    ~CRoxieContextBase()
    {
        ::Release(rereadResults);
        ::Release(persists);
        ::Release(temporaries);
        ::Release(deserializedResultStore);
    }

    // interface IRoxieServerContext
    virtual bool collectingDetailedStatistics() const override
    {
        return (workUnit != nullptr) || (statsWu != nullptr);
    }

    virtual unsigned getElapsedMs() const override
    {
        return msTick() - startTime;
    }

    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value) const override
    {
        logctx.noteStatistic(kind, value);
    }

    virtual void setStatistic(StatisticKind kind, unsigned __int64 value) const override
    {
        logctx.setStatistic(kind, value);
    }

    virtual void mergeStats(unsigned activityId, const CRuntimeStatisticCollection &from) const override
    {
        logctx.mergeStats(activityId, from);
    }

    virtual void gatherStats(CRuntimeStatisticCollection & merged) const override
    {
        logctx.gatherStats(merged);
    }

    virtual StringBuffer &getStats(StringBuffer &ret) const override
    {
        return logctx.getStats(ret);
    }
    virtual void recordStatistics(IStatisticGatherer &progress) const override
    {
        globalStats.recordStatistics(progress, false);
    }
    virtual void CTXLOGa(TracingCategory category, const LogMsgCategory & cat, LogMsgCode code, const char *prefix, const char *text) const override
    {
        logctx.CTXLOGa(category, cat, code, prefix, text);
    }

    virtual void CTXLOGaeva(IException *E, const char *file, unsigned line, const char *prefix, const char *format, va_list args) const
    {
        logctx.CTXLOGaeva(E, file, line, prefix, format, args);
    }

    virtual void CTXLOGl(LogItem *log) const
    {
        logctx.CTXLOGl(log);
    }

    virtual unsigned logString(const char *text) const
    {
        if (text && *text)
        {
            CTXLOG("USER: %s", text);
            return strlen(text);
        }
        else
            return 0;
    }

    virtual const IContextLogger &queryContextLogger() const
    {
        return logctx;
    }

    virtual StringBuffer &getLogPrefix(StringBuffer &ret) const
    {
        logctx.getLogPrefix(ret);
        return ret.append(':').append(factory->queryQueryName());
    }

    virtual bool isIntercepted() const
    {
        return logctx.isIntercepted();
    }

    virtual bool isBlind() const
    {
        return logctx.isBlind();
    }

    virtual unsigned queryTraceLevel() const
    {
        return logctx.queryTraceLevel();
    }
    virtual ISpan * queryActiveSpan() const override
    {
        return logctx.queryActiveSpan();
    }
    virtual void setActiveSpan(ISpan * span) override
    {
        const_cast<IRoxieContextLogger&>(logctx).setActiveSpan(span);
    }
    virtual IProperties * getClientHeaders() const override
    {
        return logctx.getClientHeaders();
    }
    virtual IProperties * getSpanContext() const override
    {
        return logctx.getSpanContext();
    }
    virtual void setSpanAttribute(const char *name, const char *value) const override
    {
        logctx.setSpanAttribute(name, value);
    }
    virtual void setSpanAttribute(const char *name, __uint64 value) const override
    {
        logctx.setSpanAttribute(name, value);
    }
    virtual const char *queryGlobalId() const override
    {
        return logctx.queryGlobalId();
    }
    virtual const char *queryCallerId() const override
    {
        return logctx.queryCallerId();
    }
    virtual const char *queryLocalId() const override
    {
        return logctx.queryLocalId();
    }
    virtual const CRuntimeStatisticCollection & queryStats() const override
    {
        return logctx.queryStats();
    }
    virtual void noteLibrary(IQueryFactory *library)
    {
        loadedLibraries.appendUniq(*LINK(library));
    }
    virtual void checkAbort() override
    {
        // MORE - really should try to apply limits at agent end too
#ifdef __linux__
        if (linuxYield)
            sched_yield();
#endif
        if (aborted) // NOTE - don't bother getting lock before reading this (for speed) - a false read is very unlikely and not a problem
        {
            CriticalBlock b(abortLock);
            if (!exception)
                exception.setown(MakeStringException(ROXIE_INTERNAL_ERROR, "Query was aborted"));
            throw exception.getLink();
        }

        if (graph)
            graph->checkAbort();
        if (options.timeLimit && (msTick() - startTime > options.timeLimit))
        {
            unsigned oldLimit = options.timeLimit;
            //timeLimit = 0; // to prevent exceptions in cleanup - this means only one arm gets stopped!
            CriticalBlock b(abortLock);
            IException *E = MakeStringException(ROXIE_TIMEOUT, "Query %s exceeded time limit (%d ms) - terminated", factory->queryQueryName(), oldLimit);
            if (!exceptionLogged)
            {
                logOperatorException(E, NULL, 0, NULL);
                exceptionLogged = true;
            }
            throw E;
        }
        if (workUnit && (msTick() - lastWuAbortCheck > 5000))
        {
            CriticalBlock b(abortLock);
            if (workUnit->aborting())
            {
                if (!exception)
                    exception.setown(MakeStringException(ROXIE_INTERNAL_ERROR, "Query was aborted"));
                throw exception.getLink();
            }
            lastWuAbortCheck = msTick();
        }
    }

    virtual unsigned checkInterval() const
    {
        unsigned interval = MAX_ABORT_CHECK_INTERVAL;
        if (options.timeLimit)
        {
            interval = options.timeLimit / 10;
            if (interval < MIN_ABORT_CHECK_INTERVAL)
                interval = MIN_ABORT_CHECK_INTERVAL;
            if (interval > MAX_ABORT_CHECK_INTERVAL)
                interval = MAX_ABORT_CHECK_INTERVAL;
        }
        return interval;
    }

    virtual void notifyAbort(IException *E) override
    {
        CriticalBlock b(abortLock);
        if (!aborted && QUERYINTERFACE(E, InterruptedSemaphoreException) == NULL)
        {
            aborted = true;
            exception.set(E);
            setWUState(WUStateAborting);
        }
    }

    virtual void notifyException(IException *E) override
    {
        CriticalBlock b(abortLock);
        if (!exception && QUERYINTERFACE(E, InterruptedSemaphoreException) == NULL)
        {
            exception.set(E);
        }
    }

    virtual void throwPendingException() override
    {
        CriticalBlock b(abortLock);
        if (exception)
            throw exception.getClear();
    }

    virtual void setWUState(WUState state)
    {
        if (workUnit)
        {
            WorkunitUpdate w(&workUnit->lock());
            w->setState(state);
        }
    }

    virtual bool checkWuAborted()
    {
        return workUnit && workUnit->aborting();
    }

    virtual const QueryOptions &queryOptions() const
    {
        return options;
    }

    virtual cycle_t queryElapsedCycles() const
    {
        return elapsedTimer.elapsedCycles();
    }

    const char *queryAuthToken()
    {
        return authToken.str();
    }

    virtual void noteChildGraph(unsigned id, IActivityGraph *childGraph)
    {
        childGraphs.setValue(id, childGraph);
    }

    virtual IActivityGraph *getLibraryGraph(const LibraryCallFactoryExtra &extra, IRoxieServerActivity *parentActivity)
    {
        if (extra.embedded)
        {
            return factory->lookupGraph(this, extra.embeddedGraphName, probeManager, *this, parentActivity);
        }
        else
        {
            Owned<IQueryFactory> libraryQuery = factory->lookupLibrary(extra.libraryName, extra.interfaceHash, *this);
            assertex(libraryQuery);
            parentActivity->noteLibrary(libraryQuery);
            IActivityGraph *ret = libraryQuery->lookupGraph(this, "graph1", probeManager, *this, parentActivity);
            ret->setPrefix(libraryQuery->queryQueryName());
            return ret;
        }
    }

    void beginGraph(const char *graphName)
    {
        if (debugContext)
        {
            probeManager.clear(); // Hack!
            probeManager.setown(createDebugManager(debugContext, graphName));
            debugContext->checkBreakpoint(DebugStateGraphCreate, NULL, graphName);
        }
        graph.setown(factory->lookupGraph(this, graphName, probeManager, *this, NULL));
        graph->onCreate(NULL);  // MORE - is that right
        if (debugContext)
            debugContext->checkBreakpoint(DebugStateGraphStart, NULL, graphName);
        if (workUnit)
            graphStats.setown(workUnit->updateStats(graph->queryName(), SCTroxie, queryStatisticsComponentName(), graph->queryWorkflowId(), 0, false));
        else if (statsWu)
            graphStats.setown(statsWu->updateStats(graph->queryName(), SCTroxie, queryStatisticsComponentName(), graph->queryWorkflowId(), 0, true));
    }

    IWorkUnit *updateStatsWorkUnit() const
    {
        if (workUnit)
            return &workUnit->lock();
        else if (statsWu)
            return &statsWu->lock();
        else
            return nullptr;
    }

    virtual void endGraph(const ProcessInfo & startProcessInfo, unsigned __int64 startTimeStamp, cycle_t startCycles, bool aborting)
    {
        if (graph)
        {
            IException * error = NULL;
            try
            {
                unsigned __int64 elapsedTime = cycle_to_nanosec(get_cycles_now() - startCycles);
                if (debugContext)
                    debugContext->checkBreakpoint(aborting ? DebugStateGraphAbort : DebugStateGraphEnd, NULL, graph->queryName());
                if (aborting)
                    graph->abort();
                if (workUnit || statsWu)
                {
                    const char * graphName = graph->queryName();
                    StringBuffer graphDesc;
                    formatGraphTimerLabel(graphDesc, graphName);
                    WorkunitUpdate progressWorkUnit(updateStatsWorkUnit());
                    StringBuffer graphScope;
                    graphScope.append(WorkflowScopePrefix).append(graph->queryWorkflowId()).append(":").append(graphName);
                    progressWorkUnit->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTgraph, graphScope, StWhenStarted, NULL, startTimeStamp, 1, 0, StatsMergeAppend);
                    updateWorkunitStat(progressWorkUnit, SSTgraph, graphScope, StTimeElapsed, graphDesc, elapsedTime);
                    addTimeStamp(progressWorkUnit, SSTgraph, graphName, StWhenFinished, graph->queryWorkflowId());

                    ProcessInfo endProcessInfo(ReadAllInfo);
                    SystemProcessInfo delta = endProcessInfo - startProcessInfo;
                    updateWorkunitStat(progressWorkUnit, SSTgraph, graphScope, StTimeUser, graphDesc, delta.getUserNs());
                    updateWorkunitStat(progressWorkUnit, SSTgraph, graphScope, StTimeSystem, graphDesc, delta.getSystemNs());
                    updateWorkunitStat(progressWorkUnit, SSTgraph, graphScope, StNumContextSwitches, graphDesc, delta.getNumContextSwitches());
                    updateWorkunitStat(progressWorkUnit, SSTgraph, graphScope, StSizeMemory, graphDesc, endProcessInfo.getActiveResidentMemory());
                    updateWorkunitStat(progressWorkUnit, SSTgraph, graphScope, StSizePeakMemory, graphDesc, endProcessInfo.getPeakResidentMemory());
                }
                graph->reset();
            }
            catch (IException * e)
            {
                error = e;
            }
            cleanupGraphs();
            graphStats.clear();
            if (error)
                throw error;
        }
    }

    void cleanupGraphs()
    {
        IStatisticGatherer * builder = nullptr;
        if (graphStats)
            builder = &graphStats->queryStatsBuilder();

        if (graph)
            graph->gatherStatistics(builder);

        graph.clear();
        childGraphs.kill();
    }

    void runGraph()
    {
        graph->execute();
    }

    virtual void executeGraph(const char * name, bool realThor, size32_t parentExtractSize, const void * parentExtract)
    {
        assertex(parentExtractSize == 0);
        if (realThor)
        {
            Owned<IPropertyTree> compConfig = getComponentConfig();
            executeThorGraph(name, *workUnit, *compConfig);
        }
        else
        {
            OwnedActiveSpanScope graphScope = queryThreadedActiveSpan()->createInternalSpan(name);
            ProcessInfo startProcessInfo;
            if (workUnit || statsWu)
                startProcessInfo.update(ReadAllInfo);
            bool created = false;
            cycle_t startCycles = get_cycles_now();
            unsigned __int64 startTimeStamp = getTimeStampNowValue();
            try
            {
                beginGraph(name);
                created = true;
                runGraph();
            }
            catch (IException *e)
            {
                if (e->errorAudience() == MSGAUD_operator)
                    EXCLOG(e, "Exception thrown in query - cleaning up");  // if an IException is throw let EXCLOG determine if a trap should be generated
                else
                {
                    StringBuffer s;
                    CTXLOG("Exception thrown in query - cleaning up: %d: %s", e->errorCode(), e->errorMessage(s).str());
                }
                if (created)  // Partially-created graphs are liable to crash if you call abort() on them...
                    endGraph(startProcessInfo, startTimeStamp, startCycles, true);
                else
                {
                    // Bit of a hack... needed to avoid pure virtual calls if these are left to the CRoxieContextBase destructor
                    cleanupGraphs();
                }
                CTXLOG("Done cleaning up");
                throw;
            }
            catch (...)
            {
                CTXLOG("Exception thrown in query - cleaning up");
                if (created)
                    endGraph(startProcessInfo, startTimeStamp, startCycles, true);
                else
                {
                    // Bit of a hack... needed to avoid pure virtual calls if these are left to the CRoxieContextBase destructor
                    cleanupGraphs();
                }
                CTXLOG("Done cleaning up");
                throw;
            }
            endGraph(startProcessInfo, startTimeStamp, startCycles, false);
        }
    }

    virtual IActivityGraph * queryChildGraph(unsigned  id)
    {
        if (id == 0)
            return graph;
        IActivityGraph *childGraph = childGraphs.getValue(id);
        assertex(childGraph);
        return childGraph;
    }

    virtual IThorChildGraph * resolveChildQuery(__int64 activityId, IHThorArg * colocal)
    {
        // NOTE - part of ICodeContext interface
        return LINK(queryChildGraph((unsigned) activityId)->queryChildGraph());
    }

    virtual IEclGraphResults * resolveLocalQuery(__int64 id)
    {
        return queryChildGraph((unsigned) id)->queryLocalGraph();
    }

    virtual IRowManager &queryRowManager()
    {
        return *rowManager;
    }

    virtual void addAgentsReplyLen(unsigned len, unsigned duplicates, unsigned resends)
    {
        totAgentsReplyLen += len;
        if (duplicates)
            totAgentsDuplicates += duplicates;
        if (resends)
            totAgentsResends += resends;
    }

    virtual const char *loadResource(unsigned id)
    {
        ILoadedDllEntry *dll = factory->queryDll();
        return (const char *) dll->getResource(id);
    }

    virtual ICodeContext *queryCodeContext()
    {
        return this;
    }

    virtual IProbeManager *queryProbeManager() const
    {
        return probeManager;
    }

    virtual IDebuggableContext *queryDebugContext() const
    {
        return debugContext;
    }

    virtual char *getOS()
    {
#ifdef _WIN32
        return strdup("windows");
#else
        return strdup("linux");
#endif
    }

    virtual const void * fromXml(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
    {
        return createRowFromXml(rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
    }
    virtual const void * fromJson(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
    {
        return createRowFromJson(rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
    }
    virtual IEngineContext *queryEngineContext() { return NULL; }
    virtual char *getDaliServers() { throwUnexpected(); }
    // The following from ICodeContext should never be executed in agent activity. If we are on Roxie server, they will be implemented by more derived CRoxieServerContext class
    virtual void setResultBool(const char *name, unsigned sequence, bool value) { throwUnexpected(); }
    virtual void setResultData(const char *name, unsigned sequence, int len, const void * data) { throwUnexpected(); }
    virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val) { throwUnexpected(); }
    virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size) { throwUnexpected(); }
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data) { throwUnexpected(); }
    virtual void setResultReal(const char * stepname, unsigned sequence, double value) { throwUnexpected(); }
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer) { throwUnexpected(); }
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str) { throwUnexpected(); }
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size) { throwUnexpected(); }
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str) { throwUnexpected(); }
    virtual void setResultVarString(const char * name, unsigned sequence, const char * value) { throwUnexpected(); }
    virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value) { throwUnexpected(); }

    virtual unsigned getResultHash(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void printResults(IXmlWriter *output, const char *name, unsigned sequence) { throwUnexpected(); }

    virtual char *getWuid() { throwUnexpected(); }
    virtual unsigned getWorkflowId() const override { return graph->queryWorkflowId(); }
    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }

    virtual char * getExpandLogicalName(const char * logicalName) { throwUnexpected(); }
    virtual void addWuException(const char * text, unsigned code, unsigned severity, const char * source) { throwUnexpected(); }
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort) { throwUnexpected(); }
    virtual IUserDescriptor *queryUserDescriptor() { throwUnexpected(); }

    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash) { throwUnexpected(); }

    virtual unsigned getGraphLoopCounter() const override { return 0; }
    virtual unsigned getNodes() { throwUnexpected(); }
    virtual unsigned getNodeNum() { throwUnexpected(); }
    virtual char *getFilePart(const char *logicalPart, bool create=false) { throwUnexpected(); }
    virtual unsigned __int64 getFileOffset(const char *logicalPart) { throwUnexpected(); }

    virtual IDistributedFileTransaction *querySuperFileTransaction() { throwUnexpected(); }
    virtual char *getJobName() { throwUnexpected(); }
    virtual char *getJobOwner() { throwUnexpected(); }
    virtual char *getClusterName() { throwUnexpected(); }
    virtual char *getGroupName() { throwUnexpected(); }
    virtual char * queryIndexMetaData(char const * lfn, char const * xpath) { throwUnexpected(); }
    virtual unsigned getPriority() const { throwUnexpected(); }
    virtual char *getPlatform() { throwUnexpected(); }
    virtual char *getEnv(const char *name, const char *defaultValue) const { throwUnexpected(); }

    virtual IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, unsigned activityId) const
    {
        return allocatorMetaCache->ensure(meta, activityId, roxiemem::RHFnone);
    }

    virtual IEngineRowAllocator *getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned flags) const
    {
        return allocatorMetaCache->ensure(meta, activityId, (roxiemem::RoxieHeapFlags)flags);
    }

    virtual IEngineRowAllocator *getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, roxiemem::RoxieHeapFlags flags) const
    {
        return allocatorMetaCache->ensure(meta, activityId, flags);
    }

    virtual const char *cloneVString(const char *str) const
    {
        return rowManager->cloneVString(str);
    }

    virtual const char *cloneVString(size32_t len, const char *str) const
    {
        return rowManager->cloneVString(len, str);
    }

    virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
    {
        convertRowToXML(lenResult, result, info, row, flags);
    }
    virtual void getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
    {
        convertRowToJSON(lenResult, result, info, row, flags);
    }

    virtual IWorkUnit *updateWorkUnit() const
    {
        if (workUnit)
            return &workUnit->lock();
        else
            return NULL;
    }

    virtual IConstWorkUnit *queryWorkUnit() const
    {
        return workUnit;
    }

// roxiemem::IRowAllocatorMetaActIdCacheCallback
    virtual IEngineRowAllocator *createAllocator(IRowAllocatorMetaActIdCache * cache, IOutputMetaData *meta, unsigned activityId, unsigned id, roxiemem::RoxieHeapFlags flags) const
    {
        if (options.checkingHeap)
            return createCrcRoxieRowAllocator(cache, *rowManager, meta, activityId, id, flags);
        else
            return createRoxieRowAllocator(cache, *rowManager, meta, activityId, id, flags);
    }

    virtual void getResultRowset(size32_t & tcount, const byte * * & tgt, const char * stepname, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override
    {
        try
        {
            Owned<IWorkUnitRowReader> wuReader = getWorkunitRowReader(NULL, stepname, sequence, xmlTransformer, _rowAllocator, isGrouped);
            wuReader->getResultRowset(tcount, tgt);
        }
        catch (IException * e)
        {
            StringBuffer text;
            e->errorMessage(text);
            e->Release();
            throw MakeStringException(ROXIE_DATA_ERROR, "Failed to retrieve data value \"%s\".  [%s]", stepname, text.str());
        }
        catch (...)
        {
            throw MakeStringException(ROXIE_DATA_ERROR, "Failed to retrieve data value \"%s\"", stepname);
        }
    }

    virtual void getResultDictionary(size32_t & tcount, const byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) override
    {
        try
        {
            Owned<IWorkUnitRowReader> wuReader = getWorkunitRowReader(NULL, stepname, sequence, xmlTransformer, _rowAllocator, false);
            wuReader->getResultRowset(tcount, tgt);
        }
        catch (IException * e)
        {
            StringBuffer text;
            e->errorMessage(text);
            e->Release();
            throw MakeStringException(ROXIE_DATA_ERROR, "Failed to retrieve data value \"%s\".  [%s]", stepname, text.str());
        }
        catch (...)
        {
            throw MakeStringException(ROXIE_DATA_ERROR, "Failed to retrieve data value \"%s\"", stepname);
        }
    }

    virtual bool getResultBool(const char * name, unsigned sequence)
    {
        CriticalBlock b(contextCrit);
        return useContext(sequence).getPropBool(name);
    }

    virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence)
    {
        MemoryBuffer result;
        CriticalBlock b(contextCrit);
        const char *val = useContext(sequence).queryProp(name);
        if (val)
        {
            for (;;)
            {
                char c0 = *val++;
                if (!c0)
                    break;
                char c1 = *val++;
                if (!c1)
                    break; // Shouldn't really happen - we expect even length
                unsigned c2 = (hex2digit(c0) << 4) | hex2digit(c1);
                result.append((unsigned char) c2);
            }
        }
        tlen = result.length();
        tgt = result.detach();
    }
    virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence)
    {
        memset(tgt, 0, tlen);
        CriticalBlock b(contextCrit);
        IPropertyTree &ctx = useContext(sequence);
        if (ctx.hasProp(stepname))
        {
            if (ctx.isBinary(stepname))
            {
                MemoryBuffer m;
                ctx.getPropBin(stepname, m);
                if (m.length())
                {
                    assertex(m.length() == tlen);
                    m.read(tlen, tgt);
                }
            }
            else
            {
                const char *val = ctx.queryProp(stepname);
                Decimal d;
                d.setCString(val);
                if (isSigned)
                    d.getDecimal(tlen, precision, tgt);
                else
                    d.getUDecimal(tlen, precision, tgt);
            }
        }
    }
    virtual __int64 getResultInt(const char * name, unsigned sequence)
    {
        CriticalBlock b(contextCrit);
        const char *val = useContext(sequence).queryProp(name);
        if (val)
        {
            // NOTE - we use this rather than getPropInt64 since it handles uint64 values up to MAX_UINT better (for our purposes)
            return rtlStrToInt8(strlen(val), val);
        }
        else
            return 0;
    }
    virtual double getResultReal(const char * name, unsigned sequence)
    {
        CriticalBlock b(contextCrit);
        IPropertyTree &ctx = useContext(sequence);
        double ret = 0;
        if (ctx.hasProp(name))
        {
            if (ctx.isBinary(name))
            {
                MemoryBuffer buf;
                ctx.getPropBin(name, buf);
                buf.read(ret);
            }
            else
            {
                const char *val = ctx.queryProp(name);
                if (val)
                    ret = atof(val);
            }
        }
        return ret;
    }
    virtual void getResultSet(bool & tisAll, unsigned & tlen, void * & tgt, const char *stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        try
        {
            CriticalBlock b(contextCrit);
            IPropertyTree &ctx = useContext(sequence);
            IPropertyTree *val = ctx.queryPropTree(stepname);
            doExtractRawResultX(tlen, tgt, val, sequence, xmlTransformer, csvTransformer, true);
            tisAll = val ? val->getPropBool("@isAll", false) : false;
        }
        catch (IException * e)
        {
            StringBuffer text;
            e->errorMessage(text);
            e->Release();
            throw MakeStringException(ROXIE_DATA_ERROR, "Failed to retrieve set \"%s\".  [%s]", stepname, text.str());
        }
        catch (...)
        {
            throw MakeStringException(ROXIE_DATA_ERROR, "Failed to retrieve set \"%s\"", stepname);
        }
    }
    virtual void getResultRaw(unsigned & tlen, void * & tgt, const char *stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        try
        {
            CriticalBlock b(contextCrit);
            IPropertyTree &ctx = useContext(sequence);
            IPropertyTree *val = ctx.queryPropTree(stepname);
            doExtractRawResultX(tlen, tgt, val, sequence, xmlTransformer, csvTransformer, false);
        }
        catch (IException * e)
        {
            StringBuffer text;
            e->errorMessage(text);
            e->Release();
            throw MakeStringException(ROXIE_DATA_ERROR, "Failed to retrieve data value \"%s\".  [%s]", stepname, text.str());
        }
        catch (...)
        {
            throw MakeStringException(ROXIE_DATA_ERROR, "Failed to retrieve data value \"%s\"", stepname);
        }
    }
    virtual void getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence)
    {
        MemoryBuffer x;
        bool isBinary;
        {
            CriticalBlock b(contextCrit);
            IPropertyTree &ctx = useContext(sequence);
            isBinary = ctx.isBinary(name);
            ctx.getPropBin(name, x);
        }
        if (isBinary)  // No utf8 translation if previously set via setResultString
        {
            tlen = x.length();
            tgt = (char *) x.detach();
        }
        else
            rtlUtf8ToStrX(tlen, tgt, rtlUtf8Length(x.length(), x.toByteArray()), x.toByteArray());
    }
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence)
    {
        MemoryBuffer x;
        bool isBinary;
        {
            CriticalBlock b(contextCrit);
            IPropertyTree &ctx = useContext(sequence);
            isBinary = ctx.isBinary(name);
            ctx.getPropBin(name, x);
        }
        if (isBinary)
            rtlStrToStr(tlen, tgt, x.length(), x.toByteArray());
        else
            rtlUtf8ToStr(tlen, tgt, rtlUtf8Length(x.length(), x.toByteArray()), x.toByteArray());
    }
    virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence)
    {
        StringBuffer x;
        {
            CriticalBlock b(contextCrit);
            useContext(sequence).getProp(name, x);
        }
        tgt = rtlCodepageToVUnicodeX(x.length(), x.str(), "utf-8");
        tlen = rtlUnicodeStrlen(tgt);
    }
    virtual char *getResultVarString(const char * name, unsigned sequence)
    {
        CriticalBlock b(contextCrit);
        IPropertyTree &ctx = useContext(sequence);
        bool isBinary = ctx.isBinary(name);
        if (isBinary)
        {
            StringBuffer s;
            ctx.getProp(name, s);
            return s.detach();
        }
        else
        {
            MemoryBuffer x;
            ctx.getPropBin(name, x);
            return rtlUtf8ToVStr(rtlUtf8Length(x.length(), x.toByteArray()), x.toByteArray());
        }
    }
    virtual UChar *getResultVarUnicode(const char * name, unsigned sequence)
    {
        StringBuffer x;
        {
            CriticalBlock b(contextCrit);
            useContext(sequence).getProp(name, x);
        }
        return rtlVCodepageToVUnicodeX(x.str(), "utf-8");
    }
    virtual ISectionTimer * registerTimer(unsigned activityId, const char * name)
    {
        return registerStatsTimer(activityId, name, 0);
    }
    virtual ISectionTimer * registerStatsTimer(unsigned activityId, const char * name, unsigned int statsOption)
    {
        CriticalBlock b(timerCrit);
        ISectionTimer *timer = functionTimers.getValue(name);
        if (!timer)
        {
            timer = ThorSectionTimer::createTimer(globalStats, name, static_cast<ThorStatOption>(statsOption));
            functionTimers.setValue(name, timer);
            timer->Release(); // Value returned is not linked
        }
        return timer;
    }
    virtual void addWuExceptionEx(const char * text, unsigned code, unsigned severity, unsigned audience, const char * source) override { throwUnexpected(); }

protected:
    mutable CriticalSection contextCrit;
    CriticalSection timerCrit;
    CriticalSection resolveCrit;
    Owned<IPropertyTree> context;
    IPropertyTree *persists;
    IPropertyTree *temporaries;
    IPropertyTree *rereadResults;
    PTreeReaderOptions xmlStoredDatasetReadFlags;
    CDeserializedResultStore *deserializedResultStore;
    MapStringToMyClass<ThorSectionTimer> functionTimers;
    CRuntimeStatisticCollection globalStats;

    IPropertyTree &useContext(unsigned sequence)
    {
        checkAbort();
        switch ((int) sequence)
        {
        case ResultSequenceStored:
            if (context)
                return *context;
            else
                throw MakeStringException(ROXIE_CODEGEN_ERROR, "Code generation error - attempting to access stored variable on agent");
        case ResultSequencePersist:
            {
                contextCrit.assertLocked();
                if (!persists)
                    persists = createPTree(ipt_fast);
                return *persists;
            }
        case ResultSequenceOnce:
            {
                if (!workUnit)
                	return factory->queryOnceContext(logctx);
            }
            //fall through
        case ResultSequenceInternal:
            {
                contextCrit.assertLocked();
                if (!temporaries)
                    temporaries = createPTree(ipt_fast);
                return *temporaries;
            }
        default:
            {
                contextCrit.assertLocked();
                if (!rereadResults)
                    rereadResults = createPTree(ipt_fast);
                return *rereadResults;
            }
        }
    }

    IDeserializedResultStore &useResultStore(unsigned sequence)
    {
        checkAbort();
        switch ((int) sequence)
        {
        case ResultSequenceOnce:
            if (!workUnit)
                return factory->queryOnceResultStore();
            //fall through
        default:
            // No need to have separate stores for other temporaries...
            CriticalBlock b(contextCrit);
            if (!deserializedResultStore)
                deserializedResultStore = new CDeserializedResultStore;
            return *deserializedResultStore;
        }
    }

    void doExtractRawResultX(unsigned & tlen, void * & tgt, IPropertyTree *val, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, bool isSet)
    {
        tgt = NULL;
        tlen = 0;
        if (val)
        {
            if (val->isBinary())
            {
                MemoryBuffer m;
                val->getPropBin(NULL, m);
                tlen = m.length();
                tgt= m.detach();
            }
            else
            {
                const char *format = val->queryProp("@format");
                if (!format || strcmp(format, "xml")==0)
                {
                    assertex(xmlTransformer);
                    Variable2IDataVal result(&tlen, &tgt);
                    CXmlToRawTransformer rawTransformer(*xmlTransformer, xmlStoredDatasetReadFlags);
                    rawTransformer.transformTree(result, *val, !isSet);
                }
                else if (strcmp(format, "deserialized")==0)
                {
                    IDeserializedResultStore &resultStore = useResultStore(sequence);
                    resultStore.serialize(tlen, tgt, val->getPropInt("@id", -1), queryCodeContext());
                }
                else if (strcmp(format, "csv")==0)
                {
                    // MORE - never tested this code.....
                    assertex(csvTransformer);
                    Variable2IDataVal result(&tlen, &tgt);
                    MemoryBuffer m;
                    val->getPropBin(NULL, m);
                    CCsvToRawTransformer rawCsvTransformer(*csvTransformer);
                    rawCsvTransformer.transform(result, m.length(), m.toByteArray(), !isSet);
                }
                else
                    throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "no transform function available");
            }
        }
    }

    virtual IWorkUnitRowReader *createStreamedRawRowReader(IEngineRowAllocator *rowAllocator, bool isGrouped, const char *id)
    {
        throwUnexpected();  // Should only see on server
    }
    virtual IWorkUnitRowReader *getWorkunitRowReader(const char *wuid, const char *stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, IEngineRowAllocator *rowAllocator, bool isGrouped)
    {
        try
        {
            if (wuid)
            {
                Owned<IRoxieDaliHelper> daliHelper = connectToDali();
                if (daliHelper && daliHelper->connected())
                {
                    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                    Owned<IConstWorkUnit> externalWU = factory->openWorkUnit(wuid);
                    if (!externalWU)
                        throw MakeStringException(ROXIE_FILE_ERROR, "Failed to open workunit %s", wuid);
                    externalWU->remoteCheckAccess(queryUserDescriptor(), false);
                    Owned<IConstWUResult> wuResult = getWorkUnitResult(externalWU, stepname, sequence);
                    if (!wuResult)
                        throw MakeStringException(ROXIE_FILE_ERROR, "Failed to find value %s:%d in workunit %s", stepname ? stepname : "(null)", sequence, wuid);
                    return new WuResultDataReader(queryCodeContext(), rowAllocator, isGrouped, logctx, wuResult.getClear(), xmlTransformer);
                }
                else
                    throw MakeStringException(ROXIE_DALI_ERROR, "WorkUnit read: no dali connection available");
            }
            else
            {
                CriticalBlock b(contextCrit);
                IPropertyTree &ctx = useContext(sequence);
                IPropertyTree *val = ctx.queryPropTree(stepname);
                if (val)
                {
                    const char *id = val->queryProp("@id");
                    const char *format = val->queryProp("@format");
                    if (id)
                    {
                        if (!format || strcmp(format, "raw") == 0)
                        {
                            return createStreamedRawRowReader(rowAllocator, isGrouped, id);
                        }
                        else if (strcmp(format, "deserialized") == 0)
                        {
                            IDeserializedResultStore &resultStore = useResultStore(sequence);
                            return resultStore.createDeserializedReader(atoi(id));
                        }
                        else
                            throwUnexpected();
                    }
                    else
                    {
                        if (!format || strcmp(format, "xml") == 0)
                        {
                            if (xmlTransformer)
                                return new InlineXmlDataReader(*xmlTransformer, val, rowAllocator, isGrouped);
                        }
                        else if (strcmp(format, "raw") == 0)
                        {
                            return new InlineRawDataReader(queryCodeContext(), rowAllocator, isGrouped, logctx, val);
                        }
                        else
                            throwUnexpected();
                    }
                }
            }
        }
        catch (IException * e)
        {
            StringBuffer text;
            e->errorMessage(text);
            e->Release();
            throw MakeStringException(ROXIE_DATA_ERROR, "Failed to retrieve data value %s.  [%s]", stepname, text.str());
        }
        catch (...)
        {
            throw MakeStringException(ROXIE_DATA_ERROR, "Failed to retrieve data value %s", stepname);
        }
        throw MakeStringException(ROXIE_DATA_ERROR, "Failed to retrieve data value %s", stepname);
    }
};

//-----------------------------------------------------------------------------------------------

class CAgentContext : public CRoxieContextBase
{
protected:
    RoxiePacketHeader *header;

public:
    CAgentContext(const IQueryFactory *_factory, const AgentContextLogger &_logctx, IRoxieQueryPacket *_packet, bool _hasChildren)
    : CRoxieContextBase(_factory, _logctx)
    {
        if (_packet)
        {
            header = &_packet->queryHeader();
            const byte *traceInfo = _packet->queryTraceInfo();
            options.setFromAgentLoggingFlags(*traceInfo);
            bool debuggerActive = (*traceInfo & LOGGING_DEBUGGERACTIVE) != 0 && _hasChildren;  // No option to debug simple remote activity
            if (debuggerActive)
            {
                CAgentDebugContext *agentDebugContext = new CAgentDebugContext(this, logctx, *header);
                agentDebugContext->init(_packet);
                debugContext.setown(agentDebugContext);
                probeManager.setown(createDebugManager(debugContext, "agentDebugger"));
            }
        }
        else
        {
            assertex(selfTestMode);
            header = nullptr;
        }
    }
    virtual void beforeDispose()
    {
        // NOTE: This is needed to ensure that owned activities are destroyed BEFORE I am,
        // to avoid pure virtual calls when they come to call noteProcessed()
        logctx.mergeStats(0, globalStats);
        if (factory)
            factory->mergeStats(logctx);
        childGraphs.releaseAll();
    }

    virtual IRoxieServerContext *queryServerContext()
    {
        return NULL;
    }

    virtual const IResolvedFile *resolveLFN(const char *filename, bool isOpt, bool isPrivilegedUser)
    {
        CDateTime cacheDate; // Note - this is empty meaning we don't know...
        return queryAgentDynamicFileCache()->lookupDynamicFile(*this, filename, cacheDate, 0, header, isOpt, false);
    }

    virtual IRoxieWriteHandler *createWriteHandler(const char *filename, bool overwrite, bool extend, const StringArray &clusters, bool isPrivilegedUser)
    {
        throwUnexpected(); // only support writing on the server
    }

    virtual void onFileCallback(const RoxiePacketHeader &header, const char *lfn, bool isOpt, bool isLocal, bool isPrivilegedUser)
    {
        // On a agent, we need to request info using our own header (not the one passed in) and need to get global rather than just local info
        // (possibly we could get just local if the channel matches but not sure there is any point)
        Owned<const IResolvedFile> dFile = resolveLFN(lfn, isOpt, isPrivilegedUser);
        if (dFile)
        {
            MemoryBuffer mb;
            mb.append(sizeof(RoxiePacketHeader), &header);
            mb.append(lfn);
            dFile->serializePartial(mb, header.channel, isLocal);
            ((RoxiePacketHeader *) mb.toByteArray())->activityId = ROXIE_FILECALLBACK;
            Owned<IRoxieQueryPacket> reply = createRoxiePacket(mb);
            reply->queryHeader().retries = 0;
            ROQ->sendPacket(reply, *this); // MORE - the caller's log context might be better? Should we unicast? Note that this does not release the packet
            return;
        }
        ROQ->sendAbortCallback(header, lfn, *this);
        throwUnexpected();
    }

    virtual void noteProcessed(unsigned subgraphId, unsigned activityId, unsigned _idx, unsigned _processed, unsigned _strands) const
    {
        const AgentContextLogger &agentLogCtx = static_cast<const AgentContextLogger &>(logctx);
        agentLogCtx.putStatProcessed(subgraphId, activityId, _idx, _processed, _strands);
    }

    virtual void mergeActivityStats(const CRuntimeStatisticCollection &fromStats, unsigned subgraphId, unsigned activityId) const
    {
        const AgentContextLogger &agentLogCtx = static_cast<const AgentContextLogger &>(logctx);
        agentLogCtx.putStats(subgraphId, activityId, fromStats);
    }
};

IRoxieAgentContext *createAgentContext(const IQueryFactory *_factory, const AgentContextLogger &_logctx, IRoxieQueryPacket *packet, bool hasChildren)
{
    return new CAgentContext(_factory, _logctx, packet, hasChildren);
}

//-----------------------------------------------------------------------------------------------

class CRoxieServerDebugContext : extends CBaseServerDebugContext
{
    // Some questions:
    // 1. Do we let all threads go even when say step? Probably... (may allow a thread to be suspended at some point)
    // 2. Doesn't that then make a bit of a mockery of step (when there are multiple threads active)... I _think_ it actually means we DON'T try to wait for all
    //    threads to hit a stop, but allow any that hit stop while we are paused to be queued up to be returned by step.... perhaps actually stop them in critsec rather than
    //    semaphore and it all becomes easier to code... Anything calling checkBreakPoint while program state is "in debugger" will block on that critSec.
    // 3. I think we need to recheck breakpoints on Roxie server but just check not deleted
public:
    IRoxieAgentContext *ctx;

    CRoxieServerDebugContext(IRoxieAgentContext *_ctx, const IContextLogger &_logctx, IPropertyTree *_queryXGMML)
        : CBaseServerDebugContext(_logctx, _queryXGMML), ctx(_ctx)
    {
    }

    void debugCounts(IXmlWriter *output, unsigned sinceSequence, bool reset)
    {
        CriticalBlock b(debugCrit);
        if (running)
            throw MakeStringException(ROXIE_DEBUG_ERROR, "Command not available while query is running");

        if (currentGraph)
            currentGraph->mergeRemoteCounts(this);
        CBaseServerDebugContext::debugCounts(output, sinceSequence, reset);
    }

    virtual void waitForDebugger(DebugState state, IActivityDebugContext *probe)
    {
        ctx->setWUState(WUStateDebugPaused);
        CBaseServerDebugContext::waitForDebugger(state, probe);
        ctx->setWUState(WUStateDebugRunning);
    }

    virtual bool onDebuggerTimeout()
    {
        return ctx->checkWuAborted();
    }

    virtual void debugInitialize(const char *id, const char *_queryName, bool _breakAtStart)
    {
        CBaseServerDebugContext::debugInitialize(id, _queryName, _breakAtStart);
        queryRoxieDebugSessionManager().registerDebugId(id, this);
    }

    virtual void debugTerminate()
    {
        CriticalBlock b(debugCrit);
        assertex(running);
        currentState = DebugStateUnloaded;
        running = false;
        queryRoxieDebugSessionManager().deregisterDebugId(debugId);
        if (debuggerActive)
        {
            debuggerSem.signal(debuggerActive);
            debuggerActive = 0;
        }
    }

    virtual IRoxieQueryPacket *onDebugCallback(const RoxiePacketHeader &header, size32_t len, char *data)
    {
        MemoryBuffer agentInfo;
        agentInfo.setBuffer(len, data, false);
        unsigned debugSequence;
        agentInfo.read(debugSequence);
        {
            CriticalBlock b(breakCrit); // we want to wait until it's our turn before updating the graph info or the counts get ahead of the current row and life is confusing
            char agentStateChar;
            agentInfo.read(agentStateChar);
            DebugState agentState = (DebugState) agentStateChar;
            if (agentState==DebugStateGraphFinished)
            {
                unsigned numCounts;
                agentInfo.read(numCounts);
                while (numCounts)
                {
                    StringAttr edgeId;
                    unsigned edgeCount;
                    agentInfo.read(edgeId);
                    agentInfo.read(edgeCount);
                    Owned<IGlobalEdgeRecord> thisEdge = getEdgeRecord(edgeId);
                    thisEdge->incrementCount(edgeCount, sequence);
                    numCounts--;
                }
            }
            agentInfo.read(currentBreakpointUID);
            memsize_t agentActivity;
            unsigned channel;
            __uint64 tmp;
            agentInfo.read(tmp);
            agentActivity = (memsize_t)tmp;
            agentInfo.read(channel);
            assertex(currentGraph);
            currentGraph->deserializeProxyGraphs(agentState, agentInfo, (IActivityBase *) agentActivity, channel);
            if (agentState != DebugStateGraphFinished) // MORE - this is debatable - may (at least sometimes) want a child graph finished to be a notified event...
            {
                StringBuffer agentActivityId;
                agentInfo.read(agentActivityId);
                IActivityDebugContext *agentActivityCtx = agentActivityId.length() ? currentGraph->lookupActivityByEdgeId(agentActivityId.str()) : NULL;
                checkBreakpoint(agentState, agentActivityCtx , NULL);
            }
        }
        MemoryBuffer mb;
        mb.append(sizeof(RoxiePacketHeader), &header);
        StringBuffer debugIdString;
        debugIdString.appendf(".debug.%x", debugSequence);
        mb.append(debugIdString.str());
        serialize(mb);

        Owned<IRoxieQueryPacket> reply = createRoxiePacket(mb);
        reply->queryHeader().activityId = ROXIE_DEBUGCALLBACK;
        reply->queryHeader().retries = 0;
        return reply.getClear();
    }

    virtual void debugPrintVariable(IXmlWriter *output, const char *name, const char *type) const
    {
        CriticalBlock b(debugCrit);
        if (running)
            throw MakeStringException(ROXIE_DEBUG_ERROR, "Command not available while query is running");
        output->outputBeginNested("Variables", true);
        if (!type || strieq(type, "temporary"))
        {
            output->outputBeginNested("Temporary", true);
            ctx->printResults(output, name, (unsigned) ResultSequenceInternal);
            output->outputEndNested("Temporary");
        }
        if (!type || strieq(type, "global"))
        {
            output->outputBeginNested("Global", true);
            ctx->printResults(output, name, (unsigned) ResultSequenceStored);
            output->outputEndNested("Global");
        }
        output->outputEndNested("Variables");
    }

};

enum LogActReset { LogResetSkip=0, LogResetOK, LogResetInit };
static constexpr char hexchar[] = "0123456789ABCDEF";

class CRoxieServerContext : public CRoxieContextBase, implements IRoxieServerContext, implements IGlobalCodeContext, implements IEngineContext
{
    const IQueryFactory *serverQueryFactory = nullptr;
    IHpccProtocolResponse *protocol = nullptr;
    IHpccProtocolResultsWriter *results = nullptr;
    IHpccNativeProtocolResponse *nativeProtocol = nullptr;
    CriticalSection daliUpdateCrit;
    StringAttr querySetName;

    bool isRaw;
    bool sendHeartBeats;
    unsigned lastSocketCheckTime;
    unsigned lastHeartBeat;

protected:
    Owned<CRoxieWorkflowMachine> workflow;
    Owned<ITimeReporter> myTimer;
    mutable MapStringToMyClass<IResolvedFile> fileCache;
    StringArray clusterNames;
    int clusterWidth = -1;

    bool isBlocked;
    bool isNative;
    bool trim;
    LogActReset actResetLogState = LogResetInit;

    void doPostProcess()
    {
        logctx.mergeStats(0, globalStats);
        logctx.setStatistic(StTimeTotalExecute, elapsedTimer.elapsedNs());
        if (factory)
        {
            factory->mergeStats(logctx);
        }
        globalStats.reset();
        if (!protocol)
            return;

        if (!isRaw && !isBlocked)
            protocol->flush();
    }
    void addWuException(IException *E)
    {
        if (workUnit)
            ::addWuException(workUnit, E);
        else if (statsWu)
            ::addWuException(statsWu, E);
    }

    void init()
    {
        totAgentsReplyLen = 0;
        totAgentsDuplicates = 0;
        totAgentsResends = 0;
        isRaw = false;
        isBlocked = false;
        isNative = true;
        sendHeartBeats = false;
        trim = false;

        lastSocketCheckTime = startTime;
        lastHeartBeat = startTime;
        myTimer.setown(createStdTimeReporter());
    }

    void startWorkUnit()
    {
        WorkunitUpdate wu(&workUnit->lock());
        wu->subscribe(SubscribeOptionAbort);
        addTimeStamp(wu, SSTglobal, NULL, StWhenStarted);
        if (!context->getPropBool("@outputToSocket", false))
            protocol = NULL;
        updateSuppliedXmlParams(wu);
        SCMStringBuffer wuParams;
        if (workUnit->getXmlParams(wuParams, false).length())
        {
            // Merge in params from WU. Ones on command line take precedence though...
            Owned<IPropertyTree> wuParamTree = createPTreeFromXMLString(wuParams.str(), ipt_caseInsensitive|ipt_fast);
            Owned<IPropertyTreeIterator> params = wuParamTree->getElements("*");
            ForEach(*params)
            {
                IPropertyTree &param = params->query();
                if (!context->hasProp(param.queryName()))
                    context->addPropTree(param.queryName(), LINK(&param));
            }
        }
        if (workUnit->getDebugValueBool("Debug", false))
        {
            bool breakAtStart = workUnit->getDebugValueBool("BreakAtStart", true);
            wu->setState(WUStateDebugRunning);
            initDebugMode(breakAtStart, workUnit->queryWuid());
        }
        else
            wu->setState(WUStateRunning);
        clusterNames.append(workUnit->queryClusterName());
        clusterWidth = -1;
    }

    void initDebugMode(bool breakAtStart, const char *debugUID)
    {
        if (!debugPermitted || !debugEndpoint.port || nativeProtocol)
            throw MakeStringException(ROXIE_ACCESS_ERROR, "Debug query not permitted here");
        debugContext.setown(new CRoxieServerDebugContext(this, logctx, factory->cloneQueryXGMML()));
        debugContext->debugInitialize(debugUID, factory->queryQueryName(), breakAtStart);
        if (workUnit)
        {
            WorkunitUpdate wu(&workUnit->lock());
            wu->setDebugAgentListenerPort(debugEndpoint.port); //tells debugger what port to write commands to
            StringBuffer sb;
            debugEndpoint.getHostText(sb);
            wu->setDebugAgentListenerIP(sb); //tells debugger what IP to write commands to
        }
        options.timeLimit = 0;
        options.warnTimeLimit = 0;
    }

public:
    IMPLEMENT_IINTERFACE;

    CRoxieServerContext(const IQueryFactory *_factory, const IRoxieContextLogger &_logctx)
        : CRoxieContextBase(_factory, _logctx), serverQueryFactory(_factory), results(NULL)
    {
        init();
        rowManager->setMemoryLimit(options.memoryLimit);
        workflow.setown(_factory->createWorkflowMachine(workUnit, true, logctx, options));
        context.setown(createPTree(ipt_caseInsensitive|ipt_fast));
    }

    CRoxieServerContext(IConstWorkUnit *_workUnit, const IQueryFactory *_factory, const ContextLogger &_logctx)
        : CRoxieContextBase(_factory, _logctx), serverQueryFactory(_factory), results(NULL)
    {
        init();
        workUnit.set(_workUnit);
        rowManager->setMemoryLimit(options.memoryLimit);
        workflow.setown(_factory->createWorkflowMachine(workUnit, false, logctx, options));
        context.setown(createPTree(ipt_caseInsensitive|ipt_fast));

        //MORE: Use various debug settings to override settings:
        rowManager->setActivityTracking(workUnit->getDebugValueBool("traceRoxiePeakMemory", false));

        startWorkUnit();
    }

    CRoxieServerContext(IPropertyTree *_context, IHpccProtocolResponse *_protocol, const IQueryFactory *_factory, unsigned flags, const ContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags, const char *_querySetName)
        : CRoxieContextBase(_factory, _logctx), serverQueryFactory(_factory), protocol(_protocol), results(NULL), querySetName(_querySetName)
    {
        init();
        if (protocol)
        {
            nativeProtocol = dynamic_cast<IHpccNativeProtocolResponse*>(protocol);
            results = protocol->queryHpccResultsSection();
        }
        context.set(_context);
        options.setFromContext(context);
        isNative = (flags & HPCC_PROTOCOL_NATIVE);
        isRaw = (flags & HPCC_PROTOCOL_NATIVE_RAW);
        isBlocked = (flags & HPCC_PROTOCOL_BLOCKED);
        trim = (flags & HPCC_PROTOCOL_TRIM);

        xmlStoredDatasetReadFlags = _xmlReadFlags;
        sendHeartBeats = enableHeartBeat && isRaw && isBlocked && options.priority==0;

        const char *wuid = context->queryProp("@wuid");
        if (options.statsToWorkunit)
        {
            IRoxieDaliHelper *daliHelper = checkDaliConnection();
            if (daliHelper->connected())
            {
                statsWu.setown(daliHelper->createStatsWorkUnit(wuid, _factory->queryDll()->queryName()));
                WorkunitUpdate wu(&statsWu->lock());
                wu->setState(WUStateRunning);
                VStringBuffer jobname("Stats for %s", _factory->queryQueryName());
                const char *statsID = context->queryProp("@statsId");
                if (!isEmptyString(statsID))
                    jobname.appendf(" (%s)", statsID);
                wu->setJobName(jobname);
                logctx.CTXLOG("Created wu %s for query statistics", statsWu->queryWuid());
            }
        }
        else if (context->getPropBool("@debug", false))
        {
            bool breakAtStart = context->getPropBool("@break", true);
            const char *debugUID = context->queryProp("@uid");
            if (debugUID && *debugUID)
                initDebugMode(breakAtStart, debugUID);
        }

        // MORE some of these might be appropriate in wu case too?
        rowManager->setActivityTracking(context->getPropBool("_TraceMemory", false));
        rowManager->setMemoryLimit(options.memoryLimit);

        workflow.setown(_factory->createWorkflowMachine(workUnit, false, logctx, options));
    }

    virtual roxiemem::IRowManager &queryRowManager()
    {
        return *rowManager;
    }

    virtual IRowAllocatorMetaActIdCache & queryAllocatorCache() override
    {
        return *allocatorMetaCache;
    }

    virtual IRoxieDaliHelper *checkDaliConnection()
    {
        CriticalBlock b(daliUpdateCrit);
        if (!daliHelperLink)
            daliHelperLink.setown(::connectToDali());
        return daliHelperLink;
    }

    virtual void checkAbort()
    {
        CRoxieContextBase::checkAbort();
        unsigned ticksNow = msTick();
        if (options.warnTimeLimit)
        {
            unsigned elapsed = ticksNow - startTime;
            if  (elapsed > options.warnTimeLimit)
            {
                CriticalBlock b(abortLock);
                if (elapsed > options.warnTimeLimit) // we don't want critsec on the first check (for efficiency) but check again inside the critsec
                {
                    logOperatorException(NULL, NULL, 0, "SLOW (%d ms): %s", elapsed, factory->queryQueryName());
                    options.warnTimeLimit = elapsed + options.warnTimeLimit;
                }
            }
        }
        if (protocol)
        {
            if (socketCheckInterval)
            {
                if (ticksNow - lastSocketCheckTime >= socketCheckInterval)
                {
                    CriticalBlock b(abortLock);
                    if (!protocol->checkConnection())
                    {
                        DBGLOG("Client socket close detected");
                        throw MakeStringException(ROXIE_CLIENT_CLOSED, "Client socket closed");
                    }
                    lastSocketCheckTime = ticksNow;
                }
            }
            if (sendHeartBeats)
            {
                unsigned hb = ticksNow - lastHeartBeat;
                if (hb > 30000)
                {
                    lastHeartBeat = msTick();
                    protocol->sendHeartBeat();
                }
            }
        }
    }

    virtual unsigned getXmlFlags() const
    {
        return trim ? XWFtrim|XWFopt : XWFexpandempty;
    }

    virtual const IProperties *queryXmlns(unsigned seqNo)
    {
        IConstWorkUnit *cw = serverQueryFactory->queryWorkUnit();
        if (cw)
        {
            Owned<IConstWUResult> result = cw->getResultBySequence(seqNo);
            if (result)
                return result->queryResultXmlns();  // This is not safe - result is (theoretically, if not actually) freed!
        }
        return NULL;
    }
    virtual memsize_t getMemoryUsage()
    {
        return rowManager->getMemoryUsage();
    }

    virtual unsigned getAgentsReplyLen() const
    {
        return totAgentsReplyLen;
    }

    virtual unsigned getAgentsDuplicates() const
    {
        return totAgentsDuplicates;
    }

    virtual unsigned getAgentsResends() const
    {
        return totAgentsResends;
    }

    virtual void process()
    {
        MTIME_SECTION(myTimer, "Process");
        QueryTerminationCleanup threadCleanup(true);
        EclProcessFactory pf = (EclProcessFactory) factory->queryDll()->getEntry("createProcess");
        Owned<IEclProcess> p = pf();
        try
        {
            if (debugContext)
                debugContext->checkBreakpoint(DebugStateReady, NULL, NULL);
            if (workflow)
                workflow->perform(this, p);
            else
            {
                GlobalCodeContextExtra gctx(this, 0);
                p->perform(&gctx, 0);
            }
        }
        catch(WorkflowException *E)
        {
            if (debugContext)
                debugContext->checkBreakpoint(DebugStateException, NULL, static_cast<IException *>(E));
            addWuException(E);
            doPostProcess();
            throw;
        }
        catch(IException *E)
        {
            if (debugContext)
                debugContext->checkBreakpoint(DebugStateException, NULL, E);
            addWuException(E);
            doPostProcess();
            throw;
        }
        catch(...)
        {
            if (debugContext)
                debugContext->checkBreakpoint(DebugStateFailed, NULL, NULL);
            if (workUnit)
            {
                Owned<IException> E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception caught in CRoxieServerContext::process");
                addWuException(E);
            }
            doPostProcess();
            throw;
        }
        if (debugContext)
            debugContext->checkBreakpoint(DebugStateFinished, NULL, NULL);
        doPostProcess();
    }

    void setWuStats(IWorkUnit *w, bool failed)
    {
        if (aborted)
            w->setState(WUStateAborted);
        else if (failed)
            w->setState(WUStateFailed);
        else if (workflow && workflow->hasItemsWaiting())
            w->setState(WUStateWait);
        else
            w->setState(WUStateCompleted);
        addTimeStamp(w, SSTglobal, NULL, StWhenFinished);
        updateWorkunitTimings(w, myTimer);
        Owned<IStatisticGatherer> gatherer = createGlobalStatisticGatherer(w);
        CRuntimeStatisticCollection merged(accumulatedStatistics);
        logctx.gatherStats(merged);
        merged.recordStatistics(*gatherer, false);

        //MORE: If executed more than once (e.g., scheduled), then TimeElapsed isn't particularly correct.
        gatherer->updateStatistic(StTimeElapsed, elapsedTimer.elapsedNs(), StatsMergeReplace);

        WuStatisticTarget statsTarget(w, "roxie");
        rowManager->reportPeakStatistics(statsTarget, 0);
    }

    virtual void done(bool failed)
    {
        if (debugContext)
            debugContext->debugTerminate();
        if (workUnit)
        {
#ifdef _CONTAINERIZED
            // signal to any lingering Thor's that job is complete and they can quit before timeout.
            executeGraphOnLingeringThor(*workUnit, 0, nullptr);
#endif

            if (options.failOnLeaks && !failed)
            {
                cleanupGraphs();
                probeManager.clear();
                ::Release(deserializedResultStore);
                deserializedResultStore = nullptr;
                if (rowManager && rowManager->allocated())
                {
                    rowManager->reportLeaks();
                    failed = true;
                    Owned <IException> E = makeStringException(ROXIE_INTERNAL_ERROR, "Row leaks detected");
                    ::addWuException(workUnit, E);
                }
            }
            WorkunitUpdate w(&workUnit->lock());
            setWuStats(w, failed);
            if(w->queryEventScheduledCount() > 0 && w->getState() != WUStateWait)
            {
                try
                {
                    w->deschedule();
                }
                catch(IException * e)
                {
                    int code = e->errorCode();
                    VStringBuffer msg("Failed to deschedule workunit %s: ", w->queryWuid());
                    e->errorMessage(msg);
                    addExceptionToWorkunit(w, SeverityError, "Roxie", code, msg.str(), NULL, 0, 0, 0);
                    e->Release();
                    OWARNLOG("%s (%d)", msg.str(), code);
                }
            }
            while (clusterNames.ordinality())
                restoreCluster();
        }
        else if (statsWu)
        {
            WorkunitUpdate w(&statsWu->lock());
            setWuStats(w, failed);
        }
    }

    virtual ICodeContext *queryCodeContext()
    {
        return this;
    }

    virtual IRoxieServerContext *queryServerContext()
    {
        return this;
    }
    virtual const IQueryFactory *queryQueryFactory() const override
    {
        return factory;
    }


    virtual IGlobalCodeContext *queryGlobalCodeContext()
    {
        return this;
    }

    virtual char *getDaliServers()
    {
        try
        {
            IRoxieDaliHelper *daliHelper = checkDaliConnection();
            if (daliHelper)
            {
                StringBuffer ip;
                daliHelper->getDaliIp(ip);
                return ip.detach();
            }
        }
        catch (IException *E)
        {
            E->Release();
        }
        return strdup("");
    }
    virtual IHpccProtocolResponse *queryProtocol()
    {
        return protocol;
    }
    virtual IEngineContext *queryEngineContext() { return this; }

    virtual DALI_UID getGlobalUniqueIds(unsigned num, SocketEndpoint *_foreignNode)
    {
        if (num==0)
            return 0;
        SocketEndpoint foreignNode;
        if (_foreignNode && !_foreignNode->isNull())
            foreignNode.set(*_foreignNode);
        else
        {
            Owned<IRoxieDaliHelper> dali = ::connectToDali();
            if (!dali)
                return 0;
            StringBuffer daliIp;
            dali->getDaliIp(daliIp);
            foreignNode.set(daliIp.str());
        }
        return ::getGlobalUniqueIds(num, &foreignNode);
    }
    virtual bool allowDaliAccess() const
    {
        Owned<IRoxieDaliHelper> dali = ::connectToDali();
        return dali != nullptr;
    }
    virtual bool allowSashaAccess() const
    {
        return nullptr != workUnit; // allow if dynamic query only
    }
    virtual StringBuffer &getQueryId(StringBuffer &result, bool isShared) const
    {
        if (workUnit)
        {
            if (isShared)
                result.append('Q');
            result.append(workUnit->queryWuid());
        }
        else if (isShared)
            result.append('Q').append(factory->queryHash());
        else
            logctx.getLogPrefix(result);
        return result;
    }
    virtual void getManifestFiles(const char *type, StringArray &files) const override
    {
        ILoadedDllEntry *dll = factory->queryDll();
        StringBuffer id;
        const StringArray &dllFiles = dll->queryManifestFiles(type, getQueryId(id, true).str(), tempDirectory);
        ForEachItemIn(idx, dllFiles)
            files.append(dllFiles.item(idx));
        ForEachItemIn(lidx, loadedLibraries)
        {
            IQueryFactory &lfactory = loadedLibraries.item(lidx);
            ILoadedDllEntry *ldll = lfactory.queryDll();
            // Libraries share the same copy of the jar (and the classloader) for all queries that use the library
            StringBuffer lid;
            lid.append('Q').append(lfactory.queryHash());
            const StringArray &ldllFiles = ldll->queryManifestFiles(type, lid.str(), tempDirectory);
            if (ldllFiles.length())
                files.append(nullptr);
            ForEachItemIn(ldidx, ldllFiles)
                files.append(ldllFiles.item(ldidx));
        }
    }

    mutable CIArrayOf<TerminationCallbackInfo> callbacks;
    mutable CriticalSection callbacksCrit;

    virtual void onTermination(QueryTermCallback callback, const char *key, bool isShared) const
    {
        TerminationCallbackInfo *term(new TerminationCallbackInfo(callback, key));
        if (isShared)
            factory->onTermination(term);
        else
        {
            CriticalBlock b(callbacksCrit);
            callbacks.append(*term);
        }
    }

    virtual void setResultBool(const char *name, unsigned sequence, bool value)
    {
        if (isSpecialResultSequence(sequence))
        {
            CriticalBlock b(contextCrit);
            useContext(sequence).setPropBool(name, value);
        }
        else if (results)
        {
            results->setResultBool(name, sequence, value);
        }

        if (workUnit)
        {
            try
            {
                WorkunitUpdate wu(&workUnit->lock());
                CriticalBlock b(daliUpdateCrit);  // MORE - do we really need these locks?
                wu->setResultBool(name, sequence, value);
            }
            catch(IException *e)
            {
                StringBuffer text;
                e->errorMessage(text);
                CTXLOG("Error trying to update dali: %s", text.str());
                e->Release();
            }
            catch(...)
            {
                CTXLOG("Unknown exception trying to update dali");
            }
        }
    }
    virtual void setResultData(const char *name, unsigned sequence, int len, const void * data)
    {
        if (isSpecialResultSequence(sequence))
        {
            StringBuffer s;
            const byte *field = (const byte *) data;
            for (int i = 0; i < len; i++)
                s.append(hexchar[field[i] >> 4]).append(hexchar[field[i] & 0x0f]);
            CriticalBlock b(contextCrit);
            IPropertyTree &ctx = useContext(sequence);
            ctx.setProp(name, s.str());
        }
        else if (results)
        {
            results->setResultData(name, sequence, len, data);
        }
        if (workUnit)
        {
            try
            {
                WorkunitUpdate wu(&workUnit->lock());
                CriticalBlock b(daliUpdateCrit);
                wu->setResultData(name, sequence, len, data);
            }
            catch(IException *e)
            {
                StringBuffer text;
                e->errorMessage(text);
                CTXLOG("Error trying to update dali: %s", text.str());
                e->Release();
            }
            catch(...)
            {
                CTXLOG("Unknown exception trying to update dali");
            }
        }
    }
    virtual void appendResultDeserialized(const char *name, unsigned sequence, size32_t count, rowset_t data, bool extend, IOutputMetaData *meta) override
    {
        CriticalBlock b(contextCrit);
        IPropertyTree &ctx = useContext(sequence);
        IDeserializedResultStore &resultStore = useResultStore(sequence);
        IPropertyTree *val = ctx.queryPropTree(name);
        if (extend && val)
        {
            int oldId = val->getPropInt("@id", -1);
            const char * oldFormat = val->queryProp("@format");
            assertex(oldId != -1);
            assertex(oldFormat && strcmp(oldFormat, "deserialized")==0);
            size32_t oldCount;
            rowset_t oldData;
            resultStore.queryResult(oldId, oldCount, oldData);
            Owned<IEngineRowAllocator> allocator = getRowAllocator(meta, 0);
            RtlLinkedDatasetBuilder builder(allocator);
            builder.appendRows(oldCount, oldData);
            builder.appendRows(count, data);
            rtlReleaseRowset(count, data);
            val->setPropInt("@id", resultStore.addResult(builder.getcount(), builder.linkrows(), meta));
        }
        else
        {
            if (!val)
                val = ctx.addPropTree(name, createPTree(ipt_fast));
            val->setProp("@format", "deserialized");
            val->setPropInt("@id", resultStore.addResult(count, data, meta));
        }

    }
    virtual void appendResultRawContext(const char *name, unsigned sequence, int len, const void * data, int numRows, bool extend, bool saveInContext)
    {
        if (saveInContext)
        {
            CriticalBlock b(contextCrit);
            IPropertyTree &ctx = useContext(sequence);
            ctx.appendPropBin(name, len, data);
            ctx.queryPropTree(name)->setProp("@format", "raw");
        }

        if (workUnit)
        {
            try
            {
                WorkunitUpdate wu(&workUnit->lock());
                CriticalBlock b(daliUpdateCrit);
                wu->setResultDataset(name, sequence, len, data, numRows, extend);
            }
            catch(IException *e)
            {
                StringBuffer text;
                e->errorMessage(text);
                CTXLOG("Error trying to update dali: %s", text.str());
                e->Release();
            }
            catch(...)
            {
                CTXLOG("Unknown exception trying to update dali");
            }
        }
    }
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data)
    {
        if (isSpecialResultSequence(sequence))
        {
            CriticalBlock b(contextCrit);
            IPropertyTree &ctx = useContext(sequence);
            ctx.setPropBin(name, len, data);
            ctx.queryPropTree(name)->setProp("@format", "raw");
        }
        else if (results)
        {
            results->setResultRaw(name, sequence, len, data);
        }

        if (workUnit)
        {
            try
            {
                WorkunitUpdate wu(&workUnit->lock());
                CriticalBlock b(daliUpdateCrit);
                wu->setResultRaw(name, sequence, len, data);
            }
            catch(IException *e)
            {
                StringBuffer text;
                e->errorMessage(text);
                CTXLOG("Error trying to update dali: %s", text.str());
                e->Release();
            }
            catch(...)
            {
                CTXLOG("Unknown exception trying to update dali");
            }
        }
    }
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer)
    {
        if (isSpecialResultSequence(sequence))
        {
            CriticalBlock b(contextCrit);
            IPropertyTree &ctx = useContext(sequence);
            ctx.setPropBin(name, len, data);
            ctx.queryPropTree(name)->setProp("@format", "raw");
            ctx.queryPropTree(name)->setPropBool("@isAll", isAll);
        }
        else if (results)
        {
            results->setResultSet(name, sequence, isAll, len, data, transformer);
        }

        if (workUnit)
        {
            try
            {
                WorkunitUpdate wu(&workUnit->lock());
                CriticalBlock b(daliUpdateCrit);
                wu->setResultSet(name, sequence, isAll, len, data, transformer);
            }
            catch(IException *e)
            {
                StringBuffer text;
                e->errorMessage(text);
                CTXLOG("Error trying to update dali: %s", text.str());
                e->Release();
            }
            catch(...)
            {
                CTXLOG("Unknown exception trying to update dali");
            }
        }
    }
    virtual void setResultXml(const char *name, unsigned sequence, const char *xml)
    {
        Owned<IPropertyTree> tree = createPTreeFromXMLString(xml, ipt_caseInsensitive|ipt_fast);
        CriticalBlock b(contextCrit);
        useContext(sequence).setPropTree(name, tree.getClear());
    }

    virtual void setResultDecimal(const char *name, unsigned sequence, int len, int precision, bool isSigned, const void *val)
    {
        if (isSpecialResultSequence(sequence))
        {
            MemoryBuffer m;
            serializeFixedData(len, val, m);
            CriticalBlock b(contextCrit);
            useContext(sequence).setPropBin(name, m.length(), m.toByteArray());
        }
        else if (results)
        {
            results->setResultDecimal(name, sequence, len, precision, isSigned, val);
        }
        if (workUnit)
        {
            try
            {
                WorkunitUpdate wu(&workUnit->lock());
                CriticalBlock b(daliUpdateCrit);
                wu->setResultDecimal(name, sequence, len, precision, isSigned, val);
            }
            catch(IException *e)
            {
                StringBuffer text;
                e->errorMessage(text);
                CTXLOG("Error trying to update dali: %s", text.str());
                e->Release();
            }
            catch(...)
            {
                CTXLOG("Unknown exception trying to update dali");
            }
        }
    }
    virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size)
    {
        if (isSpecialResultSequence(sequence))
        {
            CriticalBlock b(contextCrit);
            useContext(sequence).setPropInt64(name, value);
        }
        else if (results)
        {
            results->setResultInt(name, sequence, value, size);
        }
        if (workUnit)
        {
            try
            {
                WorkunitUpdate wu(&workUnit->lock());
                CriticalBlock b(daliUpdateCrit);
                wu->setResultInt(name, sequence, value);
            }
            catch(IException *e)
            {
                StringBuffer text;
                e->errorMessage(text);
                CTXLOG("Error trying to update dali: %s", text.str());
                e->Release();
            }
            catch(...)
            {
                CTXLOG("Unknown exception trying to update dali");
            }
        }
    }

    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size)
    {
        if (isSpecialResultSequence(sequence))
        {
            CriticalBlock b(contextCrit);
            useContext(sequence).setPropInt64(name, value);
        }
        else if (results)
        {
            results->setResultUInt(name, sequence, value, size);
        }

        if (workUnit)
        {
            try
            {
                WorkunitUpdate wu(&workUnit->lock());
                CriticalBlock b(daliUpdateCrit);
                wu->setResultUInt(name, sequence, value);
            }
            catch(IException *e)
            {
                StringBuffer text;
                e->errorMessage(text);
                CTXLOG("Error trying to update dali: %s", text.str());
                e->Release();
            }
            catch(...)
            {
                CTXLOG("Unknown exception trying to update dali");
            }
        }
    }

    virtual void setResultReal(const char *name, unsigned sequence, double value)
    {
        if (isSpecialResultSequence(sequence))
        {
            CriticalBlock b(contextCrit);
            useContext(sequence).setPropBin(name, sizeof(value), &value);
        }
        else if (results)
        {
            results->setResultReal(name, sequence, value);
        }
        if (workUnit)
        {
            try
            {
                WorkunitUpdate wu(&workUnit->lock());
                CriticalBlock b(daliUpdateCrit);
                wu->setResultReal(name, sequence, value);
            }
            catch(IException *e)
            {
                StringBuffer text;
                e->errorMessage(text);
                CTXLOG("Error trying to update dali: %s", text.str());
                e->Release();
            }
            catch(...)
            {
                CTXLOG("Unknown exception trying to update dali");
            }
        }
    }
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str)
    {
        if (isSpecialResultSequence(sequence))
        {
            CriticalBlock b(contextCrit);
            useContext(sequence).setPropBin(name, len, str);
        }
        else if (results)
        {
            results->setResultString(name, sequence, len, str);
        }
        if (workUnit)
        {
            try
            {
                WorkunitUpdate wu(&workUnit->lock());
                CriticalBlock b(daliUpdateCrit);
                wu->setResultString(name, sequence, len, str);
            }
            catch(IException *e)
            {
                StringBuffer text;
                e->errorMessage(text);
                CTXLOG("Error trying to update dali: %s", text.str());
                e->Release();
            }
            catch(...)
            {
                CTXLOG("Unknown exception trying to update dali");
            }
        }
    }
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str)
    {
        if (isSpecialResultSequence(sequence))
        {
            rtlDataAttr buff;
            unsigned bufflen = 0;
            rtlUnicodeToCodepageX(bufflen, buff.refstr(), len, str, "utf-8");
            CriticalBlock b(contextCrit);
            useContext(sequence).setPropBin(name, bufflen, buff.getstr());
        }
        else if (results)
        {
            results->setResultUnicode(name, sequence, len, str);
        }
        if (workUnit)
        {
            try
            {
                WorkunitUpdate wu(&workUnit->lock());
                CriticalBlock b(daliUpdateCrit);
                wu->setResultUnicode(name, sequence, len, str);
            }
            catch(IException *e)
            {
                StringBuffer text;
                e->errorMessage(text);
                CTXLOG("Error trying to update dali: %s", text.str());
                e->Release();
            }
            catch(...)
            {
                CTXLOG("Unknown exception trying to update dali");
            }
        }
    }
    virtual void setResultVarString(const char * name, unsigned sequence, const char * value)
    {
        setResultString(name, sequence, strlen(value), value);
    }
    virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value)
    {
        setResultUnicode(name, sequence, rtlUnicodeStrlen(value), value);
    }
    virtual void setResultDataset(const char * name, unsigned sequence, size32_t len, const void *data, unsigned numRows, bool extend)
    {
        appendResultRawContext(name, sequence, len, data, numRows, extend, true);
    }

    virtual IWorkUnitRowReader *createStreamedRawRowReader(IEngineRowAllocator *rowAllocator, bool isGrouped, const char *id)
    {
        if (!nativeProtocol)
            throwUnexpected();
        return new StreamedRawDataReader(this, rowAllocator, isGrouped, logctx, *nativeProtocol->querySafeSocket(), id);
    }

    virtual void printResults(IXmlWriter *output, const char *name, unsigned sequence)
    {
        CriticalBlock b(contextCrit);
        IPropertyTree &tree = useContext(sequence);
        if (name)
        {
            const char *val = tree.queryProp(name);
            if (val)
                output->outputCString(val, name);
        }
        else
        {
            StringBuffer hack;
            toXML(&tree, hack);
            output->outputString(0, NULL, NULL); // Hack upon hack...
            output->outputQuoted(hack.str());
        }
    }

    virtual const IResolvedFile *resolveLFN(const char *fileName, bool isOpt, bool isPrivilegedUser)
    {
        StringBuffer expandedName;
        expandLogicalFilename(expandedName, fileName, workUnit, false, false);

        CriticalBlock b(resolveCrit);
        Linked<const IResolvedFile> ret = fileCache.getValue(expandedName);
        if (!ret)
        {
            ret.setown(factory->queryPackage().lookupFileName(fileName, isOpt, false, false, workUnit, false, isPrivilegedUser));
            if (ret)
            {
                IResolvedFile *add = const_cast<IResolvedFile *>(ret.get());
                fileCache.setValue(expandedName, add);
            }
        }
        return ret.getClear();
    }

    virtual IRoxieWriteHandler *createWriteHandler(const char *filename, bool overwrite, bool extend, const StringArray &clusters, bool isPrivilegedUser)
    {
        return factory->queryPackage().createWriteHandler(filename, overwrite, extend, clusters, workUnit, isPrivilegedUser);
    }

    virtual void endGraph(const ProcessInfo & startProcessInfo, unsigned __int64 startTimeStamp, cycle_t startCycles, bool aborting) override
    {
        fileCache.kill();
        CRoxieContextBase::endGraph(startProcessInfo, startTimeStamp, startCycles, aborting);
    }

    virtual void onFileCallback(const RoxiePacketHeader &header, const char *lfn, bool isOpt, bool isLocal, bool isPrivilegedUser)
    {
        Owned<const IResolvedFile> dFile = resolveLFN(lfn, isOpt, isPrivilegedUser);
        if (dFile)
        {
            MemoryBuffer mb;
            mb.append(sizeof(RoxiePacketHeader), &header);
            mb.append(lfn);
            dFile->serializePartial(mb, header.channel, isLocal);
            ((RoxiePacketHeader *) mb.toByteArray())->activityId = ROXIE_FILECALLBACK;
            Owned<IRoxieQueryPacket> reply = createRoxiePacket(mb);
            reply->queryHeader().retries = 0;
            ROQ->sendPacket(reply, *this); // MORE - the caller's log context might be better? Should we unicast? Note that this does not release the packet
            return;
        }
        ROQ->sendAbortCallback(header, lfn, *this);
        throwUnexpected();
    }

    IConstWUResult *getExternalResult(const char * wuid, const char *name, unsigned sequence)
    {
        Owned <IRoxieDaliHelper> daliHelper = connectToDali();
        if (daliHelper && daliHelper->connected())
        {
            Owned<IConstWorkUnit> externalWU = daliHelper->attachWorkunit(wuid);
            if (externalWU)
            {
                externalWU->remoteCheckAccess(queryUserDescriptor(), false);
                return getWorkUnitResult(externalWU, name, sequence);
            }
            else
            {
                throw MakeStringException(0, "Missing or invalid workunit name %s in getExternalResult()", nullText(wuid));
            }
        }
        else
            throw MakeStringException(ROXIE_DALI_ERROR, "WorkUnit read: no dali connection available");
    }

    unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence)
    {
        Owned<IConstWUResult> r = getExternalResult(wuid, name, sequence);
        if (!r)
            throw MakeStringException(0, "Failed to retrieve hash value %s from workunit %s", name, wuid);
        return r->getResultHash();
    }

    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        UNIMPLEMENTED;
    }

    virtual void addWuException(const char * text, unsigned code, unsigned _severity, const char * source)
    {
        addWuExceptionEx(text, code, _severity, MSGAUD_operator, source);
    }
    virtual void addWuExceptionEx(const char * text, unsigned code, unsigned _severity, unsigned audience, const char * source)
    {
        ErrorSeverity severity = (ErrorSeverity) _severity;
        CTXLOG("%s", text);
        if (severity > SeverityInformation)
            LOG(mapToLogMsgCategory(severity, (MessageAudience)audience), "%d - %s", code, text);

        if (workUnit || statsWu)
        {
            WorkunitUpdate wu(updateStatsWorkUnit());
            addExceptionToWorkunit(wu, severity, source, code, text, NULL, 0 ,0, 0);
        }
    }
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort)
    {
        CTXLOG("%s", text);
        OERRLOG("%d - %s", code, text);
        if (workUnit || statsWu)
        {
            WorkunitUpdate wu(updateStatsWorkUnit());
            addExceptionToWorkunit(wu, SeverityError, "user", code, text, filename, lineno, column, 0);
        }
        if (isAbort)
            throw makeStringException(MSGAUD_user, code, text);
    }
    IUserDescriptor *queryUserDescriptor()
    {
        if (workUnit)
            return workUnit->queryUserDescriptor();//ad-hoc mode
        else
        {
            Owned<IRoxieDaliHelper> daliHelper = connectToDali(false);
            if (daliHelper)
                return daliHelper->queryUserDescriptor();//predeployed query mode
        }
        return NULL;
    }

    virtual bool isResult(const char * name, unsigned sequence)
    {
        CriticalBlock b(contextCrit);
        return useContext(sequence).hasProp(name);
    }
    virtual unsigned getWorkflowIdDeprecated() override { throwUnexpected(); }
    virtual char *getClusterName()
    {
        if (workUnit)
        {
            return strdup(workUnit->queryClusterName());
        }
        else
        {
            // predeployed queries with no workunit should return the querySet name
            return strdup(querySetName.str()); // StringAttr::str()  will return "" rather than NULL
        }
    }
    virtual char *getGroupName()
    {
#ifdef _CONTAINERIZED
        // in a containerized setup, the group is moving..
        return strdup("unknown");
#else
        StringBuffer groupName;
        if (workUnit && clusterNames.length())
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
                StringBufferAdaptor a(groupName);
                clusterInfo->getRoxieProcess(a);
            }
        }
        return groupName.detach();
#endif
    }
    virtual char *queryIndexMetaData(char const * lfn, char const * xpath) { throwUnexpected(); }
    virtual char *getEnv(const char *name, const char *defaultValue) const
    {
        return serverQueryFactory->getEnv(name, defaultValue);
    }
    virtual char *getJobName()
    {
        if (workUnit)
        {
            return strdup(workUnit->queryJobName());
        }
        return strdup(factory->queryQueryName());
    }
    virtual char *getJobOwner()
    {
        if (workUnit)
        {
            return strdup(workUnit->queryUser());
        }
        return strdup("");
    }
    virtual char *getPlatform()
    {
#ifdef _CONTAINERIZED
        /* NB: platform specs. are defined if agent is running in the context of
         * another engine, e.g. query has been submitted to Thor, but some code is
        * executing outside of it.
        *
        * If not defined then assumed to be executing in roxie context,
        * where platform() defaults to "roxie".
        */
        StringBuffer type;
        if (!topology->getProp("platform/@type", type))
            type.set("roxie");
        return type.detach();
#else
        if (clusterNames.length())
        {
            const char * cluster = clusterNames.tos();
            Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster);
            if (!clusterInfo)
                throw MakeStringException(-1, "Unknown Cluster '%s'", cluster);
            return strdup(clusterTypeString(clusterInfo->getPlatform(), false));
        }
        else
            return strdup("roxie");
#endif
    }
    virtual char *getWuid()
    {
        if (workUnit)
        {
            return strdup(workUnit->queryWuid());
        }
        else
        {
            return strdup("");
        }
    }

    // persist-related code
    virtual char * getExpandLogicalName(const char * logicalName)
    {
        StringBuffer lfn;
        expandLogicalFilename(lfn, logicalName, workUnit, false, false);
        return lfn.detach();
    }
    virtual void setWorkflowCondition(bool value) { if(workflow) workflow->setCondition(value); }
    virtual void returnPersistVersion(const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile)
    {
        if (workflow)
            workflow->returnPersistVersion(logicalName, eclCRC, allCRC, isFile);
    }
    virtual void fail(int code, const char *text)
    {
        addWuExceptionEx(text, code, 2, MSGAUD_user, "user");
    }

    void doNotify(char const * name, char const * text)
    {
        doNotify(name, text, NULL);
    }

    void doNotify(char const * name, char const * text, const char * target)
    {
        Owned<IRoxieDaliHelper> daliHelper = connectToDali();
        if (daliHelper && daliHelper->connected())
        {
            Owned<IScheduleEventPusher> pusher(getScheduleEventPusher());
            pusher->push(name, text, target);
        }
        else
            throw MakeStringException(ROXIE_DALI_ERROR, "doNotify: no dali connection available");
    }

    virtual unsigned __int64 getDatasetHash(const char * logicalName, unsigned __int64 crc)
    {
        StringBuffer fullname;
        expandLogicalFilename(fullname, logicalName, workUnit, false, false);
        Owned<IDistributedFile> file = wsdfs::lookup(fullname.str(),queryUserDescriptor(),AccessMode::readMeta,false,false,nullptr,defaultPrivilegedUser,INFINITE);
        if (file)
        {
            WorkunitUpdate wu = updateWorkUnit();
            wu->noteFileRead(file);
            crc = crcLogicalFileTime(file, crc, fullname);
        }
        return crc;
    }

    virtual int queryLastFailCode()
    {
        if(!workflow)
            return 0;
        return workflow->queryLastFailCode();
    }
    virtual void getLastFailMessage(size32_t & outLen, char * &outStr, const char * tag)
    {
        const char * text = "";
        if(workflow)
            text = workflow->queryLastFailMessage();
        rtlExceptionExtract(outLen, outStr, text, tag);
    }
    virtual void getEventName(size32_t & outLen, char * & outStr)
    {
        const char * text = "";
        if(workflow)
            text = workflow->queryEventName();
        rtlExtractTag(outLen, outStr, text, NULL, "Event");
    }
    virtual void getEventExtra(size32_t & outLen, char * & outStr, const char * tag)
    {
        const char * text = "";
        if(workflow)
            text = workflow->queryEventExtra();
        rtlExtractTag(outLen, outStr, text, tag, "Event");
    }

    virtual bool fileExists(const char * filename) { throwUnexpected(); }
    virtual void deleteFile(const char * logicalName) { throwUnexpected(); }

    virtual unsigned getNodes()
    {
        if (clusterNames.length())
        {
            if (clusterWidth == -1)
            {
#ifdef _CONTAINERIZED
                /* NB: platform specs. are defined if agent is running in the context of
                 * another engine, e.g. query has been submitted to Thor, but some code is
                 * executing outside of it.
                 *
                 * If not defined then assumed to be executing in roxie context,
                 * where getNodes() defaults to 'numChannels'.
                 */
                clusterWidth = topology->getPropInt("platform/@width", numChannels);
#else
                const char * cluster = clusterNames.tos();
                Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster);
                if (!clusterInfo)
                    throw MakeStringException(-1, "Unknown cluster '%s'", cluster);
                if (clusterInfo->getPlatform() == RoxieCluster)
                    clusterWidth = numChannels;  // We assume it's the current roxie - that's ok so long as roxie's don't call other roxies.
                else
                    clusterWidth = clusterInfo->getSize();
#endif
            }
            return clusterWidth;
        }
        else
            return numChannels;
    }
    virtual unsigned getNodeNum() { return 0; }
    virtual char *getFilePart(const char *logicalPart, bool create=false) { UNIMPLEMENTED; }
    virtual unsigned __int64 getFileOffset(const char *logicalPart) { throwUnexpected(); }

    virtual IDistributedFileTransaction *querySuperFileTransaction()
    {
        CriticalBlock b(resolveCrit);
        if (!superfileTransaction.get())
            superfileTransaction.setown(createDistributedFileTransaction(queryUserDescriptor(), queryCodeContext()));
        return superfileTransaction.get();
    }
    virtual void finalize(unsigned seqNo)
    {
        if (!protocol)
            throwUnexpected();
        protocol->finalize(seqNo);
    }
    virtual unsigned getPriority() const { return options.priority; }
    virtual IConstWorkUnit *queryWorkUnit() const { return workUnit; }
    virtual const char *queryStatsWuid() const override { return statsWu ? statsWu->queryWuid() : nullptr; }
    virtual bool outputResultsToSocket() const { return protocol != NULL; }

    virtual void selectCluster(const char * newCluster)
    {
        if (workUnit)
        {
            const char *oldCluster = workUnit->queryClusterName();
            SCMStringBuffer bStr;
            ClusterType targetClusterType = getClusterType(workUnit->getDebugValue("targetClusterType", bStr).str(), RoxieCluster);
            if (targetClusterType==RoxieCluster)
            {
                if (!streq(oldCluster, newCluster))
                    throw MakeStringException(-1, "Error - cannot switch cluster if not targetting thor jobs");
            }
            clusterNames.append(oldCluster);
            WorkunitUpdate wu = updateWorkUnit();
            if (wu)
                wu->setClusterName(newCluster);
            clusterWidth = -1;
        }
    }
    virtual void restoreCluster()
    {
        WorkunitUpdate wu = updateWorkUnit();
        if (wu)
            wu->setClusterName(clusterNames.item(clusterNames.length()-1));
        clusterNames.pop();
        clusterWidth = -1;
    }

    virtual bool okToLogStartStopError()
    {
        if (!actResetLogPeriod)
            return false;
        // Each query starts with actResetLogState set to LogResetInit
        // State is changed to LogResetOK and a timer is started
        // If same query runs again then time since last logged is checked
        // If two of the same query run at the same time and it is
        // past the logging skip period then both will log activity reset msgs
        if (actResetLogState == LogResetInit)
        {
            unsigned timeNow = msTick();
            unsigned timePrev = serverQueryFactory->getTimeActResetLastLogged();
            if ((timeNow - timePrev) > (actResetLogPeriod*1000))
            {
                serverQueryFactory->setTimeActResetLastLogged(timeNow);
                actResetLogState = LogResetOK;
                return true;
            }
            else
            {
                actResetLogState = LogResetSkip;
                return false;
            }
        }
        else if (actResetLogState == LogResetOK)
            return true;
        else
            return false;
    }
};

//================================================================================================

class CSoapRoxieServerContext : public CRoxieServerContext
{
private:
    StringAttr queryName;

public:
    CSoapRoxieServerContext(IPropertyTree *_context, IHpccProtocolResponse *_protocol, const IQueryFactory *_factory, unsigned flags, const ContextLogger &_logctx, PTreeReaderOptions xmlReadFlags, const char *_querySetName)
        : CRoxieServerContext(_context, _protocol, _factory, flags, _logctx, xmlReadFlags, _querySetName)
    {
        queryName.set(_context->queryName());
    }

    virtual void process()
    {
        EclProcessFactory pf = (EclProcessFactory) factory->queryDll()->getEntry("createProcess");
        Owned<IEclProcess> p = pf();
        if (workflow)
            workflow->perform(this, p);
        else
        {
            GlobalCodeContextExtra gctx(this, 0);
            p->perform(&gctx, 0);
        }
    }
};

IRoxieServerContext *createRoxieServerContext(IPropertyTree *context, IHpccProtocolResponse *protocol, const IQueryFactory *factory, unsigned flags, const ContextLogger &_logctx, PTreeReaderOptions readFlags, const char *querySetName)
{
    if (flags & HPCC_PROTOCOL_NATIVE)
        return new CRoxieServerContext(context, protocol, factory, flags, _logctx, readFlags, querySetName);
    return new CSoapRoxieServerContext(context, protocol, factory, flags, _logctx, readFlags, querySetName);
}

IRoxieServerContext *createOnceServerContext(const IQueryFactory *factory, const IRoxieContextLogger &_logctx)
{
    return new CRoxieServerContext(factory, _logctx);
}

IRoxieServerContext *createWorkUnitServerContext(IConstWorkUnit *wu, const IQueryFactory *factory, const ContextLogger &_logctx)
{
    return new CRoxieServerContext(wu, factory, _logctx);
}
