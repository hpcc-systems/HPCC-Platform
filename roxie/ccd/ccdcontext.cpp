/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#include "wujobq.hpp"
#include "nbcd.hpp"
#include "rtlread_imp.hpp"
#include "thorplugin.hpp"
#include "thorxmlread.hpp"
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

using roxiemem::IRowManager;

//=======================================================================================================================

#define DEBUGEE_TIMEOUT 10000
class CSlaveDebugContext : public CBaseDebugContext
{
    /*

    Some thoughts on slave debugging
    1. Something like a ping can be used to get data from slave when needed
    2. Should disable IBYTI processing (always use primary) - DONE
       and server-side caching - DONE
    3. Roxie server can know what slave transactions are pending by intercepting the sends - no need for slave to call back just to indicate start of slave subgraph
    4. There is a problem when a slave hits a breakpoint in that the breakpoint cound have been deleted by the time it gets a chance to tell the Roxie server - can't
       happen in local case because of the critical block at the head of checkBreakpoint but the local copy of BPs out on slave CAN get out of date. Should we care?
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
    CSlaveDebugContext(IRoxieSlaveContext *_ctx, const IRoxieContextLogger &_logctx, RoxiePacketHeader &_header)
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
            RoxiePacketHeader newHeader(header, ROXIE_DEBUGCALLBACK);
            loop // retry indefinately, as more than likely Roxie server is waiting for user input ...
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
                output->flush(true);
                output.clear();
                if (callback->wait(5000))
                    break;
            }
            if (traceLevel > 6)
                { StringBuffer s; DBGLOG("Processing information from Roxie server in response to %s", newHeader.toString(s).str()); }

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
        // MORE - Implies a server -> slave child -> slave grandchild type situation - need to pass call on to Roxie server (rather as I do for file callback)
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
#define DEFAULT_PERSIST_COPIES (-1)
#define PERSIST_LOCK_TIMEOUT 10000
#define PERSIST_LOCK_SLEEP 5000

class CRoxieWorkflowMachine : public WorkflowMachine
{
    class PersistVersion : public CInterface
    {
    public:
        PersistVersion(char const * _logicalName, unsigned _eclCRC, unsigned __int64 _allCRC, bool _isFile) : logicalName(_logicalName), eclCRC(_eclCRC), allCRC(_allCRC), isFile(_isFile) {}
        StringAttr logicalName;
        unsigned eclCRC;
        unsigned __int64 allCRC;
        bool isFile;
    };

public:
    CRoxieWorkflowMachine(IPropertyTree *_workflowInfo, IConstWorkUnit *_wu, bool _doOnce, const IRoxieContextLogger &_logctx)
    : WorkflowMachine(_logctx)
    {
        workunit = _wu;
        workflowInfo = _workflowInfo;
        doOnce = _doOnce;
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
    virtual void end()
    {
        workflow.clear();
    }
    virtual void schedulingStart() { throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "Scheduling not supported in roxie"); }
    virtual bool schedulingPull() { throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "Scheduling not supported in roxie"); }
    virtual bool schedulingPullStop() { throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "Scheduling not supported in roxie"); }
    virtual void reportContingencyFailure(char const * type, IException * e) {}
    virtual void checkForAbort(unsigned wfid, IException * handling) {}
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
        Owned<IDistributedFile> f = queryDistributedFileDirectory().lookup(lfn, NULL);  // MORE - need a userdescriptor from workunit
        if (f)
            return true;
        return false;
    }

    inline IUserDescriptor *queryUserDescriptor()
    {
        return workunit->queryUserDescriptor();
    }

    bool checkPersistUptoDate(IRuntimeWorkflowItem & item, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile, StringBuffer &errText)
    {
        StringBuffer lfn, crcName, eclName;
        expandLogicalFilename(lfn, logicalName, workunit, false, false);
        crcName.append(lfn).append("$crc");
        eclName.append(lfn).append("$eclcrc");

        if (!isResult(lfn, ResultSequencePersist))
            errText.appendf("Building PERSIST('%s'): It hasn't been calculated before", logicalName);
        else if (!isResult(crcName, ResultSequencePersist))
            errText.appendf("Rebuilding PERSIST('%s'): Saved CRC isn't present", logicalName);
        else if (isFile && !fileExists(lfn))
            errText.appendf("Rebuilding PERSIST('%s'): Persistent file does not exist", logicalName);
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
        loop
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
        loop
        {
            StringBuffer dummy;
            if (checkPersistUptoDate(item, logicalName, eclCRC, allCRC, isFile, dummy) && !rebuildAllPersists)
            {
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
            logctx.CTXLOG("PERSIST('%s') is up to date (after being calculated by another job)", logicalName);
            changePersistLockMode(persistLock, RTM_LOCK_READ, logicalName, true);
            return true;
        }
        if (errText.length())
            logctx.CTXLOG("%s", errText.str());
        return false;
    }

    void updatePersist(IRemoteConnection *persistLock, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC)
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

    IRemoteConnection *startPersist(const char * logicalName)
    {
        setBlockedOnPersist(logicalName);
        IRemoteConnection *persistLock = getPersistReadLock(logicalName);
        WorkunitUpdate w(&workunit->lock());
        w->setState(WUStateRunning);
        return persistLock;
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
                Owned<IDistributedFile> f = queryDistributedFileDirectory().lookup(goer, queryUserDescriptor(), true);
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

    IConstWorkUnit *workunit;
    IPropertyTree *workflowInfo;
    Owned<PersistVersion> persist;
    IArray persistReadLocks;
    bool doOnce;
};

CRoxieWorkflowMachine *createRoxieWorkflowMachine(IPropertyTree *_workflowInfo, IConstWorkUnit *_wu, bool _doOnce, const IRoxieContextLogger &_logctx)
{
    return new CRoxieWorkflowMachine(_workflowInfo, _wu, _doOnce, _logctx);
}

//=======================================================================================================================

typedef byte *row_t;
typedef row_t * rowset_t;

class DeserializedDataReader : public CInterface, implements IWorkUnitRowReader
{
    const rowset_t data;
    size32_t count;
    unsigned idx;
public:
    IMPLEMENT_IINTERFACE;
    DeserializedDataReader(size32_t _count, rowset_t _data)
    : data(_data), count(_count)
    {
        idx = 0;
    }
    virtual const void * nextInGroup()
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
    virtual void getResultRowset(size32_t & tcount, byte * * & tgt)
    {
        tcount = count;
        if (data)
            rtlLinkRowset(data);
        tgt = data;
    }
};

class CDeserializedResultStore : public CInterface, implements IDeserializedResultStore
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
                rtlReleaseRowset(counts.item(idx), (byte**) rows);
            }
        }
    }
    virtual int addResult(size32_t count, rowset_t data, IOutputMetaData *meta)
    {
        SpinBlock b(lock);
        stored.append(data);
        counts.append(count);
        metas.append(meta);
        return stored.ordinality()-1;
    }
    virtual void queryResult(int id, size32_t &count, rowset_t &data) const
    {
        count = counts.item(id);
        data = stored.item(id);
    }
    virtual IWorkUnitRowReader *createDeserializedReader(int id) const
    {
        return new DeserializedDataReader(counts.item(id), stored.item(id));
    }
    virtual void serialize(unsigned & tlen, void * & tgt, int id, ICodeContext *codectx) const
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

class WorkUnitRowReaderBase : public CInterface, implements IWorkUnitRowReader
{
protected:
    Linked<IEngineRowAllocator> rowAllocator;
    bool isGrouped;

public:
    IMPLEMENT_IINTERFACE;
    WorkUnitRowReaderBase(IEngineRowAllocator *_rowAllocator, bool _isGrouped)
        : rowAllocator(_rowAllocator), isGrouped(_isGrouped)
    {

    }

    virtual void getResultRowset(size32_t & tcount, byte * * & tgt)
    {
        bool atEOG = true;
        RtlLinkedDatasetBuilder builder(rowAllocator);
        loop
        {
            const void *ret = nextInGroup();
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
    Owned<ISerialStream> bufferStream;
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
    virtual const void *nextInGroup()
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
        atomic_inc(&rowsIn);
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

    virtual bool nextBlock(unsigned & tlen, void * & tgt, void * & base)
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

    virtual bool nextBlock(unsigned & tlen, void * & tgt, void * & base)
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
            client.readBlock(reply, readTimeout);
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
            IException *ee = MakeStringException(MSGAUD_internal, errCode, "%s", text.str());
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

    virtual bool nextBlock(unsigned & tlen, void * & tgt, void * & base)
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
    IMPLEMENT_IINTERFACE;
    InlineXmlDataReader(IXmlToRowTransformer &_rowTransformer, IPropertyTree *_xml, IEngineRowAllocator *_rowAllocator, bool _isGrouped)
        : WorkUnitRowReaderBase(_rowAllocator, _isGrouped), xml(_xml), rowTransformer(_rowTransformer)
    {
        columns.setown(new XmlDatasetColumnProvider);
        rows.setown(xml->getElements("Row")); // NOTE - the 'hack for Gordon' as found in thorxmlread is not implemented here. Does it need to be ?
        rows->first();
    }

    virtual const void *nextInGroup()
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

class CRoxieContextBase : public CInterface, implements IRoxieSlaveContext, implements ICodeContext, implements roxiemem::ITimeLimiter, implements IRowAllocatorMetaActIdCacheCallback
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
    Owned<IPropertyTree> probeQuery;
    unsigned lastWuAbortCheck;
    unsigned startTime;
    unsigned totSlavesReplyLen;

    QueryOptions options;
    Owned<IConstWorkUnit> workUnit;
    Owned<IRoxieDaliHelper> daliHelperLink;
    Owned<IDistributedFileTransaction> superfileTransaction;

    CriticalSection statsCrit;
    const IRoxieContextLogger &logctx;

protected:
    CriticalSection resultsCrit;
    IPointerArrayOf<FlushingStringBuffer> resultMap;
    bool exceptionLogged;
    bool aborted;
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
        : factory(_factory), logctx(_logctx), options(factory->queryOptions())
    {
        startTime = lastWuAbortCheck = msTick();
        persists = NULL;
        temporaries = NULL;
        deserializedResultStore = NULL;
        rereadResults = NULL;
        xmlStoredDatasetReadFlags = ptr_none;
        aborted = false;
        exceptionLogged = false;
        totSlavesReplyLen = 0;

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
    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value) const
    {
        logctx.noteStatistic(kind, value);
    }

    virtual void mergeStats(const CRuntimeStatisticCollection &from) const
    {
        logctx.mergeStats(from);
    }

    virtual const CRuntimeStatisticCollection &queryStats() const
    {
        return logctx.queryStats();
    }

    virtual void CTXLOGa(TracingCategory category, const char *prefix, const char *text) const
    {
        logctx.CTXLOGa(category, prefix, text);
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
        return logctx.getLogPrefix(ret);
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

    virtual void checkAbort()
    {
        // MORE - really should try to apply limits at slave end too
#ifdef __linux__
        if (linuxYield)
            sched_yield();
#endif
#ifdef _DEBUG
        if (shuttingDown)
            throw MakeStringException(ROXIE_FORCE_SHUTDOWN, "Roxie is shutting down");
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

    virtual void notifyAbort(IException *E)
    {
        CriticalBlock b(abortLock);
        if (!aborted && QUERYINTERFACE(E, InterruptedSemaphoreException) == NULL)
        {
            aborted = true;
            exception.set(E);
            setWUState(WUStateAborting);
        }
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

    const char *queryAuthToken()
    {
        return authToken.str();
    }

    virtual void noteChildGraph(unsigned id, IActivityGraph *childGraph)
    {
        if (queryTraceLevel() > 10)
            CTXLOG("CSlaveContext %p noteChildGraph %d=%p", this, id, childGraph);
        childGraphs.setValue(id, childGraph);
    }

    virtual IActivityGraph *getLibraryGraph(const LibraryCallFactoryExtra &extra, IRoxieServerActivity *parentActivity)
    {
        if (extra.embedded)
        {
            return factory->lookupGraph(extra.embeddedGraphName, probeManager, *this, parentActivity);
        }
        else
        {
            Owned<IQueryFactory> libraryQuery = factory->lookupLibrary(extra.libraryName, extra.interfaceHash, *this);
            assertex(libraryQuery);
            return libraryQuery->lookupGraph("graph1", probeManager, *this, parentActivity);
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
        else if (probeAllRows || probeQuery != NULL)
            probeManager.setown(createProbeManager());
        graph.setown(factory->lookupGraph(graphName, probeManager, *this, NULL));
        graph->onCreate(this, NULL);  // MORE - is that right
        if (debugContext)
            debugContext->checkBreakpoint(DebugStateGraphStart, NULL, graphName);
        if (workUnit)
            graphStats.setown(workUnit->updateStats(graph->queryName(), SCTroxie, queryStatisticsComponentName(), 0));
    }

    virtual void endGraph(cycle_t startCycles, bool aborting)
    {
        if (graph)
        {
            unsigned __int64 elapsedTime = cycle_to_nanosec(get_cycles_now() - startCycles);
            if (debugContext)
                debugContext->checkBreakpoint(aborting ? DebugStateGraphAbort : DebugStateGraphEnd, NULL, graph->queryName());
            if (aborting)
                graph->abort();
            if (workUnit)
            {
                unsigned __int64 totalTimeNs = 0;
                unsigned __int64 totalThisTimeNs = 0;
                const char *totalTimeStr = "Total cluster time";
                getWorkunitTotalTime(workUnit, "roxie", totalTimeNs, totalThisTimeNs);

                const char * graphName = graph->queryName();
                StringBuffer graphDesc;
                formatGraphTimerLabel(graphDesc, graphName);
                WorkunitUpdate progressWorkUnit(&workUnit->lock());
                updateWorkunitTimeStat(progressWorkUnit, SSTgraph, graphName, StTimeElapsed, graphDesc, elapsedTime);
                updateWorkunitTimeStat(progressWorkUnit, SSTglobal, GLOBAL_SCOPE, StTimeElapsed, NULL, totalThisTimeNs+elapsedTime);
                progressWorkUnit->setStatistic(SCTsummary, "roxie", SSTglobal, GLOBAL_SCOPE, StTimeElapsed, totalTimeStr, totalTimeNs+elapsedTime, 1, 0, StatsMergeReplace);
            }
            graph->reset();
            graph.clear();
            childGraphs.kill();
            if (graphStats)
                graphStats.clear();
        }
    }

    void runGraph()
    {
        try
        {
            graph->execute();

            if (probeQuery)
                graph->getProbeResponse(probeQuery);

        }
        catch(...)
        {
            if (probeQuery)
                graph->getProbeResponse(probeQuery);
            throw;
        }
    }

    virtual void executeGraph(const char * name, bool realThor, size32_t parentExtractSize, const void * parentExtract)
    {
        assertex(parentExtractSize == 0);
        if (queryTraceLevel() > 8)
            CTXLOG("Executing graph %s", name);

        if (realThor)
        {
            executeThorGraph(name);
        }
        else
        {
            bool created = false;
            cycle_t startCycles = get_cycles_now();
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
                    endGraph(startCycles, true);
                else
                {
                    // Bit of a hack... needed to avoid pure virtual calls if these are left to the CRoxieContextBase destructor
                    graph.clear();
                    childGraphs.kill();
                }
                CTXLOG("Done cleaning up");
                throw;
            }
            catch (...)
            {
                CTXLOG("Exception thrown in query - cleaning up");
                if (created)
                    endGraph(startCycles, true);
                else
                {
                    // Bit of a hack... needed to avoid pure virtual calls if these are left to the CRoxieContextBase destructor
                    graph.clear();
                    childGraphs.kill();
                }
                CTXLOG("Done cleaning up");
                throw;
            }
            endGraph(startCycles, false);
        }
    }

    virtual IActivityGraph * queryChildGraph(unsigned  id)
    {
        if (queryTraceLevel() > 10)
            CTXLOG("CSlaveContext %p resolveChildGraph %d", this, id);
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

    virtual void addSlavesReplyLen(unsigned len)
    {
        CriticalBlock b(statsCrit); // MORE: change to atomic_add, or may not need it at all?
        totSlavesReplyLen += len;
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

    // The following from ICodeContext should never be executed in slave activity. If we are on Roxie server, they will be implemented by more derived CRoxieServerContext class
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
    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }

    virtual char * getExpandLogicalName(const char * logicalName) { throwUnexpected(); }
    virtual void addWuException(const char * text, unsigned code, unsigned severity, const char * source) { throwUnexpected(); }
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort) { throwUnexpected(); }
    virtual IUserDescriptor *queryUserDescriptor() { throwUnexpected(); }

    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash) { throwUnexpected(); }

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

    virtual void getResultRowset(size32_t & tcount, byte * * & tgt, const char * stepname, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
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

    virtual void getResultDictionary(size32_t & tcount, byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher)
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

    static unsigned hex2digit(char c)
    {
        // MORE - what about error cases?
        if (c >= 'a')
            return (c - 'a' + 10);
        else if (c >= 'A')
            return (c - 'A' + 10);
        return (c - '0');
    }
    virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence)
    {
        MemoryBuffer result;
        CriticalBlock b(contextCrit);
        const char *val = useContext(sequence).queryProp(name);
        if (val)
        {
            loop
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
        if (isSpecialResultSequence(sequence))
        {
            MemoryBuffer m;
            CriticalBlock b(contextCrit);
            useContext(sequence).getPropBin(stepname, m);
            if (m.length())
            {
                assertex(m.length() == tlen);
                m.read(tlen, tgt);
            }
            else
                memset(tgt, 0, tlen);
        }
        else
        {
            StringBuffer x;
            {
                CriticalBlock b(contextCrit);
                useContext(sequence).getProp(stepname, x);
            }
            Decimal d;
            d.setString(x.length(), x.str());
            if (isSigned)
                d.getDecimal(tlen, precision, tgt);
            else
                d.getUDecimal(tlen, precision, tgt);
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
        CriticalBlock b(contextCrit);
        useContext(sequence).getProp(name, x);
        return rtlVCodepageToVUnicodeX(x.str(), "utf-8");
    }

protected:
    mutable CriticalSection contextCrit;
    Owned<IPropertyTree> context;
    IPropertyTree *persists;
    IPropertyTree *temporaries;
    IPropertyTree *rereadResults;
    PTreeReaderOptions xmlStoredDatasetReadFlags;
    CDeserializedResultStore *deserializedResultStore;

    IPropertyTree &useContext(unsigned sequence)
    {
        checkAbort();
        switch (sequence)
        {
        case ResultSequenceStored:
            if (context)
                return *context;
            else
                throw MakeStringException(ROXIE_CODEGEN_ERROR, "Code generation error - attempting to access stored variable on slave");
        case ResultSequencePersist:
            {
                CriticalBlock b(contextCrit);
                if (!persists)
                    persists = createPTree();
                return *persists;
            }
        case ResultSequenceInternal:
            {
                CriticalBlock b(contextCrit);
                if (!temporaries)
                    temporaries = createPTree();
                return *temporaries;
            }
        case ResultSequenceOnce:
            {
                return factory->queryOnceContext(logctx);
            }
        default:
            {
                CriticalBlock b(contextCrit);
                if (!rereadResults)
                    rereadResults = createPTree();
                return *rereadResults;
            }
        }
    }

    IDeserializedResultStore &useResultStore(unsigned sequence)
    {
        checkAbort();
        switch (sequence)
        {
        case ResultSequenceOnce:
            return factory->queryOnceResultStore();
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

    // Copied from eclgraph.cpp, in the hope that we will be deleting that code soon
    void executeThorGraph(const char *graphName)
    {
        assertex(workUnit);
        StringAttr wuid(workUnit->queryWuid());
        StringAttr owner(workUnit->queryUser());
        StringAttr cluster(workUnit->queryClusterName());

        int priority = workUnit->getPriorityValue();
        unsigned timelimit = workUnit->getDebugValueInt("thorConnectTimeout", defaultThorConnectTimeout);
        Owned<IConstWUClusterInfo> c = getTargetClusterInfo(cluster.str());
        if (!c)
            throw MakeStringException(0, "Invalid thor cluster %s", cluster.str());
        SCMStringBuffer queueName;
        c->getThorQueue(queueName);
        Owned<IJobQueue> jq = createJobQueue(queueName.str());

        bool resubmit;
        do // loop if pause interrupted graph and needs resubmitting on resume
        {
            resubmit = false; // set if job interrupted in thor
            class CWorkunitResumeHandler : public CInterface, implements ISDSSubscription
            {
                IConstWorkUnit &wu;
                StringBuffer xpath;
                StringAttr wuid;
                SubscriptionId subId;
                CriticalSection crit;
                Semaphore sem;

                void unsubscribe()
                {
                    CriticalBlock b(crit);
                    if (subId)
                    {
                        SubscriptionId _subId = subId;
                        subId = 0;
                        querySDS().unsubscribe(_subId);
                    }
                }
            public:
                IMPLEMENT_IINTERFACE;
                CWorkunitResumeHandler(IConstWorkUnit &_wu) : wu(_wu)
                {
                    xpath.append("/WorkUnits/");
                    wuid.set(wu.queryWuid());
                    xpath.append(wuid.get()).append("/Action");
                    subId = 0;
                }
                ~CWorkunitResumeHandler()
                {
                    unsubscribe();
                }
                void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
                {
                    CriticalBlock b(crit);
                    if (0 == subId) return;
                    if (valueLen==strlen("resume") && (0 == strncmp("resume", (const char *)valueData, valueLen)))
                        sem.signal();
                }
                bool wait()
                {
                    subId = querySDS().subscribe(xpath.str(), *this, false, true);
                    assertex(subId);
                    PROGLOG("Job %s paused, waiting for resume/abort", wuid.get());
                    bool ret = true;
                    while (!sem.wait(10000))
                    {
                        wu.forceReload();
                        if (WUStatePaused != wu.getState() || wu.aborting())
                        {
                            PROGLOG("Aborting pause job %s, state : %s", wuid.get(), wu.queryStateDesc());
                            ret = false;
                            break;
                        }
                    }
                    unsubscribe();
                    return ret;
                }
            } workunitResumeHandler(*workUnit);

            if (WUStatePaused == workUnit->getState()) // check initial state - and wait if paused
            {
                if (!workunitResumeHandler.wait())
                    throw new WorkflowException(0,"User abort requested", 0, WorkflowException::ABORT, MSGAUD_user);
            }
            setWUState(WUStateBlocked);

            class cPollThread: public Thread  // MORE - why do we ned a thread here?
            {
                Semaphore sem;
                bool stopped;
                IJobQueue *jq;
                IConstWorkUnit *wu;
            public:

                bool timedout;
                CTimeMon tm;
                cPollThread(IJobQueue *_jq, IConstWorkUnit *_wu, unsigned timelimit)
                    : tm(timelimit)
                {
                    stopped = false;
                    jq = _jq;
                    wu = _wu;
                    timedout = false;
                }
                ~cPollThread()
                {
                    stop();
                }
                int run()
                {
                    while (!stopped) {
                        sem.wait(ABORT_POLL_PERIOD);
                        if (stopped)
                            break;
                        if (tm.timedout()) {
                            timedout = true;
                            stopped = true;
                            jq->cancelInitiateConversation();
                        }
                        else if (wu->aborting()) {
                            stopped = true;
                            jq->cancelInitiateConversation();
                        }

                    }
                    return 0;
                }
                void stop()
                {
                    stopped = true;
                    sem.signal();
                }
            } pollthread(jq, workUnit, timelimit*1000);

            pollthread.start();

            PROGLOG("Enqueuing on %s to run wuid=%s, graph=%s, timelimit=%d seconds, priority=%d", queueName.str(), wuid.str(), graphName, timelimit, priority);
            IJobQueueItem* item = createJobQueueItem(wuid.str());
            item->setOwner(owner.str());
            item->setPriority(priority);
            Owned<IConversation> conversation = jq->initiateConversation(item);
            bool got = conversation.get()!=NULL;
            pollthread.stop();
            pollthread.join();
            if (!got)
            {
                if (pollthread.timedout)
                    throw MakeStringException(0, "Query %s failed to start within specified timelimit (%d) seconds", wuid.str(), timelimit);
                throw MakeStringException(0, "Query %s cancelled (1)",wuid.str());
            }
            // get the thor ep from whoever picked up

            SocketEndpoint thorMaster;
            MemoryBuffer msg;
            if (!conversation->recv(msg,1000*60)) {
                throw MakeStringException(0, "Query %s cancelled (2)",wuid.str());
            }
            thorMaster.deserialize(msg);
            msg.clear().append(graphName);
            SocketEndpoint myep;
            myep.setLocalHost(0);
            myep.serialize(msg);  // only used for tracing
            if (!conversation->send(msg)) {
                StringBuffer s("Failed to send query to Thor on ");
                thorMaster.getUrlStr(s);
                throw MakeStringExceptionDirect(-1, s.str()); // maybe retry?
            }

            StringBuffer eps;
            PROGLOG("Thor on %s running %s",thorMaster.getUrlStr(eps).str(),wuid.str());
            MemoryBuffer reply;
            try
            {
                if (!conversation->recv(reply,INFINITE))
                {
                    StringBuffer s("Failed to receive reply from thor ");
                    thorMaster.getUrlStr(s);
                    throw MakeStringExceptionDirect(-1, s.str());
                }
            }
            catch (IException *e)
            {
                StringBuffer s("Failed to receive reply from thor ");
                thorMaster.getUrlStr(s);
                s.append("; (").append(e->errorCode()).append(", ");
                e->errorMessage(s).append(")");
                throw MakeStringExceptionDirect(-1, s.str());
            }
            ThorReplyCodes replyCode;
            reply.read((unsigned &)replyCode);
            switch (replyCode)
            {
                case DAMP_THOR_REPLY_PAUSED:
                {
                    bool isException ;
                    reply.read(isException);
                    if (isException)
                    {
                        Owned<IException> e = deserializeException(reply);
                        VStringBuffer str("Pausing job %s caused exception", wuid.str());
                        EXCLOG(e, str.str());
                    }
                    WorkunitUpdate w(&workUnit->lock());
                    w->setState(WUStatePaused); // will trigger executeThorGraph to pause next time around.
                    WUAction action = w->getAction();
                    switch (action)
                    {
                        case WUActionPause:
                        case WUActionPauseNow:
                            w->setAction(WUActionUnknown);
                    }
                    resubmit = true; // JCSMORE - all subgraph _could_ be done, thor will check though and not rerun
                    break;
                }
                case DAMP_THOR_REPLY_GOOD:
                    break;
                case DAMP_THOR_REPLY_ERROR:
                {
                    throw deserializeException(reply);
                }
                case DAMP_THOR_REPLY_ABORT:
                    throw new WorkflowException(0,"User abort requested", 0, WorkflowException::ABORT, MSGAUD_user);
                default:
                    throwUnexpected();
            }
            workUnit->forceReload();
        }
        while (resubmit); // if pause interrupted job (i.e. with pausenow action), resubmit graph

    }
};

//-----------------------------------------------------------------------------------------------

class CSlaveContext : public CRoxieContextBase
{
protected:
    RoxiePacketHeader *header;

public:
    CSlaveContext(const IQueryFactory *_factory, const SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, bool _hasChildren)
    : CRoxieContextBase(_factory, _logctx)
    {
        if (_packet)
        {
            header = &_packet->queryHeader();
            const byte *traceInfo = _packet->queryTraceInfo();
            options.setFromSlaveLoggingFlags(*traceInfo);
            bool debuggerActive = (*traceInfo & LOGGING_DEBUGGERACTIVE) != 0 && _hasChildren;  // No option to debug simple remote activity
            if (debuggerActive)
            {
                CSlaveDebugContext *slaveDebugContext = new CSlaveDebugContext(this, logctx, *header);
                slaveDebugContext->init(_packet);
                debugContext.setown(slaveDebugContext);
                probeManager.setown(createDebugManager(debugContext, "slaveDebugger"));
            }
        }
        else
            assertex(selfTestMode);
    }
    virtual void beforeDispose()
    {
        // NOTE: This is needed to ensure that owned activities are destroyed BEFORE I am,
        // to avoid pure virtual calls when they come to call noteProcessed()
        childGraphs.kill();
    }

    virtual IRoxieServerContext *queryServerContext()
    {
        return NULL;
    }

    virtual const IResolvedFile *resolveLFN(const char *filename, bool isOpt)
    {
        CDateTime cacheDate; // Note - this is empty meaning we don't know...
        return querySlaveDynamicFileCache()->lookupDynamicFile(*this, filename, cacheDate, 0, header, isOpt, false);
    }

    virtual IRoxieWriteHandler *createLFN(const char *filename, bool overwrite, bool extend, const StringArray &clusters)
    {
        throwUnexpected(); // only support writing on the server
    }

    virtual void onFileCallback(const RoxiePacketHeader &header, const char *lfn, bool isOpt, bool isLocal)
    {
        // On a slave, we need to request info using our own header (not the one passed in) and need to get global rather than just local info
        // (possibly we could get just local if the channel matches but not sure there is any point)
        Owned<const IResolvedFile> dFile = resolveLFN(lfn, isOpt);
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

    virtual void noteProcessed(unsigned subgraphId, unsigned activityId, unsigned _idx, unsigned _processed) const
    {
        const SlaveContextLogger &slaveLogCtx = static_cast<const SlaveContextLogger &>(logctx);
        slaveLogCtx.putStatProcessed(subgraphId, activityId, _idx, _processed);
    }

    virtual void mergeActivityStats(const CRuntimeStatisticCollection &fromStats, unsigned subgraphId, unsigned activityId, const ActivityTimeAccumulator &_totalCycles, cycle_t _localCycles) const
    {
        const SlaveContextLogger &slaveLogCtx = static_cast<const SlaveContextLogger &>(logctx);
        slaveLogCtx.putStats(subgraphId, activityId, fromStats);
    }

};

IRoxieSlaveContext *createSlaveContext(const IQueryFactory *_factory, const SlaveContextLogger &_logctx, IRoxieQueryPacket *packet, bool hasChildren)
{
    return new CSlaveContext(_factory, _logctx, packet, hasChildren);
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
    IRoxieSlaveContext *ctx;

    CRoxieServerDebugContext(IRoxieSlaveContext *_ctx, const IContextLogger &_logctx, IPropertyTree *_queryXGMML, SafeSocket &_client)
        : CBaseServerDebugContext(_logctx, _queryXGMML, _client), ctx(_ctx)
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
        MemoryBuffer slaveInfo;
        slaveInfo.setBuffer(len, data, false);
        unsigned debugSequence;
        slaveInfo.read(debugSequence);
        {
            CriticalBlock b(breakCrit); // we want to wait until it's our turn before updating the graph info or the counts get ahead of the current row and life is confusing
            char slaveStateChar;
            slaveInfo.read(slaveStateChar);
            DebugState slaveState = (DebugState) slaveStateChar;
            if (slaveState==DebugStateGraphFinished)
            {
                unsigned numCounts;
                slaveInfo.read(numCounts);
                while (numCounts)
                {
                    StringAttr edgeId;
                    unsigned edgeCount;
                    slaveInfo.read(edgeId);
                    slaveInfo.read(edgeCount);
                    Owned<IGlobalEdgeRecord> thisEdge = getEdgeRecord(edgeId);
                    thisEdge->incrementCount(edgeCount, sequence);
                    numCounts--;
                }
            }
            slaveInfo.read(currentBreakpointUID);
            memsize_t slaveActivity;
            unsigned channel;
            __uint64 tmp;
            slaveInfo.read(tmp);
            slaveActivity = (memsize_t)tmp;
            slaveInfo.read(channel);
            assertex(currentGraph);
            currentGraph->deserializeProxyGraphs(slaveState, slaveInfo, (IActivityBase *) slaveActivity, channel);
            if (slaveState != DebugStateGraphFinished) // MORE - this is debatable - may (at least sometimes) want a child graph finished to be a notified event...
            {
                StringBuffer slaveActivityId;
                slaveInfo.read(slaveActivityId);
                IActivityDebugContext *slaveActivityCtx = slaveActivityId.length() ? currentGraph->lookupActivityByEdgeId(slaveActivityId.str()) : NULL;
                BreakpointActionMode action = checkBreakpoint(slaveState, slaveActivityCtx , NULL);
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
        if (!type || stricmp(type, "temporary"))
        {
            output->outputBeginNested("Temporary", true);
            ctx->printResults(output, name, (unsigned) ResultSequenceInternal);
            output->outputEndNested("Temporary");
        }
        if (!type || stricmp(type, "global"))
        {
            output->outputBeginNested("Global", true);
            ctx->printResults(output, name, (unsigned) ResultSequenceStored);
            output->outputEndNested("Global");
        }
        output->outputEndNested("Variables");
    }

};

class CRoxieServerContext : public CRoxieContextBase, implements IRoxieServerContext, implements IGlobalCodeContext
{
    const IQueryFactory *serverQueryFactory;
    CriticalSection daliUpdateCrit;
    StringAttr querySetName;

    TextMarkupFormat mlFmt;
    bool isRaw;
    bool sendHeartBeats;
    unsigned lastSocketCheckTime;
    unsigned lastHeartBeat;

protected:
    Owned<CRoxieWorkflowMachine> workflow;
    Owned<ITimeReporter> myTimer;
    mutable MapStringToMyClass<IResolvedFile> fileCache;
    SafeSocket *client;
    bool isBlocked;
    bool isHttp;
    bool trim;

    void doPostProcess()
    {
        CriticalBlock b(resultsCrit); // Probably not needed
        if (!isRaw && !isBlocked)
        {
            ForEachItemIn(seq, resultMap)
            {
                FlushingStringBuffer *result = resultMap.item(seq);
                if (result)
                    result->flush(true);
            }
        }

        if (probeQuery)
        {
            FlushingStringBuffer response(client, isBlocked, MarkupFmt_XML, false, isHttp, *this);

            // create output stream
            response.startDataset("_Probe", NULL, (unsigned) -1);  // initialize it

            // loop through all of the graphs and create a _Probe to output each xgmml
            Owned<IPropertyTreeIterator> graphs = probeQuery->getElements("Graph");
            ForEach(*graphs)
            {
                IPropertyTree &graph = graphs->query();

                StringBuffer xgmml;
                _toXML(&graph, xgmml, 0);
                response.append("\n");
                response.append(xgmml.str());
            }
        }
    }
    void addWuException(IException *E)
    {
        if (workUnit)
            ::addWuException(workUnit, E);
    }

    void init()
    {
        client = NULL;
        totSlavesReplyLen = 0;
        mlFmt = MarkupFmt_XML;
        isRaw = false;
        isBlocked = false;
        isHttp = false;
        trim = false;
        sendHeartBeats = false;

        lastSocketCheckTime = startTime;
        lastHeartBeat = startTime;
        myTimer.setown(createStdTimeReporter());
    }

    void startWorkUnit()
    {
        WorkunitUpdate wu(&workUnit->lock());
        wu->subscribe(SubscribeOptionAbort);
        addTimeStamp(wu, SSTglobal, NULL, StWhenQueryStarted);
        if (!context->getPropBool("@outputToSocket", false))
            client = NULL;
        updateSuppliedXmlParams(wu);
        SCMStringBuffer wuParams;
        if (workUnit->getXmlParams(wuParams, false).length())
        {
            // Merge in params from WU. Ones on command line take precedence though...
            Owned<IPropertyTree> wuParamTree = createPTreeFromXMLString(wuParams.str(), ipt_caseInsensitive);
            Owned<IPropertyTreeIterator> params = wuParamTree ->getElements("*");
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
    }

    void initDebugMode(bool breakAtStart, const char *debugUID)
    {
        if (!debugPermitted || !ownEP.port)
            throw MakeStringException(ROXIE_ACCESS_ERROR, "Debug queries are not permitted on this system");
        debugContext.setown(new CRoxieServerDebugContext(this, logctx, factory->cloneQueryXGMML(), *client));
        debugContext->debugInitialize(debugUID, factory->queryQueryName(), breakAtStart);
        if (workUnit)
        {
            WorkunitUpdate wu(&workUnit->lock());
            wu->setDebugAgentListenerPort(ownEP.port); //tells debugger what port to write commands to
            StringBuffer sb;
            ownEP.getIpText(sb);
            wu->setDebugAgentListenerIP(sb); //tells debugger what IP to write commands to
        }
        options.timeLimit = 0;
        options.warnTimeLimit = 0;
    }

public:
    IMPLEMENT_IINTERFACE;

    CRoxieServerContext(const IQueryFactory *_factory, const IRoxieContextLogger &_logctx)
        : CRoxieContextBase(_factory, _logctx), serverQueryFactory(_factory)
    {
        init();
        rowManager->setMemoryLimit(options.memoryLimit);
        workflow.setown(_factory->createWorkflowMachine(workUnit, true, logctx));
        context.setown(createPTree(ipt_caseInsensitive));
    }

    CRoxieServerContext(IConstWorkUnit *_workUnit, const IQueryFactory *_factory, const ContextLogger &_logctx)
        : CRoxieContextBase(_factory, _logctx), serverQueryFactory(_factory)
    {
        init();
        workUnit.set(_workUnit);
        rowManager->setMemoryLimit(options.memoryLimit);
        workflow.setown(_factory->createWorkflowMachine(workUnit, false, logctx));
        context.setown(createPTree(ipt_caseInsensitive));

        //MORE: Use various debug settings to override settings:
        rowManager->setActivityTracking(workUnit->getDebugValueBool("traceRoxiePeakMemory", false));

        startWorkUnit();
    }

    CRoxieServerContext(IPropertyTree *_context, const IQueryFactory *_factory, SafeSocket &_client, TextMarkupFormat _mlFmt, bool _isRaw, bool _isBlocked, HttpHelper &httpHelper, bool _trim, const ContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags, const char *_querySetName)
        : CRoxieContextBase(_factory, _logctx), serverQueryFactory(_factory), querySetName(_querySetName)
    {
        init();
        context.set(_context);
        options.setFromContext(context);
        client = &_client;
        mlFmt = _mlFmt;
        isRaw = _isRaw;
        isBlocked = _isBlocked;
        isHttp = httpHelper.isHttp();
        trim = _trim;
        xmlStoredDatasetReadFlags = _xmlReadFlags;
        sendHeartBeats = enableHeartBeat && isRaw && isBlocked && options.priority==0;

        const char *wuid = context->queryProp("@wuid");
        if (wuid)
        {
            IRoxieDaliHelper *daliHelper = checkDaliConnection();
            assertex(daliHelper );
            workUnit.setown(daliHelper->attachWorkunit(wuid, _factory->queryDll()));
            if (!workUnit)
                throw MakeStringException(ROXIE_DALI_ERROR, "Failed to open workunit %s", wuid);
            startWorkUnit();
        }
        else if (context->getPropBool("@debug", false))
        {
            bool breakAtStart = context->getPropBool("@break", true);
            const char *debugUID = context->queryProp("@uid");
            if (debugUID && *debugUID)
                initDebugMode(breakAtStart, debugUID);
        }
        else if (context->getPropBool("_Probe", false))
            probeQuery.setown(_factory->cloneQueryXGMML());

        // MORE some of these might be appropriate in wu case too?
        rowManager->setActivityTracking(context->getPropBool("_TraceMemory", false));
        rowManager->setMemoryLimit(options.memoryLimit);
        authToken.append(httpHelper.queryAuthToken());
        workflow.setown(_factory->createWorkflowMachine(workUnit, false, logctx));
    }

    virtual void noteProcessed(unsigned subgraphId, unsigned activityId, unsigned _idx, unsigned _processed) const
    {
        if (_processed)
        {
            if (graphStats)
            {
                IStatisticGatherer & builder = graphStats->queryStatsBuilder();
                StatsSubgraphScope graphScope(builder, subgraphId);
                StatsEdgeScope scope(builder, activityId, _idx);
                builder.addStatistic(StNumRowsProcessed, _processed);
            }
            logctx.noteStatistic(StNumRowsProcessed, _processed);
        }
    }

    virtual void mergeActivityStats(const CRuntimeStatisticCollection &fromStats, unsigned subgraphId, unsigned activityId, const ActivityTimeAccumulator &_totalCycles, cycle_t _localCycles) const
    {
        if (graphStats)
        {
            IStatisticGatherer & builder = graphStats->queryStatsBuilder();
            StatsSubgraphScope graphScope(builder, subgraphId);
            StatsActivityScope scope(builder, activityId);
            _totalCycles.addStatistics(builder);
            if (_localCycles)
                builder.addStatistic(StTimeLocalExecute, cycle_to_nanosec(_localCycles));
            fromStats.recordStatistics(builder);
        }
        logctx.mergeStats(fromStats);
    }

    virtual roxiemem::IRowManager &queryRowManager()
    {
        return *rowManager;
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
        if (client)
        {
            if (socketCheckInterval)
            {
                if (ticksNow - lastSocketCheckTime > socketCheckInterval)
                {
                    CriticalBlock b(abortLock);
                    if (!client->checkConnection())
                        throw MakeStringException(ROXIE_CLIENT_CLOSED, "Client socket closed");
                    lastSocketCheckTime = ticksNow;
                }
            }
            if (sendHeartBeats)
            {
                unsigned hb = ticksNow - lastHeartBeat;
                if (hb > 30000)
                {
                    lastHeartBeat = msTick();
                    client->sendHeartBeat(*this);
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
    virtual unsigned getMemoryUsage()
    {
        return rowManager->getMemoryUsage();
    }

    virtual unsigned getSlavesReplyLen()
    {
        return totSlavesReplyLen;
    }

    virtual void process()
    {
        MTIME_SECTION(myTimer, "Process");
        EclProcessFactory pf = (EclProcessFactory) factory->queryDll()->getEntry("createProcess");
        Owned<IEclProcess> p = pf();
        try
        {
            if (debugContext)
                debugContext->checkBreakpoint(DebugStateReady, NULL, NULL);
            if (workflow)
                workflow->perform(this, p);
            else
                p->perform(this, 0);
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

    virtual void done(bool failed)
    {
        if (debugContext)
            debugContext->debugTerminate();
        if (workUnit)
        {
            WorkunitUpdate w(&workUnit->lock());
            w->setState(aborted ? WUStateAborted : (failed ? WUStateFailed : WUStateCompleted));
            addTimeStamp(w, SSTglobal, NULL, StWhenQueryFinished);
            updateWorkunitTimings(w, myTimer);
            Owned<IStatisticGatherer> gatherer = createGlobalStatisticGatherer(w);
            logctx.queryStats().recordStatistics(*gatherer);

            WuStatisticTarget statsTarget(w, "roxie");
            rowManager->reportPeakStatistics(statsTarget, 0);
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

    virtual IGlobalCodeContext *queryGlobalCodeContext()
    {
        return this;
    }

    // interface ICodeContext
    virtual FlushingStringBuffer *queryResult(unsigned sequence)
    {
        if (!client && workUnit)
            return NULL;    // when outputting to workunit only, don't output anything to stdout
        CriticalBlock procedure(resultsCrit);
        while (!resultMap.isItem(sequence))
            resultMap.append(NULL);
        FlushingStringBuffer *result = resultMap.item(sequence);
        if (!result)
        {
            result = new FlushingStringBuffer(client, isBlocked, mlFmt, isRaw, isHttp, *this);
            result->isSoap = isHttp;
            result->trim = trim;
            result->queryName.set(context->queryName());
            resultMap.replace(result, sequence);
        }
        return result;
    }

    virtual char *getDaliServers()
    {
        //MORE: Should this now be implemented using IRoxieDaliHelper?
        throwUnexpected();
    }

    virtual void setResultBool(const char *name, unsigned sequence, bool value)
    {
        if (isSpecialResultSequence(sequence))
        {
            CriticalBlock b(contextCrit);
            useContext(sequence).setPropBool(name, value);
        }
        else
        {
            FlushingStringBuffer *r = queryResult(sequence);
            if (r)
            {
                r->startScalar(name, sequence);
                if (isRaw)
                    r->append(sizeof(value), (char *)&value);
                else
                    r->append(value ? "true" : "false");
            }
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
        static char hexchar[] = "0123456789ABCDEF";
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
        else
        {
            FlushingStringBuffer *r = queryResult(sequence);
            if (r)
            {
                r->startScalar(name, sequence);
                r->encodeData(data, len);
            }
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
    virtual void appendResultDeserialized(const char *name, unsigned sequence, size32_t count, rowset_t data, bool extend, IOutputMetaData *meta)
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
                val = ctx.addPropTree(name, createPTree());
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
        else
        {
            FlushingStringBuffer *r = queryResult(sequence);
            if (r)
            {
                r->startScalar(name, sequence);
                if (isRaw)
                    r->append(len, (const char *) data);
                else
                    UNIMPLEMENTED;
            }
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
        else
        {
            FlushingStringBuffer *r = queryResult(sequence);
            if (r)
            {
                r->startScalar(name, sequence);
                if (isRaw)
                    r->append(len, (char *)data);
                else if (mlFmt==MarkupFmt_XML)
                {
                    assertex(transformer);
                    CommonXmlWriter writer(getXmlFlags()|XWFnoindent, 0);
                    transformer->toXML(isAll, len, (byte *)data, writer);
                    r->append(writer.str());
                }
                else if (mlFmt==MarkupFmt_JSON)
                {
                    assertex(transformer);
                    CommonJsonWriter writer(getXmlFlags()|XWFnoindent, 0);
                    transformer->toXML(isAll, len, (byte *)data, writer);
                    r->append(writer.str());
                }
                else
                {
                    assertex(transformer);
                    r->append('[');
                    if (isAll)
                        r->appendf("*]");
                    else
                    {
                        SimpleOutputWriter x;
                        transformer->toXML(isAll, len, (const byte *) data, x);
                        r->appendf("%s]", x.str());
                    }
                }
            }
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
        CriticalBlock b(contextCrit);
        useContext(sequence).setPropTree(name, createPTreeFromXMLString(xml, ipt_caseInsensitive));
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
        else
        {
            FlushingStringBuffer *r = queryResult(sequence);
            if (r)
            {
                r->startScalar(name, sequence);
                if (isRaw)
                    r->append(len, (char *)val);
                else
                {
                    StringBuffer s;
                    if (isSigned)
                        outputXmlDecimal(val, len, precision, NULL, s);
                    else
                        outputXmlUDecimal(val, len, precision, NULL, s);
                    r->append(s);
                }
            }
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
        else
        {
            FlushingStringBuffer *r = queryResult(sequence);
            if (r)
            {
                if (isRaw)
                {
                    r->startScalar(name, sequence);
                    r->append(sizeof(value), (char *)&value);
                }
                else
                    r->setScalarInt(name, sequence, value, size);
            }
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
        else
        {
            FlushingStringBuffer *r = queryResult(sequence);
            if (r)
            {
                if (isRaw)
                {
                    r->startScalar(name, sequence);
                    r->append(sizeof(value), (char *)&value);
                }
                else
                    r->setScalarUInt(name, sequence, value, size);
            }
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
        else
        {
            FlushingStringBuffer *r = queryResult(sequence);
            if (r)
            {
                r->startScalar(name, sequence);
                r->append(value);
            }
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
        else
        {
            FlushingStringBuffer *r = queryResult(sequence);
            if (r)
            {
                r->startScalar(name, sequence);
                if (r->isRaw)
                {
                    r->append(len, str);
                }
                else
                {
                    r->encodeString(str, len);
                }
            }
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
        else
        {
            FlushingStringBuffer *r = queryResult(sequence);
            if (r)
            {
                r->startScalar(name, sequence);
                if (r->isRaw)
                {
                    r->append(len*2, (const char *) str);
                }
                else
                {
                    rtlDataAttr buff;
                    unsigned bufflen = 0;
                    rtlUnicodeToCodepageX(bufflen, buff.refstr(), len, str, "utf-8");
                    r->encodeString(buff.getstr(), bufflen, true); // output as UTF-8
                }
            }
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
        return new StreamedRawDataReader(this, rowAllocator, isGrouped, logctx, *client, id);
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

    virtual const IResolvedFile *resolveLFN(const char *fileName, bool isOpt)
    {
        CriticalBlock b(contextCrit);
        StringBuffer expandedName;
        expandLogicalFilename(expandedName, fileName, workUnit, false, !workUnit);
        Linked<const IResolvedFile> ret = fileCache.getValue(expandedName);
        if (!ret)
        {
            ret.setown(factory->queryPackage().lookupFileName(fileName, isOpt, false, false, workUnit));
            if (ret)
            {
                IResolvedFile *add = const_cast<IResolvedFile *>(ret.get());
                fileCache.setValue(expandedName, add);
            }
        }
        return ret.getClear();
    }

    virtual IRoxieWriteHandler *createLFN(const char *filename, bool overwrite, bool extend, const StringArray &clusters)
    {
        return factory->queryPackage().createFileName(filename, overwrite, extend, clusters, workUnit);
    }

    virtual void endGraph(cycle_t startCycles, bool aborting)
    {
        fileCache.kill();
        CRoxieContextBase::endGraph(startCycles, aborting);
    }

    virtual void onFileCallback(const RoxiePacketHeader &header, const char *lfn, bool isOpt, bool isLocal)
    {
        Owned<const IResolvedFile> dFile = resolveLFN(lfn, isOpt);
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
            Owned<IConstWorkUnit> externalWU = daliHelper->attachWorkunit(wuid, NULL);
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
        ErrorSeverity severity = (ErrorSeverity) _severity;
        CTXLOG("%s", text);
        if (severity > SeverityInformation)
            OERRLOG("%d - %s", code, text);
        if (workUnit)
        {
            WorkunitUpdate wu(&workUnit->lock());
            addExceptionToWorkunit(wu, severity, source, code, text, NULL, 0 ,0);
        }
    }
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort)
    {
        CTXLOG("%s", text);
        OERRLOG("%d - %s", code, text);
        if (workUnit)
        {
            WorkunitUpdate wu(&workUnit->lock());
            addExceptionToWorkunit(wu, SeverityError, "user", code, text, filename, lineno, column);
        }
        if (isAbort)
            rtlFailOnAssert();      // minimal implementation
    }
    IUserDescriptor *queryUserDescriptor()
    {
        return NULL; // TBD - Richard, where do user credentials for a roxie query come from
    }

    virtual bool isResult(const char * name, unsigned sequence)
    {
        CriticalBlock b(contextCrit);
        return useContext(sequence).hasProp(name);
    }

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
    virtual char *getGroupName() { throwUnexpected(); }
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
    virtual char *getJobOwner() { throwUnexpected(); }
    virtual char *getPlatform()
    {
        return strdup("roxie");
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
        addWuException(text, code, 2, "user");
    }

    virtual unsigned getWorkflowId() { return workflow->queryCurrentWfid(); }

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

    virtual void doWait(unsigned code, char const * extra) { UNIMPLEMENTED; }
    virtual void doWaitCond(unsigned code, char const * extra, int sequence, char const * alias, unsigned wfid) { UNIMPLEMENTED; }

    static unsigned __int64 crcLogicalFileTime(IDistributedFile * file, unsigned __int64 crc, const char * filename)
    {
        CDateTime dt;
        file->getModificationTime(dt);
        unsigned __int64 modifiedTime = dt.getSimple();
        return rtlHash64Data(sizeof(modifiedTime), &modifiedTime, crc);
    }

    virtual unsigned __int64 getDatasetHash(const char * logicalName, unsigned __int64 crc)
    {
        StringBuffer fullname;
        expandLogicalFilename(fullname, logicalName, workUnit, false, false);
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
        return crc;
    }

    virtual int queryLastFailCode() { UNIMPLEMENTED; }
    virtual void getLastFailMessage(size32_t & outLen, char * &outStr, const char * tag) { UNIMPLEMENTED; }
    virtual void getEventName(size32_t & outLen, char * & outStr) { UNIMPLEMENTED; }
    virtual void getEventExtra(size32_t & outLen, char * & outStr, const char * tag) { UNIMPLEMENTED; }

    virtual bool fileExists(const char * filename) { throwUnexpected(); }
    virtual void deleteFile(const char * logicalName) { throwUnexpected(); }

    virtual unsigned getNodes() { return numChannels; }
    virtual unsigned getNodeNum() { return 0; }
    virtual char *getFilePart(const char *logicalPart, bool create=false) { UNIMPLEMENTED; }
    virtual unsigned __int64 getFileOffset(const char *logicalPart) { throwUnexpected(); }

    virtual IDistributedFileTransaction *querySuperFileTransaction()
    {
        CriticalBlock b(contextCrit);
        if (!superfileTransaction.get())
            superfileTransaction.setown(createDistributedFileTransaction(queryUserDescriptor()));
        return superfileTransaction.get();
    }
    virtual void flush(unsigned seqNo) { throwUnexpected(); }
    virtual unsigned getPriority() const { return options.priority; }
    virtual IConstWorkUnit *queryWorkUnit() const { return workUnit; }
    virtual bool outputResultsToSocket() const { return client != NULL; }

    virtual void selectCluster(const char * cluster) { throwUnexpected(); }
    virtual void restoreCluster() { throwUnexpected(); }

};

//================================================================================================

class CSoapRoxieServerContext : public CRoxieServerContext
{
private:
    StringAttr queryName;

public:
    CSoapRoxieServerContext(IPropertyTree *_context, const IQueryFactory *_factory, SafeSocket &_client, HttpHelper &httpHelper, const ContextLogger &_logctx, PTreeReaderOptions xmlReadFlags, const char *_querySetName)
        : CRoxieServerContext(_context, _factory, _client, MarkupFmt_XML, false, false, httpHelper, httpHelper.getTrim(), _logctx, xmlReadFlags, _querySetName)
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
            p->perform(this, 0);
    }

    virtual void flush(unsigned seqNo)
    {
        CriticalBlock b(resultsCrit);
        CriticalBlock b1(client->queryCrit());

        StringBuffer responseHead, responseTail;
        responseHead.append("<").append(queryName).append("Response");
        responseHead.append(" sequence=\"").append(seqNo).append("\"");
        responseHead.append(" xmlns=\"urn:hpccsystems:ecl:").appendLower(queryName.length(), queryName.str()).append("\">");
        responseHead.append("<Results><Result>");
        unsigned len = responseHead.length();
        client->write(responseHead.detach(), len, true);

        ForEachItemIn(seq, resultMap)
        {
            FlushingStringBuffer *result = resultMap.item(seq);
            if (result)
            {
                result->flush(true);
                for(;;)
                {
                    size32_t length;
                    void *payload = result->getPayload(length);
                    if (!length)
                        break;
                    client->write(payload, length, true);
                }
            }
        }

        responseTail.append("</Result></Results>");
        responseTail.append("</").append(queryName).append("Response>");
        len = responseTail.length();
        client->write(responseTail.detach(), len, true);
    }
};

class CJsonRoxieServerContext : public CRoxieServerContext
{
private:
    StringAttr queryName;

public:
    CJsonRoxieServerContext(IPropertyTree *_context, const IQueryFactory *_factory, SafeSocket &_client, HttpHelper &httpHelper, const ContextLogger &_logctx, PTreeReaderOptions xmlReadFlags, const char *_querySetName)
        : CRoxieServerContext(_context, _factory, _client, MarkupFmt_JSON, false, false, httpHelper, httpHelper.getTrim(), _logctx, xmlReadFlags, _querySetName)
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
            p->perform(this, 0);
    }

    virtual void flush(unsigned seqNo)
    {
        CriticalBlock b(resultsCrit);
        CriticalBlock b1(client->queryCrit());

        StringBuffer responseHead, responseTail;
        appendfJSONName(responseHead, "%sResponse", queryName.get()).append(" {");
        appendJSONValue(responseHead, "sequence", seqNo);
        appendJSONName(responseHead, "Results").append(" {");

        unsigned len = responseHead.length();
        client->write(responseHead.detach(), len, true);

        bool needDelimiter = false;
        ForEachItemIn(seq, resultMap)
        {
            FlushingStringBuffer *result = resultMap.item(seq);
            if (result)
            {
                result->flush(true);
                for(;;)
                {
                    size32_t length;
                    void *payload = result->getPayload(length);
                    if (!length)
                        break;
                    if (needDelimiter)
                    {
                        StringAttr s(","); //write() will take ownership of buffer
                        size32_t len = s.length();
                        client->write((void *)s.detach(), len, true);
                        needDelimiter=false;
                    }
                    client->write(payload, length, true);
                }
                needDelimiter=true;
            }
        }

        responseTail.append("}}");
        len = responseTail.length();
        client->write(responseTail.detach(), len, true);
    }

    virtual FlushingStringBuffer *queryResult(unsigned sequence)
    {
        if (!client && workUnit)
            return NULL;    // when outputting to workunit only, don't output anything to stdout
        CriticalBlock procedure(resultsCrit);
        while (!resultMap.isItem(sequence))
            resultMap.append(NULL);
        FlushingStringBuffer *result = resultMap.item(sequence);
        if (!result)
        {
            result = new FlushingJsonBuffer(client, isBlocked, isHttp, *this);
            result->trim = trim;
            result->queryName.set(context->queryName());
            resultMap.replace(result, sequence);
        }
        return result;
    }
};

IRoxieServerContext *createRoxieServerContext(IPropertyTree *context, const IQueryFactory *factory, SafeSocket &client, bool isXml, bool isRaw, bool isBlocked, HttpHelper &httpHelper, bool trim, const ContextLogger &_logctx, PTreeReaderOptions readFlags, const char *querySetName)
{
    if (httpHelper.isHttp())
    {
        if (httpHelper.queryContentFormat()==MarkupFmt_JSON)
            return new CJsonRoxieServerContext(context, factory, client, httpHelper, _logctx, readFlags, querySetName);
        return new CSoapRoxieServerContext(context, factory, client, httpHelper, _logctx, readFlags, querySetName);
    }
    else
        return new CRoxieServerContext(context, factory, client, isXml ? MarkupFmt_XML : MarkupFmt_Unknown, isRaw, isBlocked, httpHelper, trim, _logctx, readFlags, querySetName);
}

IRoxieServerContext *createOnceServerContext(const IQueryFactory *factory, const IRoxieContextLogger &_logctx)
{
    return new CRoxieServerContext(factory, _logctx);
}

IRoxieServerContext *createWorkUnitServerContext(IConstWorkUnit *wu, const IQueryFactory *factory, const ContextLogger &_logctx)
{
    return new CRoxieServerContext(wu, factory, _logctx);
}
