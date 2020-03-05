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
#include "jthread.hpp"
#include "jregexp.hpp"

#include "wujobq.hpp"
#include "thorplugin.hpp"
#include "environment.hpp"

#include "udptopo.hpp"

#include "ccd.hpp"
#include "ccdcontext.hpp"
#include "ccdlistener.hpp"
#include "ccddali.hpp"
#include "ccdquery.hpp"
#include "ccdqueue.ipp"
#include "ccdsnmp.hpp"
#include "ccdstate.hpp"

//======================================================================================================================

static void controlException(StringBuffer &response, IException *E, const IRoxieContextLogger &logctx)
{
    try
    {
        if (traceLevel)
            logctx.logOperatorException(E, __FILE__, __LINE__, "controlException");
        response.appendf("<Exception><Source>Roxie</Source><Code>%d</Code><Message>", E->errorCode());
        StringBuffer s;
        E->errorMessage(s);
        encodeXML(s.str(), response);
        response.append("</Message></Exception>");
        E->Release();
    }
    catch(IException *EE)
    {
        logctx.logOperatorException(EE, __FILE__, __LINE__, "controlException - While reporting exception");
        EE->Release();
    }
#ifndef _DEBUG
    catch(...) {}
#endif
}

//================================================================================================================

class CascadeManager : public CInterface
{
    static Semaphore globalLock;
    StringBuffer errors;

    IArrayOf<ISocket> activeChildren;
    Owned<const ITopologyServer> topology;
    const SocketEndpointArray &servers;
    UnsignedArray activeIdxes;
    bool entered;
    bool connected;
    bool isMaster;
    CriticalSection revisionCrit;
    int myEndpoint;
    const IRoxieContextLogger &logctx;

    void unlockChildren()
    {
        try
        {
            class casyncfor: public CAsyncFor
            {
            public:
                casyncfor(CascadeManager *_parent) : parent(_parent) { }
                void Do(unsigned i)
                {
                    parent->unlockChild(i);
                }
            private:
                CascadeManager *parent;
            } afor(this);
            afor.For(activeChildren.length(), activeChildren.length());
        }
        catch (IException *E)
        {
            if (traceLevel)
                logctx.logOperatorException(E, __FILE__, __LINE__, "In unlockChildren");
            E->Release();
        }
    }

    void unlockAll()
    {
        if (entered)
        {
            unlockChildren();
            entered = false;
            if (traceLevel > 5)
                DBGLOG("globalLock released");
            globalLock.signal();
            globalSignals++;
        }
    }

    void connectChild(unsigned idx)
    {
        if (idx < servers.ordinality())
        {
            const SocketEndpoint &ep = servers.item(idx);
            try
            {
                if (traceLevel)
                {
                    StringBuffer epStr;
                    ep.getUrlStr(epStr);
                    DBGLOG("connectChild connecting to %s", epStr.str());
                }
                ISocket *sock = ISocket::connect_timeout(ep, 2000);
                assertex(sock);
                activeChildren.append(*sock);
                activeIdxes.append(idx);
                if (traceLevel)
                {
                    StringBuffer epStr;
                    ep.getUrlStr(epStr);
                    DBGLOG("connectChild connected to %s", epStr.str());
                }
            }
            catch(IException *E)
            {
                logctx.logOperatorException(E, __FILE__, __LINE__, "CascadeManager connection failed");
                connectChild((idx+1) * 2 - 1);
                connectChild((idx+1) * 2);
                errors.append("<Endpoint ep='");
                ep.getUrlStr(errors);
                errors.append("'><Exception><Code>").append(E->errorCode()).append("</Code><Message>");
                E->errorMessage(errors).append("</Message></Exception></Endpoint>");
                logctx.CTXLOG("Connection failed - %s", errors.str());
                E->Release();
            }
        }
    }

public:
    void doChildQuery(unsigned idx, const char *queryText, StringBuffer &reply)
    {
        ISocket &sock = activeChildren.item(idx);
        CSafeSocket ss(LINK(&sock));
        unsigned txtlen = queryText ? strlen(queryText) : 0;
        unsigned revlen = txtlen;
        _WINREV(revlen);
        ss.write(&revlen, sizeof(revlen));
        if (txtlen)
        {
            ss.write(queryText, txtlen);
            bool dummy;
            while (ss.readBlock(reply, WAIT_FOREVER, NULL, dummy, dummy, maxBlockSize)) {}
        }
    }

    int lockChild(unsigned idx)
    {
        StringBuffer lockReply;
        StringBuffer lockQuery;
        lockQuery.appendf("<control:childlock thisEndpoint='%d' parent='%d'/>", activeIdxes.item(idx), myEndpoint);
        doChildQuery(idx, lockQuery.str(), lockReply);
        Owned<IPropertyTree> lockResult = createPTreeFromXMLString(lockReply.str(), ipt_caseInsensitive|ipt_fast);
        int lockCount = lockResult ? lockResult->getPropInt("Lock", 0) : 0;
        if (lockCount)
        {
            return lockCount;
        }
        else
            throw MakeStringException(ROXIE_LOCK_ERROR, "Did not get lock for child %d (%s)", idx, lockReply.str());
    }

    void unlockChild(unsigned idx)
    {
        try
        {
            StringBuffer dummy;
            doChildQuery(idx, "<control:childlock unlock='1'/>", dummy);
            if (traceLevel)
                DBGLOG("UnlockChild %d returned %s", idx, dummy.str());
        }
        catch (IException *E)
        {
            if (traceLevel)
                logctx.logOperatorException(E, __FILE__, __LINE__, "In unlockChild");
            E->Release();
        }
    }

private:
    unsigned lockChildren()
    {
        for (;;)
        {
            int got = 1;
            CriticalSection cs;
            try
            {
                class casyncfor: public CAsyncFor
                {
                public:
                    casyncfor(CascadeManager *_parent, int &_got, CriticalSection &_cs)
                        : parent(_parent), got(_got), cs(_cs){ }
                    void Do(unsigned i)
                    {
                        int childLocks = parent->lockChild(i);
                        CriticalBlock b(cs);
                        if (childLocks <= 0)
                            got = childLocks;
                        else if (got > 0)
                            got += childLocks;
                    }
                private:
                    CascadeManager *parent;
                    int &got;
                    CriticalSection &cs;
                } afor(this, got, cs);
                afor.For(activeChildren.length(), activeChildren.length());
            }
            catch (IException *E)
            {
                if (traceLevel)
                    logctx.logOperatorException(E, __FILE__, __LINE__, "In lockChildren");
                E->Release();
                got = 0;  // Something went wrong - abandon this attempt
            }
            if (got <= 0)
            {
                unlockChildren();
                if (!got)
                    throw MakeStringException(ROXIE_LOCK_ERROR, "lock failed");
                if (traceLevel)
                    DBGLOG("Lock succeeded but revision updated - go around again");
            }
            else
                return got-1;
        }
    }

    void getGlobalLock()
    {
        if (traceLevel > 5)
            DBGLOG("in getGlobalLock");
        if (!globalLock.wait(2000))  // since all lock in the same order it's ok to block for a bit here
            throw MakeStringException(ROXIE_LOCK_ERROR, "lock failed");
        globalLocks++;
        entered = true;
        if (traceLevel > 5)
            DBGLOG("globalLock locked");
    }

    unsigned lockAll()
    {
        try
        {
            return lockChildren() + 1;
        }
        catch(...)
        {
            if (traceLevel>5)
                DBGLOG("Failed to get child locks - unlocking");
            assertex(entered);
            entered = false;
            globalLock.signal();
            globalSignals++;
            if (traceLevel > 5)
                DBGLOG("globalLock released");
            throw;
        }
    }


public:
    CascadeManager(const IRoxieContextLogger &_logctx, const ITopologyServer *_topology) : topology(_topology), servers(_topology->queryServers(roxiePort)), logctx(_logctx)
    {
        entered = false;
        connected = false;
        isMaster = false;
        myEndpoint = -1;
        logctx.Link();
    }

    ~CascadeManager()
    {
        unlockAll();
        logctx.Release();
    }
    inline bool checkEntered(){return entered;}
    void doLockChild(IPropertyTree *xml, const char *logText, StringBuffer &reply)
    {
        if (traceLevel > 5)
            DBGLOG("doLockChild: %s", logText);
        isMaster = false;
        bool unlock = xml->getPropBool("@unlock", false);
        if (unlock)
        {
            unlockAll();
            reply.append("<Lock>0</Lock>");
        }
        else
        {
            assertex(!entered);
            myEndpoint = xml->getPropInt("@thisEndpoint", 0);
            if (!connected)
            {
                connectChild((myEndpoint+1) * 2 - 1);
                connectChild((myEndpoint+1) * 2);
                connected = true;
            }

            try
            {
                getGlobalLock();
                unsigned locksGot = lockAll();
                reply.append("<Lock>").append(locksGot).append("</Lock>");
                assertex(entered);
            }
            catch (IException *E)
            {
                if (traceLevel > 5)
                    logctx.logOperatorException(E, __FILE__, __LINE__, "Trying to get global lock");
                E->Release();
                reply.append("<Lock>0</Lock>");
            }
        }
    }

    void doLockChild(const char *queryText, StringBuffer &reply)
    {
        Owned<IPropertyTree> xml = createPTreeFromXMLString(queryText,ipt_fast);
        doLockChild(xml, queryText, reply);
    }

    bool doLockGlobal(StringBuffer &reply, bool lockAll)
    {
        assertex(!entered);
        assertex(!connected);
        isMaster = true;
        myEndpoint = -1;
        unsigned attemptsLeft = maxLockAttempts;
        connectChild(0);
        connected = true;

        unsigned lockDelay = 0;
        unsigned locksGot = 0;
        Owned<IRandomNumberGenerator> randomizer;
        for (;;)
        {
            try
            {
                locksGot = lockChildren();
                break;
            }
            catch (IException *E)
            {
                unsigned errCode = E->errorCode();
                if (traceLevel > 5)
                    logctx.logOperatorException(E, __FILE__, __LINE__, "In doLockGlobal()");
                E->Release();

                if ( (!--attemptsLeft) || (errCode == ROXIE_CLUSTER_SYNC_ERROR))
                {
                    reply.append("<Lock>0</Lock>");
                    return false;
                }
                if (!randomizer) randomizer.setown(createRandomNumberGenerator());
                lockDelay += 1000 + randomizer->next() % 1000;
                Sleep(lockDelay);
            }
        }
        if (traceLevel > 5)
            DBGLOG("doLockGlobal got %d locks", locksGot);
        reply.append("<Lock>").append(locksGot).append("</Lock>");
        reply.append("<NumServers>").append(servers.ordinality()).append("</NumServers>");
        if (lockAll)
            return locksGot == servers.ordinality();
        else
            return locksGot > servers.ordinality()/2;
    }

    enum CascadeMergeType { CascadeMergeNone, CascadeMergeStats, CascadeMergeQueries };

    void doControlQuery(SocketEndpoint &ep, const char *queryText, StringBuffer &reply)
    {
        Owned<IPropertyTree> xml = createPTreeFromXMLString(queryText,ipt_fast); // control queries are case sensitive
        doControlQuery(ep, xml, queryText, reply);
    }

    void doControlQuery(SocketEndpoint &ep, IPropertyTree *xml, const char *queryText, StringBuffer &reply)
    {
        if (logctx.queryTraceLevel() > 5)
            logctx.CTXLOG("doControlQuery (%d): %.80s", isMaster, queryText);
        // By this point we should have cascade-connected thanks to a prior <control:lock>
        // So do the query ourselves and in all child threads;
        const char *name = xml->queryName();
        CascadeMergeType mergeType=CascadeMergeNone;
        if (strstr(name, "querystats"))
            mergeType=CascadeMergeStats;
        else if (strstr(name, "queries"))
            mergeType=CascadeMergeQueries;
        Owned<IPropertyTree> mergedReply;
        if (mergeType!=CascadeMergeNone)
            mergedReply.setown(createPTree("Endpoint",ipt_fast));

        class casyncfor: public CAsyncFor
        {
            const char *queryText;
            CascadeManager *parent;
            IPropertyTree *mergedReply;
            CascadeMergeType mergeType;
            StringBuffer &reply;
            CriticalSection crit;
            SocketEndpoint &ep;
            unsigned numChildren;
            const IRoxieContextLogger &logctx;
            IPropertyTree *xml;

        public:
            casyncfor(IPropertyTree *_xml, const char *_queryText, CascadeManager *_parent, IPropertyTree *_mergedReply, CascadeMergeType _mergeType,
                      StringBuffer &_reply, SocketEndpoint &_ep, unsigned _numChildren, const IRoxieContextLogger &_logctx)
                : queryText(_queryText), parent(_parent), mergedReply(_mergedReply), mergeType(_mergeType), reply(_reply), ep(_ep), numChildren(_numChildren), logctx(_logctx), xml(_xml)
            {
            }
            void Do(unsigned i)
            {
                if (logctx.queryTraceLevel() > 5)
                    logctx.CTXLOG("doControlQuery::do (%d of %d): %.80s", i, numChildren, queryText);
                if (i == numChildren)
                    doMe();
                else
                {
                    StringBuffer childReply;
                    parent->doChildQuery(i, queryText, childReply);
                    Owned<IPropertyTree> replyXML = createPTreeFromXMLString(childReply,ipt_fast);
                    if (!replyXML)
                    {
                        StringBuffer err;
                        err.appendf("doControlQuery::do (%d of %d): %.80s received invalid response %s", i, numChildren, queryText, childReply.str());
                        logctx.CTXLOG("%s", err.str());
                        throw MakeStringException(ROXIE_INTERNAL_ERROR, "%s", err.str());
                    }
                    Owned<IPropertyTreeIterator> meat = replyXML->getElements("Endpoint");
                    ForEach(*meat)
                    {
                        CriticalBlock cb(crit);
                        if (mergedReply)
                        {
                            if (mergeType == CascadeMergeStats)
                                mergeStats(mergedReply, &meat->query());
                            else if (mergeType == CascadeMergeQueries)
                                mergeQueries(mergedReply, &meat->query());
                        }
                        else
                            toXML(&meat->query(), reply);
                    }
                }
            }
            void doMe()
            {
                StringBuffer myReply;
                myReply.append("<Endpoint ep='");
                ep.getUrlStr(myReply);
                myReply.append("'>\n");
                try
                {
                    globalPackageSetManager->doControlMessage(xml, myReply, logctx);
                }
                catch(IException *E)
                {
                    controlException(myReply, E, logctx);
                }
                catch(...)
                {
                    controlException(myReply, MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception"), logctx);
                }
                myReply.append("</Endpoint>\n");
                CriticalBlock cb(crit);
                if (mergedReply)
                {
                    Owned<IPropertyTree> replyXML = createPTreeFromXMLString(myReply,ipt_fast);
                    if (mergeType == CascadeMergeStats)
                        mergeStats(mergedReply, replyXML);
                    else if (mergeType == CascadeMergeQueries)
                        mergeQueries(mergedReply, replyXML);
                }
                else
                    reply.append(myReply);
            }
        } afor(xml, queryText, this, mergedReply, mergeType, reply, ep, activeChildren.ordinality(), logctx);
        afor.For(activeChildren.ordinality()+(isMaster ? 0 : 1), 10);
        activeChildren.kill();
        if (mergedReply)
            toXML(mergedReply, reply, 0, (mergeType == CascadeMergeQueries) ? XML_Embed|XML_LineBreak|XML_SortTags : XML_Format);
        if (logctx.queryTraceLevel() > 5)
            logctx.CTXLOG("doControlQuery (%d) finished: %.80s", isMaster, queryText);
    }

};

Semaphore CascadeManager::globalLock(1);

//================================================================================================================

class AccessTableEntry : public CInterface
{
    bool allow[2];
    IpSubNet subnet;
    RegExpr queries;
    StringBuffer errorMsg;
    int errorCode;
    StringBuffer queryText;
    SpinLock crappyUnsafeRegexLock;

public:
    AccessTableEntry(bool _allow, bool _allowBlind, const char *_base, const char *_mask, const char *_queries, const char *_errorMsg, int _errorCode)
    {

        // TBD IPv6 (not sure exactly what needs doing here)
        allow[false] = _allow;
        allow[true] = _allowBlind;
        errorMsg.append(_errorMsg);
        errorCode = _errorCode;

        if (!_base)
        {
            if (_mask)
                throw MakeStringException(ROXIE_ACL_ERROR, "ip not specified");
            _base = _mask = "0.0.0.0";
        }
        else if (!_mask)
            _mask = "255.255.255.255";
        if (!subnet.set(_base,_mask))
            throw MakeStringException(ROXIE_ACL_ERROR, "Invalid mask");

        if (!_queries)
            _queries=".*";
        queries.init(_queries, true);
        queryText.append(_queries);
    }

    bool match(IpAddress &peer, const char *query, bool isBlind, bool &access, StringBuffer &errMsg, int &errCode)
    {
        {
            //MORE: This could use a regex class that is thread safe and remove this spin lock
            SpinBlock b(crappyUnsafeRegexLock);
            if (!queries.find(query))
                return false;
        }
        if (!subnet.test(peer))
            return false;
        access = allow[isBlind];
        errMsg.clear().append(errorMsg.str());
        errCode = errorCode;
        return true;
    }

    const char * queryAccessTableEntryInfo(StringBuffer &info)
    {
        info.append("<AccessInfo ");
        info.appendf(" allow='%d'", allow[false]);
        info.appendf(" allowBlind='%d'", allow[true]);

        info.append(" base='");
        subnet.getNetText(info);
        info.append("' mask='");
        subnet.getMaskText(info);
        info.appendf("' filter='%s'", queryText.str());

        info.appendf(" errorMsg='%s'", errorMsg.str());

        info.appendf(" errorCode='%d'", errorCode);

        info.append("/>\n");

        return info.str();
    }
};

//================================================================================================================================

class ActiveQueryLimiter : implements IActiveQueryLimiter, public CInterface
{
    IHpccProtocolListener *parent;
    IHpccProtocolMsgSink *sink;
    bool accepted;
public:
    IMPLEMENT_IINTERFACE;
    ActiveQueryLimiter(IHpccProtocolListener *_parent) : parent(_parent)
    {
        sink = parent->queryMsgSink();
        CriticalBlock b(sink->getActiveCrit());
        if (sink->getIsSuspended())
        {
            accepted = false;
            if (traceLevel > 1)
                DBGLOG("Rejecting query since Roxie server pool %d is suspended ", parent->queryPort());
        }
        else
        {
            unsigned threadsActive = sink->getActiveThreadCount();
            unsigned poolSize = sink->getPoolSize();
            accepted = (threadsActive < poolSize);
            if (accepted && threadsActive > sink->getMaxActiveThreads())
            {
                sink->setMaxActiveThreads(threadsActive);
                if (traceLevel > 1)
                    DBGLOG("Maximum queries active %d of %d for pool %d", threadsActive, poolSize, parent->queryPort());
            }
            if (!accepted && traceLevel > 5)
                DBGLOG("Too many active queries (%d >= %d)", threadsActive, poolSize);
        }
        sink->incActiveThreadCount();
    }
    ~ActiveQueryLimiter()
    {
        CriticalBlock b(sink->getActiveCrit());
        sink->decActiveThreadCount();
    }
    virtual bool isAccepted(){return accepted;}
};

IActiveQueryLimiter *createActiveQueryLimiter(IHpccProtocolListener *listener)
{
    return new ActiveQueryLimiter(listener);
}

class CActiveQueryLimiterFactory : public CInterface, implements IActiveQueryLimiterFactory
{
public:
    IMPLEMENT_IINTERFACE;
    CActiveQueryLimiterFactory(){}
    IActiveQueryLimiter *create(IHpccProtocolListener *listener)
    {
        return createActiveQueryLimiter(listener);
    }
};

static Owned<IActiveQueryLimiterFactory> queryLimiterFactory;
IActiveQueryLimiterFactory *ensureLimiterFactory()
{
    if (!queryLimiterFactory)
        queryLimiterFactory.setown(new CActiveQueryLimiterFactory());
    return queryLimiterFactory;
}

//================================================================================================================================

class RoxieListener : public Thread, implements IHpccProtocolListener, implements IHpccProtocolMsgSink, implements IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE;
    RoxieListener(unsigned _poolSize, bool _suspended) : Thread("RoxieListener")
    {
        running = false;
        suspended = _suspended;
        poolSize = _poolSize;
        threadsActive = 0;
        maxThreadsActive = 0;
    }
    virtual IHpccProtocolMsgSink *queryMsgSink()
    {
        return this;
    }
    virtual CriticalSection &getActiveCrit()
    {
        return activeCrit;
    }
    virtual bool getIsSuspended()
    {
        return suspended;
    }
    virtual unsigned getActiveThreadCount()
    {
        return threadsActive;
    }
    virtual unsigned getPoolSize()
    {
        return poolSize;
    }
    virtual unsigned getMaxActiveThreads()
    {
        return maxThreadsActive;
    }
    virtual void setMaxActiveThreads(unsigned val)
    {
        maxThreadsActive=val;
    }
    virtual void incActiveThreadCount()
    {
        threadsActive++;
    }
    virtual void decActiveThreadCount()
    {
        threadsActive--;
    }

    static void updateAffinity()
    {
#ifdef CPU_ZERO
        if (sched_getaffinity(0, sizeof(cpu_set_t), &cpuMask))
        {
            if (traceLevel)
                DBGLOG("Unable to get CPU affinity - thread affinity settings will be ignored");
            cpuCores = 0;
            lastCore = 0;
            CPU_ZERO(&cpuMask);
        }
        else
        {
#if __GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 6)
            cpuCores = CPU_COUNT(&cpuMask);
#else
            cpuCores = 0;
            unsigned setSize = CPU_SETSIZE;
            while (setSize--)
            {
                if (CPU_ISSET(setSize, &cpuMask))
                    ++cpuCores;
            }
#endif /* GLIBC */
            if (traceLevel)
                traceAffinity(&cpuMask);
        }
#endif
    }

    virtual void start()
    {
        // Note we allow a few additional threads than requested - these are the threads that return "Too many active queries" responses
        pool.setown(createThreadPool("RoxieSocketWorkerPool", this, NULL, poolSize+5, INFINITE));
        assertex(!running);
        Thread::start();
        started.wait();
    }

    virtual bool stop(unsigned timeout)
    {
        if (running)
        {
            running = false;
            join();
            Release();
        }
        return pool->joinAll(false, timeout);
    }

    void reportBadQuery(const char *name, const IRoxieContextLogger &logctx)
    {
        // MORE - may want to put in a mechanism to avoid swamping SNMP with bad query reports if someone kicks off a thor job with a typo....
        logctx.logOperatorException(NULL, NULL, 0, "Unknown query %s", name);
    }

    void checkWuAccess(bool isBlind)
    {
        // Could do some LDAP access checking here (via Dali?)
    }

    void checkAccess(IpAddress &peer, const char *queryName, const char *queryText, bool isBlind)
    {
        bool allowed = true;
        StringBuffer errorMsg;
        int errorCode = -1;
        ForEachItemIn(idx, accessTable)
        {
            AccessTableEntry &item = accessTable.item(idx);
            item.match(peer, queryName, isBlind, allowed, errorMsg, errorCode);
            item.match(peer, queryText, isBlind, allowed, errorMsg, errorCode);
        }
        if (!allowed)
        {
            StringBuffer peerStr;
            peer.getIpText(peerStr);
            StringBuffer qText;
            if (queryText && *queryText)
                decodeXML(queryText, qText);

            StringBuffer errText;
            if (errorCode != -1)
                errText.appendf("errorCode = %d : ", errorCode);
            else
                errorCode = ROXIE_ACCESS_ERROR;

            if (errorMsg.length())
                throw MakeStringException(errorCode, "Cannot run %s : %s from host %s because %s %s", queryName, qText.str(), peerStr.str(), errText.str(), errorMsg.str());
            else
                throw MakeStringException(errorCode, "Access to %s : %s from host %s is not allowed %s", queryName, qText.str(), peerStr.str(), errText.str());
        }
    }

    virtual void addAccess(bool allow, bool allowBlind, const char *ip, const char *mask, const char *query, const char *errorMsg, int errorCode)
    {
        accessTable.append(*new AccessTableEntry(allow, allowBlind, ip, mask, query, errorMsg, errorCode));
    }

    void queryAccessInfo(StringBuffer &info)
    {
        info.append("<ACCESSINFO>\n");
        ForEachItemIn(idx, accessTable)
        {
            AccessTableEntry &item = accessTable.item(idx);
            item.queryAccessTableEntryInfo(info);
        }
        info.append("</ACCESSINFO>\n");
    }

    void setThreadAffinity(int numCores)
    {
#ifdef CPU_ZERO
        // Note - strictly speaking not threadsafe but any race conditions are (a) unlikely and (b) harmless
        if (cpuCores)
        {
            if (numCores > 0 && numCores < (int) cpuCores)
            {
                cpu_set_t threadMask;
                CPU_ZERO(&threadMask);
                unsigned cores = 0;
                unsigned offset = lastCore;
                unsigned core;
                for (core = 0; core < CPU_SETSIZE; core++)
                {
                    unsigned useCore = (core + offset) % CPU_SETSIZE;
                    if (CPU_ISSET(useCore, &cpuMask))
                    {
                        CPU_SET(useCore, &threadMask);
                        cores++;
                        if ((int) cores == numCores)
                        {
                            lastCore = useCore+1;
                            break;
                        }
                    }
                }
                if (traceLevel > 3)
                    traceAffinity(&threadMask);
                pthread_setaffinity_np(GetCurrentThreadId(), sizeof(cpu_set_t), &threadMask);
            }
            else
            {
                if (traceLevel > 3)
                    traceAffinity(&cpuMask);
                pthread_setaffinity_np(GetCurrentThreadId(), sizeof(cpu_set_t), &cpuMask);
            }
        }
        clearAffinityCache();
#endif
    }

protected:
    unsigned poolSize;
    bool running;
    bool suspended;
    Semaphore started;
    Owned<IThreadPool> pool;

    unsigned threadsActive;
    unsigned maxThreadsActive;
    CriticalSection activeCrit;

#ifdef CPU_ZERO
    static cpu_set_t cpuMask;
    static unsigned cpuCores;
    static unsigned lastCore;

private:
    static void traceAffinity(cpu_set_t *mask)
    {
        StringBuffer trace;
        for (unsigned core = 0; core < CPU_SETSIZE; core++)
        {
            if (CPU_ISSET(core, mask))
                trace.appendf(",%d", core);
        }
        if (trace.length())
            DBGLOG("Process affinity is set to use core(s) %s", trace.str()+1);
    }
#endif

    CIArrayOf<AccessTableEntry> accessTable;
};

#ifdef CPU_ZERO
cpu_set_t RoxieListener::cpuMask;
unsigned RoxieListener::cpuCores;
unsigned RoxieListener::lastCore;
#endif

extern void updateAffinity(unsigned __int64 affinity)
{
    if (affinity)  // 0 means use the value already set for this process
    {
#ifndef CPU_ZERO
        throw makeStringException(ROXIE_INTERNAL_ERROR, "Setting Roxie affinity is not supported on this operating system");
#else
        cpu_set_t cpus;
        CPU_ZERO(&cpus);
        for (unsigned core = 0; core < CPU_SETSIZE; core++)
        {
            if (affinity & 1)
                CPU_SET(core, &cpus);
            affinity >>= 1;
        }

        //MORE: I think this only sets the affinity of the process, not of the threads.
        //It would require code to iterate through /proc/<pid>/task/*
        if (sched_setaffinity(0, sizeof(cpu_set_t), &cpus))
            throw makeStringException(errno, "Failed to set affinity");
        clearAffinityCache();
#endif
    }
    RoxieListener::updateAffinity();
}

class RoxieWorkUnitListener : public RoxieListener
{
    Owned<IJobQueue> queue;
public:
    RoxieWorkUnitListener(unsigned _poolSize, bool _suspended)
      : RoxieListener(_poolSize, _suspended)
    {
    }

    virtual const SocketEndpoint& queryEndpoint() const
    {
        throwUnexpected(); // MORE get rid of this function altogether?
    }

    virtual unsigned int queryPort() const
    {
        return 0;
    }

    virtual StringArray &getTargetNames(StringArray &targets)
    {
        CloneArray(targets, allQuerySetNames);
        return targets;
    }

    virtual IHpccProtocolMsgContext *createMsgContext(time_t startTime)
    {
        UNIMPLEMENTED;
    }

    virtual void runOnce(const char *query)
    {
        Owned<IPooledThread> worker = createNew();
        worker->init((void *) query);
        worker->threadmain();
    }

    virtual void noteQuery(IHpccProtocolMsgContext *msgctx, const char *peer, bool failed, unsigned bytesOut, unsigned elapsed, unsigned memused, unsigned slavesReplyLen, bool continuationNeeded)
    {
    }

    virtual void onQueryMsg(IHpccProtocolMsgContext *msgctx, IPropertyTree *msg, IHpccProtocolResponse *protocol, unsigned flags, PTreeReaderOptions readFlags, const char *target, unsigned idx, unsigned &memused, unsigned &slaveReplyLen)
    {
        UNIMPLEMENTED;
    }
    virtual bool stop(unsigned timeout)
    {
        if (queue)
        {
            DBGLOG("RoxieWorkUnitListener::stop");
            queue->cancelAcceptConversation();
        }
        return RoxieListener::stop(timeout);
    }

    virtual void stopListening()
    {
        if (queue)
        {
            DBGLOG("RoxieWorkUnitListener::stopListening");
            queue->cancelAcceptConversation();
        }
    }

    virtual void disconnectQueue()
    {
        if (queue)
        {
            DBGLOG("RoxieWorkUnitListener::disconnectQueue");
            queue->cancelAcceptConversation();
            queue.clear();
        }
    }

    virtual int run()
    {
        running = true;
        started.signal();
        Owned<IRoxieDaliHelper> daliHelper = connectToDali();
        while (running)
        {
            if (daliHelper->connected())
            {
#ifdef _CONTAINERIZED
                VStringBuffer queueNames("%s.agent", topology->queryProp("@name"));
#else
                SCMStringBuffer queueNames;
                if (topology->hasProp("@queueNames")) // MORE - perhaps should be in the farmProcess(0) section
                    queueNames.set(topology->queryProp("@queueNames"));
                else
                    getRoxieQueueNames(queueNames, topology->queryProp("@name"));
#endif
                if (queueNames.length())
                {
                    if (traceLevel)
                        DBGLOG("roxie: Waiting on queue(s) '%s'", queueNames.str());
                    try
                    {
                        queue.setown(createJobQueue(queueNames.str()));
                        queue->connect(false);
                        daliHelper->noteQueuesRunning(queueNames.str());
                        while (running && daliHelper->connected())
                        {
                            Owned<IJobQueueItem> item = queue->dequeue();
                            if (item.get())
                            {
                                if (traceLevel)
                                    PROGLOG("roxie: Dequeued workunit request '%s'", item->queryWUID());
                                pool->start((void *) item->queryWUID());
                            }
                        }
                        queue.clear();
                    }
                    catch (IException *E)
                    {
                        if (traceLevel)
                            EXCLOG(E, "roxie: Dali connection lost");
                        E->Release();
                        daliHelper->disconnect();
                        queue.clear();
                    }
                }
            }
            else
            {
                if (traceLevel)
                    DBGLOG("roxie: Waiting for dali connection before waiting for queue");
                while (running && !daliHelper->connected())
                    Sleep(ROXIE_DALI_CONNECT_TIMEOUT);
            }
        }
        return 0;
    }

    virtual IPooledThread* createNew();
};


class RoxieQueryWorker : implements IPooledThread, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    RoxieQueryWorker(RoxieListener *_pool)
    {
        pool = _pool;
        qstart = msTick();
        time(&startTime);
    }

    //  interface IPooledThread
    virtual void init(void *) override
    {
        qstart = msTick();
        time(&startTime);
    }

    virtual bool canReuse() const override
    {
        return true;
    }

    virtual bool stop() override
    {
        IERRLOG("RoxieQueryWorker stopped with queries active");
        return true;
    }

protected:
    RoxieListener *pool;
    unsigned qstart;
    time_t startTime;

    void noteQuery(bool failed, unsigned elapsedTime, unsigned priority)
    {
        Owned <IJlibDateTime> now = createDateTimeNow();
        unsigned y,mo,d,h,m,s,n;
        now->getLocalTime(h, m, s, n);
        now->getLocalDate(y, mo, d);
        lastQueryTime = h*10000 + m * 100 + s;
        lastQueryDate = y*10000 + mo * 100 + d;

        switch(priority)
        {
        case 0: loQueryStats.noteQuery(failed, elapsedTime); break;
        case 1: hiQueryStats.noteQuery(failed, elapsedTime); break;
        case 2: slaQueryStats.noteQuery(failed, elapsedTime); break;
        default: unknownQueryStats.noteQuery(failed, elapsedTime); return; // Don't include unknown in the combined stats
        }
        combinedQueryStats.noteQuery(failed, elapsedTime);
    }
};

/**
 * RoxieWorkUnitWorker is the threadpool member that runs a query submitted as a
 * workunit via a job queue. A temporary IQueryFactory object is created for the
 * workunit and then executed.
 *
 * Any slaves that need to load the query do so using a lazy load mechanism, checking
 * whether the wuid named in the logging prefix info can be loaded any time a query
 * is received for which no factory exists. Any query that a slave loads as a
 * result is added to a cache to ensure that it stays around until the server's query
 * terminates - a ROXIE_UNLOAD message is broadcast at that time to allow the slaves
 * to release any cached IQueryFactory objects.
 *
 **/

class RoxieWorkUnitWorker : public RoxieQueryWorker
{
public:
    RoxieWorkUnitWorker(RoxieListener *_pool)
        : RoxieQueryWorker(_pool)
    {
    }

    virtual void init(void *_r) override
    {
        wuid.set((const char *) _r);
        RoxieQueryWorker::init(_r);
    }

    virtual void threadmain() override
    {
        assertex(wuid.length());
        bool standalone = *wuid.str()=='-';
        Owned<IRoxieDaliHelper> daliHelper;
        Owned<IConstWorkUnit> wu;
        Owned<const IQueryDll> dll;
        if (standalone)
        {
            Owned<ILoadedDllEntry> standAloneDll = createExeDllEntry(wuid.get()+1);
            StringBuffer wuXML;
            if (getEmbeddedWorkUnitXML(standAloneDll, wuXML))
            {
                wu.setown(createLocalWorkUnit(wuXML));
                dll.setown(createExeQueryDll(wuid.get()+1));
            }
        }
        else
        {
            daliHelper.setown(connectToDali());
            wu.setown(daliHelper->attachWorkunit(wuid.get(), NULL));
        }
        Owned<StringContextLogger> logctx = new StringContextLogger(wuid.get());
        if (wu->hasDebugValue("GlobalId"))
        {
            SCMStringBuffer globalId;
            SocketEndpoint ep;
            ep.setLocalHost(0);
            SCMStringBuffer callerId;
            logctx->setGlobalId(wu->getDebugValue("GlobalId", globalId).str(), ep, GetCurrentProcessId());
            wu->getDebugValue("CallerId", callerId);
            if (callerId.length())
                logctx->setCallerId(callerId.str());
        }
        Owned<IQueryFactory> queryFactory;
        try
        {
            checkWorkunitVersionConsistency(wu);
            if (daliHelper)
                daliHelper->noteWorkunitRunning(wuid.get(), true);
            if (!wu)
                throw MakeStringException(ROXIE_DALI_ERROR, "Failed to open workunit %s", wuid.get());
            queryFactory.setown(createServerQueryFactoryFromWu(wu, dll));
        }
        catch (IException *E)
        {
            reportException(wu, E, *logctx);
            if (daliHelper)
                daliHelper->noteWorkunitRunning(wuid.get(), false);
            throw;
        }
#ifndef _DEBUG
        catch(...)
        {
            reportUnknownException(wu, *logctx);
            throw;
        }
#endif

        doMain(wu, queryFactory, *logctx);
        sendUnloadMessage(queryFactory->queryHash(), wuid.get(), *logctx);
        queryFactory.clear();
        if (daliHelper)
            daliHelper->noteWorkunitRunning(wuid.get(), false);
        if (standalone && traceLevel)
        {
            StringBuffer wuXML;
            exportWorkUnitToXML(wu, wuXML, true, true, true);
            DBGLOG("%s", wuXML.str());
        }
        clearKeyStoreCache(false);   // Bit of a kludge - cache should really be smarter
    }

    void doMain(IConstWorkUnit *wu, IQueryFactory *queryFactory, StringContextLogger &logctx)
    {
        bool failed = true; // many paths to failure, only one to success...
        unsigned memused = 0;
        unsigned slavesReplyLen = 0;
        unsigned priority = (unsigned) -2;
        try
        {
            queryCount++;
            bool isBlind = wu->getDebugValueBool("blindLogging", false);
            if (pool)
            {
                pool->checkWuAccess(isBlind);
                Owned<IActiveQueryLimiter> l = createActiveQueryLimiter(pool);
                if (!l->isAccepted())
                {
                    IException *e = MakeStringException(ROXIE_TOO_MANY_QUERIES, "Too many active queries");
                    if (trapTooManyActiveQueries)
                        logctx.logOperatorException(e, __FILE__, __LINE__, NULL);
                    throw e;
                }
                int bindCores = wu->getDebugValueInt("bindCores", coresPerQuery);
                if (bindCores > 0)
                    pool->setThreadAffinity(bindCores);
            }
            isBlind = isBlind || blindLogging;
            logctx.setBlind(isBlind);
            priority = queryFactory->queryOptions().priority;
            switch (priority)
            {
            case 0: loQueryStats.noteActive(); break;
            case 1: hiQueryStats.noteActive(); break;
            case 2: slaQueryStats.noteActive(); break;
            }
            combinedQueryStats.noteActive();
            Owned<IRoxieServerContext> ctx = queryFactory->createContext(wu, logctx);
            try
            {
                ctx->process();
                memused = (unsigned)(ctx->getMemoryUsage() / 0x100000);
                slavesReplyLen = ctx->getSlavesReplyLen();
                ctx->done(false);
                failed = false;
            }
            catch(...)
            {
                memused = (unsigned)(ctx->getMemoryUsage() / 0x100000);
                slavesReplyLen = ctx->getSlavesReplyLen();
                ctx->done(true);
                throw;
            }
        }
        catch (WorkflowException *E)
        {
            reportException(wu, E, logctx);
            E->Release();
        }
        catch (IException *E)
        {
            reportException(wu, E, logctx);
            E->Release();
        }
#ifndef _DEBUG
        catch(...)
        {
            reportUnknownException(wu, logctx);
        }
#endif
        unsigned elapsed = msTick() - qstart;
        noteQuery(failed, elapsed, priority);
        queryFactory->noteQuery(startTime, failed, elapsed, memused, slavesReplyLen, 0);
        if (logctx.queryTraceLevel() && (logctx.queryTraceLevel() > 2 || logFullQueries || logctx.intercept))
        {
            StringBuffer s;
            logctx.getStats(s);

            StringBuffer txidInfo;
            const char *globalId = logctx.queryGlobalId();
            if (globalId && *globalId)
            {
                txidInfo.append(" [GlobalId: ").append(globalId);
                SCMStringBuffer s;
                wu->getDebugValue("CallerId", s);
                if (s.length())
                    txidInfo.append(", CallerId: ").append(s.str());
                s.set(logctx.queryLocalId());
                if (s.length())
                    txidInfo.append(", LocalId: ").append(s.str());
                txidInfo.append(']');
            }

            logctx.CTXLOG("COMPLETE: %s%s complete in %d msecs memory=%d Mb priority=%d slavesreply=%d%s", wuid.get(), txidInfo.str(), elapsed, memused, priority, slavesReplyLen, s.str());
        }
    }

private:
#ifndef _DEBUG
    void reportUnknownException(IConstWorkUnit *wu, const IRoxieContextLogger &logctx)
    {
        Owned<IException> E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception");
        reportException(wu, E, logctx);
    }
#endif
    void reportException(IConstWorkUnit *wu, IException *E, const IRoxieContextLogger &logctx)
    {
        logctx.CTXLOG("FAILED: %s", wuid.get());
        StringBuffer error;
        E->errorMessage(error);
        logctx.CTXLOG("EXCEPTION: %s", error.str());
        addWuException(wu, E);
        WorkunitUpdate w(&wu->lock());
        w->setState(WUStateFailed);
    }

    StringAttr wuid;
};

class RoxieProtocolMsgContext : implements IHpccProtocolMsgContext, public CInterface
{
public:
    StringAttr queryName;
    StringAttr uid = "-";
    StringAttr callerId;
    Owned<CascadeManager> cascade;
    Owned<IDebuggerContext> debuggerContext;
    Owned<CDebugCommandHandler> debugCmdHandler;
    Owned<StringContextLogger> logctx;
    Owned<IQueryFactory> queryFactory;

    SocketEndpoint ep;
    time_t startTime;
public:
    IMPLEMENT_IINTERFACE;

    RoxieProtocolMsgContext(const SocketEndpoint &_ep, time_t _startTime) : startTime(_startTime)
    {
        ep.set(_ep);
        unknownQueryStats.noteActive();
        queryCount++;
    }
    ~RoxieProtocolMsgContext()
    {
    }

    inline ContextLogger &ensureContextLogger()
    {
        if (!logctx)
        {
            unsigned instanceId = getNextInstanceId();
            StringBuffer ctxstr;
            logctx.setown(new StringContextLogger(ep.getIpText(ctxstr).appendf(":%u{%u}", ep.port, instanceId).str()));
        }
        return *logctx;
    }
    virtual void initQuery(StringBuffer &target, const char *name)
    {
        if (!name || !*name)
            throw MakeStringException(ROXIE_UNKNOWN_QUERY, "ERROR: Query name not specified");

        queryName.set(name);
        queryFactory.setown(globalPackageSetManager->getQuery(name, &target, NULL, *logctx));
        if (!queryFactory)
        {
            logctx->logOperatorException(NULL, NULL, 0, "Unknown query %s", name);
            if (globalPackageSetManager->getActivePackageCount())
            {
                StringBuffer targetMsg;
                if (target.length())
                    targetMsg.append(", in target ").append(target);
                throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown query %s%s", queryName.get(), targetMsg.str());
            }

            throw MakeStringException(ROXIE_NO_PACKAGES_ACTIVE, "Unknown query %s (no packages active)", queryName.get());
        }
        queryFactory->checkSuspended();
        IConstWorkUnit *workunit = queryFactory->queryWorkUnit();
        if (workunit && workunit->getDebugValueBool("generateGlobalId", false) && isEmptyString(logctx->queryGlobalId()))
        {
            StringBuffer gen_id;
            appendGloballyUniqueId(gen_id);
            setTransactionId(gen_id.str(), nullptr, true);
        }
    }
    virtual void noteQueryActive()
    {
        unsigned priority = getQueryPriority();
        switch (priority)
        {
        case 0: loQueryStats.noteActive(); break;
        case 1: hiQueryStats.noteActive(); break;
        case 2: slaQueryStats.noteActive(); break;
        }
        unknownQueryStats.noteComplete();
        combinedQueryStats.noteActive();
    }
    IQueryFactory *queryQueryFactory(){return queryFactory;}
    virtual IContextLogger *queryLogContext()
    {
        return &ensureContextLogger();
    }
    inline CascadeManager &ensureCascadeManager()
    {
        if (!cascade)
            cascade.setown(new CascadeManager(ensureContextLogger(), getTopology()));
        return *cascade;
    }

    virtual void setTransactionId(const char *id, const char *caller, bool global) override
    {
        if (!id || !*id)
            return;
        uid.set(id);
        ensureContextLogger();
        if (!isEmptyString(logctx->queryGlobalId())) //globalId wins
            return;
        StringBuffer s;
        ep.getIpText(s).appendf(":%u{%s}", ep.port, uid.str()); //keep no matter what for existing log parsers
        if (global)
        {
            s.append('[');
            logctx->setGlobalId(id, ep, 0);
            if (caller && *caller)
            {
                setCallerId(caller);
                logctx->setCallerId(caller);
            }
            if (callerId.length())
                s.appendf("caller:%s", callerId.str());

            const char *local = logctx->queryLocalId(); //generated in setGlobalId above
            if (local && *local)
            {
                if (callerId.length())
                    s.append(',');
                s.appendf("local:%s", local);
            }
            s.append("]");
        }
        logctx->set(s.str());
    }
    virtual void setCallerId(const char *id)
    {
        if (!id || !*id)
            return;
        callerId.set(id);
    }
    inline IDebuggerContext &ensureDebuggerContext(const char *id)
    {
        if (!debuggerContext)
        {
            if (!id)
    #ifdef _DEBUG
                id="*";
    #else
                throw MakeStringException(ROXIE_DEBUG_ERROR, "Debug id not specified");
    #endif
            uid.set(id);
            debuggerContext.setown(queryRoxieDebugSessionManager().lookupDebuggerContext(id));
            if (!debuggerContext)
                throw MakeStringException(ROXIE_DEBUG_ERROR, "No active query matching context %s found", id);
        }
        return *debuggerContext;
    }
    inline CDebugCommandHandler &ensureDebugCommandHandler()
    {
        if (!debugCmdHandler.get())
            debugCmdHandler.setown(new CDebugCommandHandler);
        return *debugCmdHandler;
    }

    virtual bool checkSetBlind(bool blind)
    {
        blind = blind || blindLogging;
        ensureContextLogger().setBlind(blind);
        return blind;
    }
    virtual void verifyAllowDebug()
    {
        if (!debugPermitted || !ep.port)
            throw MakeStringException(ROXIE_ACCESS_ERROR, "Debug queries are not permitted on this system");
    }
    virtual bool logFullQueries()
    {
        return ::logFullQueries;
    }
    virtual bool trapTooManyActiveQueries()
    {
        return ::trapTooManyActiveQueries;
    }
    virtual bool getStripWhitespace()
    {
        return queryFactory ? queryFactory->queryOptions().stripWhitespaceFromStoredDataset : (defaultXmlReadFlags & ptr_ignoreWhiteSpace);
    }
    virtual int getBindCores()
    {
        return queryFactory ? queryFactory->queryOptions().bindCores : 0;
    }
    virtual void setTraceLevel(unsigned traceLevel)
    {
        if (logctx)
            logctx->setTraceLevel(traceLevel);
    }
    virtual void setIntercept(bool intercept)
    {
        if (logctx)
            logctx->setIntercept(intercept);
    }
    virtual bool getIntercept()
    {
        return (logctx) ? logctx->intercept : false;
    }
    virtual void outputLogXML(IXmlStreamFlusher &out)
    {
        if (logctx)
            logctx->outputXML(out);
    }
    virtual void writeLogXML(IXmlWriter &writer)
    {
        if (logctx)
        {
            writer.outputBeginNested("Tracing", true);
            logctx->writeXML(writer);
            writer.outputEndNested("Tracing");
        }
    }

    virtual unsigned getQueryPriority()
    {
        return queryFactory ? queryFactory->queryOptions().priority : (unsigned) -2;
    }
    void noteQueryStats(bool failed, unsigned elapsedTime)
    {
        Owned <IJlibDateTime> now = createDateTimeNow();
        unsigned y,mo,d,h,m,s,n;
        now->getLocalTime(h, m, s, n);
        now->getLocalDate(y, mo, d);
        lastQueryTime = h*10000 + m * 100 + s;
        lastQueryDate = y*10000 + mo * 100 + d;

        switch(getQueryPriority())
        {
        case 0: loQueryStats.noteQuery(failed, elapsedTime); break;
        case 1: hiQueryStats.noteQuery(failed, elapsedTime); break;
        case 2: slaQueryStats.noteQuery(failed, elapsedTime); break;
        default: unknownQueryStats.noteQuery(failed, elapsedTime); return; // Don't include unknown in the combined stats
        }
        combinedQueryStats.noteQuery(failed, elapsedTime);
    }
    void noteQuery(const char *peer, bool failed, unsigned elapsed, unsigned memused, unsigned slavesReplyLen, unsigned bytesOut, bool continuationNeeded)
    {
        noteQueryStats(failed, elapsed);
        if (queryFactory)
        {
            queryFactory->noteQuery(startTime, failed, elapsed, memused, slavesReplyLen, bytesOut);
            queryFactory.clear();
        }
        if (logctx && logctx->queryTraceLevel() && (logctx->queryTraceLevel() > 2 || logFullQueries() || logctx->intercept))
        {
            if (queryName.get())
            {
                StringBuffer s;
                logctx->getStats(s);

                StringBuffer txIds;
                if (callerId.length())
                    txIds.appendf("caller: %s", callerId.str());
                const char *localId = logctx->queryLocalId();
                if (localId && *localId)
                {
                    if (txIds.length())
                        txIds.append(", ");
                    txIds.append("local: ").append(localId);
                }
                if (txIds.length())
                    txIds.insert(0, '[').append(']');
                logctx->CTXLOG("COMPLETE: %s %s%s from %s complete in %d msecs memory=%d Mb priority=%d slavesreply=%d resultsize=%d continue=%d%s", queryName.get(), uid.get(), txIds.str(), peer, elapsed, memused, getQueryPriority(), slavesReplyLen, bytesOut, continuationNeeded, s.str());
            }
        }
    }
};


class RoxieProtocolMsgSink : implements IHpccNativeProtocolMsgSink, public CInterface
{
    CriticalSection activeCrit;
    SocketEndpoint ep;
    CIArrayOf<AccessTableEntry> accessTable;
    unsigned threadsActive;
    unsigned maxThreadsActive;
    unsigned poolSize;
    bool suspended;

public:
    IMPLEMENT_IINTERFACE;

    RoxieProtocolMsgSink(const SocketEndpoint &_ep, unsigned _poolSize, bool _suspended)
    {
        ep.set(_ep);
        threadsActive = 0;
        maxThreadsActive = 0;
        suspended = _suspended;
        poolSize = _poolSize;
    }
    virtual CriticalSection &getActiveCrit()
    {
        return activeCrit;
    }
    virtual bool getIsSuspended()
    {
        return suspended;
    }
    virtual unsigned getActiveThreadCount()
    {
        return threadsActive;
    }
    virtual unsigned getPoolSize()
    {
        return poolSize;
    }
    virtual unsigned getMaxActiveThreads()
    {
        return maxThreadsActive;
    }
    virtual void setMaxActiveThreads(unsigned val)
    {
        maxThreadsActive=val;
    }
    virtual void incActiveThreadCount()
    {
        threadsActive++;
    }
    virtual void decActiveThreadCount()
    {
        threadsActive--;
    }
    virtual void addAccess(bool allow, bool allowBlind, const char *ip, const char *mask, const char *query, const char *errorMsg, int errorCode)
    {
        accessTable.append(*new AccessTableEntry(allow, allowBlind, ip, mask, query, errorMsg, errorCode));
    }

    virtual void checkAccess(IpAddress &peer, const char *queryName, const char *queryText, bool isBlind)
    {
        bool allowed = true;
        StringBuffer errorMsg;
        int errorCode = -1;
        ForEachItemIn(idx, accessTable)
        {
            AccessTableEntry &item = accessTable.item(idx);
            item.match(peer, queryName, isBlind, allowed, errorMsg, errorCode);
            item.match(peer, queryText, isBlind, allowed, errorMsg, errorCode);
        }
        if (!allowed)
        {
            StringBuffer peerStr;
            peer.getIpText(peerStr);
            StringBuffer qText;
            if (queryText && *queryText)
                decodeXML(queryText, qText);

            StringBuffer errText;
            if (errorCode != -1)
                errText.appendf("errorCode = %d : ", errorCode);
            else
                errorCode = ROXIE_ACCESS_ERROR;

            if (errorMsg.length())
                throw MakeStringException(errorCode, "Cannot run %s : %s from host %s because %s %s", queryName, qText.str(), peerStr.str(), errText.str(), errorMsg.str());
            else
                throw MakeStringException(errorCode, "Access to %s : %s from host %s is not allowed %s", queryName, qText.str(), peerStr.str(), errText.str());
        }
    }

    virtual void queryAccessInfo(StringBuffer &info)
    {
        info.append("<ACCESSINFO>\n");
        ForEachItemIn(idx, accessTable)
        {
            AccessTableEntry &item = accessTable.item(idx);
            item.queryAccessTableEntryInfo(info);
        }
        info.append("</ACCESSINFO>\n");
    }

    IHpccProtocolMsgContext *createMsgContext(time_t startTime)
    {
        return new RoxieProtocolMsgContext(ep, startTime);
    }
    virtual StringArray &getTargetNames(StringArray &targets)
    {
        CloneArray(targets, allQuerySetNames);
        return targets;
    }

    inline RoxieProtocolMsgContext * checkGetRoxieMsgContext(IHpccProtocolMsgContext *msgctx)
    {
        if (!msgctx)
            throw MakeStringExceptionDirect(ROXIE_INTERNAL_ERROR, "Protocol message context cannot be null"); //if protocols become plugins have to be cautious
        RoxieProtocolMsgContext *roxieMsgCtx = dynamic_cast<RoxieProtocolMsgContext*>(msgctx);
        if (!roxieMsgCtx)
            throw MakeStringExceptionDirect(ROXIE_INTERNAL_ERROR, "Invalid protocol message context");
        return roxieMsgCtx;
    }

    inline RoxieProtocolMsgContext * checkGetRoxieMsgContext(IHpccProtocolMsgContext *msgctx, IPropertyTree *msg)
    {
        if (!msg)
            throw MakeStringExceptionDirect(ROXIE_INTERNAL_ERROR, "Protocol message cannot be null"); //if protocols become plugins have to be cautious
        return checkGetRoxieMsgContext(msgctx);
    }

    virtual void onQueryMsg(IHpccProtocolMsgContext *msgctx, IPropertyTree *msg, IHpccProtocolResponse *protocol, unsigned flags, PTreeReaderOptions xmlReadFlags, const char *target, unsigned idx, unsigned &memused, unsigned &slavesReplyLen)
    {
        RoxieProtocolMsgContext *roxieMsgCtx = checkGetRoxieMsgContext(msgctx, msg);
        IQueryFactory *f = roxieMsgCtx->queryQueryFactory();
        Owned<IRoxieServerContext> ctx = f->createContext(msg, protocol, flags, *roxieMsgCtx->logctx, xmlReadFlags, target);
        if (!(flags & HPCC_PROTOCOL_NATIVE))
        {
            ctx->process();
            if (msgctx->getIntercept())
            {
                Owned<IXmlWriter> logwriter = protocol->writeAppendContent(nullptr);
                msgctx->writeLogXML(*logwriter);
            }

            protocol->finalize(idx);
            memused += (unsigned)(ctx->getMemoryUsage() / 0x100000);
            slavesReplyLen += ctx->getSlavesReplyLen();
        }
        else
        {
            try
            {
                ctx->process();
                memused = (unsigned)(ctx->getMemoryUsage() / 0x100000);
                slavesReplyLen = ctx->getSlavesReplyLen();
                ctx->done(false);
            }
            catch(...)
            {
                memused = (unsigned)(ctx->getMemoryUsage() / 0x100000);
                slavesReplyLen = ctx->getSlavesReplyLen();
                ctx->done(true);
                throw;
            }
        }
    }
    virtual void onControlMsg(IHpccProtocolMsgContext *msgctx, IPropertyTree *msg, IHpccProtocolResponse *protocol)
    {
        StringBuffer reply;
        RoxieProtocolMsgContext *roxieMsgCtx = checkGetRoxieMsgContext(msgctx, msg);
        const char *name = msg->queryName();
        IContextLogger &logctx = *msgctx->queryLogContext();

        StringBuffer xml;
        toXML(msg, xml, 0, 0);

        if (strieq(name, "control:lock"))
        {
            if (logctx.queryTraceLevel() > 8)
                logctx.CTXLOG("Got lock request %s", xml.str());
            roxieMsgCtx->ensureCascadeManager().doLockGlobal(reply, false);
            if (logctx.queryTraceLevel() > 8)
                logctx.CTXLOG("lock reply %s", reply.str());
            unknownQueryStats.noteComplete();
        }
        else if (strieq(name, "control:childlock"))
        {
            if (logctx.queryTraceLevel() > 8)
                logctx.CTXLOG("Got childlock request %s", xml.str());
            roxieMsgCtx->ensureCascadeManager().doLockChild(msg, xml.str(), reply);
            if (logctx.queryTraceLevel() > 8)
                logctx.CTXLOG("childlock reply %s", reply.str());
            unknownQueryStats.noteComplete();
        }
        else
        {
            bool lock = msg->getPropBool("@lock", false);
            bool lockAll = msg->getPropBool("@lockAll", false);
            if (!roxieMsgCtx->ensureCascadeManager().checkEntered() && (lock || lockAll)) //only if not already locked
            {
                if (logctx.queryTraceLevel() > 8)
                    logctx.CTXLOG("controll msg lock%s attribute", lockAll ? "All" : "");
                roxieMsgCtx->ensureCascadeManager().doLockGlobal(reply, false);
                if (logctx.queryTraceLevel() > 8)
                    logctx.CTXLOG("lock%s attribute reply %s", lockAll ? "All" : "", reply.str());
            }

            bool doControlQuery = true;
            if (strieq(name, "control:aclupdate"))
            {
                IPropertyTree *aclTree = msg->queryPropTree("ACL");
                if (aclTree)
                {
                    Owned<IPropertyTreeIterator> accesses = aclTree->getElements("Access");
                    ForEach(*accesses)
                    {
                        IPropertyTree &access = accesses->query();
                        try
                        {
                            addAccess(access.getPropBool("@allow", true), access.getPropBool("@allowBlind", true), access.queryProp("@ip"), access.queryProp("@mask"), access.queryProp("@query"), access.queryProp("@error"), access.getPropInt("@errorCode", -1));
                        }
                        catch (IException *E)
                        {
                            StringBuffer s, x;
                            E->errorMessage(s);
                            E->Release();
                            toXML(&access, x, 0, 0);
                            throw MakeStringException(ROXIE_ACL_ERROR, "Error in access statement %s: %s", x.str(), s.str());
                        }
                    }
                }
            }
            else if (strieq(name, "control:queryaclinfo"))
            {
                reply.append("<Endpoint ep='");
                ep.getUrlStr(reply);
                reply.append("'>\n");

                queryAccessInfo(reply);
                reply.append("</Endpoint>\n");
                doControlQuery = false;
            }

            if (doControlQuery)
            {
                roxieMsgCtx->ensureCascadeManager().doControlQuery(ep, msg, xml.str(), reply);
            }
        }
        if (reply.length())
            protocol->appendContent(MarkupFmt_XML, reply.str(), "Control");

    }

    virtual void onDebugMsg(IHpccProtocolMsgContext *msgctx, const char *uid, IPropertyTree *msg, IXmlWriter &out)
    {
        RoxieProtocolMsgContext *roxieMsgCtx = checkGetRoxieMsgContext(msgctx, msg);
        roxieMsgCtx->ensureDebugCommandHandler().doDebugCommand(msg, &roxieMsgCtx->ensureDebuggerContext(uid), out);
    }

    virtual void noteQuery(IHpccProtocolMsgContext *msgctx, const char *peer, bool failed, unsigned bytesOut, unsigned elapsed, unsigned memused, unsigned slavesReplyLen, bool continuationNeeded)
    {
        RoxieProtocolMsgContext *roxieMsgCtx = checkGetRoxieMsgContext(msgctx);
        roxieMsgCtx->noteQuery(peer, failed, elapsed, memused, slavesReplyLen, bytesOut, continuationNeeded);
    }

};

extern IHpccProtocolMsgSink *createRoxieProtocolMsgSink(const IpAddress &ip, unsigned short port, unsigned poolSize, bool suspended)
{
    SocketEndpoint ep(port, ip);
    return new RoxieProtocolMsgSink(ep, poolSize, suspended);
}

//=================================================================================

IArrayOf<IHpccProtocolListener> socketListeners;
MapStringToMyClass<SharedObject> protocolDlls;
MapStringToMyClass<IHpccProtocolPlugin> protocolPlugins;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    socketListeners.kill();
    protocolPlugins.kill();
    protocolDlls.kill();
}

IHpccProtocolPlugin *ensureProtocolPlugin(IHpccProtocolPluginContext &protocolCtx, const char *soname)
{
    IHpccProtocolPlugin *plugin = protocolPlugins.getValue(soname ? soname : "native");
    if (plugin)
        return plugin;
    if (!soname)
    {
        Owned<IHpccProtocolPlugin> protocolPlugin = loadHpccProtocolPlugin(&protocolCtx, ensureLimiterFactory());
        protocolPlugins.setValue("native", protocolPlugin);
        return protocolPlugin.getClear();
    }
    Owned<SharedObject> so = new SharedObject();
    if (!so->load(soname, true, true))
        throw MakeStringException(-1, "Failed to load protocol library %s", soname);

    protocolDlls.setValue(soname, so.getLink());

    HpccProtocolInstallFunction *protocolInstall = (HpccProtocolInstallFunction *) GetSharedProcedure(so->getInstanceHandle(), "loadHpccProtocolPlugin");
    if (!protocolInstall)
        throw MakeStringException(-1, "Failed to load protocol library %s loadHpccProtocolPlugin function", soname);
    Owned<IHpccProtocolPlugin> protocolPlugin = protocolInstall(&protocolCtx, ensureLimiterFactory());
    if (!protocolPlugin)
        throw MakeStringException(-1, "Protocol library %s loadHpccProtocolPlugin function failed", soname);
    protocolPlugins.setValue(soname, protocolPlugin.getLink());
    return protocolPlugin.getClear();
}

extern void disconnectRoxieQueues()
{
    ForEachItemIn(idx, socketListeners)
    {
        socketListeners.item(idx).disconnectQueue();
    }
}

IPooledThread *RoxieWorkUnitListener::createNew()
{
    return new RoxieWorkUnitWorker(this);
}

IHpccProtocolListener *createRoxieWorkUnitListener(unsigned poolSize, bool suspended)
{
    if (traceLevel)
        DBGLOG("Creating Roxie workunit listener, pool size %d%s", poolSize, suspended?" SUSPENDED":"");
    return new RoxieWorkUnitListener(poolSize, suspended);
}

//================================================================================================================================
