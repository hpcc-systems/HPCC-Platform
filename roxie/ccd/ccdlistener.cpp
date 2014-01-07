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
#include "jthread.hpp"
#include "jregexp.hpp"

#include "wujobq.hpp"

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

static void sendSoapException(SafeSocket &client, IException *E, const char *queryName)
{
    try
    {
        if (!queryName)
            queryName = "Unknown"; // Exceptions when parsing query XML can leave queryName unset/unknowable....

        StringBuffer response;
        response.append("<").append(queryName).append("Response");
        response.append(" xmlns=\"urn:hpccsystems:ecl:").appendLower(strlen(queryName), queryName).append("\">");
        response.appendf("<Results><Result><Exception><Source>Roxie</Source><Code>%d</Code>", E->errorCode());
        response.append("<Message>");
        StringBuffer s;
        E->errorMessage(s);
        encodeXML(s.str(), response);
        response.append("</Message></Exception></Result></Results>");
        response.append("</").append(queryName).append("Response>");
        client.write(response.str(), response.length());
    }
    catch(IException *EE)
    {
        StringBuffer error("While reporting exception: ");
        EE->errorMessage(error);
        DBGLOG("%s", error.str());
        EE->Release();
    }
#ifndef _DEBUG
    catch(...) {}
#endif
}

static void sendJsonException(SafeSocket &client, IException *E, const char *queryName)
{
    try
    {
        if (!queryName)
            queryName = "Unknown"; // Exceptions when parsing query XML can leave queryName unset/unknowable....

        StringBuffer response("{");
        appendfJSONName(response, "%sResponse", queryName).append(" {");
        appendJSONName(response, "Results").append(" {");
        appendJSONName(response, "Exception").append(" [{");
        appendJSONValue(response, "Source", "Roxie");
        appendJSONValue(response, "Code", E->errorCode());
        StringBuffer s;
        appendJSONValue(response, "Message", E->errorMessage(s).str());
        response.append("}]}}}");
        client.write(response.str(), response.length());
    }
    catch(IException *EE)
    {
        StringBuffer error("While reporting exception: ");
        DBGLOG("%s", EE->errorMessage(error).str());
        EE->Release();
    }
#ifndef _DEBUG
    catch(...) {}
#endif
}

static void sendHttpException(SafeSocket &client, TextMarkupFormat fmt, IException *E, const char *queryName)
{
    if (fmt==MarkupFmt_JSON)
        sendJsonException(client, E, queryName);
    else
        sendSoapException(client, E, queryName);
}

//================================================================================================================

class CHttpRequestAsyncFor : public CInterface, public CAsyncFor
{
private:
    const char *queryName, *queryText;
    const ContextLogger &logctx;
    IArrayOf<IPropertyTree> &requestArray;
    Linked<IQueryFactory> f;
    SafeSocket &client;
    HttpHelper &httpHelper;
    PTreeReaderOptions xmlReadFlags;
    unsigned &memused;
    unsigned &slaveReplyLen;
    CriticalSection crit;

public:
    CHttpRequestAsyncFor(const char *_queryName, IQueryFactory *_f, IArrayOf<IPropertyTree> &_requestArray, SafeSocket &_client, HttpHelper &_httpHelper, unsigned &_memused, unsigned &_slaveReplyLen, const char *_queryText, const ContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags) :
      f(_f), requestArray(_requestArray), client(_client), httpHelper(_httpHelper), memused(_memused), slaveReplyLen(_slaveReplyLen), logctx(_logctx), xmlReadFlags(_xmlReadFlags)
    {
        queryName = _queryName;
        queryText = _queryText;
    }

    IMPLEMENT_IINTERFACE;

    void onException(IException *E)
    {
        if (!logctx.isBlind())
            logctx.CTXLOG("FAILED: %s", queryText);
        StringBuffer error("EXCEPTION: ");
        E->errorMessage(error);
        DBGLOG("%s", error.str());
        sendHttpException(client, httpHelper.queryContentFormat(), E, queryName);
        E->Release();
    }

    void Do(unsigned idx)
    {
        try
        {
            IPropertyTree &request = requestArray.item(idx);
            Owned<IRoxieServerContext> ctx = f->createContext(&request, client, httpHelper.queryContentFormat(), false, false, httpHelper, true, logctx, xmlReadFlags);
            ctx->process();
            ctx->flush(idx);
            CriticalBlock b(crit);
            memused += ctx->getMemoryUsage();
            slaveReplyLen += ctx->getSlavesReplyLen();
        }
        catch (WorkflowException * E)
        {
            onException(E);
        }
        catch (IException * E)
        {
            onException(E);
        }
        catch (...)
        {
            onException(MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception"));
        }
    }
};

//================================================================================================================

class CascadeManager : public CInterface
{
    static Semaphore globalLock;
    StringBuffer errors;

    IArrayOf<ISocket> activeChildren;
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
            atomic_inc(&globalSignals);
        }
    }

    void connectChild(unsigned idx)
    {
        if (idx < getNumNodes())
        {
            SocketEndpoint ep(roxiePort, getNodeAddress(idx));
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
        Owned<IPropertyTree> lockResult = createPTreeFromXMLString(lockReply.str(), ipt_caseInsensitive);
        int lockCount = lockResult->getPropInt("Lock", 0);
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
        loop
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
        atomic_inc(&globalLocks);
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
            atomic_inc(&globalSignals);
            if (traceLevel > 5)
                DBGLOG("globalLock released");
            throw;
        }
    }


public:
    IMPLEMENT_IINTERFACE;
    CascadeManager(const IRoxieContextLogger &_logctx) : logctx(_logctx)
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

    void doLockChild(const char *queryText, StringBuffer &reply)
    {
        if (traceLevel > 5)
            DBGLOG("doLockChild: %s", queryText);
        isMaster = false;
        Owned<IPropertyTree> xml = createPTreeFromXMLString(queryText);
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
        loop
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
        reply.append("<NumServers>").append(getNumNodes()).append("</NumServers>");
        if (lockAll)
            return locksGot == getNumNodes();
        else
            return locksGot > getNumNodes()/2;
    }

    void doControlQuery(SocketEndpoint &ep, const char *queryText, StringBuffer &reply)
    {
        if (logctx.queryTraceLevel() > 5)
            logctx.CTXLOG("doControlQuery (%d): %.80s", isMaster, queryText);
        // By this point we should have cascade-connected thanks to a prior <control:lock>
        // So do the query ourselves and in all child threads;
        Owned<IPropertyTree> mergedStats;
        if (strstr(queryText, "querystats"))
            mergedStats.setown(createPTree("Endpoint"));

        class casyncfor: public CAsyncFor
        {
            const char *queryText;
            CascadeManager *parent;
            IPropertyTree *mergedStats;
            StringBuffer &reply;
            CriticalSection crit;
            SocketEndpoint &ep;
            unsigned numChildren;
            const IRoxieContextLogger &logctx;

        public:
            casyncfor(const char *_queryText, CascadeManager *_parent, IPropertyTree *_mergedStats,
                      StringBuffer &_reply, SocketEndpoint &_ep, unsigned _numChildren, const IRoxieContextLogger &_logctx)
                : queryText(_queryText), parent(_parent), mergedStats(_mergedStats), reply(_reply), ep(_ep), numChildren(_numChildren), logctx(_logctx)
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
                    Owned<IPropertyTree> xml = createPTreeFromXMLString(childReply);
                    if (!xml)
                    {
                        StringBuffer err;
                        err.appendf("doControlQuery::do (%d of %d): %.80s received invalid response %s", i, numChildren, queryText, childReply.str());
                        logctx.CTXLOG("%s", err.str());
                        throw MakeStringException(ROXIE_INTERNAL_ERROR, "%s", err.str());
                    }
                    Owned<IPropertyTreeIterator> meat = xml->getElements("Endpoint");
                    ForEach(*meat)
                    {
                        CriticalBlock cb(crit);
                        if (mergedStats)
                        {
                            mergeStats(mergedStats, &meat->query());
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
                    Owned<IPropertyTree> xml = createPTreeFromXMLString(queryText); // control queries are case sensitive
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
                if (mergedStats)
                {
                    Owned<IPropertyTree> xml = createPTreeFromXMLString(myReply);
                    mergeStats(mergedStats, xml);
                }
                else
                    reply.append(myReply);
            }
        } afor(queryText, this, mergedStats, reply, ep, activeChildren.ordinality(), logctx);
        afor.For(activeChildren.ordinality()+(isMaster ? 0 : 1), 10);
        activeChildren.kill();
        if (mergedStats)
            toXML(mergedStats, reply);
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

class RoxieListener : public Thread, implements IRoxieListener, implements IThreadFactory
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

    virtual bool suspend(bool suspendIt)
    {
        CriticalBlock b(activeCrit);
        bool ret = suspended;
        suspended = suspendIt;
        return ret;
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

protected:
    unsigned poolSize;
    bool running;
    bool suspended;
    Semaphore started;
    Owned<IThreadPool> pool;

    unsigned threadsActive;
    unsigned maxThreadsActive;
    CriticalSection activeCrit;
    friend class ActiveQueryLimiter;

private:
    CIArrayOf<AccessTableEntry> accessTable;
};

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

    virtual void runOnce(const char*)
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
                SCMStringBuffer queueNames;
                getRoxieQueueNames(queueNames, topology->queryProp("@name"));
                if (queueNames.length())
                {
                    if (traceLevel)
                        DBGLOG("roxie: Waiting on queue(s) '%s'", queueNames.str());
                    try
                    {
                        queue.setown(createJobQueue(queueNames.str()));
                        queue->connect();
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

class RoxieSocketListener : public RoxieListener
{
    unsigned port;
    unsigned listenQueue;
    Owned<ISocket> socket;
    SocketEndpoint ep;

public:
    RoxieSocketListener(unsigned _port, unsigned _poolSize, unsigned _listenQueue, bool _suspended)
      : RoxieListener(_poolSize, _suspended)
    {
        port = _port;
        listenQueue = _listenQueue;
        ep.set(port, queryHostIP());
    }

    virtual bool stop(unsigned timeout)
    {
        if (socket)
            socket->cancel_accept();
        return RoxieListener::stop(timeout);
    }

    virtual void disconnectQueue()
    {
        // This is for dali queues only
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

    virtual void runOnce(const char *query);

    virtual int run()
    {
        DBGLOG("RoxieSocketListener (%d threads) listening to socket on port %d", poolSize, port);
        socket.setown(ISocket::create(port, listenQueue));
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
        DBGLOG("RoxieSocketListener closed query socket");
        return 0;
    }

    virtual IPooledThread *createNew();

    virtual const SocketEndpoint &queryEndpoint() const
    {
        return ep;
    }

    virtual unsigned queryPort() const
    {
        return port;
    }
};

class ActiveQueryLimiter
{
    RoxieListener *parent;
public:
    bool accepted;
    ActiveQueryLimiter(RoxieListener *_parent) : parent(_parent)
    {
        CriticalBlock b(parent->activeCrit);
        if (parent->suspended)
        {
            accepted = false;
            if (traceLevel > 1)
                DBGLOG("Rejecting query since Roxie server pool %d is suspended ", parent->queryPort());
        }
        else
        {
            accepted = (parent->threadsActive < parent->poolSize);
            if (accepted && parent->threadsActive > parent->maxThreadsActive)
            {
                parent->maxThreadsActive = parent->threadsActive;
                if (traceLevel > 1)
                    DBGLOG("Maximum queries active %d of %d for pool %d", parent->threadsActive, parent->poolSize, parent->queryPort());
            }
            if (!accepted && traceLevel > 5)
                DBGLOG("Too many active queries (%d >= %d)", parent->threadsActive, parent->poolSize);
        }
        parent->threadsActive++;
    }

    ~ActiveQueryLimiter()
    {
        CriticalBlock b(parent->activeCrit);
        parent->threadsActive--;
    }
};

class RoxieQueryWorker : public CInterface, implements IPooledThread
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
    virtual void init(void *)
    {
        qstart = msTick();
        time(&startTime);
    }

    virtual bool canReuse()
    {
        return true;
    }

    virtual bool stop()
    {
        ERRLOG("RoxieQueryWorker stopped with queries active");
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

    virtual void init(void *_r)
    {
        wuid.set((const char *) _r);
        RoxieQueryWorker::init(_r);
    }

    virtual void main()
    {
        Owned <IRoxieDaliHelper> daliHelper = connectToDali();
        Owned<IConstWorkUnit> wu = daliHelper->attachWorkunit(wuid.get(), NULL);
        daliHelper->noteWorkunitRunning(wuid.get(), true);
        if (!wu)
            throw MakeStringException(ROXIE_DALI_ERROR, "Failed to open workunit %s", wuid.get());
        SCMStringBuffer target;
        wu->getClusterName(target);
        Owned<StringContextLogger> logctx = new StringContextLogger(wuid.get());
        Owned<IQueryFactory> queryFactory;
        try
        {
            queryFactory.setown(createServerQueryFactoryFromWu(wu));
        }
        catch (IException *E)
        {
            reportException(wu, E, *logctx);
            throw E;
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
        daliHelper->noteWorkunitRunning(wuid.get(), false);
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
            atomic_inc(&queryCount);

            bool isBlind = wu->getDebugValueBool("blindLogging", false);
            if (pool)
            {
                pool->checkWuAccess(isBlind);
                ActiveQueryLimiter l(pool);
                if (!l.accepted)
                {
                    IException *e = MakeStringException(ROXIE_TOO_MANY_QUERIES, "Too many active queries");
                    if (trapTooManyActiveQueries)
                        logctx.logOperatorException(e, __FILE__, __LINE__, NULL);
                    throw e;
                }
            }
            isBlind = isBlind || blindLogging;
            logctx.setBlind(isBlind);
            priority = queryFactory->getPriority();
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
                {
                    MTIME_SECTION(logctx.queryTimer(), "Process");
                    ctx->process();
                }
                memused = ctx->getMemoryUsage();
                slavesReplyLen = ctx->getSlavesReplyLen();
                ctx->done(false);
                failed = false;
            }
            catch(...)
            {
                memused = ctx->getMemoryUsage();
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
        if (logctx.queryTraceLevel() > 2)
            logctx.dumpStats();
        if (logctx.queryTraceLevel() && (logctx.queryTraceLevel() > 2 || logFullQueries || logctx.intercept))
            logctx.CTXLOG("COMPLETE: %s complete in %d msecs memory %d Mb priority %d slavesreply %d", wuid.get(), elapsed, memused, priority, slavesReplyLen);
        logctx.flush(true, false);
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

class RoxieSocketWorker : public RoxieQueryWorker
{
    Owned<SafeSocket> client;
    Owned<CDebugCommandHandler> debugCmdHandler;
    SocketEndpoint ep;

public:
    RoxieSocketWorker(RoxieListener *_pool, SocketEndpoint &_ep)
        : RoxieQueryWorker(_pool), ep(_ep)
    {
    }

    //  interface IPooledThread
    virtual void init(void *_r)
    {
        client.setown(new CSafeSocket((ISocket *) _r));
        RoxieQueryWorker::init(_r);
    }

    virtual void main()
    {
        doMain("");
    }

    virtual void runOnce(const char *query)
    {
        doMain(query);
    }

private:
    static void sendHttpServerTooBusy(SafeSocket &client, const IRoxieContextLogger &logctx)
    {
        StringBuffer message;

        message.append("HTTP/1.0 503 Server Too Busy\r\n\r\n");
        message.append("Server too busy, please try again later");

        StringBuffer err("Too many active queries");  // write out Too many active queries - make searching for this error consistent
        if (!trapTooManyActiveQueries)
        {
            err.appendf("  %s", message.str());
            logctx.CTXLOG("%s", err.str());
        }
        else
        {
            IException *E = MakeStringException(ROXIE_TOO_MANY_QUERIES, "%s", err.str());
            logctx.logOperatorException(E, __FILE__, __LINE__, "%s", message.str());
            E->Release();
        }

        try
        {
            client.write(message.str(), message.length());
        }
        catch (IException *E)
        {
            logctx.logOperatorException(E, __FILE__, __LINE__, "Exception caught in sendHttpServerTooBusy");
            E->Release();
        }
        catch (...)
        {
            logctx.logOperatorException(NULL, __FILE__, __LINE__, "sendHttpServerTooBusy write failed (Unknown exception)");
        }
    }

    void sanitizeQuery(Owned<IPropertyTree> &queryXML, StringAttr &queryName, StringBuffer &saniText, HttpHelper &httpHelper, const char *&uid, bool &isRequest, bool &isRequestArray, bool &isBlind, bool &isDebug)
    {
        if (queryXML)
        {
            queryName.set(queryXML->queryName());
            isRequest = false;
            isRequestArray = false;
            if (httpHelper.isHttp())
            {
                if (httpHelper.queryContentFormat()==MarkupFmt_JSON)
                {
                    if (strieq(queryName, "__object__"))
                    {
                        queryXML.setown(queryXML->getPropTree("*[1]"));
                        queryName.set(queryXML->queryName());
                        isRequest = true;
                        if (!queryXML)
                            throw MakeStringException(ROXIE_DATA_ERROR, "Malformed JSON request (missing Body)");
                    }
                    else if (strieq(queryName, "__array__"))
                        throw MakeStringException(ROXIE_DATA_ERROR, "JSON request array not implemented");
                    else
                        throw MakeStringException(ROXIE_DATA_ERROR, "Malformed JSON request");
                }
                else if (strieq(queryName, "envelope"))
                {
                    queryXML.setown(queryXML->getPropTree("Body/*"));
                    if (!queryXML)
                        throw MakeStringException(ROXIE_DATA_ERROR, "Malformed SOAP request (missing Body)");
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
                    throw MakeStringException(ROXIE_DATA_ERROR, "Malformed SOAP request");
            }

            // convert to XML with attribute values in single quotes - makes replaying queries easier
            uid = queryXML->queryProp("@uid");
            if (!uid)
                uid = "-";
            isBlind = queryXML->getPropBool("@blind", false) || queryXML->getPropBool("_blind", false);
            isDebug = queryXML->getPropBool("@debug") || queryXML->getPropBool("_Probe", false);
            toXML(queryXML, saniText, 0, isBlind ? (XML_SingleQuoteAttributeValues | XML_Sanitize) : XML_SingleQuoteAttributeValues);
        }
        else
            throw MakeStringException(ROXIE_DATA_ERROR, "Malformed request");
    }
    void parseQueryPTFromString(Owned<IPropertyTree> &queryPT, HttpHelper &httpHelper, const char *text, PTreeReaderOptions options)
    {
        if (strieq(httpHelper.queryContentType(), "application/json"))
            queryPT.setown(createPTreeFromJSONString(text, ipt_caseInsensitive, options));
        else
            queryPT.setown(createPTreeFromXMLString(text, ipt_caseInsensitive, options));
    }
    void doMain(const char *runQuery)
    {
        StringBuffer rawText(runQuery);
        unsigned priority = (unsigned) -2;
        unsigned memused = 0;
        Owned<CascadeManager> cascade;
        IpAddress peer;
        bool continuationNeeded = false;
        bool isStatus = false;
readAnother:
        Owned<IDebuggerContext> debuggerContext;
        unsigned slavesReplyLen = 0;
        HttpHelper httpHelper;
        try
        {
            if (client)
            {
                client->querySocket()->getPeerAddress(peer);
                if (!client->readBlock(rawText, WAIT_FOREVER, &httpHelper, continuationNeeded, isStatus, maxBlockSize))
                {
                    if (traceLevel > 8)
                    {
                        StringBuffer b;
                        DBGLOG("No data reading query from socket");
                    }
                    client.clear();
                    return;
                }
            }
            if (continuationNeeded)
            {
                qstart = msTick();
                time(&startTime);
            }
            unknownQueryStats.noteActive();
            atomic_inc(&queryCount);
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

        TextMarkupFormat mlFmt = MarkupFmt_XML;
        bool isRaw = false;
        bool isHTTP = httpHelper.isHttp();
        bool isBlocked = false;
        bool trim = false;
        bool failed = false;
        Owned<IPropertyTree> queryXml;
        Owned<IQueryFactory> queryFactory;
        StringBuffer sanitizedText;
        StringAttr queryName;
        StringBuffer peerStr;
        peer.getIpText(peerStr);
        const char *uid = "-";
        unsigned instanceId = getNextInstanceId();
        StringBuffer ctxstr;
        Owned<StringContextLogger> _logctx = new StringContextLogger(ep.getIpText(ctxstr).appendf(":%u{%u}", ep.port, instanceId).str());
        StringContextLogger &logctx = *_logctx.get();
        try
        {
            // Note - control queries have to be formatted without spaces..
            if (strnicmp(rawText.str(), "<control:lock", 13)==0 && !isalpha(rawText.charAt(13)))
            {
                if (logctx.queryTraceLevel() > 8)
                    logctx.CTXLOG("Got lock request %s", rawText.str());
                FlushingStringBuffer response(client, false, MarkupFmt_XML, false, false, logctx);
                response.startDataset("Control", NULL, (unsigned) -1);
                if (!cascade)
                    cascade.setown(new CascadeManager(logctx));
                StringBuffer s;
                cascade->doLockGlobal(s, false);
                response.append(s);
                if (logctx.queryTraceLevel() > 8)
                    logctx.CTXLOG("lock reply %s", s.str());
                response.flush(true);
                unsigned replyLen = 0;
                client->write(&replyLen, sizeof(replyLen));
                rawText.clear();
                unknownQueryStats.noteComplete();
                goto readAnother;
            }
            else if (strnicmp(rawText.str(), "<control:childlock", 18)==0 && !isalpha(rawText.charAt(18)))
            {
                if (logctx.queryTraceLevel() > 8)
                    logctx.CTXLOG("Got childlock request %s", rawText.str());
                FlushingStringBuffer response(client, false, MarkupFmt_XML, false, false, logctx);
                response.startDataset("Control", NULL, (unsigned) -1);
                if (!cascade)
                    cascade.setown(new CascadeManager(logctx));
                StringBuffer s;
                cascade->doLockChild(rawText.str(), s);
                response.append(s);
                if (logctx.queryTraceLevel() > 8)
                    logctx.CTXLOG("childlock reply %s", s.str());
                response.flush(true);
                unsigned replyLen = 0;
                client->write(&replyLen, sizeof(replyLen));
                rawText.clear();
                unknownQueryStats.noteComplete();
                goto readAnother;
            }
            else if (strnicmp(rawText.str(), "<control:", 9)==0)
            {
                Owned<IPropertyTree> queryXML = createPTreeFromXMLString(rawText.str()); // This is just done to check it is valid XML and make error reporting better...
                queryXML.clear();
                bool doControlQuery = true;

                FlushingStringBuffer response(client, false, MarkupFmt_XML, false, isHTTP, logctx);
                response.startDataset("Control", NULL, (unsigned) -1);

                if (strnicmp(rawText.str(), "<control:aclupdate", 18)==0 && !isalpha(rawText.charAt(18)))
                {
                    queryXml.setown(createPTreeFromXMLString(rawText.str(), ipt_caseInsensitive, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_ignoreNameSpaces)));
                    IPropertyTree *aclTree = queryXml->queryPropTree("ACL");
                    if (aclTree)
                    {
                        Owned<IPropertyTreeIterator> accesses = aclTree->getElements("Access");
                        ForEach(*accesses)
                        {
                            IPropertyTree &access = accesses->query();
                            try
                            {
                                pool->addAccess(access.getPropBool("@allow", true), access.getPropBool("@allowBlind", true), access.queryProp("@ip"), access.queryProp("@mask"), access.queryProp("@query"), access.queryProp("@error"), access.getPropInt("@errorCode", -1));
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
                else if (strnicmp(rawText.str(), "<control:queryaclinfo", 21)==0 && !isalpha(rawText.charAt(21)))
                {
                    StringBuffer info;
                    info.append("<Endpoint ep='");
                    ep.getUrlStr(info);
                    info.append("'>\n");

                    pool->queryAccessInfo(info);
                    info.append("</Endpoint>\n");
                    response.append(info.str());
                    doControlQuery = false;
                }

                if (doControlQuery)
                {
                    if(!cascade)
                        cascade.setown(new CascadeManager(logctx));
                    StringBuffer s;
                    cascade->doControlQuery(ep, rawText, s);
                    response.append(s);
                }
            }
            else if (isStatus)
            {
                client->write("OK", 2);
            }
            else
            {
                try
                {
                    parseQueryPTFromString(queryXml, httpHelper, rawText.str(), (PTreeReaderOptions)(defaultXmlReadFlags | ptr_ignoreNameSpaces));
                }
                catch (IException *E)
                {
                    logctx.logOperatorException(E, __FILE__, __LINE__, "Invalid XML received from %s:%d - %s", peerStr.str(), pool->queryPort(), rawText.str());
                    logctx.CTXLOG("ERROR: Invalid XML received from %s:%d - %s", peerStr.str(), pool->queryPort(), rawText.str());
                    throw;
                }
                bool isRequest = false;
                bool isRequestArray = false;
                bool isBlind = false;
                bool isDebug = false;

                sanitizeQuery(queryXml, queryName, sanitizedText, httpHelper, uid, isRequest, isRequestArray, isBlind, isDebug);
                pool->checkAccess(peer, queryName, sanitizedText, isBlind);
                if (isDebug)
                {
                    if (!debugPermitted || !ownEP.port)
                        throw MakeStringException(ROXIE_ACCESS_ERROR, "Debug queries are not permitted on this system");
                }
                isBlind = isBlind || blindLogging;
                logctx.setBlind(isBlind);
                if (logFullQueries)
                {
                    StringBuffer soapStr;
                    (isRequest) ? soapStr.append("SoapRequest") : (isRequestArray) ? soapStr.append("SoapRequest") : soapStr.clear();
                    logctx.CTXLOG("%s %s:%d %s %s %s", isBlind ? "BLIND:" : "QUERY:", peerStr.str(), pool->queryPort(), uid, soapStr.str(), sanitizedText.str());
                }
                if (strnicmp(rawText.str(), "<debug:", 7)==0)
                {
                    if (!debugPermitted || !ownEP.port)
                        throw MakeStringException(ROXIE_ACCESS_ERROR, "Debug queries are not permitted on this system");
                    if (!debuggerContext)
                    {
                        if (!uid)
#ifdef _DEBUG
                            uid="*";
#else
                            throw MakeStringException(ROXIE_DEBUG_ERROR, "Debug id not specified");
#endif
                        debuggerContext.setown(queryRoxieDebugSessionManager().lookupDebuggerContext(uid));
                        if (!debuggerContext)
                            throw MakeStringException(ROXIE_DEBUG_ERROR, "No active query matching context %s found", uid);
                        if (!debugCmdHandler.get())
                            debugCmdHandler.setown(new CDebugCommandHandler);
                    }
                    FlushingStringBuffer response(client, false, MarkupFmt_XML, false, isHTTP, logctx);
                    response.startDataset("Debug", NULL, (unsigned) -1);
                    debugCmdHandler->doDebugCommand(queryXml, debuggerContext, response);
                }
                else
                {
                    ActiveQueryLimiter l(pool);
                    if (!l.accepted)
                    {
                        if (isHTTP)
                        {
                            sendHttpServerTooBusy(*client, logctx);
                            logctx.CTXLOG("FAILED: %s", sanitizedText.str());
                            logctx.CTXLOG("EXCEPTION: Too many active queries");
                        }
                        else
                        {
                            IException *e = MakeStringException(ROXIE_TOO_MANY_QUERIES, "Too many active queries");
                            if (trapTooManyActiveQueries)
                                logctx.logOperatorException(e, __FILE__, __LINE__, NULL);
                            throw e;
                        }
                    }
                    else
                    {
                        queryFactory.setown(globalPackageSetManager->getQuery(queryName, logctx));
                        if (isHTTP)
                            client->setHttpMode(queryName, isRequestArray, httpHelper.queryContentFormat());
                        if (queryFactory)
                        {
                            bool stripWhitespace = queryFactory->getDebugValueBool("stripWhitespaceFromStoredDataset", 0 != (ptr_ignoreWhiteSpace & defaultXmlReadFlags));
                            stripWhitespace = queryXml->getPropBool("_stripWhitespaceFromStoredDataset", stripWhitespace);
                            PTreeReaderOptions xmlReadFlags = (PTreeReaderOptions)((defaultXmlReadFlags & ~ptr_ignoreWhiteSpace) |
                                                                               (stripWhitespace ? ptr_ignoreWhiteSpace : ptr_none));
                            if (xmlReadFlags != defaultXmlReadFlags)
                            {
                                // we need to reparse input xml, as global whitespace setting has been overridden
                                parseQueryPTFromString(queryXml, httpHelper, rawText.str(), (PTreeReaderOptions)(xmlReadFlags | ptr_ignoreNameSpaces));
                                sanitizeQuery(queryXml, queryName, sanitizedText, httpHelper, uid, isRequest, isRequestArray, isBlind, isDebug);
                            }
                            IArrayOf<IPropertyTree> requestArray;
                            if (isHTTP)
                            {
                                if (isRequestArray)
                                {
                                    StringBuffer reqIterString;
                                    reqIterString.append(queryName).append("Request");

                                    Owned<IPropertyTreeIterator> reqIter = queryXml->getElements(reqIterString.str());
                                    ForEach(*reqIter)
                                    {
                                        IPropertyTree *fixedreq = createPTree(queryName, ipt_caseInsensitive);
                                        Owned<IPropertyTreeIterator> iter = reqIter->query().getElements("*");
                                        ForEach(*iter)
                                        {
                                            fixedreq->addPropTree(iter->query().queryName(), LINK(&iter->query()));
                                        }
                                        requestArray.append(*fixedreq);
                                    }
                                }
                                else
                                {
                                    IPropertyTree *fixedreq = createPTree(queryName, ipt_caseInsensitive);
                                    Owned<IPropertyTreeIterator> iter = queryXml->getElements("*");
                                    ForEach(*iter)
                                    {
                                        fixedreq->addPropTree(iter->query().queryName(), LINK(&iter->query()));
                                    }
                                    requestArray.append(*fixedreq);
                                }
                                trim = true;
                            }
                            else
                            {
                                const char *format = queryXml->queryProp("@format");
                                if (format)
                                {
                                    if (stricmp(format, "raw") == 0)
                                    {
                                        isRaw = true;
                                        mlFmt = MarkupFmt_Unknown;
                                        isBlocked = (client != NULL);
                                    }
                                    else if (stricmp(format, "bxml") == 0)
                                    {
                                        isBlocked = true;
                                    }
                                    else if (stricmp(format, "ascii") == 0)
                                    {
                                        isRaw = false;
                                        mlFmt = MarkupFmt_Unknown;
                                    }
                                    else if (stricmp(format, "xml") != 0) // xml is the default
                                        throw MakeStringException(ROXIE_INVALID_INPUT, "Unsupported format specified: %s", format);
                                }
                                trim = queryXml->getPropBool("@trim", false);;
                                logctx.setIntercept(queryXml->getPropBool("@log", false));
                                logctx.setTraceLevel(queryXml->getPropInt("@traceLevel", traceLevel));
                            }

                            priority = queryFactory->getPriority();
                            switch (priority)
                            {
                            case 0: loQueryStats.noteActive(); break;
                            case 1: hiQueryStats.noteActive(); break;
                            case 2: slaQueryStats.noteActive(); break;
                            }
                            unknownQueryStats.noteComplete();
                            combinedQueryStats.noteActive();
                            if (isHTTP)
                            {
                                CHttpRequestAsyncFor af(queryName, queryFactory, requestArray, *client, httpHelper, memused, slavesReplyLen, sanitizedText, logctx, xmlReadFlags);
                                af.For(requestArray.length(), numRequestArrayThreads);
                            }
                            else
                            {
                                Owned<IRoxieServerContext> ctx = queryFactory->createContext(queryXml, *client, mlFmt, isRaw, isBlocked, httpHelper, trim, logctx, xmlReadFlags);
                                if (client && !ctx->outputResultsToSocket())
                                {
                                    unsigned replyLen = 0;
                                    client->write(&replyLen, sizeof(replyLen));
                                    client.clear();
                                }
                                try
                                {
                                    ctx->process();
                                    memused = ctx->getMemoryUsage();
                                    slavesReplyLen = ctx->getSlavesReplyLen();
                                    ctx->done(false);
                                }
                                catch(...)
                                {
                                    memused = ctx->getMemoryUsage();
                                    slavesReplyLen = ctx->getSlavesReplyLen();
                                    ctx->done(true);
                                    throw;
                                }
                            }
                        }
                        else
                        {
                            pool->reportBadQuery(queryName.get(), logctx);
                            if (globalPackageSetManager->getActivePackageCount())
                                throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown query %s", queryName.get());
                            else
                                throw MakeStringException(ROXIE_NO_PACKAGES_ACTIVE, "Unknown query %s (no packages active)", queryName.get());
                        }
                    }
                }
            }
        }
        catch (WorkflowException * E)
        {
            failed = true;
            logctx.CTXLOG("FAILED: %s", sanitizedText.str());
            StringBuffer error;
            E->errorMessage(error);
            logctx.CTXLOG("EXCEPTION: %s", error.str());
            unsigned code = E->errorCode();
            if (QUERYINTERFACE(E, ISEH_Exception))
                code = ROXIE_INTERNAL_ERROR;
            else if (QUERYINTERFACE(E, IOutOfMemException))
                code = ROXIE_MEMORY_ERROR;
            if (client)
            {
                if (isHTTP)
                    sendSoapException(*client, E, queryName);
                else
                    client->sendException("Roxie", code, error.str(), isBlocked, logctx);
            }
            else
            {
                fprintf(stderr, "EXCEPTION: %s", error.str());
            }
            E->Release();
        }
        catch (IException * E)
        {
            failed = true;
            logctx.CTXLOG("FAILED: %s", sanitizedText.str());
            StringBuffer error;
            E->errorMessage(error);
            logctx.CTXLOG("EXCEPTION: %s", error.str());
            unsigned code = E->errorCode();
            if (QUERYINTERFACE(E, ISEH_Exception))
                code = ROXIE_INTERNAL_ERROR;
            else if (QUERYINTERFACE(E, IOutOfMemException))
                code = ROXIE_MEMORY_ERROR;
            if (client)
            {
                if (isHTTP)
                    sendHttpException(*client, mlFmt, E, queryName);
                else
                    client->sendException("Roxie", code, error.str(), isBlocked, logctx);
            }
            else
            {
                fprintf(stderr, "EXCEPTION: %s", error.str());
            }
            E->Release();
        }
#ifndef _DEBUG
        catch(...)
        {
            failed = true;
            logctx.CTXLOG("FAILED: %s", sanitizedText.str());
            logctx.CTXLOG("EXCEPTION: Unknown exception");
            {
                if (isHTTP)
                {
                    Owned<IException> E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception");
                    sendSoapException(*client, E, queryName);
                }
                else
                    client->sendException("Roxie", ROXIE_INTERNAL_ERROR, "Unknown exception", isBlocked, logctx);
            }
        }
#endif
        if (isHTTP)
        {
            try
            {
                client->flush();
            }
            catch (IException * E)
            {
                StringBuffer error("RoxieSocketWorker failed to write to socket ");
                E->errorMessage(error);
                logctx.CTXLOG("%s", error.str());
                E->Release();

            }
//#ifndef _DEBUG
            catch(...)
            {
                logctx.CTXLOG("RoxieSocketWorker failed to write to socket (Unknown exception)");
            }
//#endif
        }
        unsigned bytesOut = client? client->bytesOut() : 0;
        unsigned elapsed = msTick() - qstart;
        noteQuery(failed, elapsed, priority);
        if (queryFactory)
        {
            queryFactory->noteQuery(startTime, failed, elapsed, memused, slavesReplyLen, bytesOut);
            queryFactory.clear();
        }
        if (logctx.queryTraceLevel() > 2)
            logctx.dumpStats();
        if (logctx.queryTraceLevel() && (logctx.queryTraceLevel() > 2 || logFullQueries || logctx.intercept))
            if (queryName.get())
                logctx.CTXLOG("COMPLETE: %s %s from %s complete in %d msecs memory %d Mb priority %d slavesreply %d resultsize %d continue %d", queryName.get(), uid, peerStr.str(), elapsed, memused, priority, slavesReplyLen, bytesOut, continuationNeeded);

        if (continuationNeeded)
        {
            rawText.clear();
            goto readAnother;
        }
        else
        {
            logctx.flush(true, false);
            try
            {
                if (client && !isHTTP && !isStatus)
                {
                    if (logctx.intercept)
                    {
                        FlushingStringBuffer response(client, isBlocked, mlFmt, isRaw, false, logctx);
                        response.startDataset("Tracing", NULL, (unsigned) -1);
                        logctx.outputXML(response);
                    }
                    unsigned replyLen = 0;
                    client->write(&replyLen, sizeof(replyLen));
                }
                client.clear();
            }
            catch (IException * E)
            {
                StringBuffer error("RoxieSocketWorker failed to close socket ");
                E->errorMessage(error);
                logctx.CTXLOG("%s", error.str()); // MORE - audience?
                E->Release();

            }
//#ifndef _DEBUG
            catch(...)
            {
                logctx.CTXLOG("RoxieSocketWorker failed to close socket (Unknown exception)"); // MORE - audience?
            }
//#endif
        }
    }
};

//=================================================================================

IArrayOf<IRoxieListener> socketListeners;

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

IPooledThread *RoxieSocketListener::createNew()
{
    return new RoxieSocketWorker(this, ep);
}

void RoxieSocketListener::runOnce(const char *query)
{
    Owned<RoxieSocketWorker> p = new RoxieSocketWorker(this, ep);
    p->runOnce(query);
}

IRoxieListener *createRoxieSocketListener(unsigned port, unsigned poolSize, unsigned listenQueue, bool suspended)
{
    if (traceLevel)
        DBGLOG("Creating Roxie socket listener, pool size %d, listen queue %d%s", poolSize, listenQueue, suspended?" SUSPENDED":"");
    return new RoxieSocketListener(port, poolSize, listenQueue, suspended);
}

IRoxieListener *createRoxieWorkUnitListener(unsigned poolSize, bool suspended)
{
    if (traceLevel)
        DBGLOG("Creating Roxie workunit listener, pool size %d%s", poolSize, suspended?" SUSPENDED":"");
    return new RoxieWorkUnitListener(poolSize, suspended);
}

bool suspendRoxieListener(unsigned port, bool suspended)
{
    ForEachItemIn(idx, socketListeners)
    {
        IRoxieListener &listener = socketListeners.item(idx);
        if (listener.queryPort()==port)
            return listener.suspend(suspended);
    }
    throw MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown port %u specified in suspendRoxieListener", port);
}

//================================================================================================================================
