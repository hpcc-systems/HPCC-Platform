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

#include "jlib.hpp"
#include "limits.h"

#include "jbuff.hpp"
#include "jdebug.hpp"
#include "jio.hpp"
#include "jqueue.tpp"
#include "jset.hpp"
#include "jsort.hpp"

#include "dadfs.hpp"

#include "jhtree.hpp"

#include "thorxmlwrite.hpp"

#include "thorport.hpp"
#include "thsortu.hpp"
#include "thactivityutil.ipp"
#include "thormisc.hpp"
#include "thbufdef.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"

#include "slave.ipp"
#include "../fetch/thfetchcommon.hpp"
#include "thkeyedjoinslave.ipp"

//#define TRACE_USAGE
//#define TRACE_JOINGROUPS
#define NUMSLAVEPORTS       2

#define NEWFETCHSENDHEADERSZ (sizeof(CJoinGroup *)+sizeof(offset_t)+sizeof(size32_t))

//#define NEWFETCHSTRESS
#ifndef NEWFETCHSTRESS

#define NEWFETCHSENDMAX (0x100000*5)
#define NEWFETCHREPLYMAX (0x100000*5)
#define NEWFETCHPRMEMLIMIT (0x100000*5)
#define NEWFETCHPRBLOCKMEMLIMIT (0x100000*4)

#else

#define NEWFETCHSENDMAX 100
#define NEWFETCHREPLYMAX 50 // want to send back requests of N in chunks of <N
#define NEWFETCHPRMEMLIMIT 1 // low enough to cause 1-by-1

#endif // NEWFETCHSTRESS
#define KJ_BUFFER_SIZE (0x100000*8)

#define FETCHKEY_HEADER_SIZE (sizeof(offset_t)+sizeof(void *))
#define DEFAULTMAXRESULTPULLPOOL 1
#define DEFAULTFREEQSIZE 10
#define LOWTHROTTLE_GRANULARITY 10

class CJoinGroup;

#pragma pack(push,1)
struct LookupRowResult
{
    offset_t fpos;
    CJoinGroup *jg;
    bool eog;
};
#pragma pack(pop)
#define KEYLOOKUP_HEADER_SIZE (sizeof(LookupRowResult))

interface IJoinProcessor
{
    virtual CJoinGroup *createJoinGroup(const void *row) = 0;
    virtual void createSegmentMonitors(IIndexReadContext *ctx, const void *row) = 0;
    virtual bool addMatch(CJoinGroup &match, const void *rhs, size32_t rhsSize, offset_t recptr) = 0;
    virtual void onComplete(CJoinGroup * jg) = 0;
    virtual bool leftCanMatch(const void *_left) = 0;
#ifdef TRACE_USAGE
     virtual atomic_t &getdebug(unsigned w) = 0;
#endif
     virtual CActivityBase *queryOwner() = 0;
};

interface IJoinGroupNotify
{
    virtual void addJoinGroup(CJoinGroup *jg) = 0;
};
class CJoinGroup : public CSimpleInterface, implements IInterface
{
protected:
    CActivityBase &activity;
    OwnedConstThorRow left;
    CThorExpandingRowArray rows;
    Int64Array offsets;
    unsigned endMarkersPending, endEndCandidatesPending;
    IJoinProcessor *join;
    mutable CriticalSection crit;
    CJoinGroup *groupStart;
    unsigned candidates;

public:
    CJoinGroup *prev;  // Doubly-linked list to allow us to keep track of ones that are still in use
    CJoinGroup *next;

    CJoinGroup(CActivityBase &_activity) : activity(_activity), rows(_activity, NULL)
    {
        // Used for head object only
        prev = NULL;
        next = NULL;
        endMarkersPending = endEndCandidatesPending = 0;
        groupStart = NULL;
        candidates = 0;
    }

    inline void incMarker(unsigned n)
    {
        crit.enter();
        endMarkersPending += n;
        crit.leave();
    }
    inline void incMarkerEndCandidate(unsigned n)
    {
        crit.enter();
        endEndCandidatesPending += n;
        crit.leave();
    }
    inline bool noteEndCandidateAndTest()
    {
        CriticalBlock b(crit);
        endEndCandidatesPending--;
        return (0 == endEndCandidatesPending) && (0 == endMarkersPending);
    }
    inline bool decMarkerAndTest()
    {
        CriticalBlock b(crit);
        endMarkersPending--;
        return (0 == endEndCandidatesPending) && (0 == endMarkersPending);
    }
    inline unsigned queryPending()
    {
        CriticalBlock b(crit);
        return endMarkersPending;
    }
    inline unsigned queryPendingEndCandidate()
    {
        CriticalBlock b(crit);
        return endEndCandidatesPending;
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CJoinGroup(CActivityBase &_activity, const void *_left, IJoinProcessor *_join, CJoinGroup *_groupStart) : activity(_activity), join(_join), rows(_activity, NULL)
    {
#ifdef TRACE_USAGE
        atomic_inc(&join->getdebug(0));
#endif
#ifdef TRACE_JOINGROUPS
        ActPrintLog(join->queryOwner(), "Creating joinGroup %x, groupstart %x", this, _groupStart);
#endif
        candidates = 0;
        left.setown(_left);
        if (_groupStart)
        {
            groupStart = _groupStart;
#ifdef TRACE_JOINGROUPS
            _groupStart->notePending(__LINE__);
#else
            _groupStart->notePending();
#endif
        }
        else
        {
            groupStart = this;
            endMarkersPending = 1;
        }
        endEndCandidatesPending = 0;
    }

    ~CJoinGroup()
    {
#ifdef TRACE_USAGE
        if (groupStart)
            atomic_dec(&join->getdebug(0));
#endif
#ifdef TRACE_JOINGROUPS
        if (groupStart)
            ActPrintLog(join->queryOwner(), "Destroying joinGroup %x", this);
#endif

        if (NULL == prev) // detached from pool
        {
            CJoinGroup *finger = next;
            while (finger)
            {
                CJoinGroup *next = finger->next;
                finger->Release();
                finger = next;
            }
        }
    }
#ifdef TRACE_JOINGROUPS
    inline void notePendingEndCandidate(unsigned lineNo)
#else
    inline void notePendingEndCandidate()
#endif
    {
        groupStart->incMarkerEndCandidate(1);
#ifdef TRACE_JOINGROUPS
        ActPrintLog(join->queryOwner(), "CJoinGroup::notePendingEndCandidate %x from %d, count became %d", this, lineNo, groupStart->queryPendingEndCandidate());
#endif
    }

#ifdef TRACE_JOINGROUPS
    inline void notePending(unsigned lineNo)
#else
    inline void notePending()
#endif
    {
        groupStart->incMarker(1);
#ifdef TRACE_JOINGROUPS
        ActPrintLog(join->queryOwner(), "CJoinGroup::notePending %x from %d, count became %d", this, lineNo, groupStart->queryPending());
#endif
    }
#ifdef TRACE_JOINGROUPS
    inline void notePendingN(unsigned n,unsigned lineNo)
#else
    inline void notePendingN(unsigned n)
#endif
    {
        groupStart->incMarker(n);
#ifdef TRACE_JOINGROUPS
        ActPrintLog(join->queryOwner(), "CJoinGroup::notePendingN %x from %d, count became %d", this, lineNo, groupStart->queryPending());
#endif
    }
    inline void noteCandidates(unsigned n)
    {
        crit.enter();
        candidates += n;
        crit.leave();
    }

#ifdef TRACE_JOINGROUPS
    inline void noteEndCandidate(unsigned lineNo)
#else
    inline void noteEndCandidate()
#endif
    {
#ifdef TRACE_JOINGROUPS
        ActPrintLog(join->queryOwner(), "CJoinGroup::noteEndCandidate %x from %d, count was %d", this, lineNo, groupStart->queryPendingEndCandidate());
#endif
        if (groupStart->noteEndCandidateAndTest())
            join->onComplete(groupStart);
    }

    inline bool complete() const
    {
        return (0 == groupStart->queryPendingEndCandidate()) && (0 == groupStart->queryPending());
    }

    inline CJoinGroup *groupHead()
    {
        return groupStart;
    }
    inline bool inGroup(CJoinGroup *leader) const
    {
        return groupStart==leader;
    }

    inline const void *queryRow(unsigned idx, offset_t &fpos) const
    {
        // Single threaded by now
        fpos = offsets.item(idx);
        return rows.query(idx);
    }

#ifdef TRACE_JOINGROUPS
    inline void noteEnd(unsigned c, unsigned lineNo)
#else
    inline void noteEnd(unsigned c)
#endif
    {
#ifdef TRACE_JOINGROUPS
        ActPrintLog(join->queryOwner(), "CJoinGroup::noteEnd %x from %d, count was %d", this, lineNo, groupStart->queryPending());
#endif
        assertex(!complete());
        {
            CriticalBlock b(crit);
            candidates += c;
        }
        if (groupStart->decMarkerAndTest())
            join->onComplete(groupStart);
    }

    inline const void *queryLeft() const
    {
        return left;
    }

    inline void addRightMatch(const void *right, offset_t fpos)
    {
        assertex(!complete());
        CriticalBlock b(crit);
        rows.append(right);
        offsets.append(fpos);
    }

    inline unsigned rowsSeen() const
    {
        CriticalBlock b(crit);
        return rows.ordinality();
    }

    inline unsigned candidateCount() const
    {
        CriticalBlock b(crit);
        return candidates;
    }
};

#ifdef TRACE_JOINGROUPS
#define notePending() notePending(__LINE__)
#define notePendingN(n) notePendingN(n, __LINE__)
#define notePendingEndCandidate() notePendingEndCandidate(__LINE__)
#define noteEnd(a) noteEnd(a, __LINE__)
#define noteEndCandidate() noteEndCandidate(__LINE__)
#endif

#ifdef TRACE_USAGE
static int unsignedcompare(unsigned *i1, unsigned *i2)
{
    return *i2-*i1;
}
#endif

class CJoinGroupPool
{
    CActivityBase &activity;
    CJoinGroup *groupStart;
public:
    CJoinGroup head;
    CriticalSection crit;
    bool preserveGroups, preserveOrder;

    CJoinGroupPool(CActivityBase &_activity) : activity(_activity), head(_activity)
    {
        head.next = &head;
        head.prev = &head;
        groupStart = NULL;
    }
    ~CJoinGroupPool()
    {
        CJoinGroup *finger = head.next;
        while (finger != &head)
        {
            CJoinGroup *next = finger->next;
            finger->Release();
            finger = next;
        }
    }
    void setOrdering(bool _preserveGroups, bool _preserveOrder)
    {
        preserveGroups = _preserveGroups;
        preserveOrder = _preserveOrder;
    }
    CJoinGroup *createJoinGroup(const void *row, CActivityBase &activity, IJoinProcessor *join)
    {
        CJoinGroup *jg = new CJoinGroup(activity, row, join, groupStart);
        if (preserveGroups && !groupStart)
        {
            jg->notePending(); // Make sure we wait for the group end
            groupStart = jg;
        }
        CriticalBlock c(crit);
        jg->next = &head;
        jg->prev = head.prev;
        head.prev->next = jg;
        head.prev = jg;
        return jg;
    }
    void endGroup()
    {
        if (groupStart)
            groupStart->noteEnd(0);
        groupStart = NULL;
    }
    inline void _removeJoinGroup(CJoinGroup *goer)
    {
        goer->next->prev = goer->prev;
        goer->prev->next = goer->next;
        goer->prev = NULL;
        goer->next = NULL;
    }
    inline void removeJoinGroup(CJoinGroup *goer)
    {
        CriticalBlock c(crit);
        _removeJoinGroup(goer);
    }
    CJoinGroup *processRemoveGroup(CJoinGroup *head)
    {
        assertex(head == head->groupHead());
        CJoinGroup *finger = head;
        loop
        {
            CJoinGroup *next = finger->next;
            if (!next->inGroup(head))
                break;
            finger = next;
            assertex(next->complete());
        }
        head->prev->next = finger->next;  // set next of elem before head to elem after end of group
        finger->next->prev = head->prev; // set prev of elem after eog to elem before head
        head->prev = NULL; // group extract set head of group to NULL
        CJoinGroup *next = finger->next;
        finger->next = NULL;       // end of group  
        return next;
    }
    void processCompletedGroups(CJoinGroup *jg, IJoinGroupNotify &notify)
    {
        CriticalBlock c(crit);
        if (preserveOrder)
        {
            CJoinGroup *finger = head.next;
            if (preserveGroups)
            {
                unsigned cnt=0;
                while (finger != &head)
                {
                    if (finger->complete())
                    {
                        CJoinGroup *next = processRemoveGroup(finger);
                        notify.addJoinGroup(finger); // means doneQueue owns linked list
                        finger = next;
                        ++cnt;
                    }
                    else
                        break;
                }
            }
            else
            {
                while (finger != &head)
                {
                    if (finger->complete())
                    {
                        CJoinGroup *next = finger->next;
                        removeJoinGroup(finger);
                        notify.addJoinGroup(finger);
                        finger = next;
                    }
                    else
                        break;
                }
            }
        }
        else if (preserveGroups)
        {
            CJoinGroup *next = processRemoveGroup(jg);
            notify.addJoinGroup(jg); // means doneQueue owns linked list
            assertex(!next->inGroup(jg));
        }
        else
        {
            removeJoinGroup(jg);
            notify.addJoinGroup(jg);
        }
    }
#ifdef TRACE_USAGE
    void getStats(StringBuffer &str)
    {
        UnsignedArray counts;
        {
            CriticalBlock c(crit);
            CJoinGroup *p = &head;
            loop
            {
                p = p->next;
                if (p == &head) break;
                counts.append(p->rowsSeen());
            }
        }
        counts.sort(unsignedcompare);
        unsigned i = 0;
        unsigned tc = counts.ordinality();
        if (tc)
        {
            str.append("CJoinGroup rowsSeen : ");
            loop
            {
                str.append(counts.item(i));
                i++;
                if (i == tc) break;
                if (i > 10) break; // i.e show max top 10
                str.append(",");
            }
        }
        str.newline().append("total CJoinGroups=").append(tc);
    }
#endif
};

enum AdditionStats { AS_Seeks, AS_Scans, AS_Accepted, AS_PostFiltered, AS_PreFiltered,  AS_DiskSeeks, AS_DiskAccepted, AS_DiskRejected };

interface IRowStreamSetInput : extends IRowStream
{
    virtual void setInput(IRowStream *input) = 0;
};

class CKeyedJoinSlave : public CSlaveActivity, public CThorDataLink, implements IJoinProcessor, implements IJoinGroupNotify
{
#ifdef TRACE_JOINGROUPS
    unsigned groupsPendsNoted, fetchReadBack, groupPendsEnded, doneGroupsDeQueued, wroteToFetchPipe, groupsComplete;
#endif
    IHThorKeyedJoinArg *helper;
    IHThorArg *inputHelper;
    IRowStreamSetInput *resultDistStream;
    CPartDescriptorArray indexParts, dataParts;
    Owned<IKeyIndexSet> tlkKeySet, partKeySet;
    IThorDataLink *input;
    bool preserveGroups, preserveOrder, eos, inputStopped, needsDiskRead, atMostProvided, remoteDataFiles;
    unsigned joinFlags, abortLimit, parallelLookups, freeQSize, filePartTotal;
    size32_t fixedRecordSize;
    CJoinGroupPool *pool;
    unsigned atMost, keepLimit;
    Owned<IExpander> eexp;
    MemoryBuffer tlkMb;
    CriticalSection onCompleteCrit, stopInputCrit;
    QueueOf<CJoinGroup, false> doneGroups;
    unsigned currentMatchIdx, currentJoinGroupSize, currentAdded, currentMatched;
    Owned<CJoinGroup> djg, doneJG;
    OwnedConstThorRow defaultRight;
    unsigned portbase, node;
    IArrayOf<IDelayedFile> fetchFiles;
    FPosTableEntry *localFPosToNodeMap; // maps fpos->local part #
    FPosTableEntry *globalFPosToNodeMap; // maps fpos->node for all parts of file. If file is remote, localFPosToNodeMap will have all parts
    unsigned pendingGroups, superWidth;
    Semaphore pendingGroupSem;
    CriticalSection pendingGroupCrit, statCrit, lookupCrit;
    UnsignedArray tags;
    UInt64Array _statsArr;
    unsigned __int64 *statsArr;
    unsigned additionalStats;
    rowcount_t rowLimit;
    __int64 lastSeeks, lastScans;
    StringAttr indexName;
    bool localKey, keyHasTlk, onFailTransform;
    Owned<IEngineRowAllocator> joinFieldsAllocator, keyLookupAllocator, fetchInputAllocator, indexInputAllocator;
    Owned<IEngineRowAllocator> fetchInputMetaAllocator;
    Owned<IRowInterfaces> fetchInputMetaRowIf, fetchOutputRowIf;
    MemoryBuffer rawFetchMb;

#ifdef TRACE_USAGE
    atomic_t debugats[10];
    unsigned lastTick;
#endif

    class CKeyedFetchHandler : public CSimpleInterface, implements IThreaded
    {
        CKeyedJoinSlave &owner;
        CThreadedPersistent threaded;
        bool writeWaiting, replyWaiting, stopped, aborted;
        unsigned pendingSends, pendingReplies, nodes, minFetchSendSz, totalSz, fetchMin;
        size32_t perRowMin;
        unsigned maxRequests, blockRequestsAt;
        PointerArrayOf<CThorExpandingRowArray> dstLists;
        CriticalSection crit, sendCrit;
        Semaphore pendingSendsSem, pendingReplySem;
        mptag_t requestMpTag, resultMpTag;

        static int slaveLookup(const void *_key, const void *e)
        {
            offset_t key = *(offset_t *)_key;
            FPosTableEntry &entry = *(FPosTableEntry *)e;
            if (key < entry.base)
                return -1;
            else if (key >= entry.top)
                return 1;
            else
                return 0;
        }
    public:
        class CKeyedFetchResultProcessor : public CSimpleInterface, implements IThreaded
        {
            CThreadedPersistent threaded;
            CKeyedJoinSlave &owner;
            ICommunicator &comm;
            mptag_t resultMpTag;
            bool aborted;
        public:
            CKeyedFetchResultProcessor(CKeyedJoinSlave &_owner, ICommunicator &_comm, mptag_t _mpTag) : threaded("CKeyedFetchResultProcessor", this), owner(_owner), comm(_comm), resultMpTag(_mpTag)
            {
                aborted = false;
                threaded.start();
            }
            ~CKeyedFetchResultProcessor()
            {
                stop();
                threaded.join();
            }
            void stop()
            {
                if (!aborted)
                {
                    aborted = true;
                    comm.cancel(RANK_ALL, resultMpTag);
                }
            }
            bool done()
            {
                return aborted;
            }
            virtual void main()
            {
                try
                {
                    unsigned endRequestsCount = owner.container.queryJob().querySlaves();
                    Owned<IBitSet> endRequests = createThreadSafeBitSet(); // NB: verification only
                    while (!aborted)
                    {
                        rank_t sender;
                        CMessageBuffer msg;
                        if (comm.recv(msg, RANK_ALL, resultMpTag, &sender))
                        {
                            if (!msg.length())
                            {
                                unsigned prevVal = endRequests->testSet(((unsigned)sender)-1);
                                assertex(0 == prevVal);
                                if (0 == --endRequestsCount) // wait for all processors to signal end
                                    break;
                                continue;
                            }
                            unsigned count;
                            msg.read(count);

                            CThorExpandingRowArray received(owner, owner.fetchOutputRowIf);
                            size32_t recvSz = msg.remaining();
                            received.deserialize(recvSz, msg.readDirect(recvSz));

                            unsigned c=0, c2=0;
                            while (c<count && !aborted)
                            {
                                OwnedConstThorRow row = received.getClear(c++);
                                const byte *rowPtr = (const byte *)row.get();
                                offset_t fpos;
                                CJoinGroup *jg;
                                memcpy(&fpos, rowPtr, sizeof(fpos));
                                rowPtr += sizeof(fpos);
                                memcpy(&jg, rowPtr, sizeof(jg));
                                rowPtr += sizeof(jg);
                                const void *fetchRow = *((const void **)rowPtr);
                                if (fetchRow)
                                {
                                    LinkThorRow(fetchRow);
                                    jg->addRightMatch(fetchRow, fpos);
                                }
                                jg->noteEnd(0);
                                ++c2;
                                if (LOWTHROTTLE_GRANULARITY == c2) // prevent sender when busy waking up to send very few.
                                {
                                    owner.fetchHandler->decPendingReplies(c2);
                                    c2 = 0;
                                }
                            }
                            if (c2)
                                owner.fetchHandler->decPendingReplies(c2);
                        }
                    }
                }
                catch (IException *e)
                {
                    owner.fireException(e);
                    e->Release();
                }
                aborted = true;
                owner.pendingGroupSem.signal(); // release puller if blocked on fetch groups pending, in case this has stopped early.
            }
        } *resultProcessor;
        class CKeyedFetchRequestProcessor : public CSimpleInterface, implements IThreaded
        {
            CThreadedPersistent threaded;
            CKeyedJoinSlave &owner;
            ICommunicator &comm;
            mptag_t requestMpTag, resultMpTag;
            bool aborted;

            static int partLookup(const void *_key, const void *e)
            {
                FPosTableEntry &entry = *(FPosTableEntry *)e;
                offset_t keyFpos = *(offset_t *)_key;
                if (keyFpos < entry.base)
                    return -1;
                else if (keyFpos >= entry.top)
                    return 1;
                else
                    return 0;
            }

        public:
            CKeyedFetchRequestProcessor(CKeyedJoinSlave &_owner, ICommunicator &_comm, mptag_t _requestMpTag, mptag_t _resultMpTag) : threaded("CKeyedFetchRequestProcessor", this), owner(_owner), comm(_comm), requestMpTag(_requestMpTag), resultMpTag(_resultMpTag)
            {
                aborted = false;
                threaded.start();
            }
            ~CKeyedFetchRequestProcessor()
            {
                stop();
                threaded.join();
            }
            void stop()
            {
                if (!aborted)
                {
                    aborted = true;
                    comm.cancel(RANK_ALL, requestMpTag);
                }
            }
            virtual void main()
            {
                try
                {
                    rank_t sender;
                    CMessageBuffer msg;
                    unsigned endRequestsCount = owner.container.queryJob().querySlaves();
                    Owned<IBitSet> endRequests = createThreadSafeBitSet(); // NB: verification only

                    Owned<IRowInterfaces> fetchDiskRowIf = createRowInterfaces(owner.helper->queryDiskRecordSize(),owner.queryId(),owner.queryCodeContext());
                    while (!aborted)
                    {
                        CMessageBuffer replyMb;
                        unsigned retCount = 0;
                        replyMb.append(retCount); // place holder;
                        if (comm.recv(msg, RANK_ALL, requestMpTag, &sender))
                        {
                            if (!msg.length())
                            {
                                unsigned prevVal = endRequests->testSet(((unsigned)sender)-1);
                                assertex(0 == prevVal);
                                // I will not get anymore from 'sender', tell sender I've processed all and there will be no more results.
                                if (!comm.send(msg, sender, resultMpTag, LONGTIMEOUT))
                                    throw MakeActivityException(&owner, 0, "CKeyedFetchRequestProcessor {3} - comm send failed");
                                if (0 == --endRequestsCount)
                                    break;
                                continue;
                            }
                            unsigned count;
                            msg.read(count);

                            CThorExpandingRowArray received(owner, owner.fetchInputMetaRowIf);
                            CThorExpandingRowArray replyRows(owner, owner.fetchOutputRowIf);
                            size32_t recvSz =  msg.remaining();
                            received.deserialize(recvSz, msg.readDirect(recvSz));
                            size32_t replySz = 0;
                            unsigned c = 0;
                            while (count--)
                            {
#ifdef TRACE_JOINGROUPS
                                owner.fetchReadBack++;
#endif
                                if (aborted)
                                    break;
                                OwnedConstThorRow row = received.getClear(c++);
                                const byte *rowPtr = (const byte *)row.get();
                                offset_t fpos;
                                memcpy(&fpos, rowPtr, sizeof(fpos));
                                rowPtr += sizeof(fpos);
                                rowPtr += sizeof(CJoinGroup *);

                                const void *fetchKey = NULL;
                                if (owner.fetchInputAllocator)
                                {
                                    const void *fetchRow;
                                    memcpy(&fetchRow, rowPtr, sizeof(const void *));
                                    fetchKey = *((const void **)rowPtr);
                                }

                                unsigned __int64 localFpos;
                                unsigned files = owner.dataParts.ordinality();
                                unsigned filePartIndex = 0;
                                switch (files)
                                {
                                    case 0:
                                        assertex(false);
                                    case 1:
                                    {
                                        if (isLocalFpos(fpos))
                                            localFpos = getLocalFposOffset(fpos);
                                        else
                                            localFpos = fpos-owner.localFPosToNodeMap[0].base;
                                        break;
                                    }
                                    default:
                                    {
                                        // which of multiple parts this slave is dealing with.
                                        FPosTableEntry *result = (FPosTableEntry *)bsearch(&fpos, owner.localFPosToNodeMap, files, sizeof(FPosTableEntry), partLookup);
                                        if (isLocalFpos(fpos))
                                            localFpos = getLocalFposOffset(fpos);
                                        else
                                            localFpos = fpos-result->base;
                                        filePartIndex = result->index;
                                        break;
                                    }
                                }

                                RtlDynamicRowBuilder fetchOutRow(owner.fetchOutputRowIf->queryRowAllocator());
                                byte *fetchOutPtr = fetchOutRow.getSelf();
                                memcpy(fetchOutPtr, row.get(), FETCHKEY_HEADER_SIZE);
                                fetchOutPtr += FETCHKEY_HEADER_SIZE;

                                IFileIO &iFileIO = owner.queryFilePartIO(filePartIndex);
                                Owned<ISerialStream> stream = createFileSerialStream(&iFileIO, localFpos);
                                CThorStreamDeserializerSource ds(stream);

                                RtlDynamicRowBuilder fetchedRowBuilder(fetchDiskRowIf->queryRowAllocator());
                                size32_t fetchedLen = fetchDiskRowIf->queryRowDeserializer()->deserialize(fetchedRowBuilder, ds);
                                OwnedConstThorRow diskFetchRow = fetchedRowBuilder.finalizeRowClear(fetchedLen);

                                RtlDynamicRowBuilder joinFieldsRow(owner.joinFieldsAllocator);
                                const void *fJoinFieldsRow = NULL;
                                if (owner.helper->fetchMatch(fetchKey, diskFetchRow))
                                {
                                    size32_t sz = owner.helper->extractJoinFields(joinFieldsRow, diskFetchRow.get(), fpos, (IBlobProvider*)NULL); // JCSMORE it right that passing NULL IBlobProvider here??
                                    fJoinFieldsRow = joinFieldsRow.finalizeRowClear(sz);
                                    replySz += FETCHKEY_HEADER_SIZE + sz;
                                    owner.statsArr[AS_DiskAccepted]++;
                                    if (owner.statsArr[AS_DiskAccepted] > owner.rowLimit)
                                        owner.onLimitExceeded();
                                }
                                else
                                {
                                    replySz += FETCHKEY_HEADER_SIZE;
                                    owner.statsArr[AS_DiskRejected]++;
                                }
                                size32_t fopsz = owner.fetchOutputRowIf->queryRowAllocator()->queryOutputMeta()->getRecordSize(fetchOutRow.getSelf());
                                // must be easier way? Is it sizeof(const void *)?
                                memcpy(fetchOutPtr, &fJoinFieldsRow, sizeof(const void *));  // child row of fetchOutputRow
                                replyRows.append(fetchOutRow.finalizeRowClear(fopsz));
                                owner.statsArr[AS_DiskSeeks]++;
                                ++retCount;
                                if (replySz>=NEWFETCHREPLYMAX) // send back in chunks
                                {
                                    replyMb.writeDirect(0, sizeof(unsigned), &retCount);
                                    retCount = 0;
                                    replySz = 0;
                                    replyRows.serialize(replyMb);
                                    replyRows.kill();
                                    if (!comm.send(replyMb, sender, resultMpTag, LONGTIMEOUT))
                                        throw MakeActivityException(&owner, 0, "CKeyedFetchRequestProcessor {1} - comm send failed");
                                    replyMb.rewrite(sizeof(retCount));
                                }
                            }
                            if (retCount)
                            {
                                replyMb.writeDirect(0, sizeof(unsigned), &retCount);
                                retCount = 0;
                                replyRows.serialize(replyMb);
                                replyRows.kill();
                                if (!comm.send(replyMb, sender, resultMpTag, LONGTIMEOUT))
                                    throw MakeActivityException(&owner, 0, "CKeyedFetchRequestProcessor {2} - comm send failed");
                                replyMb.rewrite(sizeof(retCount));
                            }
                        }
                    }
                }
                catch (IException *e)
                {
                    owner.fireException(e);
                    e->Release();
                }
                aborted = true;
            }
        } *requestProcessor;
    public:
        CKeyedFetchHandler(CKeyedJoinSlave &_owner) : threaded("CKeyedFetchHandler", this), owner(_owner)
        {
            minFetchSendSz = NEWFETCHSENDMAX;
            totalSz = 0;
            if (minFetchSendSz < (NEWFETCHSENDHEADERSZ+owner.helper->queryFetchInputRecordSize()->getMinRecordSize()))
                minFetchSendSz = NEWFETCHSENDHEADERSZ+owner.helper->queryFetchInputRecordSize()->getMinRecordSize();
            nodes = owner.container.queryJob().querySlaves();
            stopped = aborted = writeWaiting = replyWaiting = false;
            pendingSends = pendingReplies = 0;
            for (unsigned n=0; n<nodes; n++)
                dstLists.append(new CThorExpandingRowArray(owner, owner.fetchInputMetaRowIf));
            fetchMin = owner.helper->queryJoinFieldsRecordSize()->getMinRecordSize();
            perRowMin = NEWFETCHSENDHEADERSZ+fetchMin;
            maxRequests = NEWFETCHPRMEMLIMIT<perRowMin ? 1 : (NEWFETCHPRMEMLIMIT / perRowMin);
            blockRequestsAt = NEWFETCHPRBLOCKMEMLIMIT<perRowMin ? 1 : (NEWFETCHPRBLOCKMEMLIMIT / perRowMin);
            assertex(blockRequestsAt<=maxRequests);

            requestMpTag = (mptag_t)owner.tags.popGet();
            resultMpTag = (mptag_t)owner.tags.popGet();
            requestProcessor = new CKeyedFetchRequestProcessor(owner, owner.queryJobChannel().queryJobComm(), requestMpTag, resultMpTag); // remote receive of fetch fpos'
            resultProcessor = new CKeyedFetchResultProcessor(owner, owner.queryJobChannel().queryJobComm(), resultMpTag); // asynchronously receiving results back

            threaded.start();
        }
        ~CKeyedFetchHandler()
        {
            abort();
            threaded.join();

            ::Release(requestProcessor);
            ::Release(resultProcessor);
            ForEachItemIn(l, dstLists)
            {
                CThorExpandingRowArray *dstList = dstLists.item(l);
                delete dstList;
            }
        }
        bool resultsDone()
        {
            return resultProcessor->done();
        }
        void addRow(offset_t fpos, CJoinGroup *jg)
        {
            RtlDynamicRowBuilder fetchInRow(owner.fetchInputMetaAllocator);
            byte *fetchInRowPtr = fetchInRow.getSelf();
            memcpy(fetchInRowPtr, &fpos, sizeof(fpos));
            fetchInRowPtr += sizeof(fpos); 
            memcpy(fetchInRowPtr, &jg, sizeof(jg));
            fetchInRowPtr += sizeof(jg);

            size32_t sz = 0;
            if (owner.fetchInputAllocator)
            {
                RtlDynamicRowBuilder fetchRow(owner.fetchInputAllocator);
                sz = owner.helper->extractFetchFields(fetchRow, jg->queryLeft());
                const void *fFetchRow = fetchRow.finalizeRowClear(sz);
                memcpy(fetchInRowPtr, &fFetchRow, sizeof(const void *));  // child row of fetchInRow
            }

            if (totalSz + (FETCHKEY_HEADER_SIZE+sz) > minFetchSendSz)
            {
                sendAll(); // in effect send remaining
                totalSz = 0;
            }
            unsigned dstNode;
            if (owner.remoteDataFiles)
                dstNode = owner.node; // JCSMORE - do directly
            else
            {
                if (1 == owner.filePartTotal)
                    dstNode = owner.globalFPosToNodeMap[0].index;
                else if (isLocalFpos(fpos))
                    dstNode = getLocalFposPart(fpos);
                else
                {
                    const void *result = bsearch(&fpos, owner.globalFPosToNodeMap, owner.filePartTotal, sizeof(FPosTableEntry), slaveLookup);
                    if (!result)
                        throw MakeThorException(TE_FetchOutOfRange, "FETCH: Offset not found in offset table; fpos=%" I64F "d", fpos);
                    dstNode = ((FPosTableEntry *)result)->index;
                }
            }

            {
                CriticalBlock b(crit);
                //must be easier way?
                size32_t sz = owner.fetchInputMetaAllocator->queryOutputMeta()->getRecordSize(fetchInRow.getSelf());
                dstLists.item(dstNode)->append(fetchInRow.finalizeRowClear(sz));
                totalSz += FETCHKEY_HEADER_SIZE+sz;
                ++pendingSends;
                if (writeWaiting)
                {
                    writeWaiting = false;
                    pendingSendsSem.signal();
                }
            }
        }
        void stop(bool stopPending)
        {
            crit.enter();
            if (stopped)
                crit.leave();
            else
            {
                stopped = true;
                if (writeWaiting)
                {
                    writeWaiting = false;
                    pendingSendsSem.signal();
                }
                crit.leave();
                threaded.join();
                if (stopPending) // stop groups in progress
                {
                    if (aborted) // don't stop request processor unless aborting, other nodes may depend on it's reply.
                        requestProcessor->stop();
                    resultProcessor->stop();
                }
            }
        }
        void abort()
        {
            if (!aborted)
            {
                aborted = true;
                stop(true);
            }
        }
        void sendStop()
        {
            // signal to request processor that *this* node isn't going to send anymore
            unsigned n=0;
            for (; n<nodes; n++)
            {
                CMessageBuffer msg;
                if (!owner.queryJobChannel().queryJobComm().send(msg, n+1, requestMpTag, LONGTIMEOUT))
                    throw MakeActivityException(&owner, 0, "CKeyedFetchHandler::stop - comm send failed");
            }
        }
        void decPendingReplies(unsigned c=1)
        {
            CriticalBlock b(crit);
            assertex(pendingReplies >= c);
            pendingReplies -= c;
            if (replyWaiting)
            {
                replyWaiting=false;
                pendingReplySem.signal();
            }
        }
        void sendAll()
        {
            CriticalBlock b(sendCrit); // want to block here, if other is replyWaiting
            unsigned n=0;
            for (; n<nodes; n++)
            {
                if (aborted)
                    return;
                CMessageBuffer msg;
                {
                    CriticalBlock b(crit); // keep writer out during flush to this dstNode
                    unsigned total = dstLists.item(n)->ordinality();
                    if (total)
                    {
                        assertex(!replyWaiting);
                        CThorExpandingRowArray dstList(owner, owner.fetchInputMetaRowIf);
                        unsigned dstP=0;
                        loop
                        {
                            // delay if more than max or if few sends and growing # of replies
                            bool throttleBig = pendingReplies >= blockRequestsAt;
                            loop
                            {
                                if (!throttleBig)
                                {
                                    bool throttleSmall = (pendingSends <= LOWTHROTTLE_GRANULARITY) && (pendingReplies >= LOWTHROTTLE_GRANULARITY*2);
                                    if (!throttleSmall)
                                        break;
                                }

                                replyWaiting = true;
                                { CriticalUnblock ub(crit);
                                    while (!pendingReplySem.wait(5000))
                                    {
                                        owner.ActPrintLog("KJ: replyWaiting blocked");
                                    }
                                }
                                if (throttleBig) // break out if some received and reason for blocking was high number of pendingReplies.
                                    break;
                            }
                            if (aborted)
                                return;
                            if (0 == dstP) // delay detach until necessary as may have been blocked and more added.
                            {
                                dstList.swap(*dstLists.item(n));
                                total = dstList.ordinality();
                            }
                            unsigned requests = maxRequests - pendingReplies;
                            assertex(requests);
                            if (total < requests)
                                requests = total;
                            msg.append(requests);
                            unsigned r=0;
                            IOutputRowSerializer *serializer = owner.fetchInputMetaRowIf->queryRowSerializer();
                            CMemoryRowSerializer s(msg);
                            for (; r<requests; r++)
                            {
                                OwnedConstThorRow row = dstList.getClear(dstP++);
                                serializer->serialize(s,(const byte *)row.get());
                            }
                            pendingSends -= requests;
                            pendingReplies += requests;
                            total -= requests;
                            { CriticalUnblock ub(crit);
                                if (!owner.queryJobChannel().queryJobComm().send(msg, n+1, requestMpTag, LONGTIMEOUT))
                                    throw MakeActivityException(&owner, 0, "CKeyedFetchHandler - comm send failed");
                            }
                            if (0 == total)
                                break;
                            msg.clear();
                        }                   
                    }
                }
            }
        }
    // IThreaded
        virtual void main()
        {
            try
            {
                CMessageBuffer msg;
                loop
                {
                    crit.enter();
                    if (aborted || stopped)
                    {
                        crit.leave();
                        break;
                    }
                    if (0 == pendingSends)
                    {
                        writeWaiting = true;
                        crit.leave();
                        pendingSendsSem.wait();
                    }
                    else
                        crit.leave();
                    sendAll();
                }
                if (!aborted)
                {
                    sendAll();
                    sendStop();
                }
            }
            catch (IException *e)
            {
                owner.fireException(e);
                e->Release();
            }
        }
    friend class CKeyedFetchRequestProcessor;
    friend class CKeyedFetchResultProcessor;
    } *fetchHandler;
    class CKeyLocalLookup : public CSimpleInterface, implements IRowStreamSetInput, implements IStopInput
    {
        CKeyedJoinSlave &owner;
        Linked<IRowStream> in;
        unsigned nextTlk;
        IKeyManager *tlkManager;
        bool eos, eog;
        IKeyIndex *currentTlk;
        CJoinGroup *currentJG;
        RtlDynamicRowBuilder indexReadFieldsRow;

        IKeyManager *partManager;
        IKeyIndex *currentPart;
        bool inputStopped;
        unsigned nextPart;
        unsigned candidateCount;
        __int64 lastSeeks, lastScans;

        inline void noteStats(unsigned seeks, unsigned scans)
        {
            CriticalBlock b(owner.statCrit);
            owner.statsArr[AS_Seeks] += seeks-lastSeeks;
            owner.statsArr[AS_Scans] += scans-lastScans;
            lastSeeks = seeks;
            lastScans = scans;
#ifdef TRACE_USAGE
            if (msTick()-owner.lastTick > 5000)
            {
                owner.trace();
                owner.lastTick = msTick();
            }
#endif
        }
        void reset()
        {
            eos = eog = false;
            nextTlk = 0;
            currentJG = NULL;
            currentTlk = NULL;
            lastSeeks = lastScans = 0;
            inputStopped = false;
            nextPart = 0; // only used for superkeys of single part keys
            currentPart = NULL;
            candidateCount = 0;
        }
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CKeyLocalLookup(CKeyedJoinSlave &_owner) : owner(_owner), indexReadFieldsRow(_owner.indexInputAllocator)
        {
            tlkManager = owner.keyHasTlk ? createKeyManager(NULL, owner.fixedRecordSize, NULL) : NULL;
            if (owner.localKey && owner.partKeySet->numParts() > 1)
                partManager = createKeyMerger(owner.partKeySet, owner.fixedRecordSize, 0, NULL);
            else
                partManager = createKeyManager(NULL, owner.fixedRecordSize, NULL);
            reset();
        }
        ~CKeyLocalLookup()
        {
            in.clear();
            ::Release(tlkManager);

            partManager->Release();
        }

        CJoinGroup *extractIndexFields(ARowBuilder & lhs)
        {
            CriticalBlock b(owner.lookupCrit);
            try
            {
                while (!eos)
                {
                    OwnedConstThorRow row = in->nextRow();
                    if (!row)
                    {
                        if (owner.preserveGroups)
                        {
                            if (owner.needsDiskRead)
                                owner.notePendingGroup();
                            owner.pool->endGroup();
                        }
                        row.setown(in->nextRow());
                        if (!row)
                        {
                            eos = true;
                            break;
                        }
                    }
                    
                    if (owner.helper->leftCanMatch(row))
                    {
                        owner.helper->extractIndexReadFields(lhs, row);

                        return owner.pool->createJoinGroup(row.getClear(), owner, &owner);
                    }
                    else
                    {
                        {
                            CriticalBlock b(owner.statCrit);
                            owner.statsArr[AS_PreFiltered]++;
                        }
                        switch (owner.joinFlags & JFtypemask)
                        {
                            case JFleftouter:
                            case JFleftonly:
                            {
                                CJoinGroup *jg = owner.pool->createJoinGroup(row.getClear(), owner, &owner);
                                jg->noteEnd(0); // will queue on doneGroups, may be used if excl.
                                if (!owner.preserveGroups) // if preserving groups, JG won't be complete until lhs eog hit
                                    return NULL;
                            }
                            default: // don't bother creating join group, will not be used, loop around and get another candidate.
                                break;
                        }
                    }
                }
            }
            catch (IException *e)
            {
                ::ActPrintLog(&owner, e);
                throw;
            }
            return NULL;
        }
        // IStopInput
        virtual void stopInput()
        {
            owner.stopInput();
        }
        // IRowStreamSetInput
        const void *nextRow()
        {
            try
            {
                loop
                {
                    if (currentPart)
                    {
                        while (partManager->lookup(true))
                        {
                            ++candidateCount;
                            if (candidateCount > owner.atMost)
                                break;
                            KLBlobProviderAdapter adapter(partManager);
                            offset_t fpos;
                            byte const * keyRow = partManager->queryKeyBuffer(fpos);
                            if (owner.helper->indexReadMatch(indexReadFieldsRow.getSelf(), keyRow, fpos, &adapter))
                            {
                                if (currentJG->rowsSeen() >= owner.keepLimit)
                                    break;
                                { CriticalBlock b(owner.statCrit);
                                    owner.statsArr[AS_Accepted]++;
                                }
                                currentJG->notePendingN(1);

                                RtlDynamicRowBuilder lookupRow(owner.keyLookupAllocator);
                                LookupRowResult *lookupRowResult = (LookupRowResult *)lookupRow.getSelf();
                                lookupRowResult->fpos = fpos;
                                lookupRowResult->jg = currentJG;
                                lookupRowResult->eog = false;
                                if (!owner.needsDiskRead)
                                {
                                    void *joinFieldsPtr = (void *)(lookupRowResult+1);
                                    RtlDynamicRowBuilder joinFieldsRow(owner.joinFieldsAllocator);
                                    size32_t sz = owner.helper->extractJoinFields(joinFieldsRow, keyRow, fpos, &adapter);
                                    const void *fJoinFieldsRow = joinFieldsRow.finalizeRowClear(sz);
                                    memcpy(joinFieldsPtr, &fJoinFieldsRow, sizeof(const void *));
                                }
#ifdef TRACE_JOINGROUPS
                                ::ActPrintLog(&owner, "CJoinGroup [result] %x from %d", currentJG, __LINE__);
#endif
                                noteStats(partManager->querySeeks(), partManager->queryScans());
                                size32_t lorsz = owner.keyLookupAllocator->queryOutputMeta()->getRecordSize(lookupRow.getSelf());
                                // must be easier way
                                return lookupRow.finalizeRowClear(lorsz);
                            }
                            else
                            {
                                CriticalBlock b(owner.statCrit);
                                owner.statsArr[AS_PostFiltered]++;
                            }
                        }
                        partManager->releaseSegmentMonitors();
                        currentPart = NULL;
                        if (owner.localKey)
                        { // merger done
                        }
                        else if (!owner.keyHasTlk)
                        {
                            if (nextPart < owner.partKeySet->numParts())
                            {
                                currentPart = owner.partKeySet->queryPart(nextPart++);
                                partManager->setKey(currentPart);
                                owner.helper->createSegmentMonitors(partManager, indexReadFieldsRow.getSelf());
                                partManager->finishSegmentMonitors();
                                partManager->reset();
                            }
                        }
                    }
                    else if (currentTlk)
                    {
                        loop
                        {
                            if (!tlkManager->lookup(false)) break;
                            if (tlkManager->queryFpos()) // don't bail out if part0 match, test again for 'real' tlk match.
                            {
                                unsigned partNo = (unsigned)tlkManager->queryFpos();
                                partNo = owner.superWidth ? owner.superWidth*nextTlk+(partNo-1) : partNo-1;

                                currentPart = owner.partKeySet->queryPart(partNo);
                                partManager->setKey(currentPart);
                                owner.helper->createSegmentMonitors(partManager, indexReadFieldsRow.getSelf());
                                partManager->finishSegmentMonitors();
                                partManager->reset();
                                break;
                            }
                        }
                        if (!currentPart)
                        {
                            if (++nextTlk < owner.tlkKeySet->numParts())
                            {
                                tlkManager->releaseSegmentMonitors();
                                currentTlk = owner.tlkKeySet->queryPart(nextTlk);
                                tlkManager->setKey(currentTlk);
                                owner.helper->createSegmentMonitors(tlkManager, indexReadFieldsRow.getSelf());
                                tlkManager->finishSegmentMonitors();
                                tlkManager->reset();
                            }
                            else // end of input row processing
                                currentTlk = NULL;
                        }
                    }
                    else
                    {
                        if (currentJG)
                        {
                            if (!owner.preserveGroups && owner.needsDiskRead)
                                owner.notePendingGroup();
                            currentJG->noteEnd(0);
                            if (tlkManager)
                                tlkManager->releaseSegmentMonitors();

                            RtlDynamicRowBuilder lookupRow(owner.keyLookupAllocator);
                            LookupRowResult *lookupRowResult = (LookupRowResult *)lookupRow.getSelf();
                            // output an end marker for the matches to this group

                            lookupRowResult->fpos = candidateCount;
                            lookupRowResult->jg = currentJG;
                            lookupRowResult->eog = true;
                            if (!owner.needsDiskRead) // need to mark null childrow
                            {
                                void *joinFieldsPtr = (void *)(lookupRowResult+1);
                                const void *fJoinFieldsRow = NULL;
                                memcpy(joinFieldsPtr, &fJoinFieldsRow, sizeof(const void *));
                            }
#ifdef TRACE_JOINGROUPS
                            ::ActPrintLog(&owner, "CJoinGroup [end marker returned] %x from %d", currentJG, __LINE__);
#endif
                            noteStats(partManager->querySeeks(), partManager->queryScans());
                            currentJG = NULL;
                            size32_t lorsz = owner.keyLookupAllocator->queryOutputMeta()->getRecordSize(lookupRow.getSelf());
                            // must be easier way
                            return lookupRow.finalizeRowClear(lorsz);
                        }                       

                        currentJG = extractIndexFields(indexReadFieldsRow);
                        //GH: More - reuse of indexReadFieldsRow without finalize means this could be leaking.
                        if (!currentJG)
                            break;

                        currentJG->notePendingEndCandidate();

                        candidateCount = 0;
                        if (0 == owner.partKeySet->numParts()) // if empty key
                        {
                            // will terminate row/group next cycle
                        }
                        else if (!owner.keyHasTlk)
                        {
                            currentPart = owner.partKeySet->queryPart(0);
                            if (!owner.localKey || 1 == owner.partKeySet->numParts())
                            {
                                nextPart = 1;
                                partManager->setKey(currentPart);
                            }
                            owner.helper->createSegmentMonitors(partManager, indexReadFieldsRow.getSelf());
                            partManager->finishSegmentMonitors();
                            partManager->reset();
                        }
                        else
                        {
                            nextTlk = 0;
                            currentTlk = owner.tlkKeySet->queryPart(nextTlk);
                            tlkManager->setKey(currentTlk);
                            owner.helper->createSegmentMonitors(tlkManager, indexReadFieldsRow.getSelf());
                            tlkManager->finishSegmentMonitors();
                            tlkManager->reset();
                        }
                    }
                }
            }
            catch (IException *e)
            {
                ::ActPrintLog(&owner, e);
                throw;
            }
            noteStats(partManager->querySeeks(), partManager->queryScans());
            return NULL;
        }

        virtual void stop()
        {
            stopInput();
        }
        virtual void setInput(IRowStream *_in)
        {
            in.set(_in);
            reset();
        }
    };
    class CPRowStream : public CSimpleInterface, implements IRowStreamSetInput, implements IThreadFactory
    {
        unsigned maxPoolSize, queueSize;
        CKeyedJoinSlave &owner;
        Linked<IRowStream> in;
        SimpleInterThreadQueueOf<const void, false> lookupQ;
        Owned<IThreadPool> keyLookupPool;
        bool stopped;

        void clear()
        {
            OwnedConstThorRow row;
            do
            {
                row.setown(lookupQ.dequeueNow());
            } while (row);
        }

    public:
        class CKeyLookupPoolMember : public CSimpleInterface, implements IPooledThread
        {
            Owned<IRowStreamSetInput> lookupStream;
            CPRowStream &owner;
        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CKeyLookupPoolMember(CPRowStream &_owner) : owner(_owner)
            {
                lookupStream.setown(new CKeyLocalLookup(owner.owner));
            }
            void init(void *param)
            {
                lookupStream->setInput(owner.in);
            }
            void main()
            {
                do
                {
                    OwnedConstThorRow lookupRow = lookupStream->nextRow();
                    if (!lookupRow)
                        break;
                    if (owner.stopped)
                        break;
                    owner.lookupQ.enqueue(lookupRow.getClear());
                } while (!owner.stopped);
            }
            bool stop() { return false; }
            bool canReuse() { return true; }
        };
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CPRowStream(CKeyedJoinSlave &_owner, unsigned _maxPoolSize, unsigned _queueSize) : owner(_owner), maxPoolSize(_maxPoolSize)
        {
            stopped = false;
            queueSize = _queueSize<maxPoolSize?maxPoolSize:_queueSize;
            lookupQ.setLimit(queueSize);

            keyLookupPool.setown(createThreadPool("KeyLookupPool", this, &owner, maxPoolSize));
            unsigned i=0;
            for (; i<maxPoolSize; i++)
                keyLookupPool->start(NULL);
        }
        ~CPRowStream()
        {
            clear();
        }

        // IThreadFactory impl.
        IPooledThread *createNew()
        {
            return new CKeyLookupPoolMember(*this);
        }

        // IRowStreamSetInput impl.
        const void *nextRow()
        {
            if (stopped) return NULL;
            const void *row = lookupQ.dequeue();
            if (!row)
                stopped = true;
            return row;
        }
        virtual void stop()
        {
            stopped = true;
            if (keyLookupPool)
            {
                lookupQ.stop();
                keyLookupPool.clear();
                clear();
            }
        }
        virtual void setInput(IRowStream *_in) { in.set(_in); }
    friend class CKeyLookupPoolMember;
    };
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CKeyedJoinSlave(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
#ifdef TRACE_JOINGROUPS
        groupsPendsNoted = fetchReadBack = groupPendsEnded = doneGroupsDeQueued = wroteToFetchPipe = groupsComplete = 0;
#endif
        input = NULL;
        inputHelper = NULL;
        preserveGroups = preserveOrder = eos = false;
        resultDistStream = NULL;
        tlkKeySet.setown(createKeyIndexSet());
        partKeySet.setown(createKeyIndexSet());
        pool = NULL;
        currentMatchIdx = currentJoinGroupSize = currentAdded = currentMatched = 0;
        inputStopped = true;
        portbase = 0;
        pendingGroups = 0;
        superWidth = 0;
        additionalStats = 0;
        lastSeeks = lastScans = 0;
        onFailTransform = localKey = keyHasTlk = false;
        remoteDataFiles = false;
        fetchHandler = NULL;
        globalFPosToNodeMap = NULL;
        localFPosToNodeMap = NULL;

#ifdef TRACE_USAGE
        unsigned it=0;
        for (; it<10; it++)
            atomic_set(&debugats[it],0);
        lastTick = 0;
#endif
        helper = (IHThorKeyedJoinArg *)queryHelper();
        reInit = 0 != (helper->getFetchFlags() & (FFvarfilename|FFdynamicfilename));
    }
    ~CKeyedJoinSlave()
    {
        delete [] globalFPosToNodeMap;
        delete [] localFPosToNodeMap;
        while (doneGroups.ordinality())
        {
            CJoinGroup *jg = doneGroups.dequeue();
            jg->Release();
        }
        ::Release(fetchHandler);
        ::Release(inputHelper);
        if (portbase)
            freePort(portbase, NUMSLAVEPORTS*3);
        ::Release(resultDistStream);
        defaultRight.clear();
        if (pool) delete pool;
    }
    inline void resetLastStats()
    {
// NB: part manager retains seek/scan counts across setKey calls
//      lastSeeks = lastScans = 0;
    }

#ifdef TRACE_USAGE
    void trace()
    {
        StringBuffer s;
        { CriticalBlock b(onCompleteCrit);
            s.appendf("CJoinGroups=%d, doneGroups=%d, ",atomic_read(&debugats[0]), doneGroups.ordinality());
            pool->getStats(s);
        }
        ActPrintLog(s.str());
    }
#endif

    IFileIO &queryFilePartIO(unsigned partNum)
    {
        assertex(partNum<dataParts.ordinality());
        return *fetchFiles.item(partNum).queryFileIO();
    }
    inline void noteStats(unsigned seeks, unsigned scans)
    {
        CriticalBlock b(statCrit);
        statsArr[AS_Seeks] += seeks-lastSeeks;
        statsArr[AS_Scans] += scans-lastScans;
        lastSeeks = seeks;
        lastScans = scans;
    }
    void notePendingGroup()
    {
#ifdef TRACE_JOINGROUPS
        ActPrintLog("notePendingGroup was: %d", pendingGroups);
#endif
        pendingGroupCrit.enter();
#ifdef TRACE_JOINGROUPS
        groupsPendsNoted++;
#endif
        ++pendingGroups;
        pendingGroupCrit.leave();
    }
    void noteEndGroup()
    {
        pendingGroupCrit.enter();
        --pendingGroups;
#ifdef TRACE_JOINGROUPS
        groupPendsEnded++;
        ActPrintLog("noteEndGroup become: %d", pendingGroups);
#endif
        pendingGroupCrit.leave();
        pendingGroupSem.signal();
    }
    bool waitPendingGroups()
    {
        loop
        {
            {
                if (eos)
                    return false;
                unsigned p;
                {
                    CriticalBlock b(pendingGroupCrit);
                    p = pendingGroups;
                }
                CriticalBlock b(onCompleteCrit);
                if (p)
                {
                    if (doneGroups.ordinality())
                        return true;
                    if (fetchHandler->resultsDone())
                        return false;
                    // if not wait for them.
                }
                else
                {
                    CriticalBlock b(onCompleteCrit);
                    return 0 != doneGroups.ordinality();
                }
            }
            pendingGroupSem.wait();
            if (abortSoon)
                return false;
        }
        return true;
    }
    virtual void addJoinGroup(CJoinGroup *jg)
    {
#ifdef TRACE_JOINGROUPS
        groupsComplete++;
#endif
        doneGroups.enqueue(jg);
        if (needsDiskRead)
            noteEndGroup();
    }
    virtual CJoinGroup *createJoinGroup(const void *row)
    {
        UNIMPLEMENTED;
        return NULL;
    }
    virtual void createSegmentMonitors(IIndexReadContext *ctx, const void *row)
    {
        UNIMPLEMENTED;
    }
    virtual bool addMatch(CJoinGroup &match, const void *rhs, size32_t rhsSize, offset_t recptr)
    {
        UNIMPLEMENTED;
        return false;
    }
    virtual void onComplete(CJoinGroup *jg)
    {
        // can be called by pulling end of input group, or end of group on result (i.e. thread contention)
        CriticalBlock b(onCompleteCrit); // Keep record order the way we want it
        pool->processCompletedGroups(jg, *this);
    }
    virtual bool leftCanMatch(const void *_left) { UNIMPLEMENTED; return false; }

#ifdef TRACE_USAGE
    virtual atomic_t &getdebug(unsigned w)
    {
        return debugats[w];
    }
#endif
    virtual CActivityBase *queryOwner() { return this; }
    virtual void stopInput()
    {
        CriticalBlock b(stopInputCrit);
        if (!inputStopped)
        {
            inputStopped = true;
            CSlaveActivity::stopInput(input, "(LEFT)");
            input = NULL;
        }
    }
    void doAbortLimit(CJoinGroup *jg)
    {
        helper->onMatchAbortLimitExceeded();
        CommonXmlWriter xmlwrite(0);
        if (inputHelper && inputHelper->queryOutputMeta() && inputHelper->queryOutputMeta()->hasXML())
        {
            inputHelper->queryOutputMeta()->toXML((byte *) jg->queryLeft(), xmlwrite);
        }
        throw MakeActivityException(this, 0, "More than %d match candidates in keyed join for row %s", abortLimit, xmlwrite.str());
    }
    bool checkAbortLimit(CJoinGroup *jg)
    {
        if (jg->candidateCount() > abortLimit)
        {
            if (0 == (joinFlags & JFmatchAbortLimitSkips))
                doAbortLimit(jg);
            return true;
        }
        return false;
    }
    bool abortLimitAction(CJoinGroup *jg, OwnedConstThorRow &row)
    {
        Owned<IException> e;
        try
        {
            return checkAbortLimit(jg);
        }
        catch (IException *_e)
        {
            if (!onFailTransform)
                throw;
            e.setown(_e);
        }
        RtlDynamicRowBuilder trow(queryRowAllocator());
        size32_t transformedSize = helper->onFailTransform(trow, jg->queryLeft(), defaultRight, 0, e.get());
        if (0 != transformedSize)
            row.setown(trow.finalizeRowClear(transformedSize));
        return true;
    }
    virtual void onLimitExceeded() { return; }
    
    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        parallelLookups = (unsigned)container.queryJob().getWorkUnitValueInt("parallelKJLookups", DEFAULTMAXRESULTPULLPOOL);
        freeQSize = (unsigned)container.queryJob().getWorkUnitValueInt("freeQSize", DEFAULTFREEQSIZE);
        joinFlags = helper->getJoinFlags();
        keepLimit = helper->getKeepLimit();
        atMost = helper->getJoinLimit();
        if (atMost == 0)
        {
            if (JFleftonly == (joinFlags & JFleftonly))
                keepLimit = 1; // don't waste time and memory collating and returning record which will be discarded.
            atMostProvided = false;
            atMost = (unsigned)-1;
        }
        else
            atMostProvided = true;
        abortLimit = helper->getMatchAbortLimit();
        if (abortLimit == 0) abortLimit = (unsigned)-1;
        if (keepLimit == 0) keepLimit = (unsigned)-1;
        if (abortLimit < atMost)
            atMost = abortLimit;
        rowLimit = (rowcount_t)helper->getRowLimit();
        additionalStats = 5; // (seeks, scans, accepted, prefiltered, postfiltered)
        needsDiskRead = helper->diskAccessRequired();
        globalFPosToNodeMap = NULL;
        localFPosToNodeMap = NULL;
        fetchHandler = NULL;
        filePartTotal = 0;

        if (needsDiskRead)
            additionalStats += 3; // (diskSeeks, diskAccepted, diskRejected)
        unsigned aS = additionalStats;
        while (aS--) _statsArr.append(0);
        statsArr = _statsArr.getArray();
        
        fixedRecordSize = helper->queryIndexRecordSize()->getFixedSize(); // 0 if variable and unused
        node = queryJobChannel().queryMyRank()-1;
        onFailTransform = (0 != (joinFlags & JFonfail)) && (0 == (joinFlags & JFmatchAbortLimitSkips));

        joinFieldsAllocator.setown(getRowAllocator(helper->queryJoinFieldsRecordSize()));
        if (onFailTransform || (joinFlags & JFleftouter))
        {
            RtlDynamicRowBuilder rr(joinFieldsAllocator);
            size32_t sz = helper->createDefaultRight(rr);
            defaultRight.setown(rr.finalizeRowClear(sz));
        }

        // decode data from master

        data.read(indexName);
        unsigned numTags;
        data.read(numTags);
        unsigned t;
        for (t=0; t<numTags; t++)
        {
            mptag_t tag = container.queryJobChannel().deserializeMPTag(data);
            tags.append(tag);
            queryJobChannel().queryJobComm().flush(tag);
        }
        indexParts.kill();
        dataParts.kill();
        tlkKeySet.setown(createKeyIndexSet());
        partKeySet.setown(createKeyIndexSet());
        unsigned numIndexParts;
        data.read(numIndexParts);
        if (numIndexParts)
        {
            unsigned numSuperIndexSubs;
            data.read(superWidth);
            data.read(numSuperIndexSubs);
            if (numSuperIndexSubs)
            {
                CPartDescriptorArray _indexParts;
                deserializePartFileDescriptors(data, _indexParts);
                unsigned s, ip;
                for (s=0; s<numSuperIndexSubs; s++)
                {
                    for (ip=0; ip<superWidth; ip++)
                    {
                        unsigned which = ip*numSuperIndexSubs+s;
                        IPartDescriptor &part = _indexParts.item(which);
                        indexParts.append(*LINK(&part));
                    }
                }
            }
            else
                deserializePartFileDescriptors(data, indexParts);


            localKey = indexParts.item(0).queryOwner().queryProperties().getPropBool("@local", false);
            unsigned ip=0;
            do
            {
                IPartDescriptor &filePart = indexParts.item(ip++);
                unsigned crc=0;
                filePart.getCrc(crc);
                RemoteFilename rfn;
                filePart.getFilename(0, rfn);
                StringBuffer filename;
                rfn.getPath(filename);
                Owned<IDelayedFile> lfile = queryThor().queryFileCache().lookup(*this, filePart);
                partKeySet->addIndex(createKeyIndex(filename.str(), crc, *lfile, false, false));
            }
            while (ip<numIndexParts);

            data.read(keyHasTlk);
            if (keyHasTlk)
            {
                unsigned tlks;
                size32_t tlkSz;
                data.read(tlks);
                UnsignedArray posArray, lenArray;
                while (tlks--)
                {
                    data.read(tlkSz);
                    posArray.append(tlkMb.length());
                    lenArray.append(tlkSz);
                    tlkMb.append(tlkSz, data.readDirect(tlkSz));
                }
                ForEachItemIn(p, posArray)
                {
                    Owned<IFileIO> iFileIO = createIFileI(lenArray.item(p), tlkMb.toByteArray()+posArray.item(p));
                    StringBuffer name("TLK");
                    name.append('_').append(container.queryId()).append('_');
                    tlkKeySet->addIndex(createKeyIndex(name.append(p).str(), 0, *iFileIO, true, false)); // MORE - not the right crc
                }
            }
            if (needsDiskRead)
            {
                data.read(remoteDataFiles); // if true, all fetch parts will be serialized
                unsigned numDataParts;
                data.read(numDataParts);
                if (numDataParts)
                {
                    deserializePartFileDescriptors(data, dataParts);
                    localFPosToNodeMap = new FPosTableEntry[numDataParts];
                    unsigned f;
                    FPosTableEntry *e;
                    for (f=0, e=&localFPosToNodeMap[0]; f<numDataParts; f++, e++)
                    {
                        IPartDescriptor &part = dataParts.item(f);
                        e->base = part.queryProperties().getPropInt64("@offset");
                        e->top = e->base + part.queryProperties().getPropInt64("@size");
                        e->index = f; // NB: index == which local part in dataParts
                    }
                }
                if (remoteDataFiles) // global offset map not needed if remote and have all fetch parts inc. map (from above)
                {
                    if (numDataParts)
                        filePartTotal = numDataParts;
                }
                else
                {
                    data.read(filePartTotal);
                    if (filePartTotal)
                    {
                        size32_t offsetMapSz = 0;
                        data.read(offsetMapSz);
                        globalFPosToNodeMap = new FPosTableEntry[filePartTotal];
                        const void *offsetMapBytes = (FPosTableEntry *)data.readDirect(offsetMapSz);
                        memcpy(globalFPosToNodeMap, offsetMapBytes, offsetMapSz);
                    }
                }
                unsigned encryptedKeyLen;
                void *encryptedKey;
                helper->getFileEncryptKey(encryptedKeyLen,encryptedKey);
                if (0 != encryptedKeyLen)
                {
                    bool dfsEncrypted = numDataParts?dataParts.item(0).queryOwner().queryProperties().getPropBool("@encrypted"):false;
                    if (dfsEncrypted) // otherwise ignore (warning issued by master)
                        eexp.setown(createAESExpander256(encryptedKeyLen, encryptedKey));
                    memset(encryptedKey, 0, encryptedKeyLen);
                    free(encryptedKey);
                }
                Owned<IOutputMetaData> fetchInputMeta;
                if (0 != helper->queryFetchInputRecordSize()->getMinRecordSize())
                {
                    fetchInputAllocator.setown(getRowAllocator(helper->queryFetchInputRecordSize()));
                    fetchInputMeta.setown(createOutputMetaDataWithChildRow(fetchInputAllocator, FETCHKEY_HEADER_SIZE));
                }
                else
                    fetchInputMeta.setown(createFixedSizeMetaData(FETCHKEY_HEADER_SIZE));
                fetchInputMetaRowIf.setown(createRowInterfaces(fetchInputMeta,queryId(),queryCodeContext()));
                fetchInputMetaAllocator.set(fetchInputMetaRowIf->queryRowAllocator());

                Owned<IOutputMetaData> fetchOutputMeta = createOutputMetaDataWithChildRow(joinFieldsAllocator, FETCHKEY_HEADER_SIZE);
                fetchOutputRowIf.setown(createRowInterfaces(fetchOutputMeta,queryId(),queryCodeContext()));

                fetchHandler = new CKeyedFetchHandler(*this);

                FPosTableEntry *fPosToNodeMap = globalFPosToNodeMap ? globalFPosToNodeMap : localFPosToNodeMap;
                unsigned c;
                for (c=0; c<filePartTotal; c++)
                {
                    FPosTableEntry &e = fPosToNodeMap[c];
                    ActPrintLog("Table[%d] : base=%" I64F "d, top=%" I64F "d, slave=%d", c, e.base, e.top, e.index);
                }
                unsigned i=0;
                for(; i<dataParts.ordinality(); i++)
                {
                    Owned<IDelayedFile> dFile = queryThor().queryFileCache().lookup(*this, dataParts.item(i), eexp);
                    fetchFiles.append(*dFile.getClear());
                }
            }
        }
        else
            needsDiskRead = false;

        if (needsDiskRead)
        {
            Owned<IOutputMetaData> meta = createFixedSizeMetaData(KEYLOOKUP_HEADER_SIZE);
            keyLookupAllocator.setown(getRowAllocator(meta));
        }
        else
        {
            Owned<IOutputMetaData> meta = createOutputMetaDataWithChildRow(joinFieldsAllocator, KEYLOOKUP_HEADER_SIZE);
            keyLookupAllocator.setown(getRowAllocator(meta));
        }

        indexInputAllocator.setown(getRowAllocator(helper->queryIndexReadInputRecordSize()));

        ////////////////////

        pool = new CJoinGroupPool(*this);
        if (parallelLookups > 1)
        {
            CPRowStream *seq = new CPRowStream(*this, parallelLookups, freeQSize);
            resultDistStream = seq;
        }
        else
        {
            parallelLookups = 0;
            resultDistStream = new CKeyLocalLookup(*this);
        }
        appendOutputLinked(this);
    }
    virtual void abort()
    {
        CSlaveActivity::abort();
        if (resultDistStream)
            resultDistStream->stop();
        pendingGroupSem.signal();
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        assertex(inputs.ordinality() == 1);

        eos = false;
        input = inputs.item(0);
        startInput(input);
        inputHelper = LINK(input->queryFromActivity()->queryContainer().queryHelper());
        inputStopped = false;
        preserveOrder = ((joinFlags & JFreorderable) == 0);
        preserveGroups = input->isGrouped();
        ActPrintLog("KJ: parallelLookups=%d, freeQSize=%d, preserveGroups=%s, preserveOrder=%s", parallelLookups, freeQSize, preserveGroups?"true":"false", preserveOrder?"true":"false");

        pool->setOrdering(preserveGroups, preserveOrder);
        resultDistStream->setInput(input);

        dataLinkStart();
    }
    virtual void stop()
    {
        if (fetchHandler)
            fetchHandler->stop(true);
        if (!eos)
        {
            eos = true;
            resultDistStream->stop();
        }
        stopInput();
#ifdef TRACE_JOINGROUPS
        ActPrintLog("groupsPendsNoted = %d", groupsPendsNoted);
        ActPrintLog("fetchReadBack = %d", fetchReadBack);
        ActPrintLog("groupPendsEnded = %d", groupPendsEnded);
        ActPrintLog("doneGroupsDeQueued = %d", doneGroupsDeQueued);
        ActPrintLog("wroteToFetchPipe = %d", wroteToFetchPipe);
        ActPrintLog("groupsComplete = %d", groupsComplete);
#endif
        dataLinkStop();
    }
    const void *doDenormTransform(RtlDynamicRowBuilder &target, CJoinGroup &group)
    {
        offset_t fpos;
        unsigned idx = 0;
        unsigned matched = djg->rowsSeen();
        size32_t retSz = 0;
        OwnedConstThorRow lhs;
        lhs.set(group.queryLeft());
        switch (container.getKind())
        {
            case TAKkeyeddenormalize:
            {
                while (idx < matched)
                {
                    const void *rhs = group.queryRow(idx, fpos);
                    size32_t transformedSize = helper->transform(target, lhs, rhs, fpos, idx+1);
                    if (transformedSize)
                    {
                        retSz = transformedSize;
                        currentAdded++;
                        lhs.setown(target.finalizeRowClear(transformedSize));
                        if (currentAdded==keepLimit)
                            break;
                    }
                    idx++;
                }
                if (retSz)
                    return lhs.getClear();
                break;
            }
            case TAKkeyeddenormalizegroup:
            {
                PointerArray rows;
                while (idx < matched && idx < keepLimit)
                {
                    const void *rhs = group.queryRow(idx, fpos);
                    rows.append((void *)rhs);
                    idx++;
                }
                retSz = helper->transform(target, lhs, rows.item(0), rows.ordinality(), (const void **)rows.getArray());
                if (retSz)
                    return target.finalizeRowClear(retSz);
                break;
            }
            default:
                assertex(false);
        }
        return NULL;
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!abortSoon && !eos)
        {
            loop
            {
                if (djg.get())
                {
                    RtlDynamicRowBuilder row(queryRowAllocator(), false);
                    size32_t transformedSize = 0;
                    if (!currentMatched || djg->candidateCount() > atMost)
                    {
                        switch (joinFlags & JFtypemask)
                        {
                        case JFleftouter:
                        case JFleftonly:
                            switch (container.getKind())
                            {
                                case TAKkeyedjoin:
                                {
                                    transformedSize = helper->transform(row.ensureRow(), djg->queryLeft(), defaultRight, (__uint64)0, 0U);
                                    break;
                                }
                                case TAKkeyeddenormalize:
                                {
                                    // return lhs, already finalized
                                    OwnedConstThorRow ret;
                                    ret.set(djg->queryLeft());
                                    currentAdded++;
                                    djg.clear();
                                    dataLinkIncrement();
                                    return ret.getClear();
                                }
                                case TAKkeyeddenormalizegroup:
                                {
                                    transformedSize = helper->transform(row.ensureRow(), djg->queryLeft(), NULL, 0, (const void **)NULL); // no dummrhs (hthor and roxie don't pass)
                                    break;
                                }
                            }
                            if (transformedSize)
                                currentAdded++;
                        }
                        djg.clear();
                    }
                    else if (!(joinFlags & JFexclude))
                    {
                        switch (container.getKind())
                        {
                            case TAKkeyedjoin:
                            {
                                if (currentMatchIdx < currentMatched)
                                {
                                    offset_t fpos;
                                    const void *rhs = djg->queryRow(currentMatchIdx, fpos);
                                    row.ensureRow();
                                    transformedSize = helper->transform(row, djg->queryLeft(), rhs, fpos, currentMatchIdx+1);
                                    if (transformedSize)
                                    {
                                        currentAdded++;
                                        if (currentAdded==keepLimit)
                                            djg.clear();
                                    }
                                    currentMatchIdx++;
                                }
                                else
                                    djg.clear();
                                break;
                            }
                            case TAKkeyeddenormalize:
                            case TAKkeyeddenormalizegroup:
                            {
                                OwnedConstThorRow ret = doDenormTransform(row, *djg);
                                djg.clear();
                                if (ret)
                                {
                                    dataLinkIncrement();
                                    return ret.getClear();
                                }
                                transformedSize = 0;
                                break;
                            }
                            default:
                                assertex(false);
                        }
                    }
                    else
                        djg.clear();
                    if (transformedSize)
                    {
                        dataLinkIncrement();
                        return row.finalizeRowClear(transformedSize);
                    }
                }
                if (doneJG.get())
                {
                    if (preserveGroups)
                    {
                        currentJoinGroupSize += currentAdded;
                        CJoinGroup *next = doneJG->next;
                        doneJG->next = NULL;
                        doneJG.setown(next);
                        if (next) // if NULL == end-of-group marker
                        {
                            next->prev = NULL;
                            OwnedConstThorRow abortRow;
                            if (abortLimitAction(doneJG, abortRow)) // discard lhs row (yes, even if it is an outer join)
                            {
                                // don't clear doneJG, in preserveGroups case, it will advance to next, next time around.
                                if (!preserveGroups)
                                    doneJG.clear();
                                if (abortRow.get())
                                {
                                    dataLinkIncrement();
                                    return abortRow.getClear();
                                }
                                continue; // throw away this match
                            }
                            djg.set(next);
                        }
                        else
                        {
                            if (currentJoinGroupSize) // some recs, return eog.
                                break;
                            continue;
                        }
                    }
                    else
                        djg.setown(doneJG.getClear());
                    currentAdded = 0;
                    currentMatchIdx = 0;
                    currentMatched = djg->rowsSeen();
                }
                else
                {
                    { 
                        CriticalBlock b(onCompleteCrit);
                        doneJG.setown(doneGroups.dequeue());
                    }
                    if (doneJG.get())
                    {
#ifdef TRACE_JOINGROUPS
                        doneGroupsDeQueued++;
#endif
                        OwnedConstThorRow abortRow;
                        if (!abortLimitAction(doneJG, abortRow))
                        {
                            djg.set(doneJG);
                            currentMatched = djg->rowsSeen();
                        }
                        currentAdded = 0;
                        currentMatchIdx = 0;
                        if (preserveGroups)
                            currentJoinGroupSize = 0;
                        else
                            doneJG.clear();
                        if (abortRow.get())
                        {
                            dataLinkIncrement();
                            return abortRow.getClear();
                        }
                    }
                    else
                    {
                        OwnedConstThorRow resultRow = resultDistStream->nextRow();
                        if (!resultRow)
                        {
                            if (doneGroups.ordinality())
                                continue;
                            else if (needsDiskRead)
                            {
                                fetchHandler->stop(false);
                                if (waitPendingGroups())
                                    continue;
                            }
                            eos = true;
                            resultDistStream->stop();
                            break;
                        }

                        const LookupRowResult *lookupRowResult = (const LookupRowResult *)resultRow.get();
                        CJoinGroup *jg = lookupRowResult->jg;
                        if (lookupRowResult->eog)
                        {
                            jg->noteCandidates((unsigned)lookupRowResult->fpos); // fpos holds candidates for end of group
                            jg->noteEndCandidate(); // any onFail transform will be done when dequeued
                            if (!onFailTransform) // unless going to transform later, check and abort now if necessary.
                                checkAbortLimit(jg);
                        }
                        else
                        {
                            if (jg->rowsSeen() <= atMost)
                            {
                                if (needsDiskRead)
                                {
                                    jg->notePending();
#ifdef TRACE_JOINGROUPS
                                    wroteToFetchPipe++;
#endif
                                    fetchHandler->addRow(lookupRowResult->fpos, jg);
                                }
                                else
                                {
                                    const void *resultRowPtr = (const void *)(lookupRowResult+1);
                                    const void *rhs = *((const void **)resultRowPtr);
                                    LinkThorRow(rhs);
                                    jg->addRightMatch(rhs, lookupRowResult->fpos);
                                }
                            }
                            jg->noteEnd(0);
                        }
                    }
                }
            }
        }
        return NULL;
    }

    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }

    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canStall = true;
        info.unknownRowsOutput = true;
    }

    void serializeStats(MemoryBuffer &mb)
    {
        CSlaveActivity::serializeStats(mb);
        ForEachItemIn(s, _statsArr)
            mb.append(_statsArr.item(s));
    }

friend class CKeyedFetchHandler;
friend class CKeyedFetchHandler::CKeyedFetchRequestProcessor;
friend class CKeyedFetchHandler::CKeyedFetchResultProcessor;
friend class CKeyLocalLookup;
friend class CPRowStream;
friend class CPRowStream::CKeyLookupPoolMember;
};


CActivityBase *createKeyedJoinSlave(CGraphElementBase *container) 
{ 
    return new CKeyedJoinSlave(container); 
}

