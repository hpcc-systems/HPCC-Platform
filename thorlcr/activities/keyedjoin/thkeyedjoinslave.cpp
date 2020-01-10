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
#include "jflz.hpp"
#include "jqueue.tpp"
#include "jset.hpp"
#include "jsort.hpp"
#include "jsorta.hpp"

#include "thorcommon.ipp"

#include "dadfs.hpp"

#include "jhtree.hpp"

#include "rtlcommon.hpp"

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
#include "thkeyedjoincommon.hpp"
#include "thkeyedjoinslave.ipp"

#include <vector>
#include <atomic>
#include <deque>
#include <algorithm>
#include <typeinfo>
#include <type_traits>

static const unsigned defaultKeyLookupQueuedBatchSize = 1000;
static const unsigned defaultKeyLookupFetchQueuedBatchSize = 1000;
static const unsigned defaultMaxKeyLookupThreads = 10;
static const unsigned defaultMaxFetchThreads = 10;
static const unsigned defaultKeyLookupMaxQueued = 10000;
static const unsigned defaultKeyLookupMaxDone = 10000;
static const unsigned defaultMinKeyLookupJoinGroupMB = 50;
static const unsigned defaultKeyLookupMaxLocalHandlers = 10;
static const unsigned defaultKeyLookupMaxHandlersPerRemoteSlave = 2;
static const unsigned defaultKeyLookupMaxFetchHandlers = 10;
static const unsigned defaultKeyLookupMaxLocalFetchHandlers = 10;
static const unsigned defaultKeyLookupProcessBatchLimit = 1000;
static const unsigned defaultFetchLookupProcessBatchLimit = 10000;


/*
 * This KeyedJoin implementation is comprised of:
 * 1) A LHS read ahead thread (see CReadAheadThread)
 *  - LHS rows are read, index fields are extracted, a pending CJoinGroup is created and the row+JG is queued.
 *  - The total queued is limited (will block). See keyLookupMaxQueued.
 *
 * 2) A lookup handler per key handler (base is CLookupHandler)
 *  - These run concurrently, but max running in parallel are limited to lookupThreadLimiter and fetchThreadLimiter
 *  - Lookup handlers are either local (reading index parts directly), or remote, sending requests to be processed remotely.
 *  - Local vs Remote is determined automatically by the locality of the part, but can be overriden by various options, e.g. "forceRemoteLookup"
 *  NB: Remote processing is currently handled by a service in the Thor slaves processes, in future it is to be moved to dafilesrv.
 *  - Look up handlers process requests in chunks, i.e. when a threshold is reached a handler is started and processing begins.
 *  - All RHS matched results are added to the CJoinGroup being processed by the lookup handlers in parallel.
 *  - Any abort/atmost limit is also recorded as a flag.
 *  - If a Full Keyed Join is involved, lookup handlers queue further request to fetch handlers.
 *  - All pending CJoinGroup events are tracked by CJoinGroup::pending, such that when it reaches 0, the CJoinGroup is done.
 *  - The total size of each CJoinGroup is tracked (see totalRhsSz)
 *
 * Ordering:
 *  - if preserveOrdering is on (it is by default), streamed rhs match rows are added to independent buckets in the CJoinGroup,
 *    in order to preserve their global order.
 *  - if a full keyed join is involved, the keyed lookup only returns the fpos, and a placeholder in the row bucket is created
 *    and filled upon completion of the fetch cycle.
 *  NB: Strictly speaking the RHS match order may not be semantically important, but a deterministic order is.
 *
 *  - When preserving order, the LHS order must remain the same.
 *    This is achieved by only dequeuing the head of the pending group list (which is ordered) if complete, and any others that
 *    follow it if they are also complete. IOW a completing group that is not at the head will not move to the done queue.
 *
 *  - When CJoinGroup's are complete, they are moved from the pending queue to the done queue (see doneJoinGroupList).
 *  - The # of queued done groups is limited (will block). See keyLookupMaxDone.
 *  - The done group queue (doneJoinGroupList) is read by the main KeyedJoin's nextRow() implementation.
 *
 * Memory usage & Spilling:
 *
 * 1) Each pending CJoinGroup contains a lhs row and up to 'atmost' rhs rows.
 * 2) There is a capped limit of pending in-flight CJoinGroup's
 * 3) There is a capped limit of done (yet unread) completed CJoinGroup's
 *
 * Since the # matches are unknown when the requests are made, and the rhs rows can be variable length,
 * the amount of memory in use is very variable.
 * If ATMOST is at the default value of 10K and there are lots of parts and up to the limit of pending
 * groups, a lot of memory can be consumed.
 *
 * If memory is high roxiemem will attempt to callback to KJ to free some memory.
 * Strategy:
 * 1) The current (approx.) memory footprint of KJ is calculated by scanning the pending and done groups
 * 2) If below a threshold (see: keyLookupMinQueuedBytes), then no further action is taken
 * 3) If above:
 *  - Spill join groups until 'spillAmount' reached.
 *  - Increase spillAmount for next round (double it).
 *  - Decrease the pending and done queue limits, so that KJ starts processing less.
 *  - Repeat this process each time this CB is called until down to minimum targets. Minimum memory: keyLookupMinQueuedBytes or minimum queued limits (5)
 *
 * Notes:
 *     Prefer spilling completed groups, as they are not contended by threads adding rhs rows. NB: onCompleteCrit crit prevents queues being manipulated.
 *     Walk the tail of the 'done' groups 1st (those are JG's ready to be read by downstream activity).
 *     Walk pending complete groups
 *     Walk pending incomplete groups. NB: incomplete groups are still being built, and may allocate. Need to avoid deadlock (see activeAddRightCount)
 *     Each spilt group creates a IRowStreamWithFpos stored in the CJoinGroup, which is used as a stream when reading back.
 *
 * Complications:
 *     When preserving order during spilling, complete groups are implicitly already ordered, and a simple row stream can be written to disk.
 *     NB: uses a compressed writer
 *     NB: also true if not preserving order.
 *
 *     However if have to consider incomplete groups, then rhs rows may still arrive and their order must also be tracked.
 *     Spilling a partial group 1st writes a header with {size, skip} markers for each bucket, so that the reader will know the order
 *     and where to start for each bucket.
 *     A new SpillMarker marker is written at the end of each bucket (NB: a bucket per lookup part).
 *     A incomplete CJoinGroup that has spilt, may spill again, or may complete meaning some is on disk and some in memory.
 *     The last marker written is tracked in the spilling object, so that it can be updated if it spills again, with the size and skip amount to next chunk.
 *     NB: a pointer to the writer object (IRowStreamWithFpos) is kept in the CJoinGroup.
 *
 * Typical scenario (e.g. subgraph with:  Memory-Hungry-KJ + SORT
 *  - roxiemem freeBufferedRows callback is called. KJ determines it is using above the minimum amount of memory.
 *  - some spilling takes place.
 *  - There is continued memory pressure on KJ since Sort has higher priority and KJ keeps spilling.
 *  - Eventually either the lower min. limit for KJ is reached or the min. number of pending and done groups in play is reached.
 *
 */


class CJoinGroup;


enum AllocatorTypes { AT_Transform=1, AT_LookupWithJG, AT_JoinFields, AT_FetchRequest, AT_FetchResponse, AT_JoinGroup, AT_JoinGroupRhsRows, AT_FetchDisk, AT_LookupResponse };


struct Row
{
    const void *rhs;
    offset_t fpos;
};
struct RowArray
{
    Row *rows;
    rowidx_t maxRows;
    rowidx_t numRows;
};

interface IRowStreamWithFpos : extends IInterface
{
    virtual void open() = 0;
    virtual const void *getRow(offset_t &fpos) = 0;
    virtual void add(CJoinGroup *jg, bool first) = 0;
};

interface IJoinProcessor
{
    virtual void onComplete(CJoinGroup * jg) = 0;
    virtual unsigned addRowEntry(unsigned partNo, const void *rhs, offset_t fpos, RowArray *&rowArrays, unsigned &numRowArrays) = 0;
    virtual IRowStreamWithFpos *getJoinGroupStream(CJoinGroup *jg) = 0;
};

class CJoinGroup : public CSimpleInterfaceOf<IInterface>
{
protected:
    OwnedConstThorRow leftRow;
    mutable CriticalSection crit;
    std::atomic<unsigned> pending{0};
    std::atomic<unsigned> candidates{0};
    IJoinProcessor *join = nullptr;
    std::atomic<size32_t> totalRhsSz{0};
    unsigned numRowArrays = 0;
    RowArray *rowArrays = nullptr;
    IRowStreamWithFpos *rowStream = nullptr;
    GroupFlags groupFlags = gf_null;
    static const GroupFlags GroupFlagLimitMask = (GroupFlags)0x03;
    std::atomic<int> activeAddRightCount{0}; // Protect addRightMatch* from spillIncomplete contention

public:
    struct JoinGroupRhsState
    {
        JoinGroupRhsState() { clear(); }
        inline void clear() { arr = nullptr; pos = 0; }
        RowArray *arr;
        rowidx_t pos;
    };
    struct SpillMarker
    {
        size32_t skip = 0;
        size32_t size = 0;
    };
    CJoinGroup *prev = nullptr;  // Doubly-linked list to allow us to keep track of ones that are still in use
    CJoinGroup *next = nullptr;

protected:
    class CJoinGroupRowWriter : public CSimpleInterfaceOf<IInterface>, implements IRowSerializerTarget
    {
        Owned<IFileIO> iFileIO;
        Owned<IFileIOStream> iFileIOStream;
        Linked<IOutputRowSerializer> serializer;
        unsigned nesting = 0;
        MemoryBuffer nested;

    // IRowSerializerTarget impl.
        virtual void put(size32_t len, const void * ptr) override
        {
            if (nesting)
                nested.append(len, ptr);
            else
                iFileIOStream->write(len, ptr);
        }
        virtual size32_t beginNested(size32_t count) override
        {
            nesting++;
            unsigned pos = nested.length();
            nested.append((size32_t)0);
            return pos;
        }
        virtual void endNested(size32_t sizePos) override
        {
            size32_t sz = nested.length()-(sizePos + sizeof(size32_t));
            nested.writeDirect(sizePos, sizeof(sz), &sz);
            nesting--;
            if (!nesting)
            {
                iFileIOStream->write(nested.length(), nested.toByteArray());
                nested.clear();
            }
        }
    public:
        CJoinGroupRowWriter(IFile *iFile, IOutputRowSerializer *_serializer, bool append, bool compress) : serializer(_serializer)
        {
            if (compress)
            {
                iFileIO.setown(createCompressedFileWriter(iFile, 0, append, false, nullptr, COMPRESS_METHOD_LZ4));
                iFileIOStream.setown(createIOStream(iFileIO));
            }
            else
            {
                iFileIO.setown(iFile->open(append ? IFOreadwrite : IFOcreate));
                iFileIOStream.setown(createBufferedIOStream(iFileIO));
            }
            if (append)
                iFileIOStream->seek(0, IFSend);
        }
        ~CJoinGroupRowWriter()
        {
            iFileIOStream.clear(); // flushes
        }
        void write(const void *row, offset_t fpos)
        {
            serializer->serialize(*this, (const byte *)row);
            iFileIOStream->write(sizeof(fpos), &fpos);
        }
        offset_t queryPosition() const
        {
            return iFileIOStream->tell();
        }
        void addMarker()
        {
            CJoinGroup::SpillMarker marker;
            iFileIOStream->write(sizeof(marker), &marker);
        }
        void updateMarker(offset_t pos, const CJoinGroup::SpillMarker &marker)
        {
            iFileIOStream->flush();
            iFileIO->write(pos, sizeof(marker), &marker);
        }
        void writeOffsetTable(unsigned n)
        {
            const std::vector<CJoinGroup::SpillMarker> markers(n);
            iFileIOStream->write(n * sizeof(CJoinGroup::SpillMarker), &markers[0]);
        }
    };

    void freeRowArrays() // use if rows themselves have already been freed.
    {
        if (rowArrays)
        {
            RowArray *cur = rowArrays;
            do
            {
                if (cur->rows)
                    ReleaseRoxieRow(cur->rows);
                ++cur;
            } while (--numRowArrays);
            ReleaseRoxieRow(rowArrays);
            rowArrays = nullptr;
        }
    }
    void freeRows()
    {
        if (rowArrays)
        {
            RowArray *cur = rowArrays;
            do
            {
                if (cur->rows)
                {
                    const Row *row = cur->rows;
                    unsigned numRows = cur->numRows;
                    while (numRows--)
                    {
                        if (row->rhs)
                            ReleaseRoxieRow(row->rhs);
                        ++row;
                    }
                    ReleaseRoxieRow(cur->rows);
                }
                ++cur;
            } while (--numRowArrays);
            ReleaseRoxieRow(rowArrays);
            rowArrays = nullptr;
            totalRhsSz = 0;
        }
    }
    void freeRowsAndStream()
    {
        ::Release(rowStream);
        rowStream = nullptr;
        freeRows();
    }

    inline const void *_getNextRhsClear(offset_t &fpos, JoinGroupRhsState &rhsState)
    {
        while (rhsState.arr != (rowArrays+numRowArrays)) // end of array marker
        {
            if (rhsState.arr->rows)
            {
                while (rhsState.pos < rhsState.arr->numRows)
                {
                    Row &row = rhsState.arr->rows[rhsState.pos++];
                    if (row.rhs)
                    {
                        fpos = row.fpos;
                        const void *ret = row.rhs;
                        row.rhs = nullptr;
                        return ret;
                    }
                }
            }
            rhsState.arr++;
            rhsState.pos = 0;
        }
        fpos = 0;
        return nullptr;
    }
    inline const void *_getFirstRhsClear(offset_t &fpos, JoinGroupRhsState &rhsState)
    {
        if (!rowArrays)
        {
            fpos = 0;
            return nullptr;
        }
        rhsState.arr = &rowArrays[0];
        rhsState.pos = 0;
        return _getNextRhsClear(fpos, rhsState);
    }
    void addActiveRightAndBlock()
    {
        activeAddRightCount++; // NB: could happen after spillIncomplete() has set to -INT_MAX
        while (true)
        {
            crit.enter();
            /*
             * If managed to enter crit and spillIncomplete() is active on this group
             * let go of the crit and retry, to allow spillIncomplete() to take the crit
             */
            if (activeAddRightCount > 0)
                break; // spillIncomplete() cannot now start
            /*
             * If activeAddRightCount<0, then spillIncomplete() has started, loop and relock crit, to give spillIncomplete() a chance to grab crit
             */
            crit.leave();
        }
    }
    void releaseActiveRightAndUnlock()
    {
        --activeAddRightCount;
        crit.leave();
    }
    class CSpillSafeBlock
    {
        CJoinGroup *jg;
    public:
        CSpillSafeBlock(CJoinGroup *_jg) : jg(_jg)
        {
            jg->addActiveRightAndBlock();
        }
        ~CSpillSafeBlock()
        {
            jg->releaseActiveRightAndUnlock();
        }
    };
public:
    CJoinGroup(const void *_leftRow, IJoinProcessor *_join)
        : join(_join)
    {
        leftRow.set(_leftRow);
    }
    ~CJoinGroup()
    {
        freeRowsAndStream();
    }
    void *operator new(size_t size, roxiemem::IRowManager *rowManager, activity_id activityId)
    {
        return rowManager->allocate(size, activityId); // NB: can't encode AT_JoinGroup here with createCompoundActSeqId, because row manager limits act id to 2^20
    }
    void operator delete(void *ptr, roxiemem::IRowManager *rowManager, activity_id activityId)
    {
        ReleaseRoxieRow(ptr);
    }
    void operator delete(void *ptr)
    {
        ReleaseRoxieRow(ptr);
    }
    inline unsigned incCandidates()
    {
        return candidates.fetch_add(1)+1;
    }
    inline unsigned addCandidates(unsigned n)
    {
        return candidates.fetch_add(n)+n;
    }
    inline bool hasAbortLimitBeenHit() const
    {
        return 0 != (groupFlags & gf_limitabort);
    }
    inline void setAbortLimitHit()
    {
        CriticalBlock b(crit);
        addFlag(gf_limitabort);
        freeRowsAndStream();
    }
    inline bool hasAtMostLimitBeenHit() const
    {
        return 0 != (groupFlags & gf_limitatmost);
    }
    inline void setAtMostLimitHit()
    {
        CriticalBlock b(crit);
        addFlag(gf_limitatmost);
        freeRowsAndStream();
    }
    inline const void *queryLeft() const
    {
        return leftRow;
    }
    inline const void *getFirstRhsClear(offset_t &fpos, JoinGroupRhsState &rhsState)
    {
        dbgassertex(complete());
        CriticalBlock b(crit); // NB: only needed because of potential contention with spilling.
        if (rowStream)
        {
            rowStream->open();
            const void *row = rowStream->getRow(fpos);
            if (row)
                return row;
            rowStream->Release();
            rowStream = nullptr;
        }
        return _getFirstRhsClear(fpos, rhsState);
    }
    inline const void *getNextRhsClear(offset_t &fpos, JoinGroupRhsState &rhsState)
    {
        CriticalBlock b(crit); // NB: only needed because of potential contention with spilling.
        if (rowStream)
        {
            const void *row = rowStream->getRow(fpos);
            if (row)
                return row;
            rowStream->Release();
            rowStream = nullptr;
            return _getFirstRhsClear(fpos, rhsState);
        }
        return _getNextRhsClear(fpos, rhsState);
    }
    inline bool complete() const { return 0 == pending; }
    inline void incPending()
    {
        pending++;
    }
    inline void decPending()
    {
        if (1 == pending.fetch_sub(1))
            join->onComplete(this);
    }
    inline unsigned addRightMatchPending(unsigned partNo, offset_t fpos)
    {
        CSpillSafeBlock cb(this);
        if (hasFlag(GroupFlagLimitMask)) // NB: flag can be triggered asynchronously, via setAbortLimitHit() / setAtMostLimitHit()
            return NotFound;
        return join->addRowEntry(partNo, nullptr, fpos, rowArrays, numRowArrays);
    }
    inline void addRightMatchCompletePending(unsigned partNo, unsigned sequence, size32_t rightSz, const void *right)
    {
        CriticalBlock b(crit); // NB: only needed because of potential contention with spilling. NB2: this method doesn't allocate
        if (hasFlag(GroupFlagLimitMask))
        {
            ReleaseRoxieRow(right);
            return;
        }
        RowArray &rowArray = rowArrays[partNo];
        rowArray.rows[sequence].rhs = right;
        totalRhsSz += rightSz;
    }
    inline void addRightMatch(unsigned partNo, size32_t rightSz, const void *right, offset_t fpos)
    {
        CSpillSafeBlock cb(this);
        if (hasFlag(GroupFlagLimitMask)) // NB: flag can be triggered asynchronously, via setAbortLimitHit() / setAtMostLimitHit()
        {
            ReleaseRoxieRow(right);
            return;
        }
        join->addRowEntry(partNo, right, fpos, rowArrays, numRowArrays);
        totalRhsSz += rightSz;
    }
    inline size32_t getRhsSize() const { return totalRhsSz; }
    inline void addFlag(GroupFlags flag)
    {
        groupFlags = static_cast<GroupFlags>(groupFlags | flag);
    }
    inline void removeFlag(GroupFlags flag)
    {
        groupFlags = static_cast<GroupFlags>(((unsigned)groupFlags) & ~((unsigned)flag));
    }
    inline bool hasFlag(GroupFlags flag) const
    {
        return 0 != (groupFlags & flag);
    }
    inline bool isEmpty() const
    {
        return (0 == totalRhsSz) && (nullptr == rowStream);
    }
    void spill()
    {
        if (rowStream) // grief, this group has spilt at least once before
            rowStream->add(this, false);
        else
            rowStream = join->getJoinGroupStream(this);
    }
    size32_t spillCompleteTS(size32_t threshold)
    {
        CriticalBlock b(crit);
        size32_t sz = getRhsSize();
        if (sz < threshold) // it's possible it's been consumed or partially consumed by time got crit
            return 0;
        spill();
        freeRowArrays(); // rows will have already been release, release rowArrays as not needed anymore
        totalRhsSz = 0;
        return sz;
    }
    size32_t spillIncomplete(size32_t threshold)
    {
        int expectedState=0;
        if (!activeAddRightCount.compare_exchange_strong(expectedState, -INT_MAX))
        {
            // If any thread is in addRightMatch* for this JG, then skip this group to avoid potential deadlock if addRightMatch is allocating
            return 0;
        }

        size32_t retSz = 0;
        CriticalBlock b(crit);
        if (totalRhsSz >= threshold) // it's possible it's been consumed or partially consumed by time got crit
        {
            spill();
            retSz = totalRhsSz;
            totalRhsSz = 0;
        }
        /*
         * NB: adding because it was 0 at exchange to -INT_MAX (effectively subtracting INT_MAX),
         * however addRightMatch* calls may be blocked, waiting for this function to finish, and have incremented activeAddRightCount,
         * so add back INT_MAX
         */
        activeAddRightCount += INT_MAX;
        return retSz;
    }
    // The simple case, where know complete (and therefore ordered), or don't care about order
    void write(IFile *iFile, IOutputRowSerializer *serializer, bool append)
    {
        Owned<CJoinGroupRowWriter> rowWriter = new CJoinGroupRowWriter(iFile, serializer, append, true);
        CJoinGroup::JoinGroupRhsState rhsState;
        offset_t fpos;
        const void *row = _getFirstRhsClear(fpos, rhsState);
        while (row)
        {
            rowWriter->write(row, fpos);
            ReleaseRoxieRow(row);
            row = _getNextRhsClear(fpos, rhsState);
        }
    }
    // non trivial case where writing incomplete and need to preserve order
    void write(IFile *iFile, IOutputRowSerializer *serializer, bool append, std::vector<offset_t> *writeMarkerOffsets)
    {
        // NB: called inside crit, indirectly via spillComplete() or spillIncomplete()
        Owned<CJoinGroupRowWriter> rowWriter = new CJoinGroupRowWriter(iFile, serializer, append, false);

        dbgassertex(rowArrays);

        if (!rowStream) // never spilt, write offset header
        {
            /*
             * A header of 1st offsets to chunks is laid down 1st.
             * The marker is updated when a chunk is written and the last marker for each stream
             * is tracked, so it can be updated when the next chunk for the same stream is written.
             *
             */
            rowWriter->writeOffsetTable(numRowArrays);
            for (unsigned a=0; a<numRowArrays; a++) // 0 if preserveOrder==false
                (*writeMarkerOffsets)[a] = a * sizeof(CJoinGroup::SpillMarker);
        }

        CJoinGroup::JoinGroupRhsState rhsState;
        rhsState.arr = &rowArrays[0];
        rhsState.pos = 0;

        CJoinGroup::SpillMarker marker;
        offset_t lastBucketMarkerOffset = (offset_t)-1;
        while (rhsState.arr != (rowArrays+numRowArrays)) // end of array marker
        {
            if (rhsState.arr->rows)
            {
                unsigned which = rhsState.arr-rowArrays;

                if (rhsState.arr->numRows)
                {
                    lastBucketMarkerOffset = (*writeMarkerOffsets)[which];

                    offset_t startPosition = rowWriter->queryPosition();
                    /* NB: if startPosition immediately follows the lastBucketMarkerOffset,
                     * then could remove marker and continue same chunk, but would have to update
                     * previous marker and we don't have it's position (prob. not worth recoding for this).
                     */

                    while (rhsState.pos < rhsState.arr->numRows)
                    {
                        Row &row = rhsState.arr->rows[rhsState.pos++];
                        if (row.rhs)
                        {
                            rowWriter->write(row.rhs, row.fpos);
                            ReleaseRoxieRow(row.rhs);
                            row.rhs = nullptr;
                        }
                    }
                    rhsState.arr->numRows = 0;

                    offset_t endPosition = rowWriter->queryPosition();
                    dbgassertex(startPosition != endPosition);

                    marker.skip = startPosition-(lastBucketMarkerOffset+sizeof(CJoinGroup::SpillMarker));
                    marker.size = endPosition - startPosition;
                    rowWriter->updateMarker(lastBucketMarkerOffset, marker);

                    lastBucketMarkerOffset = endPosition;
                    (*writeMarkerOffsets)[which] = lastBucketMarkerOffset;
                    rowWriter->addMarker();
                }
            }
            rhsState.arr++;
            rhsState.pos = 0;
        }
        dbgassertex((offset_t)-1 != lastBucketMarkerOffset); // sanity check that something written
    }
};

class CJoinGroupList
{
    CJoinGroup *head = nullptr, *tail = nullptr;
    unsigned count = 0;

    void removeAll()
    {
        while (head)
        {
            CJoinGroup *next = head->next;
            head->Release();
            head = next;
        }
    }
public:
    CJoinGroupList() { }
    ~CJoinGroupList()
    {
        removeAll();
    }
    void clear()
    {
        removeAll();
        head = tail = nullptr;
    }
    inline unsigned queryCount() const { return count; }
    inline CJoinGroup *queryHead() const { return head; }
    inline CJoinGroup *queryTail() const { return tail; }
    CJoinGroup *removeHead()
    {
        if (!head)
            return nullptr;
        CJoinGroup *ret = head;
        head = head->next;
        ret->next = nullptr;
        if (head)
        {
            dbgassertex(head->prev == ret);
            head->prev = nullptr;
        }
        else
            tail = nullptr;
        --count;
        return ret;
    }
    CJoinGroup *remove(CJoinGroup *joinGroup)
    {
        CJoinGroup *prev = joinGroup->prev;
        CJoinGroup *next = joinGroup->next;
        if (joinGroup == tail) // implying next=null also
            tail = prev;
        else
            next->prev = prev;
        if (joinGroup == head) // implying prev=null also
            head = next;
        else
            prev->next = next;
        joinGroup->prev = nullptr;
        joinGroup->next = nullptr;
        --count;
        return joinGroup; // now detached
    }
    void addToTail(CJoinGroup *joinGroup)
    {
        dbgassertex(nullptr == joinGroup->next);
        dbgassertex(nullptr == joinGroup->prev);
        if (!head)
        {
            head = tail = joinGroup;
        }
        else
        {
            tail->next = joinGroup;
            joinGroup->prev = tail;
            tail = joinGroup;
        }
        ++count;
    }
};

enum AdditionStats { AS_Seeks, AS_Scans, AS_Accepted, AS_PostFiltered, AS_PreFiltered,  AS_DiskSeeks, AS_DiskAccepted, AS_DiskRejected };

struct PartIO
{
    PartIO() {}
    ~PartIO()
    {
        ::Release(prefetcher);
        ::Release(translator);
        ::Release(iFileIO);
        ::Release(stream);
    }
    PartIO(const PartIO &other)
    {
        iFileIO = LINK(other.iFileIO);
        translator = LINK(other.translator);
        prefetcher = LINK(other.prefetcher);
        stream = LINK(other.stream);
    }
    PartIO(PartIO &&other)
    {
        iFileIO = other.iFileIO;
        translator = other.translator;
        prefetcher = other.prefetcher;
        stream = other.stream;
        other.iFileIO = nullptr;
        other.translator = nullptr;
        other.prefetcher = nullptr;
        other.stream = nullptr;
    }
    PartIO& operator = (const PartIO &other)
    {
        ::Release(iFileIO);
        ::Release(translator);
        ::Release(prefetcher);
        ::Release(stream);
        iFileIO = LINK(other.iFileIO);
        translator = LINK(other.translator);
        prefetcher = LINK(other.prefetcher);
        stream = LINK(other.stream);
        return *this;
    }
    IFileIO *iFileIO = nullptr;
    ITranslator *translator = nullptr;
    ISourceRowPrefetcher *prefetcher = nullptr;
    ISerialStream *stream = nullptr;
};

class CKeyedJoinSlave : public CSlaveActivity, implements IJoinProcessor, implements roxiemem::IBufferedRowCallback
{
    typedef CSlaveActivity PARENT;

    // CLimiter is used to track a accumulated count of items, e.g. rows or threads
    class CLimiter
    {
        unsigned count = 0;         // nominal count of active items (e.g. rows or threads)
        unsigned max = 0;           // if count exceeds max+leeway is reached blocking will occur, if count falls below max blocks will be released
        unsigned leewayPercent = 0; // leeway allows a gap to occur between time blocking starts and time blocking is released, to avoid thrashing.
        unsigned blocked = 0;       // number of callers blocked due to exceeding max+leewayPercent(%).
        unsigned leeway = 0;
        Semaphore sem;
        CriticalSection crit;
        bool enabled = true;

        void unblock()
        {
            sem.signal(blocked);
            blocked = 0;
        }
        void _set(unsigned _max, unsigned _leewayPercent=0)
        {
            max = _max;
            leewayPercent = _leewayPercent;
            leeway = 0;
            if (leewayPercent)
            {
                leeway = max * leewayPercent / 100;
                if (0 == leeway)
                    leeway = 1;
            }
        }
    public:
        CLimiter()
        {
        }
        CLimiter(unsigned _max, unsigned _leewayPercent=0)
        {
            _set(_max, _leewayPercent);
        }
        unsigned queryMax() const { return max; }
        void set(unsigned _max, unsigned _leewayPercent=0)
        {
            CriticalBlock b(crit);
            _set(_max, _leewayPercent);
            if (blocked && (count < max))
                unblock();
        }
        bool preIncNonBlocking(bool markBlocked)
        {
            {
                CriticalBlock b(crit);
                if (!enabled)
                    return false;
                if (count++ < max+leeway)
                    return false;
                if (markBlocked)
                    ++blocked;
            }
            return true;
        }
        bool incNonBlocking()
        {
            {
                CriticalBlock b(crit);
                if (!enabled)
                    return false;
                if (count < max+leeway)
                {
                    ++count;
                    return false;
                }
                ++blocked;
            }
            return true;
        }
        void inc()
        {
            while (incNonBlocking())
                sem.wait();
        }
        void dec()
        {
            CriticalBlock b(crit);
            if (!enabled)
                return;
            --count;
            if (blocked && (count < max))
                unblock();
        }
        void block()
        {
            sem.wait();
        }
        void disable()
        {
            CriticalBlock b(crit);
            enabled = false;
            if (blocked)
                unblock();
        }
        void reset()
        {
            CriticalBlock b(crit);
            enabled = true;
            if (blocked)
                unblock();
            count = 0;
        }
        bool reduceLimit(unsigned newMax, unsigned largePercentage, unsigned reducePercentage, unsigned minimumReduce, unsigned minimumLimit)
        {
            if (newMax >= (max * largePercentage / 100))
            {
                // reduce by 'reducePercentage'% or 'minimumReduce' whichever is larger
                unsigned a = max * reducePercentage / 100;
                if (a < minimumReduce)
                    a = minimumReduce;
                if (max < a+minimumLimit)
                    newMax = minimumLimit;
                else
                    newMax = max - a;
            }
            else if (newMax < minimumLimit)
                newMax = minimumLimit;
            if (newMax == minimumLimit)
            {
                set(newMax, 0);
                return true;
            }
            else
            {
                set(newMax, 10);
                return false;
            }
        };
    };
    /*
     * There will 1 handler per part up to max per target, then parts will be added and dealt by existing handlers.
     * NB: No more than 'maxKeyLookupThreads' handler threads will run concurrently.
     */
    class CLookupHandler : public CInterfaceOf<IInterface>, implements IThreaded
    {
    protected:
        CKeyedJoinSlave &activity;
        IThorRowInterfaces *rowIf;
        IHThorKeyedJoinArg *helper = nullptr;
        std::vector<CThorExpandingRowArray *> queues;
        unsigned totalQueued = 0;
        CriticalSection queueCrit, batchCrit;
        CThreaded threaded;
        std::atomic<bool> running{false};
        std::atomic<bool> stopping{false};
        enum ThreadStates { ts_initial, ts_starting, ts_running, ts_stopping };
        ThreadStates state = ts_initial;
        std::vector<CThorExpandingRowArray *> batchArrays;
        std::vector<unsigned> parts; // part numbers this handler is dealing with
        std::vector<unsigned> partNumMap; // map of part # -> parts index
        unsigned myParts = 0;
        unsigned nextQueue = 0;
        bool stopped = false;
        unsigned lookupQueuedBatchSize = 1000;
        rowcount_t total = 0;
        CLimiter *limiter = nullptr;
        IArrayOf<IPartDescriptor> *allParts = nullptr; // only used for tracing purposes, set by key or fetch derived handlers
        unsigned batchProcessLimit = 0xffffffff;

        inline MemoryBuffer &doUncompress(MemoryBuffer &tgt, MemoryBuffer &src)
        {
            if (activity.messageCompression)
            {
                fastLZDecompressToBuffer(tgt, src);
                return tgt;
            }
            else
                return src;
        }
    public:
        CLookupHandler(CKeyedJoinSlave &_activity, IThorRowInterfaces *_rowIf, unsigned _batchProcessLimit) : threaded("CLookupHandler", this),
            activity(_activity), rowIf(_rowIf), batchProcessLimit(_batchProcessLimit)
        {
            helper = activity.helper;
        }
        ~CLookupHandler()
        {
            for (auto &a : batchArrays)
                a->Release();
            for (auto &a : queues)
                a->Release();
        }
        void setBatchSize(unsigned _batchSize)
        {
            lookupQueuedBatchSize = _batchSize;
        }
        void trace()
        {
            for (auto &partCopy : parts)
            {
                unsigned partNo = partCopy & partMask;
                unsigned copy = partCopy >> partBits;
                IPartDescriptor &pd = allParts->item(partNo);
                RemoteFilename rfn;
                pd.getFilename(copy, rfn);
                StringBuffer path;
                rfn.getRemotePath(path);
                VStringBuffer msg("part=%u, copy=%u, handling: %s", partNo, copy, path.str());
                trace(msg);
            }
        }
        unsigned queryPartNumIdx(unsigned partNo) const { return partNumMap[partNo]; }
        virtual void trace(const char *msg) const
        {
            VStringBuffer log("%s (%p): %s", typeid(*this).name(), this, msg);
            PROGLOG("%s", log.str());
        }
        virtual void beforeDispose() override
        {
            stop();
        }
        virtual void addPartNum(unsigned partCopy)
        {
            parts.push_back(partCopy);
            queues.push_back(new CThorExpandingRowArray(activity, rowIf));
            batchArrays.push_back(new CThorExpandingRowArray(activity));
            unsigned partNo = partCopy & partMask;
            while (partNo >= partNumMap.size())
                partNumMap.push_back(NotFound);
            partNumMap[partNo] = myParts++;
        }
        virtual void init()
        {
            stopped = false;
            nextQueue = 0;
            totalQueued = 0;
            state = ts_initial;
        }
        void join()
        {
            {
                CriticalBlock b(queueCrit);
                if (ts_initial == state)
                    return;
            }
            threaded.join();
        }
        void stop()
        {
            stopped = true;
            join();
            for (auto &queue : queues)
                queue->clearRows();
            for (auto &batchArray : batchArrays)
                batchArray->clearRows();
        }
        void enqueue(CThorExpandingRowArray &newItems, unsigned partsIdx) // NB: enqueue starts thread
        {
            CLeavableCriticalBlock b(queueCrit);
            totalQueued += newItems.ordinality();
            queues[partsIdx]->appendRows(newItems, true);
            while (true)
            {
                if (state == ts_running) // as long as running here, we know thread will process queue
                    break;
                else if (state == ts_starting) // then another thread is dealing with transition (could be blocked in incRunningLookup())
                    break;
                else if (state == ts_initial)
                {
                    state = ts_starting;
                    b.leave();
                    if (limiter)
                        limiter->inc(); // blocks if hit lookup thread limit
                    if (activity.abortSoon)
                    {
                        if (limiter)
                            limiter->dec(); // normally handled at end of thread
                        return;
                    }
                    threaded.start();
                    break;
                }
                else if (state == ts_stopping)
                {
                    state = ts_initial;
                    b.leave();
                    // stopping/stopped
                    threaded.join(); // must be sure finished
                    b.enter();
                    // cycle around to start thread again, or bail out if someone else already has.
                }
            }
        }
        void queueLookupTS(const void *row, unsigned partNo) // thread-safe queueLookup
        {
            CriticalBlock b(batchCrit);
            queueLookup(row, partNo);
        }
        void queueLookup(const void *row, unsigned partNo)
        {
            // NB: queueLookup() must be protected from re-entry by caller
            unsigned partsIdx = queryPartNumIdx(partNo);
            CThorExpandingRowArray &batchArray = *batchArrays[partsIdx];
            LinkThorRow(row);
            batchArray.append(row);
            if (batchArray.ordinality() >= lookupQueuedBatchSize)
                enqueue(batchArray, partsIdx); // NB: enqueue takes ownership of rows, i.e batchArray is cleared after call
        }
        void flushTS() // thread-safe flush
        {
            for (unsigned b=0; b<batchArrays.size(); b++)
            {
                CThorExpandingRowArray *batchArray = batchArrays[b];
                CriticalBlock block(batchCrit);
                if (batchArray->ordinality())
                    enqueue(*batchArray, b);
            }
        }
        void flush()
        {
            // NB: queueLookup() must be protected from re-entry by caller
            for (unsigned b=0; b<batchArrays.size(); b++)
            {
                CThorExpandingRowArray *batchArray = batchArrays[b];
                if (batchArray->ordinality())
                    enqueue(*batchArray, b);
            }
        }
        virtual void end()
        {
            VStringBuffer log("processed: %" I64F "u", total);
            trace(log);
        }
        virtual void process(CThorExpandingRowArray &processing, unsigned selected) = 0;
    // IThreaded
        virtual void threadmain() override
        {
            CThorExpandingRowArray processing(activity, rowIf);
            unsigned selected = NotFound;
            while (true)
            {
                {
                    CriticalBlock b(queueCrit);
                    if (0 == totalQueued)
                    {
                        if (state != ts_starting) // 1st time around the loop
                            assertex(state == ts_running);
                        state = ts_stopping; // only this thread can transition between ts_running and ts_stopping
                        break;
                    }
                    else if (ts_starting)
                        state = ts_running; // only this thread can transition between ts_starting and ts_running
                    else
                    {
                        dbgassertex(state == ts_running);
                    }
#ifdef _DEBUG
                    unsigned startQueue = nextQueue;
#endif
                    // round robin through the avail part queues
                    while (true)
                    {
                        CThorExpandingRowArray &queue = *queues[nextQueue];
                        if (queue.ordinality())
                        {
                            selected = nextQueue;
                            ++nextQueue;
                            if (nextQueue == myParts)
                                nextQueue = 0;
                            unsigned numToDequeue = queue.ordinality();
                            if (numToDequeue > batchProcessLimit)
                            {
                                numToDequeue = batchProcessLimit;
                                queue.transferRows(0, numToDequeue, processing);
                            }
                            else
                                queue.swap(processing);
                            totalQueued -= numToDequeue;
                            break;
                        }
                        else
                        {
                            ++nextQueue;
                            if (nextQueue == myParts)
                                nextQueue = 0;
                        }
#ifdef _DEBUG
                        assertex(nextQueue != startQueue); // sanity check: should never happen, as only here because totalQueued>0
#endif
                    }
                }
                try
                {
                    total += processing.ordinality();
                    process(processing, selected);
                }
                catch (IException *e)
                {
                    EXCLOG(e, nullptr);
                    activity.fireException(e);
                    e->Release();
                }
                processing.clearRows();
            }
            if (limiter)
                limiter->dec(); // unblocks any requests to start lookup threads
        }
    };

    class CKeyLookupLocalBase : public CLookupHandler
    {
        typedef CLookupHandler PARENT;
    protected:
        std::vector<Owned<const ITranslator>> translators;

        void setupTranslation(unsigned partNo, unsigned selected, IKeyManager &keyManager)
        {
            if (!translators[selected])
            {
                IPartDescriptor &part = activity.allIndexParts.item(partNo);
                IPropertyTree &props = part.queryOwner().queryProperties();
                unsigned publishedFormatCrc = (unsigned)props.getPropInt("@formatCrc", 0);
                Owned<IOutputMetaData> publishedFormat = getDaliLayoutInfo(props);
                unsigned expectedFormatCrc = helper->getIndexFormatCrc();
                unsigned projectedFormatCrc = helper->getProjectedIndexFormatCrc();
                IOutputMetaData *projectedFormat = helper->queryProjectedIndexRecordSize();

                RecordTranslationMode translationMode = getTranslationMode(activity);
                OwnedRoxieString fname = helper->getIndexFileName();
                translators[selected].setown(getTranslators(fname, expectedFormatCrc, helper->queryIndexRecordSize(), publishedFormatCrc, publishedFormat, projectedFormatCrc, projectedFormat, translationMode));
            }
            if (translators[selected])
                keyManager.setLayoutTranslator(&translators[selected]->queryTranslator());
        }
    public:
        CKeyLookupLocalBase(CKeyedJoinSlave &_activity) : CLookupHandler(_activity, _activity.keyLookupRowWithJGRowIf, _activity.keyLookupProcessBatchLimit)
        {
            limiter = &activity.lookupThreadLimiter;
            allParts = &activity.allIndexParts;
        }
        void processRows(CThorExpandingRowArray &processing, unsigned partNo, IKeyManager *keyManager)
        {
            for (unsigned r=0; r<processing.ordinality() && !stopped; r++)
            {
                OwnedConstThorRow row = processing.getClear(r);
                CJoinGroup *joinGroup = *(CJoinGroup **)row.get();

                const void *keyedFieldsRow = (byte *)row.get() + sizeof(KeyLookupHeader);
                helper->createSegmentMonitors(keyManager, keyedFieldsRow);
                keyManager->finishSegmentMonitors();
                keyManager->reset();

                // NB: keepLimit is not on hard matches and can only be applied later, since other filtering (e.g. in transform) may keep below keepLimit
                while (keyManager->lookup(true))
                {
                    unsigned candidates = joinGroup->incCandidates();
                    if (candidates > activity.abortLimit)
                    {
                        joinGroup->setAbortLimitHit(); // also clears existing rows
                        break;
                    }
                    else if (candidates > activity.atMost) // atMost - filter out group if > max hard matches
                    {
                        joinGroup->setAtMostLimitHit(); // also clears existing rows
                        break;
                    }
                    KLBlobProviderAdapter adapter(keyManager);
                    byte const * keyRow = keyManager->queryKeyBuffer();
                    size_t fposOffset = keyManager->queryRowSize() - sizeof(offset_t);
                    offset_t fpos = rtlReadBigUInt8(keyRow + fposOffset);
                    if (helper->indexReadMatch(keyedFieldsRow, keyRow,  &adapter))
                    {
                        if (activity.needsDiskRead)
                        {
                            unsigned __int64 sequence = joinGroup->addRightMatchPending(partNo, fpos);
                            if (NotFound == sequence) // means limit was hit and must have been caused by another handler
                                break;
                            joinGroup->incPending();

                            /* NB: encode *index* partNo into sequence
                             * This is used when result comes back to preserve order when calling joinGroup->addRightMatchCompletePending()
                             */
                            dbgassertex(sequence <= UINT_MAX);
                            sequence = sequence | (((unsigned __int64)partNo) << 32);

                            activity.queueFetchLookup(fpos, sequence, joinGroup);
                        }
                        else
                        {
                            RtlDynamicRowBuilder joinFieldsRowBuilder(activity.joinFieldsAllocator);
                            size32_t joinFieldsRowSz = activity.helper->extractJoinFields(joinFieldsRowBuilder, keyRow, &adapter);
                            const void *joinFieldsRow = joinFieldsRowBuilder.finalizeRowClear(joinFieldsRowSz);
                            joinGroup->addRightMatch(partNo, joinFieldsRowSz, joinFieldsRow, fpos);
                        }
                    }
                }
                keyManager->releaseSegmentMonitors();
                joinGroup->decPending(); // Every queued lookup row triggered an inc., this is the corresponding dec.
            }
        }
    };
    class CKeyLookupLocalHandler : public CKeyLookupLocalBase
    {
        typedef CKeyLookupLocalBase PARENT;

        std::vector<IKeyManager *> keyManagers;
    public:
        CKeyLookupLocalHandler(CKeyedJoinSlave &_activity) : CKeyLookupLocalBase(_activity)
        {
        }
        ~CKeyLookupLocalHandler()
        {
            for (auto &km : keyManagers)
            {
                if (km)
                    km->Release();
            }
        }
        virtual void addPartNum(unsigned partNum) override
        {
            PARENT::addPartNum(partNum);
            keyManagers.push_back(nullptr);
            translators.push_back(nullptr);
        }
        virtual void process(CThorExpandingRowArray &processing, unsigned selected) override
        {
            unsigned partCopy = parts[selected];
            unsigned partNo = partCopy & partMask;
            unsigned copy = partCopy >> partBits;
            IKeyManager *&keyManager = keyManagers[selected];
            if (!keyManager) // delayed until actually needed
            {
                keyManager = activity.createPartKeyManager(partNo, copy);
                // NB: potentially translation per part could be different if dealing with superkeys
                setupTranslation(partNo, selected, *keyManager);
            }
            processRows(processing, partNo, keyManager);
        }
    };
    class CKeyLookupMergeHandler : public CKeyLookupLocalBase
    {
        typedef CKeyLookupLocalBase PARENT;

        Owned<IKeyManager> keyManager;
    public:
        CKeyLookupMergeHandler(CKeyedJoinSlave &_activity) : CKeyLookupLocalBase(_activity)
        {
            limiter = &activity.lookupThreadLimiter;
            translators.push_back(nullptr);
        }
        virtual void process(CThorExpandingRowArray &processing, unsigned __unused) override
        {
            if (!keyManager)
            {
                Owned<IKeyIndexSet> partKeySet = createKeyIndexSet();
                for (auto &partCopy: parts)
                {
                    unsigned partNo = partCopy & partMask;
                    unsigned copy = partCopy >> partBits;
                    Owned<IKeyIndex> keyIndex = activity.createPartKeyIndex(partNo, copy, false);
                    partKeySet->addIndex(keyIndex.getClear());
                }
                keyManager.setown(createKeyMerger(helper->queryIndexRecordSize()->queryRecordAccessor(true), partKeySet, 0, nullptr, helper->hasNewSegmentMonitors(), false));
                setupTranslation(0, 0, *keyManager);
            }
            processRows(processing, 0, keyManager);
        }
    };
    class CRemoteLookupHandler : public CLookupHandler
    {
        typedef CLookupHandler PARENT;

    protected:
        rank_t lookupSlave = RANK_NULL;
        mptag_t replyTag = TAG_NULL;
        ICommunicator *comm = nullptr;
        std::vector<unsigned> handles;

        KJServiceErrorCode readErrorCode(CMessageBuffer &msg)
        {
            byte errorCode;
            msg.read(errorCode);
            if (kjse_exception == errorCode)
                throw deserializeException(msg);
            return (KJServiceErrorCode)errorCode;
        }
        void writeRowData(CThorExpandingRowArray &rows, MemoryBuffer &mb)
        {
            DelayedSizeMarker sizeMark(mb);
            MemoryBuffer tmpMB;
            MemoryBuffer &dst = activity.messageCompression ? tmpMB : mb;

            unsigned numRows = rows.ordinality();
            dst.append(numRows);
            CMemoryRowSerializer s(dst);
            IOutputRowSerializer *serializer = rowIf->queryRowSerializer();
            for (rowidx_t r=0; r<numRows; r++)
            {
                const void *row = rows.query(r);
                serializer->serialize(s, (const byte *)row);
            }
            if (activity.messageCompression)
                fastLZCompressToBuffer(mb, tmpMB.length(), tmpMB.toByteArray());
            sizeMark.write();
        }
        void initClose(CMessageBuffer &msg, KJServiceCmds cmd, unsigned handle)
        {
            msg.append((byte)cmd);
            msg.append(replyTag);
            msg.append(handle);
        }
        void doClose(KJServiceCmds closeCmd)
        {
            for (auto &h: handles)
            {
                if (h)
                {
                    CMessageBuffer msg;
                    initClose(msg, closeCmd, h);
                    if (!comm->send(msg, lookupSlave, kjServiceMpTag, LONGTIMEOUT))
                        throw MakeActivityException(&activity, 0, "CKeyLookupRemoteHandler - comm send failed");
                    msg.clear();
                    if (comm->recv(msg, lookupSlave, replyTag))
                    {
                        readErrorCode(msg);
                        bool removed;
                        msg.read(removed);
                        if (!removed)
                            IWARNLOG("KJ service failed to remove [%u]", closeCmd);
                    }
                }
            }
        }
    public:
        CRemoteLookupHandler(CKeyedJoinSlave &_activity, IThorRowInterfaces *_rowIf, unsigned _lookupSlave, unsigned batchProcessLimit)
            : CLookupHandler(_activity, _rowIf, batchProcessLimit), lookupSlave(_lookupSlave)
        {
            /* NB: There is 1 KJ service per slave, not per channel
             * create reply tag using slave IMPServer and use the slave ICommunicator
             */
            Owned<IMPServer> mpServer = getMPServer();
            replyTag = mpServer->createReplyTag();
            comm = &activity.queryJob().queryNodeComm();
        }
        virtual void init() override
        {
            PARENT::init();
            handles.resize(parts.size());
            for (auto &h: handles)
                h = 0;
        }
    };
    class CKeyLookupRemoteHandler : public CRemoteLookupHandler
    {
        typedef CRemoteLookupHandler PARENT;

        void initRead(CMessageBuffer &msg, unsigned selected, unsigned partNo, unsigned copy)
        {
            unsigned handle = handles[selected];
            byte cmd = handle ? kjs_keyread : kjs_keyopen;
            msg.append(cmd);
            msg.append(replyTag);
            if (handle)
                msg.append(handle);
            else
            {
                IPartDescriptor &part = activity.allIndexParts.item(partNo);
                unsigned crc;
                if (!part.getCrc(crc))
                    crc = 0;
                RemoteFilename rfn;
                part.getFilename(copy, rfn);
                StringBuffer fname;
                rfn.getLocalPath(fname);
                msg.append(activity.queryId()).append(fname).append(crc); // lookup key
                // serialize onCreate context
                DelayedSizeMarker sizeMark(msg);
                activity.queryHelper()->serializeCreateContext(msg);
                sizeMark.write();

                size32_t parentExtractSz;
                const byte *parentExtract = activity.queryGraph().queryParentExtract(parentExtractSz);
                msg.append(parentExtractSz);
                msg.append(parentExtractSz, parentExtract);
                msg.append(activity.startCtxMb.length());
                msg.append(activity.startCtxMb.length(), activity.startCtxMb.toByteArray());

                msg.append(activity.messageCompression);
                // NB: potentially translation per part could be different if dealing with superkeys
                IPropertyTree &props = part.queryOwner().queryProperties();
                unsigned publishedFormatCrc = (unsigned)props.getPropInt("@formatCrc", 0);
                Owned<IOutputMetaData> publishedFormat = getDaliLayoutInfo(props);
                unsigned projectedFormatCrc = helper->getProjectedIndexFormatCrc();
                IOutputMetaData *projectedFormat = helper->queryProjectedIndexRecordSize();
                unsigned expectedFormatCrc = helper->getIndexFormatCrc();

                RecordTranslationMode translationMode = getTranslationMode(activity);

                Owned<const ITranslator> translator = getTranslators(fname, expectedFormatCrc, helper->queryIndexRecordSize(), publishedFormatCrc, publishedFormat, projectedFormatCrc, projectedFormat, translationMode);
                if (translator)
                {
                    bool canRemoteTranslate = publishedFormat->queryTypeInfo()->canInterpret() && publishedFormat->queryTypeInfo()->canSerialize() &&
                                              projectedFormat->queryTypeInfo()->canInterpret() && projectedFormat->queryTypeInfo()->canSerialize();
                    if (!canRemoteTranslate)
                        throw MakeActivityException(&activity, 0, "CKeyLookupRemoteHandler - translation required, but formats unserializable");
                    msg.append(static_cast<std::underlying_type<RecordTranslationMode>::type>(translationMode));
                    msg.append(publishedFormatCrc);
                    if (!dumpTypeInfo(msg, publishedFormat->querySerializedDiskMeta()->queryTypeInfo()))
                        throw MakeActivityException(&activity, 0, "CKeyLookupRemoteHandler [dumpTypeInfo] - failed handling publishedFormat");
                    if (projectedFormat != publishedFormat)
                    {
                        msg.append(true);
                        if (!dumpTypeInfo(msg, projectedFormat->querySerializedDiskMeta()->queryTypeInfo()))
                            throw MakeActivityException(&activity, 0, "CKeyLookupRemoteHandler [dumpTypeInfo] - failed handling projectedFormat");
                    }
                    else
                        msg.append(false);
                }
                else
                    msg.append(static_cast<std::underlying_type<RecordTranslationMode>::type>(RecordTranslationMode::None));
            }
        }
        void prepAndSend(CMessageBuffer &msg, CThorExpandingRowArray &processing, unsigned selected, unsigned partNo, unsigned copy)
        {
            initRead(msg, selected, partNo, copy);
            writeRowData(processing, msg);

            if (!comm->send(msg, lookupSlave, kjServiceMpTag, LONGTIMEOUT))
                throw MakeActivityException(&activity, 0, "CKeyLookupRemoteHandler - comm send failed");
            msg.clear();
        }
    public:
        CKeyLookupRemoteHandler(CKeyedJoinSlave &_activity, unsigned _lookupSlave) : PARENT(_activity, _activity.keyLookupRowWithJGRowIf, _lookupSlave, _activity.keyLookupProcessBatchLimit)
        {
            limiter = &activity.lookupThreadLimiter;
            allParts = &activity.allIndexParts;
        }
        virtual void trace(const char *msg) const override
        {
            VStringBuffer log("%s, lookupSlave=%u", msg, lookupSlave);
            PARENT::trace(log);
        }
        virtual void process(CThorExpandingRowArray &processing, unsigned selected) override
        {
            unsigned partCopy = parts[selected];
            unsigned partNo = partCopy & partMask;
            unsigned copy = partCopy >> partBits;

            unsigned numRows = processing.ordinality();

            CMessageBuffer msg;
            prepAndSend(msg, processing, selected, partNo, copy);

            // read back results and feed in to appropriate join groups.
            unsigned received = 0;
            bool resent = false;
            while (true)
            {
                if (!comm->recv(msg, lookupSlave, replyTag))
                    break;
                KJServiceErrorCode errorCode = readErrorCode(msg);
                if (kjse_unknownhandle == errorCode)
                {
                    if (resent)
                        throwUnexpected();
                    msg.clear();
                    resent = true;
                    handles[selected] = 0;
                    prepAndSend(msg, processing, selected, partNo, copy);
                    continue;
                }
                MemoryBuffer tmpMB;
                MemoryBuffer &mb = doUncompress(tmpMB, msg);
                if (0 == received)
                    mb.read(handles[selected]);
                unsigned count;
                mb.read(count); // amount processed, could be all (i.e. numRows)
                while (count--)
                {
                    OwnedConstThorRow requestRow(processing.getClear(received++));
                    KeyLookupHeader lookupKeyHeader;
                    getHeaderFromRow(requestRow, lookupKeyHeader);
                    CJoinGroup *joinGroup = lookupKeyHeader.jg;

                    unsigned flags; // NB: server serialized a GroupFlags type, it's underlying type is unsigned
                    mb.read(flags);

                    if (flags == gf_limitabort)
                        joinGroup->setAbortLimitHit(); // also clears existing rows
                    else if (flags == gf_limitatmost) // atMost - filter out group if > max hard matches
                        joinGroup->setAtMostLimitHit(); // also clears existing rows
                    else
                    {
                        unsigned matches;
                        mb.read(matches);
                        if (matches)
                        {
                            unsigned candidates = joinGroup->addCandidates(matches);
                            if (candidates > activity.abortLimit || candidates > activity.atMost)
                            {
                                if (!activity.needsDiskRead)
                                {
                                    size32_t sz;
                                    mb.read(sz);
                                    mb.skip(sz);
                                }
                                mb.skip(matches * sizeof(unsigned __int64));
                                if (candidates > activity.abortLimit)
                                    joinGroup->setAbortLimitHit(); // also clears existing rows
                                else if (candidates > activity.atMost) // atMost - filter out group if > max hard matches
                                    joinGroup->setAtMostLimitHit(); // also clears existing rows
                            }
                            else
                            {
                                std::vector<unsigned __int64> fposs(matches);
                                if (activity.needsDiskRead)
                                {
                                    mb.read(matches * sizeof(unsigned __int64), &fposs[0]); // JCSMORE shame to serialize these if not needed, does codegen give me a tip?
                                    for (unsigned r=0; r<matches; r++)
                                    {
                                        unsigned __int64 sequence = joinGroup->addRightMatchPending(partNo, fposs[r]);
                                        if (NotFound == sequence) // means limit was hit and must have been caused by another handler
                                            break;
                                        joinGroup->incPending();

                                        /* NB: encode *index* partNo into sequence
                                         * This is used when result comes back to preserve order when calling joinGroup->addRightMatchCompletePending()
                                         */
                                        dbgassertex(sequence <= UINT_MAX);
                                        sequence = sequence | (((unsigned __int64)partNo) << 32);

                                        activity.queueFetchLookup(fposs[r], sequence, joinGroup);
                                    }
                                }
                                else
                                {
                                    size32_t sz;
                                    mb.read(sz);
                                    CThorStreamDeserializerSource keyLookupRowSource(sz, mb.readDirect(sz));
                                    mb.read(matches * sizeof(unsigned __int64), &fposs[0]); // JCSMORE shame to serialize these if not needed, does codegen give me a tip?
                                    for (unsigned r=0; r<matches; r++)
                                    {
                                        dbgassertex(!keyLookupRowSource.eos());

                                        RtlDynamicRowBuilder rowBuilder(activity.keyLookupReplyOutputMetaRowIf->queryRowAllocator());
                                        size32_t joinFieldsRowSz = activity.keyLookupReplyOutputMetaRowIf->queryRowDeserializer()->deserialize(rowBuilder, keyLookupRowSource);
                                        joinGroup->addRightMatch(partNo, joinFieldsRowSz, rowBuilder.finalizeRowClear(joinFieldsRowSz), fposs[r]);
                                    }
                                }
                            }
                        }
                    }
                    joinGroup->decPending(); // Every queued lookup row triggered an inc., this is the corresponding dec.
                }
                if (received == numRows)
                    break;
            }
        }
        virtual void end() override
        {
            PARENT::end();
            doClose(kjs_keyclose);
        }
    };
    class CFetchLocalLookupHandler : public CLookupHandler
    {
        typedef CLookupHandler PARENT;

        bool encrypted = false;
        bool compressed = false;
        Owned<IEngineRowAllocator> fetchDiskAllocator;
        CThorContiguousRowBuffer prefetchSource;
        std::vector<PartIO> partIOs;

        inline const PartIO &queryFetchPartIO(unsigned selected, unsigned partNo, unsigned copy, bool compressed, bool encrypted)
        {
            PartIO &partIO = partIOs[selected];
            if (!partIO.stream)
                partIO = activity.getFetchPartIO(partNo, copy, compressed, encrypted);
            return partIO;
        }
    public:
        CFetchLocalLookupHandler(CKeyedJoinSlave &_activity)
            : PARENT(_activity, _activity.fetchInputMetaRowIf, _activity.fetchLookupProcessBatchLimit)
        {
            Owned<IThorRowInterfaces> fetchDiskRowIf = activity.createRowInterfaces(helper->queryDiskRecordSize());
            fetchDiskAllocator.set(fetchDiskRowIf->queryRowAllocator());
            limiter = &activity.fetchThreadLimiter;
            allParts = &activity.allDataParts;
        }
        virtual void init() override
        {
            PARENT::init();
            encrypted = activity.allDataParts.item(0).queryOwner().queryProperties().getPropBool("@encrypted");
            compressed = isCompressed(activity.allDataParts.item(0).queryOwner().queryProperties());
            partIOs.resize(parts.size());
        }
        virtual void process(CThorExpandingRowArray &processing, unsigned selected) override
        {
            unsigned partCopy = parts[selected];
            unsigned partNo = partCopy & partMask;
            unsigned copy = partCopy >> partBits;

            ScopedAtomic<unsigned __int64> diskRejected(activity.statsArr[AS_DiskRejected]);
            ScopedAtomic<unsigned __int64> diskSeeks(activity.statsArr[AS_DiskSeeks]);
            for (unsigned r=0; r<processing.ordinality() && !stopped; r++)
            {
                OwnedConstThorRow row = processing.getClear(r);
                FetchRequestHeader &requestHeader = *(FetchRequestHeader *)row.get();
                CJoinGroup *joinGroup = requestHeader.jg;

                const void *fetchKey = nullptr;
                if (0 != helper->queryFetchInputRecordSize()->getMinRecordSize())
                    fetchKey = (const byte *)row.get() + sizeof(FetchRequestHeader);

                const PartIO &partIO = queryFetchPartIO(selected, partNo, copy, compressed, encrypted);
                prefetchSource.setStream(partIO.stream);
                prefetchSource.reset(requestHeader.fpos);
                partIO.prefetcher->readAhead(prefetchSource);
                const byte *diskFetchRow = prefetchSource.queryRow();

                MemoryBuffer diskFetchRowMb;
                if (partIO.translator)
                {
                    MemoryBufferBuilder aBuilder(diskFetchRowMb, 0);
                    LocalVirtualFieldCallback fieldCallback("<MORE>", requestHeader.fpos, 0);
                    partIO.translator->queryTranslator().translate(aBuilder, fieldCallback, diskFetchRow);
                    diskFetchRow = reinterpret_cast<const byte *>(diskFetchRowMb.toByteArray());
                }
                if (helper->fetchMatch(fetchKey, diskFetchRow))
                {
                    RtlDynamicRowBuilder joinFieldsRow(activity.joinFieldsAllocator);
                    size32_t joinFieldsSz = helper->extractJoinFields(joinFieldsRow, diskFetchRow, (IBlobProvider*)nullptr); // JCSMORE is it right that passing NULL IBlobProvider here??
                    const void *fetchRow = joinFieldsRow.finalizeRowClear(joinFieldsSz);

                    unsigned sequence = requestHeader.sequence & 0xffffffff;
                    unsigned indexPartNo = requestHeader.sequence >> 32; // NB: used to preserveOrder when calling addRightMatchCompletePending

                    // If !preserverOrder, right rows added to single array in jg, so pass 0
                    joinGroup->addRightMatchCompletePending(activity.preserveOrder ? indexPartNo : 0, sequence, joinFieldsSz, fetchRow);

                    if (++activity.statsArr[AS_DiskAccepted] > activity.rowLimit)
                        helper->onLimitExceeded();
                }
                else
                    diskRejected++;
                joinGroup->decPending(); // Every queued lookup row triggered an inc., this is the corresponding dec.

                diskSeeks++;
            }
        }
    };
    class CFetchRemoteLookupHandler : public CRemoteLookupHandler
    {
        typedef CRemoteLookupHandler PARENT;

        IOutputRowDeserializer *rowDeserializer = nullptr;
        IEngineRowAllocator *rowAllocator = nullptr;
        byte flags = 0;

        void initRead(CMessageBuffer &msg, unsigned selected, unsigned partNo, unsigned copy)
        {
            unsigned handle = handles[selected];
            byte cmd = handle ? kjs_fetchread : kjs_fetchopen;
            msg.append(cmd);
            msg.append(replyTag);
            if (handle)
                msg.append(handle);
            else
            {
                msg.append(activity.queryId()).append(partNo); // fetch key

                // serialize onCreate context
                DelayedSizeMarker sizeMark(msg);
                activity.queryHelper()->serializeCreateContext(msg);
                sizeMark.write();

                msg.append(flags);
                IPartDescriptor &part = activity.allDataParts.item(partNo);
                RemoteFilename rfn;
                part.getFilename(copy, rfn);
                StringBuffer fname;
                rfn.getLocalPath(fname);
                msg.append(fname);
                msg.append(activity.messageCompression);

                // NB: potentially translation per part could be different if dealing with superkeys
                IPropertyTree &props = part.queryOwner().queryProperties();
                unsigned publishedFormatCrc = (unsigned)props.getPropInt("@formatCrc", 0);
                Owned<IOutputMetaData> publishedFormat = getDaliLayoutInfo(props);
                unsigned projectedFormatCrc = helper->getProjectedFormatCrc();
                IOutputMetaData *projectedFormat = helper->queryProjectedDiskRecordSize();
                unsigned expectedFormatCrc = helper->getDiskFormatCrc();

                RecordTranslationMode translationMode = getTranslationMode(activity);

                Owned<const ITranslator> translator = getTranslators(fname, expectedFormatCrc, helper->queryDiskRecordSize(), publishedFormatCrc, publishedFormat, projectedFormatCrc, projectedFormat, translationMode);
                if (translator)
                {
                    bool canRemoteTranslate = publishedFormat->queryTypeInfo()->canInterpret() && publishedFormat->queryTypeInfo()->canSerialize() &&
                                              projectedFormat->queryTypeInfo()->canInterpret() && projectedFormat->queryTypeInfo()->canSerialize();
                    if (!canRemoteTranslate)
                        throw MakeActivityException(&activity, 0, "CFetchRemoteLookupHandler - translation required, but formats unserializable");
                    msg.append(static_cast<std::underlying_type<RecordTranslationMode>::type>(translationMode));
                    msg.append(publishedFormatCrc);
                    if (!dumpTypeInfo(msg, publishedFormat->querySerializedDiskMeta()->queryTypeInfo()))
                        throw MakeActivityException(&activity, 0, "CFetchRemoteLookupHandler [dumpTypeInfo] - failed handling publishedFormat");
                    if (projectedFormat != publishedFormat)
                    {
                        msg.append(true);
                        if (!dumpTypeInfo(msg, projectedFormat->querySerializedDiskMeta()->queryTypeInfo()))
                            throw MakeActivityException(&activity, 0, "CFetchRemoteLookupHandler [dumpTypeInfo] - failed handling projectedFormat");
                    }
                    else
                        msg.append(false);
                }
                else
                    msg.append(static_cast<std::underlying_type<RecordTranslationMode>::type>(RecordTranslationMode::None));
            }
        }
        void prepAndSend(CMessageBuffer &msg, CThorExpandingRowArray &processing, unsigned selected, unsigned partNo, unsigned copy)
        {
            initRead(msg, selected, partNo, copy);
            writeRowData(processing, msg);

            if (!comm->send(msg, lookupSlave, kjServiceMpTag, LONGTIMEOUT))
                throw MakeActivityException(&activity, 0, "CFetchRemoteLookupHandler - comm send failed");
            msg.clear();
        }
    public:
        CFetchRemoteLookupHandler(CKeyedJoinSlave &_activity, unsigned _lookupSlave)
            : PARENT(_activity, _activity.fetchInputMetaRowIf, _lookupSlave, _activity.fetchLookupProcessBatchLimit)
        {
            limiter = &activity.fetchThreadLimiter;
            allParts = &activity.allDataParts;
            rowDeserializer = activity.fetchOutputMetaRowIf->queryRowDeserializer();
            rowAllocator = activity.fetchOutputMetaRowIf->queryRowAllocator();
        }
        virtual void init() override
        {
            PARENT::init();
            flags = 0;
            if (activity.allDataParts.item(0).queryOwner().queryProperties().getPropBool("@encrypted"))
                flags |= kjf_encrypted;
            if (isCompressed(activity.allDataParts.item(0).queryOwner().queryProperties()))
                flags |= kjf_compressed;
        }
        virtual void process(CThorExpandingRowArray &processing, unsigned selected) override
        {
            unsigned partCopy = parts[selected];
            unsigned partNo = partCopy & partMask;
            unsigned copy = partCopy >> partBits;

            CMessageBuffer msg;
            prepAndSend(msg, processing, selected, partNo, copy);

            ScopedAtomic<unsigned __int64> diskSeeks(activity.statsArr[AS_DiskSeeks]);
            unsigned numRows = processing.ordinality();
            // read back results and feed in to appropriate join groups.

            unsigned received = 0;
            bool resent = false;
            while (true)
            {
                if (!comm->recv(msg, lookupSlave, replyTag))
                    break;
                KJServiceErrorCode errorCode = readErrorCode(msg);
                if (kjse_unknownhandle == errorCode)
                {
                    if (resent)
                        throwUnexpected();
                    msg.clear();
                    resent = true;
                    handles[selected] = 0;
                    prepAndSend(msg, processing, selected, partNo, copy);
                    continue;
                }

                MemoryBuffer tmpMB;
                MemoryBuffer &mb = doUncompress(tmpMB, msg);
                if (0 == received)
                    mb.read(handles[selected]);
                unsigned count;
                mb.read(count); // amount processed, could be all (i.e. numRows)
                size32_t totalRowSz = 0;
                CThorStreamDeserializerSource rowSource(nullptr);
                if (count)
                {
                    mb.read(totalRowSz);

                    Owned<ISerialStream> stream = createMemorySerialStream(mb.readDirect(totalRowSz), totalRowSz);
                    rowSource.setStream(stream);
                }

                FetchRequestHeader fetchHeader;
                FetchReplyHeader replyHeader;
                while (count--)
                {
                    dbgassertex(!rowSource.eos());

                    const void *requestRow = processing.query(received++);
                    getHeaderFromRow(requestRow, fetchHeader);
                    CJoinGroup *joinGroup = fetchHeader.jg;

                    RtlDynamicRowBuilder rowBuilder(rowAllocator);
                    size32_t sz = rowDeserializer->deserialize(rowBuilder, rowSource);
                    OwnedConstThorRow replyRow = rowBuilder.finalizeRowClear(sz);

                    getHeaderFromRow(replyRow.get(), replyHeader);

                    const void *fetchRow = *((const void **)(((byte *)replyRow.get())+sizeof(FetchReplyHeader)));
                    if (replyHeader.sequence & FetchReplyHeader::fetchMatchedMask)
                    {
                        LinkThorRow(fetchRow);
                        unsigned sequence = replyHeader.sequence & 0xffffffff;
                        unsigned indexPartNo = (replyHeader.sequence & ~FetchReplyHeader::fetchMatchedMask) >> 32;

                        size32_t fetchRowSz = activity.joinFieldsAllocator->queryOutputMeta()->getRecordSize(fetchRow);
                        // If !preserverOrder, right rows added to single array in jg, so pass 0
                        joinGroup->addRightMatchCompletePending(activity.preserveOrder ? indexPartNo : 0, sequence, fetchRowSz, fetchRow);
                    }
                    joinGroup->decPending(); // Every queued lookup row triggered an inc., this is the corresponding dec.
                    diskSeeks++; // NB: really the seek happened on the remote side, but it can't be tracked into the activity stats there.
                }

                if (received)
                {
                    unsigned accepted = 0;
                    unsigned rejected = 0;
                    mb.read(accepted);
                    mb.read(rejected);
                    activity.statsArr[AS_DiskAccepted] += accepted;
                    activity.statsArr[AS_DiskRejected] += rejected;
                }
                if (received == numRows)
                    break;
            }
        }
        virtual void end() override
        {
            PARENT::end();
            doClose(kjs_fetchclose);
        }
    };
    class CReadAheadThread : implements IThreaded
    {
        CKeyedJoinSlave &owner;
        CThreaded threaded;
    public:
        CReadAheadThread(CKeyedJoinSlave &_owner) : owner(_owner), threaded("CReadAheadThread", this)
        {
        }
        void start()
        {
            threaded.start();
        }
        void join()
        {
            threaded.join();
        }
        void stop()
        {
            owner.stopReadAhead();
            join();
        }
    // IThreaded
        virtual void threadmain() override
        {
            owner.readAhead(); // can block
        }
    } readAheadThread;
    class CHandlerContainer
    {
    public:
        IPointerArrayOf<CLookupHandler> handlers;
        std::vector<CLookupHandler *> partIdxToHandler;
        bool localKey = false;
        bool isLocalKey() const { return localKey; }
        void init()
        {
            ForEachItemIn(h, handlers)
            {
                CLookupHandler *lookupHandler = handlers.item(h);
                if (lookupHandler)
                    lookupHandler->init();
            }
        }
        void clear()
        {
            handlers.kill();
            partIdxToHandler.clear();
        }
        void trace() const
        {
            ForEachItemIn(h, handlers)
                handlers.item(h)->trace();
        }
        CLookupHandler *queryHandler(unsigned partNo)
        {
            return partIdxToHandler[partNo];
        }
        void flushTS() // thread-safe flush()
        {
            ForEachItemIn(h, handlers)
            {
                CLookupHandler *lookupHandler = handlers.item(h);
                if (lookupHandler)
                    lookupHandler->flushTS();
            }
        }
        void flush()
        {
            ForEachItemIn(h, handlers)
            {
                CLookupHandler *lookupHandler = handlers.item(h);
                if (lookupHandler)
                    lookupHandler->flush();
            }
        }
        void stop()
        {
            ForEachItemIn(h, handlers)
            {
                CLookupHandler *lookupHandler = handlers.item(h);
                if (lookupHandler)
                    lookupHandler->stop();
            }
        }
        void join()
        {
            ForEachItemIn(h, handlers)
            {
                CLookupHandler *lookupHandler = handlers.item(h);
                if (lookupHandler)
                    lookupHandler->join();
            }
        }
        void end()
        {
            ForEachItemIn(h, handlers)
            {
                CLookupHandler *lookupHandler = handlers.item(h);
                if (lookupHandler)
                    lookupHandler->end();
            }
        }
    };

    const unsigned startSpillAmountPercentage = 10;   // Initial percentage to spill, will grow if memory pressure keeps calling callback
    const memsize_t startSpillAmount = 10 * 0x100000; // (10MB) Initial amount to try to spill
    const memsize_t minRhsJGSpillSz = 1024;           // (1K) Threshold, if a join group is smaller than this, don't attempt to spill
    const unsigned minimumQueueLimit = 5;             // When spilling the pending and group limits will gradually reduce until this limit
    const unsigned unknownCopyNum = 0xff;             // in a partCopy denotes that a copy is unknown.

    IHThorKeyedJoinArg *helper = nullptr;
    StringAttr indexName;
    size32_t fixedRecordSize = 0;
    bool initialized = false;
    bool preserveGroups = false, preserveOrder = false;
    bool needsDiskRead = false;
    bool onFailTransform = false;
    bool keyHasTlk = false;
    std::vector<mptag_t> tags;
    std::vector<RelaxedAtomic<unsigned __int64>> statsArr; // (seeks, scans, accepted, prefiltered, postfiltered, diskSeeks, diskAccepted, diskRejected)
    unsigned numStats = 0;

    enum HandlerType { ht_remotekeylookup, ht_localkeylookup, ht_localfetch, ht_remotefetch };
    CHandlerContainer keyLookupHandlers;
    CHandlerContainer fetchLookupHandlers;
    CLimiter lookupThreadLimiter, fetchThreadLimiter;
    CLimiter pendingKeyLookupLimiter;
    CLimiter doneListLimiter;

    CPartDescriptorArray allDataParts;
    CPartDescriptorArray allIndexParts;
    std::vector<unsigned> localIndexParts, localFetchPartMap;
    IArrayOf<IKeyIndex> tlkKeyIndexes;
    Owned<IEngineRowAllocator> joinFieldsAllocator;
    OwnedConstThorRow defaultRight;
    unsigned joinFlags = 0;
    unsigned totalDataParts = 0;
    unsigned superWidth = 0;
    unsigned totalIndexParts = 0;
    bool partitionKey = false;
    std::vector<FPosTableEntry> globalFPosToSlaveMap; // maps fpos->part
    std::vector<unsigned> indexPartToSlaveMap;
    std::vector<unsigned> dataPartToSlaveMap;


    unsigned atMost = 0, keepLimit = 0;
    unsigned abortLimit = 0;
    rowcount_t rowLimit = 0;
    Linked<IHThorArg> inputHelper;
    unsigned keyLookupQueuedBatchSize = defaultKeyLookupQueuedBatchSize;
    unsigned maxKeyLookupThreads = defaultMaxKeyLookupThreads;
    unsigned maxFetchThreads = defaultMaxFetchThreads;
    unsigned keyLookupMaxQueued = defaultKeyLookupMaxQueued;
    memsize_t keyLookupMinQueuedBytes = defaultMinKeyLookupJoinGroupMB * 0x100000;
    unsigned keyLookupMaxDone = defaultKeyLookupMaxDone;
    unsigned maxNumLocalHandlers = defaultKeyLookupMaxLocalHandlers;
    unsigned maxNumRemoteHandlersPerSlave = defaultKeyLookupMaxHandlersPerRemoteSlave;
    unsigned maxNumRemoteFetchHandlers = defaultKeyLookupMaxFetchHandlers;
    unsigned maxNumLocalFetchHandlers = defaultKeyLookupMaxLocalFetchHandlers;
    unsigned fetchLookupQueuedBatchSize = defaultKeyLookupFetchQueuedBatchSize;
    unsigned keyLookupProcessBatchLimit = defaultKeyLookupProcessBatchLimit;
    unsigned fetchLookupProcessBatchLimit = defaultFetchLookupProcessBatchLimit;
    bool remoteKeyedLookup = false;
    bool remoteKeyedFetch = false;
    bool forceRemoteKeyedLookup = false;
    bool forceRemoteKeyedFetch = false;
    bool messageCompression = false;

    Owned<IThorRowInterfaces> keyLookupRowWithJGRowIf;
    Owned<IThorRowInterfaces> keyLookupReplyOutputMetaRowIf;

    IEngineRowAllocator *keyLookupRowWithJGAllocator = nullptr;
    Owned<IEngineRowAllocator> transformAllocator;
    bool endOfInput = false; // marked true when input exhausted, but may be groups in flight
    bool eos = false; // marked true when everything processed
    IArrayOf<IKeyManager> tlkKeyManagers;
    CriticalSection onCompleteCrit, queuedCrit, runningLookupThreadsCrit;
    std::atomic<bool> waitingForDoneGroups{false};
    Semaphore waitingForDoneGroupsSem;
    CJoinGroupList pendingJoinGroupList, doneJoinGroupList;
    Owned<CJoinGroup> currentJoinGroup;
    unsigned currentMatchIdx = 0;
    CJoinGroup::JoinGroupRhsState rhsState;

    unsigned spillAmountPercentage = startSpillAmountPercentage;
    memsize_t spillAmount = startSpillAmount;
    bool memoryCallbackInstalled = false;

    roxiemem::IRowManager *rowManager = nullptr;
    unsigned currentAdded = 0;
    unsigned currentJoinGroupSize = 0;
    MemoryBuffer startCtxMb;

    Owned<IThorRowInterfaces> fetchInputMetaRowIf; // fetch request rows, header + fetch fields
    Owned<IThorRowInterfaces> fetchOutputMetaRowIf; // fetch request reply rows, header + [join fields as child row]
    Owned<IEngineRowAllocator> fetchInputMetaAllocator;

    CriticalSection fetchFileCrit;
    std::vector<PartIO> openFetchParts;

    PartIO getFetchPartIO(unsigned partNo, unsigned copy, bool compressed, bool encrypted)
    {
        CriticalBlock b(fetchFileCrit);
        if (partNo>=openFetchParts.size())
            openFetchParts.resize(partNo+1);
        PartIO &partIO = openFetchParts[partNo];
        if (!partIO.iFileIO)
        {
            IPartDescriptor &part = allDataParts.item(partNo);
            RemoteFilename rfn;
            part.getFilename(copy, rfn);
            Owned<IFile> iFile = createIFile(rfn);

            unsigned encryptedKeyLen;
            void *encryptedKey;
            helper->getFileEncryptKey(encryptedKeyLen,encryptedKey);
            Owned<IExpander> eexp;
            if (0 != encryptedKeyLen)
            {
                if (encrypted)
                    eexp.setown(createAESExpander256(encryptedKeyLen, encryptedKey));
                memset(encryptedKey, 0, encryptedKeyLen);
                free(encryptedKey);
            }
            if (nullptr != eexp.get())
                partIO.iFileIO = createCompressedFileReader(iFile, eexp);
            else if (compressed)
                partIO.iFileIO = createCompressedFileReader(iFile);
            else
                partIO.iFileIO = iFile->open(IFOread);
            if (!partIO.iFileIO)
                throw MakeStringException(0, "Failed to open fetch file part %u: %s", partNo, iFile->queryFilename());

            partIO.stream = createFileSerialStream(partIO.iFileIO, 0, (offset_t)-1, 0);

            unsigned expectedFormatCrc = helper->getDiskFormatCrc();
            IOutputMetaData *expectedFormat = helper->queryDiskRecordSize();
            // NB: potentially translation per part could be different if dealing with superkeys
            IPropertyTree &props = part.queryOwner().queryProperties();
            unsigned publishedFormatCrc = (unsigned)props.getPropInt("@formatCrc", 0);
            Owned<IOutputMetaData> publishedFormat = getDaliLayoutInfo(props);
            unsigned projectedFormatCrc = helper->getProjectedFormatCrc();
            IOutputMetaData *projectedFormat = helper->queryProjectedDiskRecordSize();
            RecordTranslationMode translationMode = getTranslationMode(*this);
            OwnedRoxieString fname = helper->getFileName();
            partIO.translator = getTranslators(fname, expectedFormatCrc, expectedFormat, publishedFormatCrc, publishedFormat, projectedFormatCrc, projectedFormat, translationMode);
            if (partIO.translator)
            {
                partIO.prefetcher = partIO.translator->queryActualFormat().createDiskPrefetcher();
                dbgassertex(partIO.prefetcher);
            }
            else
                partIO.prefetcher = expectedFormat->createDiskPrefetcher();
        }
        return partIO;
    }
    unsigned queryMaxHandlers(HandlerType hType)
    {
        switch (hType)
        {
            case ht_remotekeylookup:
                return maxNumRemoteHandlersPerSlave;
            case ht_localkeylookup:
                return maxNumLocalHandlers;
            case ht_remotefetch:
                return maxNumRemoteFetchHandlers;
            case ht_localfetch:
                return maxNumLocalFetchHandlers;
            default:
                throwUnexpected();
        }
        return 0;
    }
    void doAbortLimit(CJoinGroup *jg)
    {
        helper->onMatchAbortLimitExceeded();
        CommonXmlWriter xmlwrite(0);
        if (inputHelper && inputHelper->queryOutputMeta() && inputHelper->queryOutputMeta()->hasXML())
            inputHelper->queryOutputMeta()->toXML((byte *) jg->queryLeft(), xmlwrite);
        throw MakeActivityException(this, 0, "More than %d match candidates in keyed join for row %s", abortLimit, xmlwrite.str());
    }
    bool checkAbortLimit(CJoinGroup *joinGroup)
    {
        if (joinGroup->hasAbortLimitBeenHit())
        {
            if (0 == (joinFlags & JFmatchAbortLimitSkips))
                doAbortLimit(joinGroup);
            return true;
        }
        return false;
    }
    bool abortLimitAction(CJoinGroup *jg, OwnedConstThorRow &row)
    {
        Owned<IException> abortLimitException;
        try
        {
            return checkAbortLimit(jg);
        }
        catch (IException *_e)
        {
            if (!onFailTransform)
                throw;
            abortLimitException.setown(_e);
        }
        RtlDynamicRowBuilder trow(queryRowAllocator());
        size32_t transformedSize = helper->onFailTransform(trow, jg->queryLeft(), defaultRight, 0, abortLimitException.get());
        if (0 != transformedSize)
            row.setown(trow.finalizeRowClear(transformedSize));
        return true;
    }
    void queueLookupForPart(unsigned partNo, const void *indexLookupRow)
    {
        // NB: only 1 thread calling this method, so call to lookupHandler->queueLookup() doesn't need protecting
        KeyLookupHeader lookupKeyHeader;
        getHeaderFromRow(indexLookupRow, lookupKeyHeader);
        lookupKeyHeader.jg->incPending(); // each queued lookup pending a result

        CLookupHandler *lookupHandler = keyLookupHandlers.queryHandler(partNo);
        lookupHandler->queueLookup(indexLookupRow, partNo);
    }
    unsigned getTlkKeyManagers(IArrayOf<IKeyManager> &keyManagers)
    {
        keyManagers.clear();
        ForEachItemIn(i, tlkKeyIndexes)
        {
            IKeyIndex *tlkKeyIndex = &tlkKeyIndexes.item(i);
            const RtlRecord &keyRecInfo = helper->queryIndexRecordSize()->queryRecordAccessor(true);
            Owned<IKeyManager> tlkManager = createLocalKeyManager(keyRecInfo, nullptr, nullptr, helper->hasNewSegmentMonitors(), false);
            tlkManager->setKey(tlkKeyIndex);
            keyManagers.append(*tlkManager.getClear());
        }
        return tlkKeyIndexes.ordinality();
    }
    IKeyIndex *createPartKeyIndex(unsigned partNo, unsigned copy, bool delayed)
    {
        IPartDescriptor &filePart = allIndexParts.item(partNo);
        unsigned crc=0;
        filePart.getCrc(crc);
        RemoteFilename rfn;
        filePart.getFilename(copy, rfn);
        StringBuffer filename;
        rfn.getPath(filename);

        if (delayed)
        {
            Owned<IFileIO> lazyFileIO = queryThor().queryFileCache().lookupIFileIO(*this, indexName, filePart);
            Owned<IDelayedFile> delayedFile = createDelayedFile(lazyFileIO);
            return createKeyIndex(filename, crc, *delayedFile, false, false);
        }
        else
        {
            /* NB: createKeyIndex here, will load the key immediately
             * But that's okay, because we are only here on demand.
             * The underlying IFileIO can later be closed by fhe file caching mechanism.
             */
            Owned<IFileIO> lazyIFileIO = queryThor().queryFileCache().lookupIFileIO(*this, indexName, filePart);
            return createKeyIndex(filename, crc, *lazyIFileIO, false, false);
        }
    }
    IKeyManager *createPartKeyManager(unsigned partNo, unsigned copy)
    {
        Owned<IKeyIndex> keyIndex = createPartKeyIndex(partNo, copy, false);
        return createLocalKeyManager(helper->queryIndexRecordSize()->queryRecordAccessor(true), keyIndex, nullptr, helper->hasNewSegmentMonitors(), false);
    }
    const void *preparePendingLookupRow(void *row, size32_t maxSz, const void *lhsRow, size32_t keySz)
    {
        CJoinGroup *jg = new (rowManager, queryId()) CJoinGroup(lhsRow, this);
        memcpy(row, &jg, sizeof(CJoinGroup *)); // NB: row will release joinGroup on destruction
        jg->incPending(); // prevent complete, must be an paired decPending() at some point
        return keyLookupRowWithJGAllocator->finalizeRow(sizeof(KeyLookupHeader)+keySz, row, maxSz);
    }
    CJoinGroup *queueLookup(const void *lhsRow)
    {
        RtlDynamicRowBuilder keyFieldsRowBuilder(keyLookupRowWithJGAllocator);
        CPrefixedRowBuilder keyFieldsPrefixBuilder(sizeof(KeyLookupHeader), keyFieldsRowBuilder);
        size32_t keyedFieldsRowSize = helper->extractIndexReadFields(keyFieldsPrefixBuilder, lhsRow);
        OwnedConstThorRow indexLookupRow;
        const void *keyedFieldsRow = keyFieldsPrefixBuilder.row();
        if (keyHasTlk)
        {
            if (!tlkKeyManagers.ordinality())
                getTlkKeyManagers(tlkKeyManagers);

            ForEachItemIn(whichKm, tlkKeyManagers)
            {
                IKeyManager &keyManager = tlkKeyManagers.item(whichKm);
                helper->createSegmentMonitors(&keyManager, keyedFieldsRow);
                keyManager.finishSegmentMonitors();
                keyManager.reset();
                if (partitionKey)
                {
                    unsigned partNo = keyManager.getPartition();  // Returns 0 if no partition info, or filter cannot be partitioned
                    if (partNo)
                    {
                        partNo = superWidth ? superWidth*whichKm+(partNo-1) : partNo-1;
                        if (!container.queryLocalData() || keyLookupHandlers.queryHandler(partNo))
                        {
                            if (!indexLookupRow)
                                indexLookupRow.setown(preparePendingLookupRow(keyFieldsRowBuilder.getUnfinalizedClear(), keyFieldsRowBuilder.getMaxLength(), lhsRow, keyedFieldsRowSize));
                            queueLookupForPart(partNo, indexLookupRow);
                        }
                    }
                    else
                    {
                        if (!indexLookupRow)
                            indexLookupRow.setown(preparePendingLookupRow(keyFieldsRowBuilder.getUnfinalizedClear(), keyFieldsRowBuilder.getMaxLength(), lhsRow, keyedFieldsRowSize));
                        for (unsigned p=0; p<totalIndexParts; p++)
                        {
                            CLookupHandler *lookupHandler = keyLookupHandlers.queryHandler(p);
                            if (lookupHandler)
                                queueLookupForPart(p, indexLookupRow);
                        }
                    }
                }
                else
                {
                    while (keyManager.lookup(false))
                    {
                        offset_t slave = extractFpos(&keyManager);
                        if (slave) // don't bail out if part0 match, test again for 'real' tlk match.
                        {
                            unsigned partNo = (unsigned)slave;
                            partNo = superWidth ? superWidth*whichKm+(partNo-1) : partNo-1;
                            if (container.queryLocalData())
                            {
                                if (nullptr == keyLookupHandlers.queryHandler(partNo))
                                    continue;
                            }
                            if (!indexLookupRow)
                                indexLookupRow.setown(preparePendingLookupRow(keyFieldsRowBuilder.getUnfinalizedClear(), keyFieldsRowBuilder.getMaxLength(), lhsRow, keyedFieldsRowSize));
                            queueLookupForPart(partNo, indexLookupRow);
                        }
                    }
                }
                keyManager.releaseSegmentMonitors();
            }
        }
        else // rootless or local key
        {
            indexLookupRow.setown(preparePendingLookupRow(keyFieldsRowBuilder.getUnfinalizedClear(), keyFieldsRowBuilder.getMaxLength(), lhsRow, keyedFieldsRowSize));
            if (keyLookupHandlers.isLocalKey())
                queueLookupForPart(0, indexLookupRow);
            else if (!remoteKeyedLookup) // either local only or legacy, either way lookup in all allIndexParts I have
            {
                ForEachItemIn(p, allIndexParts)
                    queueLookupForPart(allIndexParts.item(p).queryPartIndex(), indexLookupRow);
            }
            else // global KJ, but rootless, need to make requests to all
            {
                for (unsigned p=0; p<totalIndexParts; p++)
                {
                    CLookupHandler *lookupHandler = keyLookupHandlers.queryHandler(p);
                    if (lookupHandler)
                        queueLookupForPart(p, indexLookupRow);
                }
            }
        }
        if (!indexLookupRow)
            return nullptr;
        KeyLookupHeader lookupKeyHeader;
        getHeaderFromRow(indexLookupRow, lookupKeyHeader);
        return LINK(lookupKeyHeader.jg);
    }
    static int fetchPartLookup(const void *_key, const void *e)
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
    void queueFetchLookup(offset_t fpos, unsigned __int64 sequence, CJoinGroup *jg)
    {
        unsigned fetchPartNo;
        if (isLocalFpos(fpos))
        {
            fetchPartNo = getLocalFposPart(fpos);
            fpos = getLocalFposOffset(fpos);
        }
        else if (1 == totalDataParts)
            fetchPartNo = globalFPosToSlaveMap[0].index;
        else
        {
            FPosTableEntry *result = (FPosTableEntry *)bsearch(&fpos, &globalFPosToSlaveMap[0], globalFPosToSlaveMap.size(), sizeof(FPosTableEntry), fetchPartLookup);
            if (!result)
            {
                if (container.queryLocalData())
                    return;
                throw MakeThorException(TE_FetchOutOfRange, "FETCH: Offset not found in offset table; fpos=%" I64F "d", fpos);
            }
            fetchPartNo = result->index;
            fpos -= result->base;
        }

        CLookupHandler *fetchLookupHandler = fetchLookupHandlers.queryHandler(fetchPartNo);
        if (!fetchLookupHandler)
        {
            jg->decPending();

            // only poss. if ,LOCAL
            return;
        }

        // build request row
        RtlDynamicRowBuilder fetchInputRowBuilder(fetchInputMetaAllocator);
        FetchRequestHeader &header = *(FetchRequestHeader *)fetchInputRowBuilder.getUnfinalized();
        header.fpos = fpos;
        header.sequence = sequence;
        header.jg = jg;

        size32_t sz = sizeof(FetchRequestHeader);
        if (0 != helper->queryFetchInputRecordSize()->getMinRecordSize())
        {
            CPrefixedRowBuilder prefixBuilder(sizeof(FetchRequestHeader), fetchInputRowBuilder);
            sz += helper->extractFetchFields(prefixBuilder, jg->queryLeft());
        }
        OwnedConstThorRow fetchInputRow = fetchInputRowBuilder.finalizeRowClear(sz);
        fetchLookupHandler->queueLookupTS(fetchInputRow, fetchPartNo);
    }
    void stopReadAhead()
    {
        keyLookupHandlers.flush();
        keyLookupHandlers.join(); // wait for pending handling, there may be more fetch items as a result
        fetchLookupHandlers.flushTS();
        fetchLookupHandlers.join();

        // remote handlers will signal to other side that we are done
        keyLookupHandlers.end();
        fetchLookupHandlers.end();

        CriticalBlock b(onCompleteCrit); // protecting both pendingJoinGroupList and doneJoinGroupList
        endOfInput = true;
        bool expectedState = true;
        if (waitingForDoneGroups.compare_exchange_strong(expectedState, false))
            waitingForDoneGroupsSem.signal();
    }
    void readAhead()
    {
        endOfInput = false;
        CJoinGroup *groupStart = nullptr;
        do
        {
            if (queryAbortSoon())
                break;
            OwnedConstThorRow lhsRow = inputStream->nextRow();
            if (!lhsRow)
            {
                groupStart = nullptr; // NB: only ever set if preserveGroups on
                lhsRow.setown(inputStream->nextRow());
                if (!lhsRow)
                    break;
            }
            Linked<CJoinGroup> jg;
            if (helper->leftCanMatch(lhsRow))
                jg.setown(queueLookup(lhsRow)); // NB: will block if excessive amount queued
            else
                statsArr[AS_PreFiltered]++;
            if (!jg && ((joinFlags & JFleftonly) || (joinFlags & JFleftouter)))
            {
                size32_t maxSz;
                void *unfinalizedRow = keyLookupRowWithJGAllocator->createRow(maxSz);
                OwnedConstThorRow row = preparePendingLookupRow(unfinalizedRow, maxSz, lhsRow, 0);
                KeyLookupHeader lookupKeyHeader;
                getHeaderFromRow(row, lookupKeyHeader);
                jg.set(lookupKeyHeader.jg);
            }
            if (jg)
            {
                if (preserveGroups)
                {
                    if (!groupStart)
                    {
                        groupStart = jg;
                        groupStart->addFlag(GroupFlags::gf_head); // NB: used by onComplete to ensure start of group is queued 1st (as group entries are consumed gf_head moves forward, see onComplete)
                        groupStart->addFlag(GroupFlags::gf_start); // NB: start of group, causes eog when seen (if any proceeding grouped rows, see nextRow)
                    }
                }
                bool pendingBlock = false;
                {
                    CriticalBlock b(onCompleteCrit); // protecting both pendingJoinGroupList and doneJoinGroupList
                    pendingJoinGroupList.addToTail(LINK(jg));
                    pendingBlock = pendingKeyLookupLimiter.preIncNonBlocking(true);
                }
                jg->decPending(); // all lookups queued. joinGroup will complete when all lookups are done (i.e. they're running asynchronously)
                if (pendingBlock)
                {
                    if (preserveOrder || preserveGroups)
                    {
                        // some of the batches that are not yet queued may be holding up join groups that are ahead of others that are complete.
                        keyLookupHandlers.flush();
                        if (needsDiskRead)
                        {
                            /* because the key lookup threads could queue a bunch of disparate fetch batches, need to wait until done before flushing
                             * the ensuing fetch batches.
                             */
                            keyLookupHandlers.join();
                            fetchLookupHandlers.flushTS();
                        }
                    }
                    pendingKeyLookupLimiter.block();
                }
            }
        } while (!endOfInput);
        stopReadAhead();
    }
    const void *doDenormTransform(RtlDynamicRowBuilder &target, CJoinGroup &group)
    {
        offset_t fpos;
        CJoinGroup::JoinGroupRhsState rhsState;
        size32_t retSz = 0;
        OwnedConstThorRow lhs;
        lhs.set(group.queryLeft());
        const void *rhs = group.getFirstRhsClear(fpos, rhsState);
        switch (container.getKind())
        {
            case TAKkeyeddenormalize:
            {
                unsigned added = 0;
                unsigned idx = 0;
                while (rhs)
                {
                    ++idx;
                    size32_t transformedSize = helper->transform(target, lhs, rhs, fpos, idx);
                    if (transformedSize)
                    {
                        retSz = transformedSize;
                        added++;
                        lhs.setown(target.finalizeRowClear(transformedSize));
                        if (added==keepLimit)
                        {
                            ReleaseRoxieRow(rhs);
                            break;
                        }
                    }
                    ReleaseRoxieRow(rhs);
                    rhs = group.getNextRhsClear(fpos, rhsState);
                }
                if (retSz)
                    return lhs.getClear();
                break;
            }
            case TAKkeyeddenormalizegroup:
            {
                if (!rhs)
                    return nullptr;
                CThorExpandingRowArray rows(*this, nullptr, ers_forbidden, stableSort_none, true, ((unsigned)-1) != keepLimit ? keepLimit : 0);
                while (true) // NB: keepLimit must be >=1
                {
                    rows.append(rhs);
                    if (rows.ordinality() == keepLimit)
                        break;
                    rhs = group.getNextRhsClear(fpos, rhsState);
                    if (!rhs)
                        break;
                }
                retSz = helper->transform(target, lhs, rows.query(0), rows.ordinality(), rows.getRowArray());
                if (retSz)
                    return target.finalizeRowClear(retSz);
                break;
            }
            default:
                assertex(false);
        }
        return nullptr;
    }
    bool transferToDoneList(CJoinGroup *joinGroup, bool markBlocked) // NB: always coming from pendingJoinGroupList
    {
        doneJoinGroupList.addToTail(joinGroup);
        pendingKeyLookupLimiter.dec();
        return doneListLimiter.preIncNonBlocking(markBlocked);
    }
    void addPartToHandler(CHandlerContainer &handlerContainer, const std::vector<unsigned> &partToSlaveMap, unsigned partCopy, HandlerType hType, std::vector<unsigned> &handlerCounts, std::vector<std::vector<CLookupHandler *>> &slaveHandlers, std::vector<unsigned> &slaveHandlersRR)
    {
        // NB: This is called in partNo ascending order

        unsigned partNo = partCopy & partMask;
        unsigned copy = partCopy >> partBits;
        unsigned slave = 0;
        if (partToSlaveMap.size())
        {
            slave = partToSlaveMap[partNo];
            if (NotFound == slave) // part not local to cluster, part is handled locally/directly.
                slave = handlerCounts.size()-1; // last one reserved for out of cluster part handling.
            else
            {
                if (unknownCopyNum == copy)// this means that this is a part for a remote slave, and copy is unknown
                {
                    /* The partToSlaveMap encodes the target slave and the copy.
                     * Extract the copy, so that the correct copy is used when request
                     * arrives at the remote slave.
                     */
                    copy = slave >> slaveBits;
                    partCopy = partNo | (copy << partBits);
                }
                slave = slave & slaveMask;
            }
        }
        unsigned max = queryMaxHandlers(hType);
        unsigned &handlerCount = handlerCounts[slave];
        CLookupHandler *lookupHandler;
        if (handlerCount >= max) // allow multiple handlers (up to max) for the same slave, then RR parts onto existing handlers
        {
            // slaveHandlersRR used to track next to round-robin parts on to the available handlers for the appropriate slave.
            std::vector<CLookupHandler *> &handlers = slaveHandlers[slave];
            unsigned &next = slaveHandlersRR[slave];
            lookupHandler = handlers[next];
            ++next;
            if (next == handlers.size())
                next = 0;
        }
        else
        {
            switch (hType)
            {
                case ht_remotekeylookup:
                    lookupHandler = new CKeyLookupRemoteHandler(*this, slave+1); // +1 because 0 == master, 1st slave == 1
                    lookupHandler->setBatchSize(keyLookupQueuedBatchSize);
                    break;
                case ht_localkeylookup:
                    lookupHandler = new CKeyLookupLocalHandler(*this);
                    lookupHandler->setBatchSize(keyLookupQueuedBatchSize);
                    break;
                case ht_remotefetch:
                    lookupHandler = new CFetchRemoteLookupHandler(*this, slave+1);
                    lookupHandler->setBatchSize(fetchLookupQueuedBatchSize);
                    break;
                case ht_localfetch:
                    lookupHandler = new CFetchLocalLookupHandler(*this);
                    lookupHandler->setBatchSize(fetchLookupQueuedBatchSize);
                    break;
                default:
                    throwUnexpected();
            }
            handlerContainer.handlers.append(lookupHandler);
            slaveHandlers[slave].push_back(lookupHandler);
        }
        ++handlerCount;
        lookupHandler->addPartNum(partCopy);
        dbgassertex(partNo == handlerContainer.partIdxToHandler.size());
        handlerContainer.partIdxToHandler.push_back(lookupHandler);
    }
    void setupLookupHandlers(CHandlerContainer &handlerContainer, unsigned totalParts, ISuperFileDescriptor *superFdesc, std::vector<unsigned> &parts, const std::vector<unsigned> &partToSlaveMap, bool localKey, HandlerType localHandlerType, HandlerType missingHandlerType)
    {
        handlerContainer.clear();

        if (localKey && parts.size()>1)
        {
            /* JCSMORE - using key merger, which must deal with IKeyIndex's directly
             * would be better to implement a merger and had ability to have separate handlers per local index part
             */
            CLookupHandler *lookupHandler = new CKeyLookupMergeHandler(*this);
            lookupHandler->setBatchSize(keyLookupQueuedBatchSize);
            handlerContainer.handlers.append(lookupHandler);
            handlerContainer.partIdxToHandler.push_back(lookupHandler);
            handlerContainer.localKey = true;
            for (auto &partCopy: parts)
                lookupHandler->addPartNum(partCopy);
        }
        else
        {
            unsigned numParts = parts.size();

            std::vector<std::vector<CLookupHandler *>> slaveHandlers;
            std::vector<unsigned> handlerCounts;
            std::vector<unsigned> slaveHandlersRR;
            bool remoteLookup = partToSlaveMap.size()>0;
            unsigned slaves = remoteLookup ? queryJob().querySlaves() : 1; // if no map, all parts are treated as if local
            unsigned numHandlers = slaves+1; // +1 is for off cluster parts, which will be handled by local/direct handlers
            for (unsigned s=0; s<numHandlers; s++)
            {
                handlerCounts.push_back(0);
                slaveHandlersRR.push_back(0);
            }
            slaveHandlers.resize(numHandlers);

            unsigned currentPart = 0;
            unsigned p = 0;
            unsigned partNo = 0;
            unsigned partCopy = 0;
            while (p<totalParts)
            {
                if (currentPart<numParts)
                {
                    partCopy = parts[currentPart++];
                    partNo = partCopy & partMask;
                    if (superFdesc)
                    {
                        unsigned copy = partCopy >> partBits;
                        unsigned subfile, subpartnum;
                        superFdesc->mapSubPart(partNo, subfile, subpartnum);
                        partNo = superWidth*subfile+subpartnum;
                        partCopy = partNo | (copy << partBits);
                    }
                }
                else
                {
                    partNo = totalParts;
                    partCopy = 0;
                }

                // create remote handlers for non-local parts
                while (p<partNo)
                {
                    if (remoteLookup) // NB: only relevant if ,LOCAL and only some parts avail. otherwise if !remoteLookup all parts will have been sent
                    {
                        unsigned remotePartCopy = p | (unknownCopyNum << partBits); // copy will be looked up via map in addPartToHandler
                        addPartToHandler(handlerContainer, partToSlaveMap, remotePartCopy, missingHandlerType, handlerCounts, slaveHandlers, slaveHandlersRR);
                    }
                    else // no handler if local KJ and part not local
                        handlerContainer.partIdxToHandler.push_back(nullptr);
                    ++p;
                }

                if (p==totalParts)
                    break;
                addPartToHandler(handlerContainer, partToSlaveMap, partCopy, localHandlerType, handlerCounts, slaveHandlers, slaveHandlersRR);
                ++p;
            }
        }
        handlerContainer.trace();
    }
public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    CKeyedJoinSlave(CGraphElementBase *_container) : PARENT(_container), readAheadThread(*this), statsArr(8)
    {
        helper = static_cast <IHThorKeyedJoinArg *> (queryHelper());
        reInit = 0 != (helper->getFetchFlags() & (FFvarfilename|FFdynamicfilename)) || (helper->getJoinFlags() & JFvarindexfilename);

        keyLookupQueuedBatchSize = getOptInt(THOROPT_KEYLOOKUP_QUEUED_BATCHSIZE, defaultKeyLookupQueuedBatchSize);
        maxKeyLookupThreads = getOptInt(THOROPT_KEYLOOKUP_MAX_THREADS, defaultMaxKeyLookupThreads);
        maxFetchThreads = getOptInt(THOROPT_KEYLOOKUP_MAX_FETCH_THREADS, defaultMaxFetchThreads);
        keyLookupMaxQueued = getOptInt(THOROPT_KEYLOOKUP_MAX_QUEUED, defaultKeyLookupMaxQueued);
        keyLookupMaxDone = getOptInt(THOROPT_KEYLOOKUP_MAX_DONE, defaultKeyLookupMaxDone);
        keyLookupMinQueuedBytes = getOptInt(THOROPT_KEYLOOKUP_MIN_MB, defaultMinKeyLookupJoinGroupMB) * 0x100000;
        maxNumLocalHandlers = getOptInt(THOROPT_KEYLOOKUP_MAX_LOCAL_HANDLERS, defaultKeyLookupMaxLocalHandlers);
        maxNumRemoteHandlersPerSlave = getOptInt(THOROPT_KEYLOOKUP_MAX_REMOTE_HANDLERS, defaultKeyLookupMaxHandlersPerRemoteSlave);
        maxNumLocalFetchHandlers = getOptInt(THOROPT_KEYLOOKUP_MAX_FETCH_LOCAL_HANDLERS, defaultKeyLookupMaxLocalFetchHandlers);
        maxNumRemoteFetchHandlers = getOptInt(THOROPT_KEYLOOKUP_MAX_FETCH_REMOTE_HANDLERS, defaultKeyLookupMaxFetchHandlers);
        forceRemoteKeyedLookup = getOptBool(THOROPT_FORCE_REMOTE_KEYED_LOOKUP);
        forceRemoteKeyedFetch = getOptBool(THOROPT_FORCE_REMOTE_KEYED_FETCH);
        keyLookupProcessBatchLimit = getOptInt(THOROPT_KEYLOOKUP_PROCESS_BATCHLIMIT, defaultKeyLookupProcessBatchLimit);
        fetchLookupProcessBatchLimit = getOptInt(THOROPT_FETCHLOOKUP_PROCESS_BATCHLIMIT, defaultFetchLookupProcessBatchLimit);
        messageCompression = getOptBool(THOROPT_KEYLOOKUP_COMPRESS_MESSAGES, true);

        fetchLookupQueuedBatchSize = getOptInt(THOROPT_KEYLOOKUP_FETCH_QUEUED_BATCHSIZE, defaultKeyLookupFetchQueuedBatchSize);

        // JCSMORE - would perhaps be better to have combined limit, but would need to avoid lookup threads starving fetch threads
        lookupThreadLimiter.set(maxKeyLookupThreads);
        fetchThreadLimiter.set(maxFetchThreads);

        pendingKeyLookupLimiter.set(keyLookupMaxQueued, 10); // leeway = 10%
        doneListLimiter.set(keyLookupMaxDone, 10); // leeway = 10%

        transformAllocator.setown(getRowAllocator(queryOutputMeta(), (roxiemem::RoxieHeapFlags)(queryHeapFlags()|roxiemem::RHFpacked|roxiemem::RHFunique), AT_Transform));
        rowManager = queryJobChannel().queryThorAllocator()->queryRowManager();

        class CKeyLookupRowOutputMetaData : public CPrefixedOutputMeta
        {
        public:
            CKeyLookupRowOutputMetaData(size32_t offset, IOutputMetaData *original) : CPrefixedOutputMeta(offset, original) { }
            virtual unsigned getMetaFlags() { return original->getMetaFlags() | MDFneeddestruct; }
            virtual void destruct(byte * self) override
            {
                CJoinGroup *joinGroup;
                memcpy(&joinGroup, self, sizeof(CJoinGroup *));
                joinGroup->Release();
                CPrefixedOutputMeta::destruct(self);
            }
        };
        Owned<IOutputMetaData> keyLookupRowOutputMetaData = new CKeyLookupRowOutputMetaData(sizeof(KeyLookupHeader), helper->queryIndexReadInputRecordSize());
        keyLookupRowWithJGRowIf.setown(createRowInterfaces(keyLookupRowOutputMetaData, (roxiemem::RoxieHeapFlags)(queryHeapFlags()|roxiemem::RHFpacked|roxiemem::RHFunique), AT_LookupWithJG));
        keyLookupRowWithJGAllocator = keyLookupRowWithJGRowIf->queryRowAllocator();

        joinFieldsAllocator.setown(getRowAllocator(helper->queryJoinFieldsRecordSize(), roxiemem::RHFnone, AT_JoinFields));
        keyLookupReplyOutputMetaRowIf.setown(createRowInterfaces(helper->queryJoinFieldsRecordSize(), AT_LookupResponse));

        Owned<IOutputMetaData> fetchInputMeta = new CPrefixedOutputMeta(sizeof(FetchRequestHeader), helper->queryFetchInputRecordSize());
        fetchInputMetaRowIf.setown(createRowInterfaces(fetchInputMeta, AT_FetchRequest));
        fetchInputMetaAllocator.set(fetchInputMetaRowIf->queryRowAllocator());

        Owned<IOutputMetaData> fetchOutputMeta = createOutputMetaDataWithChildRow(joinFieldsAllocator, sizeof(FetchReplyHeader));
        fetchOutputMetaRowIf.setown(createRowInterfaces(fetchOutputMeta, AT_FetchResponse));

        appendOutputLinked(this);
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        if (!initialized)
        {
            initialized = true;
            joinFlags = helper->getJoinFlags();
            needsDiskRead = helper->diskAccessRequired();
            numStats = needsDiskRead ? 8 : 5;
            fixedRecordSize = helper->queryIndexRecordSize()->getFixedSize(); // 0 if variable and unused
            onFailTransform = (0 != (joinFlags & JFonfail)) && (0 == (joinFlags & JFmatchAbortLimitSkips));

            if (onFailTransform || (joinFlags & JFleftouter))
            {
                RtlDynamicRowBuilder rr(joinFieldsAllocator);
                size32_t sz = helper->createDefaultRight(rr);
                defaultRight.setown(rr.finalizeRowClear(sz));
            }
        }
        else
        {
            tags.clear();
            tlkKeyIndexes.kill();
            allIndexParts.kill();
            localIndexParts.clear();
            localFetchPartMap.clear();

            allDataParts.kill();
            globalFPosToSlaveMap.clear();
            keyLookupHandlers.clear();
            fetchLookupHandlers.clear();
            openFetchParts.clear();

            indexPartToSlaveMap.clear();
            dataPartToSlaveMap.clear();
            tlkKeyManagers.kill();
            partitionKey = false;
        }
        for (auto &a : statsArr)
            a = 0;
        // decode data from master. NB: can be resent and differ if in global loop
        data.read(indexName);
        data.read(totalIndexParts);
        if (totalIndexParts)
        {
            unsigned numTags;
            data.read(numTags);
            unsigned t;
            for (t=0; t<numTags; t++)
            {
                mptag_t tag = container.queryJobChannel().deserializeMPTag(data);
                tags.push_back(tag);
                queryJobChannel().queryJobComm().flush(tag);
            }
            data.read(remoteKeyedLookup);
            data.read(remoteKeyedFetch);

            // NB: if master has determined off, then force has no effect.
            if (!remoteKeyedLookup) forceRemoteKeyedLookup = false;
            if (!remoteKeyedFetch) forceRemoteKeyedFetch = false;

            data.read(superWidth);
            data.read(keyHasTlk);
            if (keyHasTlk)
            {
                MemoryBuffer tlkMb;
                unsigned tlks;
                data.read(tlks);
                UnsignedArray posArray, lenArray;
                size32_t tlkSz;
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
                    Owned<IKeyIndex> tlkKeyIndex = createKeyIndex(name.append(p).str(), 0, *iFileIO, true, false); // MORE - not the right crc
                    tlkKeyIndexes.append(*tlkKeyIndex.getClear());
                }
            }
            unsigned numIndexParts;
            data.read(numIndexParts);
            bool localKey = false;
            if (numIndexParts)
            {
                deserializePartFileDescriptors(data, allIndexParts);
                IFileDescriptor &indexFileDesc = allIndexParts.item(0).queryOwner();
                partitionKey = indexFileDesc.queryProperties().hasProp("@partitionFieldMask");
                localKey = !partitionKey && indexFileDesc.queryProperties().getPropBool("@local", false);

                unsigned numMappedParts;
                data.read(numMappedParts);
                if (numMappedParts)
                {
                    localIndexParts.resize(numMappedParts);
                    data.read(numMappedParts * sizeof(unsigned), &localIndexParts[0]);
                }
                if (remoteKeyedLookup)
                {
                    indexPartToSlaveMap.resize(totalIndexParts);
                    data.read(totalIndexParts * sizeof(unsigned), &indexPartToSlaveMap[0]);
                }
                else if (container.queryLocalData())
                    totalIndexParts = numIndexParts; // will be same unless local data only
            }

            ISuperFileDescriptor *superFdesc = numIndexParts ? allIndexParts.item(0).queryOwner().querySuperFileDescriptor() : nullptr;
            setupLookupHandlers(keyLookupHandlers, totalIndexParts, superFdesc, localIndexParts, indexPartToSlaveMap, localKey, forceRemoteKeyedLookup ? ht_remotekeylookup : ht_localkeylookup, ht_remotekeylookup);
            data.read(totalDataParts);
            if (totalDataParts)
            {
                unsigned numDataParts;
                data.read(numDataParts);
                if (numDataParts)
                    deserializePartFileDescriptors(data, allDataParts);
                unsigned numMappedParts;
                data.read(numMappedParts);
                if (numMappedParts)
                {
                    localFetchPartMap.resize(numMappedParts);
                    data.read(numMappedParts * sizeof(unsigned), &localFetchPartMap[0]);
                }
                if (remoteKeyedFetch)
                {
                    dataPartToSlaveMap.resize(totalDataParts);
                    data.read(totalDataParts * sizeof(unsigned), &dataPartToSlaveMap[0]);
                }
                else if (container.queryLocalData())
                    totalDataParts = numDataParts;
                ISuperFileDescriptor *superFdesc = numDataParts ? allDataParts.item(0).queryOwner().querySuperFileDescriptor() : nullptr;
                setupLookupHandlers(fetchLookupHandlers, totalDataParts, superFdesc, localFetchPartMap, dataPartToSlaveMap, false, forceRemoteKeyedFetch ? ht_remotefetch : ht_localfetch, ht_remotefetch);
                globalFPosToSlaveMap.resize(totalDataParts);
                FPosTableEntry *e;
                unsigned f;
                for (f=0, e=&globalFPosToSlaveMap[0]; f<totalDataParts; f++, e++)
                {
                    IPartDescriptor &part = allDataParts.item(f);
                    e->base = part.queryProperties().getPropInt64("@offset");
                    e->top = e->base + part.queryProperties().getPropInt64("@size");
                    e->index = f;
                }
                std::sort(globalFPosToSlaveMap.begin(), globalFPosToSlaveMap.end(), [](const FPosTableEntry &a, const FPosTableEntry &b) { return a.base < b.base; });
#ifdef _DEBUG
                for (unsigned c=0; c<totalDataParts; c++)
                {
                    FPosTableEntry &e = globalFPosToSlaveMap[c];
                    ActPrintLog("Table[%d] : base=%" I64F "d, top=%" I64F "d, slave=%d", c, e.base, e.top, e.index);
                }
#endif
            }
        }
        ActPrintLog("Remote Keyed Lookups = %s (forced = %s), remote fetch = %s (forced = %s)", boolToStr(remoteKeyedLookup), boolToStr(forceRemoteKeyedLookup), boolToStr(remoteKeyedFetch), boolToStr(forceRemoteKeyedFetch));
    }
// IThorDataLink
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        assertex(inputs.ordinality() == 1);
        PARENT::start();

        keepLimit = helper->getKeepLimit();
        atMost = helper->getJoinLimit();
        if (atMost == 0)
        {
            if (JFleftonly == (joinFlags & JFleftonly))
                keepLimit = 1; // don't waste time and memory collating and returning record which will be discarded.
            atMost = (unsigned)-1;
        }
        abortLimit = helper->getMatchAbortLimit();
        if (abortLimit == 0) abortLimit = (unsigned)-1;
        if (keepLimit == 0) keepLimit = (unsigned)-1;
        if (abortLimit < atMost)
            atMost = abortLimit;
        rowLimit = (rowcount_t)helper->getRowLimit();
        if (rowLimit < keepLimit)
            keepLimit = rowLimit+1; // if keepLimit is small, let it reach rowLimit+1, but any more is pointless and a waste of time/resources.
        helper->serializeStartContext(startCtxMb.clear());

        inputHelper.set(input->queryFromActivity()->queryContainer().queryHelper());
        preserveOrder = 0 == (joinFlags & JFreorderable);
        preserveGroups = input->isGrouped();
        ActPrintLog("KJ: preserveGroups=%s, preserveOrder=%s", preserveGroups?"true":"false", preserveOrder?"true":"false");

        currentMatchIdx = 0;
        rhsState.clear();
        currentAdded = 0;
        currentJoinGroupSize = 0;
        eos = false;
        endOfInput = false;
        lookupThreadLimiter.reset();
        fetchThreadLimiter.reset();
        keyLookupHandlers.init();
        fetchLookupHandlers.init();
        pendingKeyLookupLimiter.reset();
        doneListLimiter.reset();
        pendingJoinGroupList.clear();
        doneJoinGroupList.clear();
        currentJoinGroup.clear();
        waitingForDoneGroups = false;

        if ((pendingKeyLookupLimiter.queryMax() > minimumQueueLimit) || (doneListLimiter.queryMax() > minimumQueueLimit))
        {
            memoryCallbackInstalled = true;
            queryRowManager()->addRowBuffer(this);
        }

        readAheadThread.start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        OwnedConstThorRow ret;
        while (!abortSoon && !eos)
        {
            if (!currentJoinGroup)
            {
                while (true)
                {
                    {
                        CriticalBlock b(onCompleteCrit);
                        currentJoinGroup.setown(doneJoinGroupList.removeHead());
                        if (currentJoinGroup)
                        {
                            doneListLimiter.dec();
                            break;
                        }
                        if (endOfInput)
                        {
                            // if disk fetch involved, then there may still be pending rows
                            if (!pendingJoinGroupList.queryCount())
                            {
                                eos = true;
                                // all done
                                return nullptr;
                            }
                        }
                        waitingForDoneGroups = true;
                    }
                    waitingForDoneGroupsSem.wait();
                }
                bool eog = false;
                if (preserveGroups)
                {
                    currentJoinGroupSize += currentAdded;
                    if (currentJoinGroup->hasFlag(GroupFlags::gf_start))
                        eog = 0 != currentJoinGroupSize;
                    currentJoinGroupSize = 0;
                }
                currentMatchIdx = 0;
                rhsState.clear();
                currentAdded = 0;
                if (eog)
                    return nullptr;
            }
            if ((0 == currentMatchIdx) && abortLimitAction(currentJoinGroup, ret)) // only any point in checking 1st Idx
                currentJoinGroup.clear();
            else
            {
                RtlDynamicRowBuilder rowBuilder(transformAllocator, false);
                size32_t transformedSize = 0;
                if (currentJoinGroup->isEmpty() || currentJoinGroup->hasAtMostLimitBeenHit())
                {
                    switch (joinFlags & JFtypemask)
                    {
                        case JFleftouter:
                        case JFleftonly:
                            switch (container.getKind())
                            {
                                case TAKkeyedjoin:
                                {
                                    transformedSize = helper->transform(rowBuilder.ensureRow(), currentJoinGroup->queryLeft(), defaultRight, (__uint64)0, 0U);
                                    if (transformedSize)
                                        ret.setown(rowBuilder.finalizeRowClear(transformedSize));
                                    break;
                                }
                                case TAKkeyeddenormalize:
                                {
                                    // return lhs, already finalized
                                    ret.set(currentJoinGroup->queryLeft());
                                    break;
                                }
                                case TAKkeyeddenormalizegroup:
                                {
                                    transformedSize = helper->transform(rowBuilder.ensureRow(), currentJoinGroup->queryLeft(), NULL, 0, (const void **)NULL); // no dummyrhs (hthor and roxie don't pass)
                                    if (transformedSize)
                                        ret.setown(rowBuilder.finalizeRowClear(transformedSize));
                                    break;
                                }
                                default:
                                    throwUnexpected();
                            }
                            if (ret)
                                currentAdded++;
                    }
                    currentJoinGroup.clear();
                }
                else if (!(joinFlags & JFexclude))
                {
                    // will be at least 1 rhs match to be in this branch
                    switch (container.getKind())
                    {
                        case TAKkeyedjoin:
                        {
                            rowBuilder.ensureRow();
                            offset_t fpos;
                            OwnedConstThorRow rhs;
                            if (0 == currentMatchIdx)
                                rhs.setown(currentJoinGroup->getFirstRhsClear(fpos, rhsState));
                            else
                                rhs.setown(currentJoinGroup->getNextRhsClear(fpos, rhsState));
                            while (true)
                            {
                                if (!rhs)
                                {
                                    currentJoinGroup.clear();
                                    break;
                                }
                                ++currentMatchIdx;
                                transformedSize = helper->transform(rowBuilder, currentJoinGroup->queryLeft(), rhs, fpos, currentMatchIdx);
                                if (transformedSize)
                                {
                                    ret.setown(rowBuilder.finalizeRowClear(transformedSize));
                                    currentAdded++;
                                    if (currentAdded==keepLimit)
                                        currentJoinGroup.clear();
                                    break;
                                }
                                rhs.setown(currentJoinGroup->getNextRhsClear(fpos, rhsState));
                            }
                            break;
                        }
                        case TAKkeyeddenormalize:
                        case TAKkeyeddenormalizegroup:
                        {
                            ret.setown(doDenormTransform(rowBuilder, *currentJoinGroup));
                            currentJoinGroup.clear();
                            break;
                        }
                        default:
                            assertex(false);
                    }
                }
                else
                    currentJoinGroup.clear();
            }
            if (ret)
            {
                // NB: If this KJ is a global activity, there will be an associated LIMIT activity beyond the KJ. This check spots if limit exceeded a slave level.
                if (getDataLinkCount()+1 > rowLimit)
                    helper->onLimitExceeded();

                dataLinkIncrement();
                return ret.getClear();
            }
        }
        return nullptr;
    }
    virtual void stop() override
    {
        endOfInput = true; // signals to readAhead which is reading input, that is should stop asap.

        // could be blocked in readAhead(), because CJoinGroup's are no longer being processed
        pendingKeyLookupLimiter.disable();
        doneListLimiter.disable();

        readAheadThread.join();
        keyLookupHandlers.stop();
        fetchLookupHandlers.stop();

        if (memoryCallbackInstalled)
        {
            queryRowManager()->removeRowBuffer(this);
            memoryCallbackInstalled = false;
        }

        PARENT::stop();
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.canStall = true;
        info.unknownRowsOutput = true;
    }
    virtual void serializeStats(MemoryBuffer &mb) override
    {
        PARENT::serializeStats(mb);
        for (unsigned s=0; s<numStats; s++)
            mb.append(statsArr[s]);
    }
    // IJoinProcessor
    virtual void onComplete(CJoinGroup *joinGroup) override
    {
        if (hasStopped())
            return;
        bool doneListMaxHit = false;
        // moves complete CJoinGroup's from pending list to done list
        {
            CriticalBlock b(onCompleteCrit); // protecting both pendingJoinGroupList and doneJoinGroupList
            if (preserveOrder)
            {
                CJoinGroup *head = pendingJoinGroupList.queryHead();
                if (joinGroup != head)
                    return;
                do
                {
                    if (!head->complete())
                    {
                        if (head == joinGroup) // i.e. none ready
                            return;
                        else
                            break;
                    }
                    head = head->next;
                    CJoinGroup *doneJG = pendingJoinGroupList.removeHead();
                    if (transferToDoneList(doneJG, !doneListMaxHit)) // 2nd param(markBlocked), record this thread will block once only
                        doneListMaxHit = true;
                } while (head);
            }
            else if (preserveGroups)
            {
                // NB: when preserveGroups, the lhs group will always be complete at same time, so this will traverse whole group
                if (!joinGroup->hasFlag(GroupFlags::gf_head))
                    return; // intermediate rows are completing, but can't output any of those until head finishes, at which point head marker will shift to next if necessary (see below)
                unsigned numProcessed = 0;
                CJoinGroup *current = joinGroup;
                do
                {
                    if (!current->complete())
                    {
                        dbgassertex(numProcessed); // if onComplete called for a group, there should always be at least 1 complete joinGroup ready starting from signalled joinGroup
                        // update current so now marked as new head of group, so that when it completes it will be processed.
                        current->addFlag(GroupFlags::gf_head);
                        break;
                    }
                    CJoinGroup *next = current->next;
                    CJoinGroup *doneJG = pendingJoinGroupList.remove(current);
                    if (transferToDoneList(doneJG, !doneListMaxHit)) // 2nd param(markBlocked), record this thread will block once only
                        doneListMaxHit = true;
                    current = next;
                    ++numProcessed;
                } while (current);
            }
            else
            {
                CJoinGroup *doneJG = pendingJoinGroupList.remove(joinGroup);
                doneListMaxHit = transferToDoneList(doneJG, true);
            }
            bool expectedState = true;
            if (waitingForDoneGroups.compare_exchange_strong(expectedState, false))
                waitingForDoneGroupsSem.signal();
        }
        if (doneListMaxHit) // outside of crit, done group dequeue and signal may already have happened
            doneListLimiter.block();
    }
    virtual unsigned addRowEntry(unsigned partNo, const void *rhs, offset_t fpos, RowArray *&rowArrays, unsigned &numRowArrays) override
    {
        dbgassertex(partNo<totalIndexParts);
        if (!rowArrays)
        {
            // If preserving order, a row array per handler/part is used to ensure order is preserved.
            numRowArrays = preserveOrder ? totalIndexParts : 1;
            rowArrays = (RowArray *)rowManager->allocate(sizeof(RowArray)*numRowArrays, queryId()); // NB: can't encode AT_JoinGroupRhsRows here with createCompoundActSeqId, because row manager limits act id to 2^20
            memset(rowArrays, 0, sizeof(RowArray)*numRowArrays);
        }
        RowArray &rowArray = rowArrays[preserveOrder ? partNo : 0];
        if (!rowArray.rows)
        {
            rowArray.rows = (Row *)rowManager->allocate(sizeof(Row), queryId()); // NB: can't encode AT_JoinGroupRhsRows here with createCompoundActSeqId, because row manager limits act id to 2^20
            rowArray.maxRows = RoxieRowCapacity(rowArray.rows) / sizeof(Row);
            rowArray.numRows = 0;
        }
        else if (rowArray.numRows==rowArray.maxRows)
        {
            if (rowArray.maxRows<4)
                ++rowArray.maxRows;
            else
                rowArray.maxRows += rowArray.maxRows/4;
            memsize_t newCapacity;
            rowManager->resizeRow(newCapacity, (void *&)rowArray.rows, RoxieRowCapacity(rowArray.rows), rowArray.maxRows*sizeof(Row), queryId());
            rowArray.maxRows = newCapacity / sizeof(Row);
        }
        Row &row = rowArray.rows[rowArray.numRows++];
        row.rhs = rhs;
        row.fpos = fpos;
        return rowArray.numRows-1;
    }
    virtual IRowStreamWithFpos *getJoinGroupStream(CJoinGroup *jg)
    {
        class CRowStreamWithFpos : public CSimpleInterfaceOf<IRowStreamWithFpos>
        {
            Owned<IFileIO> iFileIO;
            Linked<IFile> iFile;
            Linked<IRowInterfaces> rowIf;
            CThorContiguousRowBuffer prefetchBuffer;
            Owned<ISourceRowPrefetcher> prefetcher;
            Owned<ISerialStream> strm;
            IOutputRowDeserializer *deserializer = nullptr;
            IEngineRowAllocator *allocator = nullptr;
            std::vector<CJoinGroup::SpillMarker> spillMarkers;
            std::vector<offset_t> writeMarkerOffsets;
            size32_t remaining = 0;
            unsigned orderedParts = 0;
            unsigned currentPart = 0;
            bool eos = false;
        public:
            CRowStreamWithFpos(IFile *_iFile, IRowInterfaces *_rowIf, unsigned _orderedParts) : iFile(_iFile), rowIf(_rowIf), prefetchBuffer(nullptr), orderedParts(_orderedParts)
            {
                deserializer = rowIf->queryRowDeserializer();
                allocator = rowIf->queryRowAllocator();
                writeMarkerOffsets.resize(orderedParts);
            }
            ~CRowStreamWithFpos()
            {
                if (iFile)
                    iFile->remove();
            }
            virtual void open() override
            {
                dbgassertex(!strm);
                if (orderedParts)
                    iFileIO.setown(iFile->open(IFOread));
                else
                    iFileIO.setown(createCompressedFileReader(iFile));
                offset_t offset = 0;
                if (orderedParts)
                {
                    spillMarkers.resize(orderedParts);
                    size32_t len = sizeof(CJoinGroup::SpillMarker)*orderedParts;
                    iFileIO->read(0, len, &spillMarkers[0]);
                    currentPart = 0;
                    while (true)
                    {
                        if (spillMarkers[currentPart].size)
                        {
                            offset = ((currentPart+1) * sizeof(CJoinGroup::SpillMarker)) + spillMarkers[currentPart].skip;
                            break;
                        }
                        ++currentPart;
                        assertex(currentPart != orderedParts); // Should never get here, implies nothing written
                    }
                    remaining = spillMarkers[currentPart].size;
                }
                strm.setown(createFileSerialStream(iFileIO, offset));
                prefetchBuffer.setStream(strm);
            }
            virtual const void *getRow(offset_t &fpos) override
            {
                if (eos)
                    return nullptr;

                RtlDynamicRowBuilder rowBuilder(*allocator);
                size32_t size = deserializer->deserialize(rowBuilder, prefetchBuffer);
                const void *row = rowBuilder.finalizeRowClear(size);
                prefetchBuffer.read(sizeof(fpos), &fpos);
                prefetchBuffer.finishedRow();

                /* NB: eos is marked by hitting terminator marker if ordered
                 * If not, eos is at end of stream
                 */
                if (orderedParts)
                {
                    remaining -= size + sizeof(fpos);
                    if (0 == remaining)
                    {
                        CJoinGroup::SpillMarker marker;
                        prefetchBuffer.read(sizeof(marker), &marker);

                        remaining = marker.size;
                        if (0 == remaining)
                        {
                            while (true)
                            {
                                ++currentPart;
                                if (currentPart == orderedParts)
                                {
                                    eos = true;
                                    break;
                                }
                                if (spillMarkers[currentPart].size)
                                {
                                    offset_t offset = ((currentPart+1) * sizeof(CJoinGroup::SpillMarker)) + spillMarkers[currentPart].skip;
                                    if (prefetchBuffer.tell() != offset)
                                        prefetchBuffer.reset(offset);
                                    remaining = spillMarkers[currentPart].size;
                                    break;
                                }
                            }
                        }
                        else if (marker.skip) // if 0, implies same bucket spilt twice with no other inbetween.
                            prefetchBuffer.skip(marker.skip);
                    }
                }
                else if (prefetchBuffer.eos())
                    eos = true;
                if (eos)
                {
                    iFile->remove();
                    iFile.clear();
                }
                return row;
            }
            virtual void add(CJoinGroup *jg, bool first) override
            {
                bool append = !first;

                // If preserving order, can only spill simply if complete and have not spilt before.
                if (jg->complete() && first)
                    orderedParts = false; // no offset table to read back etc.
                if (orderedParts)
                    jg->write(iFile, rowIf->queryRowSerializer(), append, &writeMarkerOffsets);
                else
                    jg->write(iFile, rowIf->queryRowSerializer(), append);
            }
        };

        StringBuffer tmpFileName;
        GetTempName(tmpFileName, "kjgroup");
        Owned<IFile> iFile = createIFile(tmpFileName);
        Owned<IRowStreamWithFpos> rowStream = new CRowStreamWithFpos(iFile, keyLookupReplyOutputMetaRowIf, preserveOrder ? totalIndexParts : 0);

        rowStream->add(jg, true);

        return rowStream.getClear();
    }

    // roxiemem::IBufferedRowCallback impl.
    virtual bool freeBufferedRows(bool critical) override
    {
        /*
         * Strategy:
         *
         * Spill join groups until 'spillAmount' reached.
         * Increase spillAmount for next round (double it).
         * Decrease the pending and done queue limits, so that KJ starts processing less.
         * Repeat this process each time this CB is called until down to minumum targets. Minimum memory: keyLookupMinQueuedBytes or minimum queued limits (5)
         *
         * Prefer spilling completed groups, as they are not contended by threads adding rhs rows. NB: onCompleteCrit crit prevents queues being maniupulated.
         * 1) Walk the tail of the 'done' groups 1st (those are JG's ready to be read by downstream activity).
         * 2) Walk pending complete groups
         * 3) Walk pending incomplete groups. NB: incomplete groups are still being built, and may allocate. Need to avoid deadlock (see activeAddRightCount)
         *
         */

        if (0 == spillAmount) // minimum limits hit, don't try to spill anymore (see end of this function)
            return false;

        std::vector<CJoinGroup *> doneJGCandidates, incompleteJGCandidates;
        memsize_t totalToSpill = 0;
        memsize_t totalInUse = 0;

        CriticalBlock b(onCompleteCrit);

        CJoinGroup *cur = doneJoinGroupList.queryTail();
        while (true)
        {
            if (!cur)
                break;
            size32_t sz = cur->getRhsSize();
            totalInUse += sz;
            if (sz >= minRhsJGSpillSz)
            {
                if (totalToSpill < spillAmount)
                {
                    doneJGCandidates.push_back(cur);
                    totalToSpill += sz;
                }
            }
            cur = cur->prev;
        }

        unsigned potentialPendingJGsToSpill = 0;
        // scan pending groups from head, as 1st ones are likely to be most full
        cur = pendingJoinGroupList.queryHead();
        while (true)
        {
            if (!cur)
                break;
            size32_t sz = cur->getRhsSize(); // NB: groups may be active, so this is may be an underestimate
            totalInUse += sz;
            if (!cur->complete()) // and therefore might grow to need to
                potentialPendingJGsToSpill++;
            if (sz >= minRhsJGSpillSz)
            {
                if (totalToSpill < spillAmount)
                {
                    totalToSpill += sz;
                    if (cur->complete())
                        doneJGCandidates.push_back(cur);
                    else
                    {
                        /*
                         * NB: Caveat: too complicated to spill incomplete groups that are pending on fetch handlers filling group, effectively at random positions
                         * whilst also requiring to preserving order
                         *
                         */
                        if (!needsDiskRead || !preserveOrder)
                            incompleteJGCandidates.push_back(cur);
                    }
                }
            }
            cur = cur->next;
        }
        if (totalInUse < keyLookupMinQueuedBytes)
            return false; // too little to spill

        memsize_t newTotalInUse = totalInUse-totalToSpill;
        if (newTotalInUse < keyLookupMinQueuedBytes)
        {
            memsize_t sz = keyLookupMinQueuedBytes - newTotalInUse;
            totalToSpill -= sz;
        }

        unsigned spiltCompleteGroups = 0;
        unsigned spiltIncompleteGroups = 0;
        memsize_t totalSpiltSz = 0;

        // spill complete groups 1st
        for (auto *jg: doneJGCandidates)
        {
            size32_t spiltSz = jg->spillCompleteTS(minRhsJGSpillSz);
            if (spiltSz)
            {
                spiltCompleteGroups++;
                totalSpiltSz += spiltSz;
            }
            if (totalSpiltSz >= totalToSpill)
                break;
        }

        // now try incomplete groups if not spilt enough
        if (totalSpiltSz < spillAmount) // not found enough in completed groups
        {
            for (auto *jg: incompleteJGCandidates)
            {
                size32_t spiltSz = jg->spillIncomplete(minRhsJGSpillSz);
                if (spiltSz)
                {
                    spiltIncompleteGroups++;
                    totalSpiltSz += spiltSz;
                }
                if (totalSpiltSz >= totalToSpill)
                    break;
            }
        }

        // For next time
        spillAmount = totalInUse * spillAmountPercentage / 100;
        spillAmountPercentage *= 2;
        if (spillAmountPercentage > 90)
            spillAmountPercentage = 90;

        if (!totalSpiltSz)
            return false;

        bool min = pendingKeyLookupLimiter.reduceLimit(pendingJoinGroupList.queryCount(), 90, 10, 20, minimumQueueLimit);
        min = doneListLimiter.reduceLimit(doneJoinGroupList.queryCount(), 90, 10, 20, minimumQueueLimit) && min;

        ActPrintLog("Spilt: %" I64F "u bytes (totalInUse before spill=%" I64F "u, %u completed groups, %u incomplete groups, %u pending groups, %u done groups. pending limit set to: %u, done limit set to: %u",
                (unsigned __int64)totalSpiltSz, (unsigned __int64)totalInUse, spiltCompleteGroups, spiltIncompleteGroups, pendingJoinGroupList.queryCount(), doneJoinGroupList.queryCount(), pendingKeyLookupLimiter.queryMax(), doneListLimiter.queryMax());
        ActPrintLog("Spilt: potentialPendingJGsToSpill = %u, doneJGCandidates = %" I64F "u", potentialPendingJGsToSpill, (unsigned __int64)doneJGCandidates.size());

        // NB: if limits at minimum and candidates (for spilling) are less than limits, then can't grow to more, disable anymore attempts to spill
        if (min && (potentialPendingJGsToSpill <= minimumQueueLimit) && (doneJGCandidates.size() <= minimumQueueLimit))
        {
            // IOW both limiters have hit mimimum. Stop trying to spill anymore
            ActPrintLog("Spilt: Minimum pending and group limits reached. Will not attempt to spill any further");
            spillAmount = 0;
        }

        return true;
    }
    virtual unsigned getSpillCost() const override
    {
        return SPILL_PRIORITY_KEYEDJOIN;
    }
    virtual unsigned getActivityId() const override
    {
        return container.queryId();
    }
};


CActivityBase *createKeyedJoinSlave(CGraphElementBase *container) 
{ 
    if (container->getOptBool("legacykj"))
        return LegacyKJ::createKeyedJoinSlave(container);
    return new CKeyedJoinSlave(container);
}

