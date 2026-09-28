/*##############################################################################

    Copyright (C) 2026 HPCC Systems®.

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

#include "eventindexloadsequence.h"
#include "eventindex.hpp"
#include <map>
#include <string>
#include <unordered_map>

// One row of sequential-leaf-load-run output, keyed by the thread that performed the loads.
struct SequenceRow
{
    __uint64 eventThreadId{0};
    __uint64 fileId{0};
    const char* metaPath{nullptr};
    offset_t fileOffset{0};
    __uint64 leafNodes{0};
    __uint64 leafSpan{0};
    __uint64 hitBlobs{0};
    __uint64 missBlobs{0};
    __uint64 branches{0};
    bool searchFlagsSet{false};
    byte searchFlags{0};
    // Actual SearchFlags value seen on a later event in this row -> occurrence count, when it
    // differs from the first-observed value.
    std::map<byte, uint32_t> searchFlagsViolations;
};

// Receives one row per completed sequential leaf-load run.
interface IIndexLoadSequenceRowSink
{
    virtual void emit(const SequenceRow& row) = 0;
};

class CIndexLoadSequenceCsvRenderer : public IIndexLoadSequenceRowSink
{
    struct ViolationEntry
    {
        __uint64 eventThreadId;
        __uint64 fileId;
        const char* metaPath;
        offset_t fileOffset;
        byte expected;
        byte actual;
        uint32_t count;
    };
public:
    CIndexLoadSequenceCsvRenderer(IBufferedSerialOutputStream& _out) : out(_out) {}

    void begin()
    {
        constexpr const char* header = "EventThreadId,FileId,meta.Path,FileOffset,SearchFlags,LeafNodes,LeafSpan,HitBlobs,MissBlobs,Branches\n";
        out.put(strlen(header), header);
    }

    virtual void emit(const SequenceRow& row) override
    {
        StringBuffer line;
        line.append(row.eventThreadId).append(',');
        line.append(row.fileId).append(',');
        encodeCSVColumn(line, row.metaPath);
        line.append(',').append(row.fileOffset).append(',');
        if (row.searchFlagsSet)
            line.appendhex((unsigned)row.searchFlags, true);
        line.append(',').append(row.leafNodes);
        line.append(',').append(row.leafSpan).append(',').append(row.hitBlobs);
        line.append(',').append(row.missBlobs).append(',').append(row.branches).append('\n');
        out.put(line.length(), line.str());

        for (const std::pair<const byte, uint32_t>& violation : row.searchFlagsViolations)
            violations.push_back({row.eventThreadId, row.fileId, row.metaPath, row.fileOffset, row.searchFlags, violation.first, violation.second});
    }

    // SearchFlags is expected to be consistent for every event accumulated into a row. Report
    // any row where that assumption did not hold, following the main table.
    void end()
    {
        if (violations.empty())
            return;
        constexpr const char* header = "EventThreadId,FileId,meta.Path,FileOffset,ExpectedSearchFlags,ActualSearchFlags,Count\n";
        out.put(strlen(header), header);
        for (const ViolationEntry& violation : violations)
        {
            StringBuffer line;
            line.append(violation.eventThreadId).append(',');
            line.append(violation.fileId).append(',');
            encodeCSVColumn(line, violation.metaPath);
            line.append(',').append(violation.fileOffset).append(',');
            line.append((unsigned)violation.expected).append(',');
            line.append((unsigned)violation.actual).append(',');
            line.append(violation.count).append('\n');
            out.put(line.length(), line.str());
        }
    }

private:
    IBufferedSerialOutputStream& out;
    std::vector<ViolationEntry> violations;
};

// Tracks, per EventThreadId, sequential runs of leaf IndexLoad events within a rolling read-size
// window, counting intervening branch and blob loads. Emits one row per completed run to the
// supplied sink.
class CIndexLoadSequenceVisitor : public CInterfaceOf<IEventVisitor>
{
    struct RunState : SequenceRow
    {
        offset_t lastOffset{0};
        __uint64 pendingHitBlobs{0};
        __uint64 pendingMissBlobs{0};
        __uint64 pendingBranches{0};
        bool active{false};
        std::string traceId;
    };

    using Runs = std::unordered_map<__uint64, RunState>;

public:
    CIndexLoadSequenceVisitor(CMetaInfoState& _metaState, IIndexLoadSequenceRowSink& _sink, __uint64 _readSize)
        : metaState(_metaState), sink(_sink), readSize(_readSize)
    {
    }

    virtual bool visitFile(const char*, uint32_t) override
    {
        return true;
    }

    virtual bool visitEvent(CEvent& event) override
    {
        switch (event.queryType())
        {
        case EventIndexLoad:
            onIndexLoad(event);
            break;
        case EventWorkerStart:
        case EventWorkerStop:
            onWorkerEvent(event);
            break;
        case EventTaskStart:
        case EventTaskStop:
            onTaskEvent(event);
            break;
        default:
            break;
        }
        return true;
    }

    virtual void departFile(uint32_t) override
    {
        for (auto& item : runs)
            flush(item.second);
    }

private:
    // Events must have a thread ID to be tracked. Events without are ignored.
#define ENSURE_HASKEY(e, id) \
    if (!e.hasAttribute(EvAttrEventThreadId)) \
        return; \
    __uint64 id = e.queryNumericValue(EvAttrEventThreadId);

    // Index and task events must satisfy their complete event contract before analysis.
#define ENSURE_COMPLETE(e) \
    if (!e.isComplete()) \
        return;

    // Records an event's SearchFlags (if explicitly set) as the row's expected value, or counts
    // it as a violation when it disagrees with a value already established for the row.
    static void observeSearchFlags(RunState& run, const CEvent& event)
    {
        if (!event.hasAttribute(EvAttrSearchFlags))
            return;
        byte flags = (byte)event.queryNumericValue(EvAttrSearchFlags);
        if (!run.searchFlagsSet)
        {
            run.searchFlagsSet = true;
            run.searchFlags = flags;
        }
        else if (flags != run.searchFlags)
            run.searchFlagsViolations[flags]++;
    }

    void onIndexLoad(const CEvent& event)
    {
        ENSURE_COMPLETE(event)
        ENSURE_HASKEY(event, tid)
        __uint64 fileId = event.queryNumericValue(EvAttrFileId);
        offset_t offset = event.queryNumericValue(EvAttrFileOffset);
        NodeKind nodeKind = queryIndexNodeKind(event);
        RunState& run = runs[tid];

        // A changed trace id always ends the current sequence for this thread.
        const char* traceId = nullptr;
        if (event.hasAttribute(EvAttrEventTraceId))
        {
            traceId = event.queryTextValue(EvAttrEventTraceId);
            if (run.active && !run.traceId.empty() && run.traceId != traceId)
                flush(tid);
        }

        switch (nodeKind)
        {
        case BranchNode:
            if (run.active && run.fileId == fileId)
            {
                run.pendingBranches++;
                observeSearchFlags(run, event);
            }
            return;
        case BlobNode:
            if (run.active && run.fileId == fileId)
            {
                observeSearchFlags(run, event);
                if (offset >= run.lastOffset && offset < run.lastOffset + readSize)
                    run.pendingHitBlobs++;
                else
                    run.pendingMissBlobs++;
            }
            return;
        case LeafNode:
            break;
        default:
            return;
        }

        bool continues = run.active && run.fileId == fileId && offset >= run.lastOffset
            && offset < run.lastOffset + readSize;
        if (!continues)
        {
            flush(tid);
            run = RunState{};
            run.active = true;
            run.eventThreadId = tid;
            run.fileId = fileId;
            run.fileOffset = offset;
            run.lastOffset = offset;
            run.leafNodes = 1;
            if (traceId)
                run.traceId = traceId;
            observeSearchFlags(run, event);
        }
        else
        {
            if (traceId)
                run.traceId = traceId;
            run.hitBlobs += run.pendingHitBlobs;
            run.missBlobs += run.pendingMissBlobs;
            run.branches += run.pendingBranches;
            run.pendingHitBlobs = run.pendingMissBlobs = run.pendingBranches = 0;
            run.lastOffset = offset;
            run.leafNodes++;
            observeSearchFlags(run, event);
        }
    }

    void onWorkerEvent(const CEvent& event)
    {
        ENSURE_HASKEY(event, tid)
        flush(tid);
    }

    void onTaskEvent(const CEvent& event)
    {
        ENSURE_COMPLETE(event)
        ENSURE_HASKEY(event, tid)
        switch (EventTask(event.queryNumericValue(EvAttrTask)))
        {
        case EventTask::Readahead:
        case EventTask::Sink:
            flush(tid);
            break;
        default:
            break;
        }
    }

#undef ENSURE_HASKEY
#undef ENSURE_COMPLETE

    void flush(__uint64 tid)
    {
        auto it = runs.find(tid);
        if (it != runs.end())
            flush(it->second);
    }

    void flush(RunState& run)
    {
        if (!run.active)
            return;
        // Runs of a single leaf load carry no sequential-read information and are suppressed.
        if (run.leafNodes > 1)
        {
            run.metaPath = metaState.queryFilePath(run.fileId);
            run.leafSpan = (run.lastOffset - run.fileOffset) / indexPageSize + 1;
            sink.emit(run);
        }
        run = RunState{};
    }

private:
    CMetaInfoState& metaState;
    IIndexLoadSequenceRowSink& sink;
    __uint64 readSize;
    Runs runs;
};

CIndexLoadSequenceOp::CIndexLoadSequenceOp()
{
}

bool CIndexLoadSequenceOp::ready() const
{
    return CEventConsumingOp::ready() && readSize != 0;
}

bool CIndexLoadSequenceOp::doOp()
{
    const EventFileProperties& properties = queryIteratorProperties();
    if (EventFileOption::Disabled == properties.options.includeThreadIds)
        return true;
    CIndexLoadSequenceCsvRenderer renderer(*out);
    // Must be heap-allocated: an attribute filter, if configured, retains a ref-counted link to
    // this visitor beyond doOp()'s scope, and a stack-local object would leave it dangling.
    Owned<CIndexLoadSequenceVisitor> visitor = new CIndexLoadSequenceVisitor(queryMetaInfoState(), renderer, readSize);
    renderer.begin();
    bool result = traverseEvents(*visitor);
    renderer.end();
    return result;
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"
#include "jstream.hpp"

namespace
{

// Test-only sink that captures emitted rows for direct inspection instead of rendering CSV.
class CCollectingRowSink : public IIndexLoadSequenceRowSink
{
public:
    virtual void emit(const SequenceRow& row) override { rows.push_back(row); }
public:
    std::vector<SequenceRow> rows;
};

constexpr __uint64 testPageSize = 8192;
constexpr __uint64 defaultReadSize = 8 * testPageSize;

// Bundles the visitor with the metaState/sink it depends on, since nearly every test needs a
// fresh instance of all three with the same default read-size window.
struct SequenceFixture
{
    CMetaInfoState metaState;
    CCollectingRowSink sink;
    CIndexLoadSequenceVisitor visitor;

    SequenceFixture(__uint64 readSize = defaultReadSize) : visitor(metaState, sink, readSize) {}
};

void addIndexLoad(IEventVisitor& visitor, __uint64 tid, __uint64 fileId, __uint64 offset, NodeKind nodeKind, const char* traceId = nullptr, int searchFlags = -1)
{
    CEvent event;
    event.reset(EventIndexLoad);
    event.setValue(EvAttrEventThreadId, tid);
    event.setValue(EvAttrFileId, fileId);
    event.setValue(EvAttrFileOffset, offset);
    event.setValue(EvAttrNodeKind, __uint64(nodeKind));
    event.setValue(EvAttrInMemorySize, __uint64(8192));
    event.setValue(EvAttrExpandTime, __uint64(0));
    event.setValue(EvAttrReadTime, __uint64(0));
    if (traceId)
        event.setValue(EvAttrEventTraceId, traceId);
    if (searchFlags >= 0)
        event.setValue(EvAttrSearchFlags, __uint64(searchFlags));
    visitor.visitEvent(event);
}

void addWorkerBoundary(IEventVisitor& visitor, EventType type, __uint64 tid)
{
    CEvent event;
    event.reset(type);
    event.setValue(EvAttrEventThreadId, tid);
    visitor.visitEvent(event);
}

void addTaskBoundary(IEventVisitor& visitor, EventType type, __uint64 tid, EventTask task)
{
    CEvent event;
    event.reset(type);
    event.setValue(EvAttrEventThreadId, tid);
    event.setValue(EvAttrTask, __uint64(task));
    visitor.visitEvent(event);
}

} // namespace

class EventIndexLoadSequenceTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventIndexLoadSequenceTest);
    CPPUNIT_TEST(testSequentialRun);
    CPPUNIT_TEST(testRollingWindowBoundary);
    CPPUNIT_TEST(testSingletonRunSuppressedByDefault);
    CPPUNIT_TEST(testLeafSpanExceedsLeafNodes);
    CPPUNIT_TEST(testPerThreadIndependence);
    CPPUNIT_TEST(testBranchInterleaving);
    CPPUNIT_TEST(testBlobHitAndMiss);
    CPPUNIT_TEST(testBlobWhileInactiveDiscardedOnReset);
    CPPUNIT_TEST(testBranchAndBlobFromDifferentFileNotAttributed);
    CPPUNIT_TEST(testFileChangeFlushPreservesFileId);
    CPPUNIT_TEST(testWorkerBoundaryForcesSplit);
    CPPUNIT_TEST(testTraceIdChangeForcesFlush);
    CPPUNIT_TEST(testSinkAndReadaheadTaskBoundariesForceFlush);
    CPPUNIT_TEST(testEventsWithoutThreadIdIgnored);
    CPPUNIT_TEST(testIncompleteIndexLoadIgnored);
    CPPUNIT_TEST(testOperationSkipsTraversalWhenThreadIdsDisabled);
    CPPUNIT_TEST(testSearchFlagsPropagatedToRow);
    CPPUNIT_TEST(testSearchFlagsAbsentLeavesRowUnset);
    CPPUNIT_TEST(testSearchFlagsViolationDetected);
    CPPUNIT_TEST(testViolationsEmittedAfterAllRows);
    CPPUNIT_TEST_SUITE_END();

public:
    void testSequentialRun()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode);
        addIndexLoad(f.visitor, 1, 100, 2 * testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(1), f.sink.rows.size());
        const SequenceRow& row = f.sink.rows[0];
        CPPUNIT_ASSERT_EQUAL(__uint64(1), row.eventThreadId);
        CPPUNIT_ASSERT_EQUAL(__uint64(100), row.fileId);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), row.fileOffset);
        CPPUNIT_ASSERT_EQUAL(__uint64(3), row.leafNodes);
        CPPUNIT_ASSERT_EQUAL(__uint64(3), row.leafSpan);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), row.hitBlobs);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), row.missBlobs);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), row.branches);
        END_TEST
    }

    // Verifies the inclusive/exclusive continuation boundary: an offset exactly at
    // lastOffset+readSize must start a new run, not continue the current one.
    void testRollingWindowBoundary()
    {
        START_TEST
        SequenceFixture f;

        // TID 1: offset readSize-1 is still within the window and must continue the run.
        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        addIndexLoad(f.visitor, 1, 100, defaultReadSize - 1, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        // TID 2: offset readSize is exactly at the boundary and must start a new run. The
        // single-leaf run that preceded it is flushed but suppressed (not asserted directly);
        // a second continuing leaf keeps the new run itself observable.
        addIndexLoad(f.visitor, 2, 100, 0, LeafNode);
        addIndexLoad(f.visitor, 2, 100, defaultReadSize, LeafNode);
        addIndexLoad(f.visitor, 2, 100, defaultReadSize + 1, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 2);

        CPPUNIT_ASSERT_EQUAL(size_t(2), f.sink.rows.size());

        const SequenceRow& continued = f.sink.rows[0];
        CPPUNIT_ASSERT_EQUAL(__uint64(1), continued.eventThreadId);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), continued.leafNodes);

        const SequenceRow& split = f.sink.rows[1];
        CPPUNIT_ASSERT_EQUAL(__uint64(2), split.eventThreadId);
        CPPUNIT_ASSERT_EQUAL(defaultReadSize, split.fileOffset);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), split.leafNodes);
        END_TEST
    }

    void testSingletonRunSuppressedByDefault()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT(f.sink.rows.empty());
        END_TEST
    }

    void testLeafSpanExceedsLeafNodes()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        addIndexLoad(f.visitor, 1, 100, 3 * testPageSize, LeafNode);
        addIndexLoad(f.visitor, 1, 100, 7 * testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(1), f.sink.rows.size());
        CPPUNIT_ASSERT_EQUAL(__uint64(3), f.sink.rows[0].leafNodes);
        CPPUNIT_ASSERT_EQUAL(__uint64(8), f.sink.rows[0].leafSpan);
        END_TEST
    }

    void testPerThreadIndependence()
    {
        START_TEST
        SequenceFixture f;

        // Interleave two threads' sequential leaf loads within the same file.
        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        addIndexLoad(f.visitor, 2, 100, 0, LeafNode);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode);
        addIndexLoad(f.visitor, 2, 100, testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);
        addWorkerBoundary(f.visitor, EventWorkerStop, 2);

        CPPUNIT_ASSERT_EQUAL(size_t(2), f.sink.rows.size());
        CPPUNIT_ASSERT_EQUAL(__uint64(1), f.sink.rows[0].eventThreadId);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[0].leafNodes);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[1].eventThreadId);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[1].leafNodes);
        END_TEST
    }

    void testBranchInterleaving()
    {
        START_TEST
        SequenceFixture f;

        // A branch load before any leaf run is active must not be counted.
        addIndexLoad(f.visitor, 1, 100, 0, BranchNode);
        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        // A branch load between continuing leaves must be counted.
        addIndexLoad(f.visitor, 1, 100, testPageSize / 2, BranchNode);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(1), f.sink.rows.size());
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[0].leafNodes);
        CPPUNIT_ASSERT_EQUAL(__uint64(1), f.sink.rows[0].branches);
        END_TEST
    }

    void testBlobHitAndMiss()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        // Within [0, readSize) of the active run's last leaf offset: a hit.
        addIndexLoad(f.visitor, 1, 100, testPageSize / 2, BlobNode);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode);
        // At or beyond lastOffset+readSize: a miss.
        addIndexLoad(f.visitor, 1, 100, testPageSize + defaultReadSize, BlobNode);
        addIndexLoad(f.visitor, 1, 100, 2 * testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(1), f.sink.rows.size());
        CPPUNIT_ASSERT_EQUAL(__uint64(3), f.sink.rows[0].leafNodes);
        CPPUNIT_ASSERT_EQUAL(__uint64(1), f.sink.rows[0].hitBlobs);
        CPPUNIT_ASSERT_EQUAL(__uint64(1), f.sink.rows[0].missBlobs);
        END_TEST
    }

    // Documents a known edge case: blob/branch loads observed while no run is active are
    // discarded when the next leaf starts a fresh run, since RunState is reset in full.
    void testBlobWhileInactiveDiscardedOnReset()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, BlobNode);
        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(1), f.sink.rows.size());
        CPPUNIT_ASSERT_EQUAL(__uint64(0), f.sink.rows[0].hitBlobs);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), f.sink.rows[0].missBlobs);
        END_TEST
    }

    // A branch/blob load for a different file than the active run must not be attributed to it,
    // even though a run is active for another file at the time.
    void testBranchAndBlobFromDifferentFileNotAttributed()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        // Unrelated file's branch/blob loads while file 100's run is active.
        addIndexLoad(f.visitor, 1, 200, 0, BranchNode);
        addIndexLoad(f.visitor, 1, 200, testPageSize / 2, BlobNode);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(1), f.sink.rows.size());
        const SequenceRow& row = f.sink.rows[0];
        CPPUNIT_ASSERT_EQUAL(__uint64(100), row.fileId);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), row.leafNodes);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), row.branches);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), row.hitBlobs);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), row.missBlobs);
        END_TEST
    }

    void testFileChangeFlushPreservesFileId()
    {
        START_TEST
        SequenceFixture f;

        // No FileInformation events are fed, so meta.Path must remain unresolved while FileId
        // still distinguishes the two files. Each file's run has 2 leaves so it remains visible
        // despite singleton-run suppression.
        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode);
        addIndexLoad(f.visitor, 1, 200, 0, LeafNode);
        addIndexLoad(f.visitor, 1, 200, testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(2), f.sink.rows.size());
        CPPUNIT_ASSERT_EQUAL(__uint64(100), f.sink.rows[0].fileId);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[0].leafNodes);
        CPPUNIT_ASSERT(isEmptyString(f.sink.rows[0].metaPath));
        CPPUNIT_ASSERT_EQUAL(__uint64(200), f.sink.rows[1].fileId);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[1].leafNodes);
        END_TEST
    }

    void testWorkerBoundaryForcesSplit()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);
        // Offset 2*testPageSize would have continued the prior run had it not been flushed.
        // A further continuing leaf keeps the post-boundary run visible despite suppression.
        addIndexLoad(f.visitor, 1, 100, 2 * testPageSize, LeafNode);
        addIndexLoad(f.visitor, 1, 100, 3 * testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(2), f.sink.rows.size());
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[0].leafNodes);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[1].leafNodes);
        END_TEST
    }

    void testTraceIdChangeForcesFlush()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, LeafNode, "trace-a");
        // The first leaf's trace id must survive new-run initialization. A different id on the
        // next leaf flushes the singleton trace-a run; two trace-b leaves then form one row.
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode, "trace-b");
        addIndexLoad(f.visitor, 1, 100, 2 * testPageSize, LeafNode, "trace-b");
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(1), f.sink.rows.size());
        CPPUNIT_ASSERT_EQUAL(testPageSize, f.sink.rows[0].fileOffset);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[0].leafNodes);
        END_TEST
    }

    void testSinkAndReadaheadTaskBoundariesForceFlush()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode);
        addTaskBoundary(f.visitor, EventTaskStop, 1, EventTask::Sink);
        addIndexLoad(f.visitor, 1, 100, 2 * testPageSize, LeafNode);
        addIndexLoad(f.visitor, 1, 100, 3 * testPageSize, LeafNode);
        addTaskBoundary(f.visitor, EventTaskStart, 1, EventTask::Readahead);
        addIndexLoad(f.visitor, 1, 100, 4 * testPageSize, LeafNode);
        addIndexLoad(f.visitor, 1, 100, 5 * testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(3), f.sink.rows.size());
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[0].leafNodes);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[1].leafNodes);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), f.sink.rows[2].leafNodes);
        END_TEST
    }

    void testEventsWithoutThreadIdIgnored()
    {
        START_TEST
        SequenceFixture f;

        CEvent event;
        event.reset(EventIndexLoad);
        event.setValue(EvAttrFileId, __uint64(100));
        event.setValue(EvAttrFileOffset, __uint64(0));
        event.setValue(EvAttrNodeKind, __uint64(LeafNode));
        // No EvAttrEventThreadId assigned.
        CPPUNIT_ASSERT(f.visitor.visitEvent(event));

        f.visitor.departFile(0);
        CPPUNIT_ASSERT(f.sink.rows.empty());
        END_TEST
    }

    void testIncompleteIndexLoadIgnored()
    {
        START_TEST
        SequenceFixture f;

        CEvent event;
        event.reset(EventIndexLoad);
        event.setValue(EvAttrEventThreadId, __uint64(1));
        event.setValue(EvAttrFileId, __uint64(100));
        event.setValue(EvAttrFileOffset, __uint64(0));
        event.setValue(EvAttrNodeKind, __uint64(LeafNode));
        CPPUNIT_ASSERT(!event.isComplete());
        CPPUNIT_ASSERT(f.visitor.visitEvent(event));

        f.visitor.departFile(0);
        CPPUNIT_ASSERT(f.sink.rows.empty());
        END_TEST
    }

    void testOperationSkipsTraversalWhenThreadIdsDisabled()
    {
        START_TEST

        class CDisabledThreadIdIterator : public CInterfaceOf<IEventIterator>
        {
        public:
            virtual bool nextEvent(CEvent&) override { return false; }
            virtual const EventFileProperties& queryFileProperties() const override { return properties; }
        private:
            EventFileProperties properties; // options.includeThreadIds defaults to Disabled
        };

        class CTestIndexLoadSequenceOp : public CIndexLoadSequenceOp
        {
        public:
            virtual unsigned getNumSources() const override { return 1; }
            virtual Owned<IEventIterator> createInputIterator() override { return new CDisabledThreadIdIterator; }
        };

        CTestIndexLoadSequenceOp op;
        op.setReadSize(defaultReadSize);
        // ready() requires a non-empty input path even though the fake iterator ignores it.
        op.setInputPath("test");
        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(output);
        op.setOutput(*stream);
        CPPUNIT_ASSERT(op.ready());
        CPPUNIT_ASSERT(op.doOp());
        stream->flush();
        CPPUNIT_ASSERT(output.isEmpty());
        END_TEST
    }

    void testSearchFlagsPropagatedToRow()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, LeafNode, nullptr, 0x15);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode, nullptr, 0x15);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(1), f.sink.rows.size());
        const SequenceRow& row = f.sink.rows[0];
        CPPUNIT_ASSERT(row.searchFlagsSet);
        CPPUNIT_ASSERT_EQUAL(byte(0x15), row.searchFlags);
        CPPUNIT_ASSERT(row.searchFlagsViolations.empty());
        END_TEST
    }

    void testSearchFlagsAbsentLeavesRowUnset()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, LeafNode);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(1), f.sink.rows.size());
        CPPUNIT_ASSERT(!f.sink.rows[0].searchFlagsSet);
        END_TEST
    }

    void testSearchFlagsViolationDetected()
    {
        START_TEST
        SequenceFixture f;

        addIndexLoad(f.visitor, 1, 100, 0, LeafNode, nullptr, 0x15);
        addIndexLoad(f.visitor, 1, 100, testPageSize, LeafNode, nullptr, 0x20);
        addIndexLoad(f.visitor, 1, 100, 2 * testPageSize, LeafNode, nullptr, 0x20);
        addWorkerBoundary(f.visitor, EventWorkerStop, 1);

        CPPUNIT_ASSERT_EQUAL(size_t(1), f.sink.rows.size());
        const SequenceRow& row = f.sink.rows[0];
        CPPUNIT_ASSERT(row.searchFlagsSet);
        CPPUNIT_ASSERT_EQUAL(byte(0x15), row.searchFlags);
        CPPUNIT_ASSERT_EQUAL(size_t(1), row.searchFlagsViolations.size());
        CPPUNIT_ASSERT_EQUAL(uint32_t(2), row.searchFlagsViolations.at(byte(0x20)));
        END_TEST
    }

    // Two independent threads each produce a row; only the second has a SearchFlags violation.
    // The violation report must follow both rows, not appear immediately after the first.
    void testViolationsEmittedAfterAllRows()
    {
        START_TEST

        class CFakeIterator : public CInterfaceOf<IEventIterator>
        {
        public:
            virtual bool nextEvent(CEvent& event) override
            {
                if (index >= events.size())
                    return false;
                event = events[index++];
                return true;
            }
            virtual const EventFileProperties& queryFileProperties() const override { return properties; }
            std::vector<CEvent> events;
            size_t index{0};
            EventFileProperties properties;
        };

        auto makeLoad = [](__uint64 tid, __uint64 offset, int searchFlags)
        {
            CEvent event;
            event.reset(EventIndexLoad);
            event.setValue(EvAttrEventThreadId, tid);
            event.setValue(EvAttrFileId, __uint64(100));
            event.setValue(EvAttrFileOffset, offset);
            event.setValue(EvAttrNodeKind, __uint64(LeafNode));
            event.setValue(EvAttrInMemorySize, __uint64(8192));
            event.setValue(EvAttrExpandTime, __uint64(0));
            event.setValue(EvAttrReadTime, __uint64(0));
            if (searchFlags >= 0)
                event.setValue(EvAttrSearchFlags, __uint64(searchFlags));
            return event;
        };

        Owned<CFakeIterator> iter = new CFakeIterator;
        iter->properties.options.includeThreadIds = EventFileOption::Enabled;
        iter->events.push_back(makeLoad(1, 0, 0x15));
        iter->events.push_back(makeLoad(1, testPageSize, 0x15));
        iter->events.push_back(makeLoad(2, 0, 0x15));
        iter->events.push_back(makeLoad(2, testPageSize, 0x20));
        iter->events.push_back(makeLoad(2, 2 * testPageSize, 0x20));

        class CTestIndexLoadSequenceOp : public CIndexLoadSequenceOp
        {
        public:
            Owned<IEventIterator> fakeIterator;
            virtual unsigned getNumSources() const override { return 1; }
            virtual Owned<IEventIterator> createInputIterator() override { return fakeIterator.getLink(); }
        };

        CTestIndexLoadSequenceOp op;
        op.fakeIterator.setown(iter.getLink());
        op.setReadSize(defaultReadSize);
        op.setInputPath("test");
        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(output);
        op.setOutput(*stream);
        CPPUNIT_ASSERT(op.ready());
        CPPUNIT_ASSERT(op.doOp());
        stream->flush();

        const char* text = output.str();
        const char* violationsHeader = strstr(text, "ExpectedSearchFlags");
        CPPUNIT_ASSERT_MESSAGE("violation report must be present", violationsHeader != nullptr);

        // Both data rows (thread 1 and thread 2) must appear before the violations section.
        const char* row1 = strstr(text, "1,100,");
        const char* row2 = strstr(text, "2,100,");
        CPPUNIT_ASSERT_MESSAGE("row for thread 1 must be present", row1 != nullptr);
        CPPUNIT_ASSERT_MESSAGE("row for thread 2 must be present", row2 != nullptr);
        CPPUNIT_ASSERT_MESSAGE("thread 1's row must precede the violations section", row1 < violationsHeader);
        CPPUNIT_ASSERT_MESSAGE("thread 2's row must precede the violations section", row2 < violationsHeader);
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EventIndexLoadSequenceTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(EventIndexLoadSequenceTest, "eventindexloadsequence");

#endif // _USE_CPPUNIT
