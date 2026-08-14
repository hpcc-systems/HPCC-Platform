/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#ifndef JEVENTCONST_HPP
#define JEVENTCONST_HPP

// The order should not be changed, or items removed. New values should always be appended before EventMax
// The meta prefix is used when there are records that provide extra meta data to help interpret
// the event data e.g. mapping file ids to filenames.
enum EventType : unsigned char
{
    EventNone,
    EventIndexCacheHit,
    EventIndexCacheMiss,
    EventIndexLoad,
    EventIndexEviction,
    EventDaliChangeMode,
    EventDaliCommit,
    EventDaliConnect,
    EventDaliEnsureLocal,
    EventDaliGet,
    EventDaliGetChildren,
    EventDaliGetChildrenFor,
    EventDaliGetElements,
    EventDaliSubscribe,
    MetaFileInformation,          // information about a file
    EventRecordingActive,         // optional event to indicate that recording was suspended/re-enabled
    EventIndexPayload,            // payload of a leaf node accessed
    EventQueryStart,
    EventQueryStop,
    EventRecordingSource,         // information about the source of the recording
    EventIndexOpen,               // open an index ready for reading
    MetaPlaneInformation,         // information about a plane
    EventRequestSend,             // remote request sent
    EventRequestReceive,          // remote request received
    EventWorkerStart,             // remote processing started
    EventWorkerStop,              // remote processing completed
    EventResponseSend,            // worker sent result
    EventResponseReceive,         // worker result received
    EventTaskStart,               // task execution started
    EventTaskStop,                // task execution completed
    EventLockWait,
    EventLockAcquire,
    EventLockRelease,
    EventSemWait,
    EventSemAcquire,
    EventSemSignal,
    EventLockTryWaitAcquire,
    EventLockTryWaitFail,
    EventLockWaitTimeout,
    EventSemWaitTimeout,
    EventMpRequestSend,
    EventMpRequestReceive,
    EventMpResponseSend,
    EventMpResponseReceive,
    EventEnqueue,
    EventDequeue,
    EventRwlockReadWait,
    EventRwlockReadAcquire,
    EventRwlockReadRelease,
    EventRwlockWriteWait,
    EventRwlockWriteAcquire,
    EventRwlockWriteRelease,
    EventRwlockReadWaitTimeout,
    EventRwlockWriteWaitTimeout,
    EventFunctionEnter,
    EventFunctionExit,
    EventMax
};

#endif
