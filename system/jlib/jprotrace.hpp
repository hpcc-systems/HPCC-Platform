/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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


#ifndef JPROTRACE_HPP
#define JPROTRACE_HPP

#ifdef _USE_PROTRACE
#include <protrace.h>
inline constexpr bool hasProtrace() { return true; }

// If we record information about every critical section and lock it can quickly swamp all the other information
// So PROTRACE_LOCKS and PROTRACE_SEMAPHORES allow semaphores and locks to be conditionally tracked.
// Do we track information about unnamed locks?
// These are usually singletons, or other rarely used locks, avoiding tracking them can reduce noise in the trace.
#define TRACK_UNNAMED_LOCKS
#else
inline constexpr bool hasProtrace() { return false; }
#endif

#ifdef TRACK_UNNAMED_LOCKS
constexpr bool trackUnnamedLocks = true;
#else
constexpr bool trackUnnamedLocks = false;
#endif

#include <stdint.h>
// Keep the #include dependencies to a minimum, as this header is included in many low level files.

#include "jeventconst.hpp"


enum class EventTask : byte;

inline unsigned getTaskStartOp(EventTask task)
{
    return 256 + (static_cast<unsigned>(task) * 2);
}

inline unsigned getTaskStopOp(EventTask task)
{
    return getTaskStartOp(task) + 1;
}

inline __uint64 combineIdSeq(unsigned id, unsigned seq)
{
    return (static_cast<uint64_t>(seq) << 32) | id;
}

inline __uint64 combineTagSeq(unsigned tag, unsigned seq)
{
    return (static_cast<uint64_t>(tag) << 32) | seq;
}

inline __uint64 getProtraceTicks(cycle_t cycles)
{
#ifdef _USE_PROTRACE
    return (cycles >> protrace::TICK_SHIFT);
#else
    return 0;
#endif
}

inline void protraceRecord([[maybe_unused]] unsigned op)
{
#ifdef _USE_PROTRACE
    protrace::record(static_cast<protrace::opcode_t>(op));
#endif
}

inline void protraceRecord([[maybe_unused]] unsigned op, [[maybe_unused]] uint64_t v1)
{
#ifdef _USE_PROTRACE
    protrace::record_payload(static_cast<protrace::opcode_t>(op), v1);
#endif
}

inline void protraceRecord([[maybe_unused]] unsigned op, [[maybe_unused]] uint64_t v1, [[maybe_unused]] uint64_t v2)
{
#ifdef _USE_PROTRACE
    protrace::record_payload(static_cast<protrace::opcode_t>(op), v1, v2);
#endif
}

inline void protraceRecordAt([[maybe_unused]] unsigned op, [[maybe_unused]] cycle_t cycles)
{
#ifdef _USE_PROTRACE
    protrace::record(static_cast<protrace::opcode_t>(op), static_cast<protrace::tick_t>(getProtraceTicks(cycles)));
#endif
}

inline void protraceRecordAt([[maybe_unused]] unsigned op, [[maybe_unused]] cycle_t cycles, [[maybe_unused]] uint64_t v1)
{
#ifdef _USE_PROTRACE
    protrace::emit(
        protrace::create_event(static_cast<protrace::opcode_t>(op), static_cast<protrace::tick_t>(getProtraceTicks(cycles))),
        protrace::create_payload(v1));
#endif
}

inline void protraceRecordAt([[maybe_unused]] unsigned op, [[maybe_unused]] cycle_t cycles, [[maybe_unused]] uint64_t v1, [[maybe_unused]] uint64_t v2)
{
#ifdef _USE_PROTRACE
    protrace::emit(
        protrace::create_event(static_cast<protrace::opcode_t>(op), static_cast<protrace::tick_t>(getProtraceTicks(cycles))),
        protrace::create_payload(v1),
        protrace::create_payload(v2));
#endif
}

inline void protraceRecordMpRequestSend([[maybe_unused]] uint64_t requestId, [[maybe_unused]] uint64_t dataSize)
{
#ifdef _USE_PROTRACE
    protraceRecord(EventMpRequestSend, requestId, dataSize);
#endif
}

inline void protraceRecordMpRequestReceive([[maybe_unused]] uint64_t requestId)
{
#ifdef _USE_PROTRACE
    protraceRecord(EventMpRequestReceive, requestId);
#endif
}

inline void protraceRecordMpResponseSend([[maybe_unused]] uint64_t responseId, [[maybe_unused]] uint64_t dataSize)
{
#ifdef _USE_PROTRACE
    protraceRecord(EventMpResponseSend, responseId, dataSize);
#endif
}

inline void protraceRecordMpResponseReceive([[maybe_unused]] uint64_t responseId)
{
#ifdef _USE_PROTRACE
    protraceRecord(EventMpResponseReceive, responseId);
#endif
}

inline void protraceRecordEnqueueItem([[maybe_unused]] int64_t elementId)
{
#ifdef _USE_PROTRACE
    protraceRecord(EventEnqueue, static_cast<uint64_t>(elementId));
#endif
}

inline void protraceRecordEnqueueItem([[maybe_unused]] const void *element)
{
#ifdef _USE_PROTRACE
    protraceRecord(EventEnqueue, static_cast<uint64_t>(reinterpret_cast<memsize_t>(element) >> 2));
#endif
}

inline void protraceRecordDequeueItem([[maybe_unused]] int64_t elementId)
{
#ifdef _USE_PROTRACE
    protraceRecord(EventDequeue, static_cast<uint64_t>(elementId));
#endif
}

inline void protraceRecordDequeueItem([[maybe_unused]] const void *element)
{
#ifdef _USE_PROTRACE
    protraceRecord(EventDequeue, static_cast<uint64_t>(reinterpret_cast<memsize_t>(element) >> 2));
#endif
}

inline void protraceRecordTaskStart(EventTask task)
{
    protraceRecord(getTaskStartOp(task));
}

inline void protraceRecordTaskStop(EventTask task)
{
    protraceRecord(getTaskStopOp(task));
}

inline void protraceNoteThreadName([[maybe_unused]] const char *name)
{
#ifdef _USE_PROTRACE
    protrace::note_thread(protrace::get_tid(), name);
#endif
}

inline void protraceNoteSemaphore([[maybe_unused]] unsigned syncId, [[maybe_unused]] const char *name)
{
#ifdef _USE_PROTRACE
    if (name)
        protrace::note_semaphore(syncId, name);
#endif
}

inline void protraceNoteLock([[maybe_unused]] unsigned syncId, [[maybe_unused]] const char *name)
{
#ifdef _USE_PROTRACE
    if (name)
        protrace::note_lock(syncId, name);
#endif
}

inline void protraceNoteSpinLock([[maybe_unused]] unsigned syncId, [[maybe_unused]] const char *name)
{
#ifdef _USE_PROTRACE
    if (name)
        protrace::note_spinlock(syncId, name);
#endif
}

extern jlib_decl bool protraceResumeRecording();
extern jlib_decl bool protraceSuspendRecording();
extern jlib_decl bool protraceClearRecording(bool clearMetadata);

// Dump the current protrace recording to disk.
// If filename is relative (or omitted), a file will be created in a temporary directory.
// On success, outputFilename contains the generated/used filename.
class StringBuffer;
extern jlib_decl void protraceSaveRecording(StringBuffer & outputFilename, const char * filename);

#endif