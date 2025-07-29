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

#pragma once

#include "platform.h"
#include "jstream.hpp"

// Global helper functions for buffered serial output
inline void append(IBufferedSerialOutputStream &target, const char *value)
{
    if (value)
        target.put(strlen(value) + 1, value);
    else
        target.put(1, "");
}

template <class T>
inline void append(IBufferedSerialOutputStream &target, const T &value)
{
    target.put(sizeof(T), &value);
}

// Global helper functions for buffered serial input
inline void read(IBufferedSerialInputStream &source, void *buffer, size32_t size)
{
    size32_t got = source.read(size, buffer);
    if (unlikely(got != size))
        throw makeStringExceptionV(0, "Failed to read the expected number of bytes %u, only read %u bytes", size, got);
}

template <class T>
inline void read(IBufferedSerialInputStream &source, T &value)
{
    size32_t got = source.read(sizeof(T), &value);
    if (unlikely(got != sizeof(T)))
        throw makeStringExceptionV(0, "Failed to read the expected number of bytes %zu, only read %u bytes", sizeof(T), got);
}

enum class NextByteStatus : byte { nextByteIsZero, nextByteIsNonZero, endOfStream };
inline NextByteStatus isNextByteZero(IBufferedSerialInputStream &src)
{
    size32_t got{0};
    const char *nextBytePtr = static_cast<const char *>(src.peek(1, got));
    if (got == 0)
        return NextByteStatus::endOfStream;

    return (*nextBytePtr == '\0') ? NextByteStatus::nextByteIsZero : NextByteStatus::nextByteIsNonZero;
}
