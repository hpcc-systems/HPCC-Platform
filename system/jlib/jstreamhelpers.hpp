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

#ifndef JSTREAMHELPERS_HPP
#define JSTREAMHELPERS_HPP

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
inline void read(IBufferedSerialInputStream &source, char *buffer, size32_t size)
{
    source.read(size, buffer);
}

template <class T>
inline void read(IBufferedSerialInputStream &source, T &value)
{
    source.read(sizeof(T), &value);
}

const byte *readDirect(IBufferedSerialInputStream &source, size32_t len)
{
    if (len)
    {
        size32_t got;
        const byte *result = static_cast<const byte *>(source.peek(len, got));
        if (got >= len)
        {
            source.skip(len);
            return result;
        }
    }
    return nullptr; // Not enough data available or len 0 was requested
}

template <class T>
inline const void *peek(IBufferedSerialInputStream &source, T &value)
{
    size32_t got{0};
    const void *ptr = source.peek(sizeof(T), got);
    if (ptr && got >= sizeof(T))
        return ptr;
    return nullptr; // Not enough data available
}

template <typename T>
inline bool peekRead(IBufferedSerialInputStream &source, T &value)
{
    unsigned len = sizeof(T);
    size32_t got;
    const char *start = (const char *)source.peek(len, got);
    if (got >= len)
    {
        memcpy_iflen(&value, start, len);
        return true;
    }
    return false;
}

inline size32_t getZeroTerminatedStringLength(IBufferedSerialInputStream &source)
{
    unsigned available{0};
    const char *start = (const char *)source.peek(1, available);
    const char *originalStart = start;
    while (start && *start)
    {
        if (--available == 0)
            throwUnexpectedX("Null terminator not found within buffer");

        ++start;
    }
    return start - originalStart;
}

inline void peekReadZeroTerminatedString(IBufferedSerialInputStream &source, StringBuffer &value)
{
    size32_t stringLen = getZeroTerminatedStringLength(source);
    if (stringLen > 0)
    {
        size32_t got;
        const char *start = (const char *)source.peek(1, got);
        if (start && *start)
            value.append(start);
    }
}

#endif
