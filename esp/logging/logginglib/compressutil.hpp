/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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


#ifndef _compressutil_ipp
#define _compressutil_ipp

#include "jlib.hpp"
#include "jbuff.hpp"
#include "loggingcommon.hpp"

/*
    Note: The first sizeof(size32_t) bytes of the compressed buffer contain the
    original (uncompressed) size of the data or 0. If 0, it indicates that
    buffer is uncompressed - this happens if the data could not be compressed 
    by a minimum of 80%.
*/

extern LOGGINGCOMMON_API size32_t LZWCompress(const void* src, size32_t srcSz, void* dest, size32_t destSz, size32_t threshold=0x1000);
extern LOGGINGCOMMON_API size32_t LZWCompress(const void* src, size32_t srcSz, StringBuffer& dest, size32_t threshold=0x1000);
extern LOGGINGCOMMON_API size32_t LZWCompress(const void* src, size32_t srcSz, MemoryBuffer& dest, size32_t threshold=0x1000);
extern LOGGINGCOMMON_API size32_t LZWCompress(MemoryBuffer& src, MemoryBuffer& dest, size32_t threshold=0x1000);
extern LOGGINGCOMMON_API size32_t LZWExpand(const void* src, size32_t srcSz, void* dest, size32_t destSz);
extern LOGGINGCOMMON_API size32_t LZWExpand(const void* src, size32_t srcSz, MemoryBuffer& dest);
extern LOGGINGCOMMON_API size32_t LZWExpand(MemoryBuffer& src, MemoryBuffer& dest);
extern LOGGINGCOMMON_API size32_t LZWExpand(const void* src, size32_t srcSz, CLargeMemoryAllocator& mem);
extern LOGGINGCOMMON_API size32_t LZWExpand(const void* src, size32_t srcSz, StringBuffer& dest);


#endif

