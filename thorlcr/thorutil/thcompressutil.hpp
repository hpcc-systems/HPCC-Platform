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


#ifndef _thcompressutil_ipp
#define _thcompressutil_ipp

#include "jlib.hpp"
#include "jbuff.hpp"

#ifdef GRAPH_EXPORTS
    #define graph_decl DECL_EXPORT
#else
    #define graph_decl DECL_IMPORT
#endif

/*
    Note: The first sizeof(size32_t) bytes of the compressed buffer contain the
    original (uncompressed) size of the data or 0. If 0, it indicates that
    buffer is uncompressed - this happens if the data could not be compressed 
    by a minimum of 80%.
*/

extern graph_decl size32_t ThorCompress(const void * src, size32_t srcSz, void * dest, size32_t destSz, size32_t threshold=0x1000);
extern graph_decl size32_t ThorCompress(const void * src, size32_t srcSz, MemoryBuffer & dest, size32_t threshold=0x1000);
extern graph_decl size32_t ThorCompress(MemoryBuffer & src, MemoryBuffer & dest, size32_t threshold=0x1000);
extern graph_decl size32_t ThorExpand(const void * src, size32_t srcSz, void * dest, size32_t destSz);
extern graph_decl size32_t ThorExpand(const void * src, size32_t srcSz, MemoryBuffer & dest);
extern graph_decl size32_t ThorExpand(MemoryBuffer & src, MemoryBuffer & dest);
extern graph_decl size32_t ThorExpand(const void * src, size32_t srcSz, CLargeMemoryAllocator &mem);


#endif

