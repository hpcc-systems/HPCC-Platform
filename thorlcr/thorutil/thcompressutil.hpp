/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */


#ifndef _thcompressutil_ipp
#define _thcompressutil_ipp

#include "jlib.hpp"
#include "jbuff.hpp"

#ifdef _WIN32
    #ifdef GRAPH_EXPORTS
        #define graph_decl __declspec(dllexport)
    #else
        #define graph_decl __declspec(dllimport)
    #endif
#else
    #define graph_decl
#endif

/*
    Note: The first sizeof(size32_t) bytes of the compressed buffer contain the
    orgininal (uncompressed) size of the data or 0. If 0, it indicates that
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

