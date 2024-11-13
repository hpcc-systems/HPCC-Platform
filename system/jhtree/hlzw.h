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

#ifndef HLZWSTRM_INCL
#define HLZWSTRM_INCL

typedef unsigned short KEYRECSIZE_T;

#include "jbuff.hpp"
#include "jlzw.hpp"
#define USE_RANDROWDIFF true

class KeyCompressor final
{
public:
    KeyCompressor() {}
    ~KeyCompressor();
    void open(void *blk,int blksize, bool isVariable, bool rowcompression, bool LZ4);
    void open(void *blk,int blksize, ICompressHandler * compressionHandler, const char * options, bool _isVariable, size32_t fixedRowSize);

    int writekey(offset_t fPtr, const char *key, unsigned datalength);
    bool write(const void * data, size32_t datalength);

    bool compressBlock(size32_t destSize, void * dest, size32_t srcSize, const void * src, ICompressHandler * compressionHandler, const char * options, bool isVariable, size32_t fixedSize);

    void openBlob(void *blk,int blksize, bool LZ4);
    unsigned writeBlob(const char *data, unsigned datalength);
    void close();
    bool adjustLimit(size32_t newLimit);

    unsigned getCurrentOffset() const { return (curOffset+0xf) & 0xfffffff0; }
    void *bufptr() const { return (comp==NULL)?bufp:comp->bufptr();}
    int buflen() const { return (comp==NULL)?bufl:comp->buflen();}
    CompressionMethod getCompressionMethod() const { return method; }

protected:
    ICompressor *comp = nullptr;
    MemoryBuffer uncompressed;
    void *bufp = nullptr;
    unsigned curOffset = 0;
    size32_t fixedRowSize = 0;
    int bufl = 0;
    bool isVariable = false;
    bool isBlob = false;
    CompressionMethod method = COMPRESS_METHOD_NONE;

    void testwrite(const void *p,size32_t s);
};

#endif
