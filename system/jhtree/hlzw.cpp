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

#include "platform.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <sys/types.h>
#include <stdlib.h>
#include <signal.h>

#ifndef _WIN32
#include <sys/time.h>
#include <sys/stat.h>
#include <assert.h>
#endif

#include "jmisc.hpp"
#include "hlzw.h"

KeyCompressor::~KeyCompressor()
{
    if (NULL != comp)
    {
        close();
        // temporary tracing
        WARNLOG("KeyCompressor not explicitly closed");
    }
}

void KeyCompressor::open(void *blk,int blksize,bool _isVariable, bool rowcompression, size32_t _fixedRowSize)
{
    isVariable = _isVariable;
    fixedRowSize = _fixedRowSize;
    isBlob = false;
    curOffset = 0;
    ::Release(comp);
    comp = NULL;
    if (rowcompression&&!_isVariable) {
        if (USE_RANDROWDIFF)
            comp = createRandRDiffCompressor();
        else
            comp = createRDiffCompressor();
    }
    else
        comp = createLZWCompressor(true);
    comp->open(blk,blksize,_fixedRowSize, false );
    method = comp->getCompressionMethod();
}

void KeyCompressor::open(void *blk,int blksize, ICompressHandler * compressionHandler, const char * options, bool _isVariable, size32_t _fixedRowSize)
{
    isVariable = _isVariable;
    isBlob = false;
    curOffset = 0;
    ::Release(comp);
    comp = compressionHandler->getCompressor(options);
    comp->open(blk,blksize, _fixedRowSize, false);
    method = comp->getCompressionMethod();
    fixedRowSize = _fixedRowSize;
}

void KeyCompressor::open(void *blk,int blksize, ICompressor * compressor, bool _isVariable, size32_t _fixedRowSize)
{
    isVariable = _isVariable;
    isBlob = false;
    curOffset = 0;
    ::Release(comp);
    comp = LINK(compressor);
    comp->open(blk,blksize, _fixedRowSize, false);
    method = comp->getCompressionMethod();
    fixedRowSize = _fixedRowSize;
}

void KeyCompressor::openBlob(CompressionMethod compression, void *blk,int blksize)
{
    isVariable = false;
    isBlob = true;
    curOffset = 0;
    ::Release(comp);
    comp = NULL;
    comp = queryCompressHandler(compression)->getCompressor();
    comp->open(blk,blksize,0, true);
    method = comp->getCompressionMethod();
}

int KeyCompressor::writekey(offset_t fPtr, const char *key, unsigned datalength, unsigned options)
{
    assert(!isBlob);
    assertex(__BYTE_ORDER == __LITTLE_ENDIAN); // otherwise the following code is wrong.

    //Copy the data into a buffer so all the data is written in a single write()
    tempKeyBuffer.clear();
    if (isVariable)
    {
        KEYRECSIZE_T rs = datalength;
        tempKeyBuffer.appendSwap(sizeof(rs), &rs);
    }

    bool hasTrailingFilePos = (options & TrailingFilePosition) != 0 && (options & NoFilePosition) == 0;
    bool hasLeadingFilePos = (options & NoFilePosition) == 0 && !hasTrailingFilePos;
    if (hasLeadingFilePos)
        tempKeyBuffer.appendSwap(sizeof(offset_t), &fPtr);
    tempKeyBuffer.append(datalength, key);
    if (hasTrailingFilePos)
        tempKeyBuffer.appendSwap(sizeof(offset_t), &fPtr);

    size32_t toWrite = tempKeyBuffer.length();
    if (comp->write(tempKeyBuffer.bufferBase(),toWrite)!=toWrite)
    {
        close();
        return 0;
    }
    return 1;
}

bool KeyCompressor::compressBlock(size32_t destSize, void * dest, size32_t srcSize, const void * src, ICompressHandler * compressionHandler, const char * options, bool isVariable, size32_t fixedSize)
{
    bool ok = false;
    Owned<ICompressor> compressor = compressionHandler->getCompressor(options);
    if (compressor->supportsBlockCompression())
    {
        size32_t written = compressor->compressBlock(destSize, dest, srcSize, src);
        if (written)
        {
            bufp = dest;
            bufl = written;
            method = compressor->getCompressionMethod();
            ok = true;
        }
    }
    else
    {
        open(dest, destSize, compressionHandler, options, isVariable, fixedSize);
        ok = write(src, srcSize);
        close();
    }
    return ok;
}

bool KeyCompressor::write(const void * data, size32_t datalength)
{
    if (comp->write(data,datalength)!=datalength)
    {
        close();
        return false;
    }
    return true;
}


unsigned KeyCompressor::writeBlob(const char *data, unsigned datalength)
{
    assert(isBlob);
    assert(datalength);
    if (!comp)
        return 0;

    unsigned originalOffset = curOffset;
    unsigned zeroPadding = (16 - (curOffset & 0xf)) & 0xf;
    if (zeroPadding)
    {
        char zeros[16] = { 0 };
        if (comp->write(zeros, zeroPadding) != zeroPadding)
        {
            close();
            return 0;
        }
        curOffset += zeroPadding;
    }

    unsigned rdatalength = datalength;
    _WINREV(rdatalength);
    if (comp->write(&rdatalength, sizeof(rdatalength))!=sizeof(rdatalength)) {
        close();
        curOffset = originalOffset;
        return 0;
    }
    curOffset += sizeof(datalength);

    unsigned written = 0;
    while (written < datalength && curOffset < 0x100000)
    {
        if (comp->write(data,sizeof(*data))!=sizeof(*data))
        {
            if (!written)
            {
                close();
                curOffset = originalOffset;
                return 0; // only room to put the length - don't!
            }
            break;
        }
        curOffset++;
        written++;
        data++;
    }
    if (written != datalength)
        close();
    return written;
}


bool KeyCompressor::adjustLimit(size32_t newLimit)
{
    return comp && comp->adjustLimit(newLimit);
}

void KeyCompressor::close()
{ // gets called either when write failed or explicitly by client
    if (comp!=NULL) {
        comp->close();
        bufp = comp->bufptr();
        bufl = comp->buflen();
        comp->Release();
        comp = NULL;
    }
}


