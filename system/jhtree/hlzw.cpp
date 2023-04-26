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

void KeyCompressor::open(void *blk,int blksize,bool _isVariable, bool rowcompression)
{
    isVariable = _isVariable;
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
    comp->open(blk,blksize);
    method = comp->getCompressionMethod();
}

void KeyCompressor::open(void *blk,int blksize, ICompressHandler * compressionHandler, bool _isVariable, size32_t _fixedRowSize)
{
    isVariable = _isVariable;
    isBlob = false;
    curOffset = 0;
    ::Release(comp);
    const char * options = nullptr;
    comp = compressionHandler->getCompressor(options);
    comp->open(blk,blksize);
    method = comp->getCompressionMethod();
    fixedRowSize = _fixedRowSize;
}

void KeyCompressor::openBlob(void *blk,int blksize)
{
    isVariable = false;
    isBlob = true;
    curOffset = 0;
    ::Release(comp);
    comp = NULL;
    comp = createLZWCompressor(true);
    comp->open(blk,blksize);
    method = comp->getCompressionMethod();
}

int KeyCompressor::writekey(offset_t fPtr, const char *key, unsigned datalength, unsigned __int64 sequence)
{
    assert(!isBlob);
    comp->startblock(); // start transaction
    // first write out length if variable
    if (isVariable) {
        KEYRECSIZE_T rs = datalength;
        _WINREV(rs);
        if (comp->write(&rs, sizeof(rs))!=sizeof(rs)) {
            close();
            return 0;
        }
    }
    // then write out fpos and key
    _WINREV(fPtr);
    if (comp->write(&fPtr,sizeof(offset_t))!=sizeof(offset_t)) {
        close();
        return 0;
    }
    if (comp->write(key,datalength)!=datalength) {
        close();
        return 0;
    }
    comp->commitblock();    // end transaction

    return 1;
}

bool KeyCompressor::write(const void * data, size32_t datalength)
{
    if (method == COMPRESS_METHOD_RANDROW && fixedRowSize)
    {
        //Ugly special casing because the RandR compressor expects single rows to be added.
        //This code could be migrated to the compressor, but that should be done carefully as a separate change.
        for (size32_t offset = 0; offset < datalength; offset += fixedRowSize)
        {
            dbgassertex(offset + fixedRowSize <= datalength); // Check datalength is a multiple of the fixedRowSize
            comp->startblock(); // start transaction
            size32_t size = comp->write((const byte *)data + offset, fixedRowSize);
            if (size != fixedRowSize)
            {
                close();
                return false;
            }
            comp->commitblock();    // end transaction
        }
    }
    else
    {
        comp->startblock(); // start transaction
        if (comp->write(data,datalength)!=datalength)
        {
            close();
            return false;
        }
        comp->commitblock();    // end transaction
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
    comp->startblock(); // start transaction
    char zero = 0;
    while (curOffset & 0xf)
    {
        if (comp->write(&zero,sizeof(zero))!=sizeof(zero)) {
            close();
            curOffset = originalOffset;
            return 0;
        }
        curOffset++;
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
        comp->startblock();
    }
    comp->commitblock();
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


