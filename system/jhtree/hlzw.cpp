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


