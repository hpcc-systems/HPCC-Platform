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


#include "jiface.hpp"
#include "jarray.hpp"
#include "jarray.tpp"
#include "jsort.hpp"
#include "jmisc.hpp"
#include "jlib.hpp"

#include <assert.h>

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/system/jlib/jarray.cpp $ $Id: jarray.cpp 62376 2011-02-04 21:59:58Z sort $");

#define FIRST_CHUNK_SIZE  8
#define DOUBLE_LIMIT      0x100000          // must be a power of 2
#define ALLOCA_LIMIT      64

inline unsigned ensurePowerOfTwo(unsigned value)
{
    value--;
    value |= (value >> 1);
    value |= (value >> 2);
    value |= (value >> 4);
    value |= (value >> 8);
    value |= (value >> 16);
    return value+1;
}

void Allocator::_space(size32_t iSize)
{
    if ( used == max )
        _reallocate(used+1, iSize);
    used++;
}

void Allocator::_ensure(aindex_t next, size32_t iSize)
{
    aindex_t req = used + next;
    if (req > max )
    {
        if ((max == 0) && (req < DOUBLE_LIMIT))
            max = ensurePowerOfTwo(req);
        _reallocate(req, iSize);
    }
}


void Allocator::kill()
{
    if (_head)
    {
        free(_head);
        _init();
    }
}

void Allocator::_reallocate(aindex_t newLen, size32_t itemSize)
{
    size32_t newMax = max;
    if (newMax == 0)
        newMax = FIRST_CHUNK_SIZE;
    if (newLen > DOUBLE_LIMIT)
    {
        newMax = (newLen + DOUBLE_LIMIT) & ~(DOUBLE_LIMIT-1);
        if (newLen >= newMax) // wraparound
        {
            PrintLog("Out of memory (overflow) in Array allocator: itemSize = %d, trying to allocate %d items", itemSize, newLen);
            throw MakeStringException(0, "Out of memory (overflow) in Array allocator: itemSize = %d, trying to allocate %d items",itemSize, newLen);
        }
    }
    else
    {
        while (newLen > newMax)
            newMax += newMax;
    }
    void *newhead = realloc(_head, itemSize * newMax);
    if (!newhead) 
    {
        PrintLog("Out of memory in Array allocator: itemSize = %d, trying to allocate %d items", itemSize, max);
        throw MakeStringException(0, "Out of memory in Array allocator: itemSize = %d, trying to allocate %d items",itemSize, max);
    }
    max = newMax;
    _head = newhead;
};


void Allocator::_doRotateR(aindex_t pos1, aindex_t pos2, size32_t iSize, aindex_t numItems)
{
    size32_t saveSize = numItems * iSize;
    size32_t blockSize = (pos2+1-pos1) * iSize;
    if (blockSize == saveSize)
        return;
    assertex(blockSize > saveSize);

    char * head= (char *)_head;
    char * lower = head + pos1 * iSize;
    char * upper = lower + (blockSize - saveSize);
    char * temp = (char *)((saveSize <= ALLOCA_LIMIT) ? alloca(saveSize) : malloc(saveSize));

    memcpy(temp, upper, saveSize);
    memmove(lower + saveSize, lower, blockSize-saveSize);
    memcpy(lower, temp, saveSize);

    if (saveSize > ALLOCA_LIMIT)
        free(temp);
}

void Allocator::_doRotateL(aindex_t pos1, aindex_t pos2, size32_t iSize, size32_t numItems)
{
    size32_t saveSize = numItems * iSize;
    size32_t blockSize = (pos2+1-pos1) * iSize;
    if (blockSize == saveSize)
        return;
    assertex(blockSize > saveSize);

    char * head= (char *)_head;
    char * lower = head + pos1 * iSize;
    char * upper = lower + (blockSize - saveSize);
    char * temp = (char *)((saveSize <= ALLOCA_LIMIT) ? alloca(saveSize) : malloc(saveSize));

    memcpy(temp, lower, saveSize);
    memmove(lower, lower + saveSize, blockSize - saveSize);
    memcpy(upper, temp, saveSize);

    if (saveSize > ALLOCA_LIMIT)
        free(temp);
}


void Allocator::_doSwap(aindex_t pos1, aindex_t pos2, size32_t iSize)
{
    char * head= (char *)_head;
    char * lower = head + pos1 * iSize;
    char * upper = head + pos2 * iSize;
    
    char * temp = (char *)alloca(iSize);
    memcpy(temp, lower, iSize);
    memmove(lower, upper, iSize );
    memcpy(upper, temp, iSize);
}



void Allocator::_doTransfer(aindex_t to, aindex_t from, size32_t iSize)
{
    if (from != to)
    {
        if (from < to)
            _doRotateL(from, to, iSize, 1);
        else
            _doRotateR(to, from, iSize, 1);
    }
}



void * Allocator::_doBAdd(void * newItem, size32_t _size, StdCompare compare, bool & isNew)
{
    return ::binary_add(newItem, _head, used-1, _size, compare, &isNew);
}


void * Allocator::_doBSearch(const void * key, size32_t _size, StdCompare compare) const
{
    return bsearch(key, _head, used, _size, compare);
}


void Allocator::_doSort(size32_t _size, StdCompare compare)
{
    if (used > 1)
        qsort(_head, used, _size, compare);
}


