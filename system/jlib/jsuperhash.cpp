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


#include "jlib.hpp"
#include "jsuperhash.hpp"
#include <assert.h>

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/system/jlib/jsuperhash.cpp $ $Id: jsuperhash.cpp 64069 2011-04-18 14:15:50Z rchapman $");

#ifndef HASHSIZE_POWER2
#define HASHSIZE_POWER2
#endif

#ifdef HASHSIZE_POWER2
#define InitialTableSize 16
#else
#define InitialTableSize 15
#endif

//#define MY_TRACE_HASH

#ifdef MY_TRACE_HASH
int my_search_tot = 0;
int my_search_num = 0;
#endif 

//-- SuperHashTable ---------------------------------------------------
SuperHashTable::SuperHashTable(void)
{
    tablesize = InitialTableSize;
    tablecount = 0;
    table = (void * *) checked_malloc(InitialTableSize*sizeof(void *),-601);
    memset(table,0,InitialTableSize*sizeof(void *));
    cache = 0;
#ifdef TRACE_HASH
    search_tot = 0;
    search_num = 0;
#endif
}

SuperHashTable::SuperHashTable(unsigned initsize)
{
    init(initsize);
}

static inline unsigned nextPowerOf2(unsigned v)
{
    assert(sizeof(unsigned)==4);
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
}

void SuperHashTable::init(unsigned initsize)
{
    if (initsize==0)
        initsize = InitialTableSize;
#ifdef HASHSIZE_POWER2
    //size should be a power of 2
    initsize = nextPowerOf2(initsize);
#endif
    tablesize = initsize;
    tablecount = 0;
    table = (void * *) checked_malloc(initsize*sizeof(void *),-602);
    memset(table,0,initsize*sizeof(void *));
    cache = 0;
#ifdef TRACE_HASH
    search_tot = 0;
    search_num = 0;
#endif
}

void SuperHashTable::reinit(unsigned initsize)
{
    doKill();
    init(initsize);
}


SuperHashTable::~SuperHashTable()
{
    doKill();
#ifdef TRACE_HASH
    dumpStats();
#endif
}

#ifdef TRACE_HASH
void SuperHashTable::dumpStats()
{
    if (tablecount && search_tot && search_num)
        printf("Hash table %d entries, %d size, average search length %d(%d/%d))\n", tablecount, tablesize, 
               (int) (search_tot/search_num), search_tot, search_num);
}
#endif
 
#ifdef TRACE_HASH
void SuperHashTable::note_searchlen(int len) const
{
    search_tot += len;
    search_num++;
}
#endif

unsigned SuperHashTable::doFind(unsigned findHash, const void * findParam) const
{
#ifdef HASHSIZE_POWER2
    unsigned v = findHash & (tablesize - 1);
#else
    unsigned v = findHash % tablesize;
#endif
    unsigned vs = v;
#ifdef TRACE_HASH
    unsigned searchlen = 0;
#endif
    while (1)
    {
#ifdef MY_TRACE_HASH
        my_search_tot++;
#endif
        void *et = table[v];
        if (!et)
            break;
        if (matchesFindParam(et, findParam, findHash))
            break;
#ifdef TRACE_HASH
        searchlen ++;
#endif
        v++;
        if (v==tablesize)
            v = 0;
        if (v==vs)
            break;
    }
#ifdef MY_TRACE_HASH
    my_search_num++;
    if(my_search_num != 0)
        printf("Hash table average search length %d\n", (int) (my_search_tot/my_search_num));
#endif
#ifdef TRACE_HASH
    note_searchlen(searchlen);
#endif
    setCache(v);
    return v;
}

unsigned SuperHashTable::doFindElement(unsigned v, const void * findET) const
{
#ifdef HASHSIZE_POWER2
    v = v & (tablesize - 1);
#else
    v = v % tablesize;
#endif
    unsigned vs = v;
#ifdef TRACE_HASH
    unsigned searchlen = 0;
#endif
    while (1)
    {
#ifdef MY_TRACE_HASH
        my_search_tot++;
#endif
        void *et = table[v];
        if (!et)
            break;
        if (matchesElement(et, findET))
            break;
#ifdef TRACE_HASH
        searchlen ++;
#endif
        v++;
        if (v==tablesize)
            v = 0;
        if (v==vs)
            break;
    }
#ifdef MY_TRACE_HASH
    my_search_num++;
    if(my_search_num != 0)
        printf("Hash table average search length %d\n", (int) (my_search_tot/my_search_num));
#endif
#ifdef TRACE_HASH
    note_searchlen(searchlen);
#endif
    setCache(v);
    return v;
}

unsigned SuperHashTable::doFindNew(unsigned v) const
{
#ifdef HASHSIZE_POWER2
    v = v & (tablesize - 1);
#else
    v = v % tablesize;
#endif
    unsigned vs = v;
#ifdef TRACE_HASH
    unsigned searchlen = 0;
#endif
    while (1)
    {
#ifdef MY_TRACE_HASH
        my_search_tot++;
#endif
        void *et = table[v];
        if (!et)
            break;
#ifdef TRACE_HASH
        searchlen ++;
#endif
        v++;
        if (v==tablesize)
            v = 0;
        if (v==vs)
            break; //table is full, should never occur
    }
#ifdef MY_TRACE_HASH
    my_search_num++;
    if(my_search_num != 0)
        printf("Hash table average search length %d\n", (int) (my_search_tot/my_search_num));
#endif
#ifdef TRACE_HASH
    note_searchlen(searchlen);
#endif
    setCache(v);
    return v;
}

unsigned SuperHashTable::doFindExact(const void *et) const
{
    unsigned i = cache;
    if (i>=tablesize || table[i]!=et)
    {
#ifdef HASHSIZE_POWER2
        i = getHashFromElement(et) & (tablesize - 1);
#else
        i = getHashFromElement(et) % tablesize;
#endif
        unsigned is = i;
        while (table[i]!=et)
        {
            i++;
            if (i==tablesize)
                i = 0;
            if (!table[i] || i==is)
                break;
        }
    }
    return i;
}

void SuperHashTable::expand()
{
    unsigned newsize = tablesize;
#ifdef HASHSIZE_POWER2
    newsize += newsize;
#else
    if (newsize>=0x3FF)
        newsize += 0x400;
    else
        newsize += newsize+1;
#endif
    void * *newtable = (void * *) checked_malloc(newsize*sizeof(void *),-603);
    memset(newtable,0,newsize*sizeof(void *));
    void * *oldtable = table;
    unsigned i;
    for (i = 0; i < tablesize; i++)
    {
        void *et = oldtable[i];
        if (et)
        {
#ifdef HASHSIZE_POWER2
            unsigned v = getHashFromElement(et) & (newsize - 1);
#else
            unsigned v = getHashFromElement(et) % newsize;
#endif
            while (newtable[v])
            {
                v++;
                if (v==newsize)
                    v = 0;
            }
            newtable[v] = et;
        }
    }
    free(table);
    table = newtable;
    tablesize = newsize;
}

bool SuperHashTable::doAdd(void * donor, bool replace)
{
    unsigned vs = getHashFromElement(donor);
    unsigned vm = doFind(vs, getFindParam(donor));
    void *et = table[vm];
    if (et)
    {
        if (replace)
        {
            onRemove(et);
            table[vm] = donor;
            onAdd(donor);
            return true;
        }
        else
            return false;
    }
    else
    {
        unsigned tablelim = getTableLimit(tablesize);
        if (tablecount>=tablelim)
        {
            expand();
            vm = doFind(vs, getFindParam(donor));
        }
        tablecount++;
        table[vm] = donor;
        onAdd(donor);
    }
    return true;
}

void SuperHashTable::addNew(void * donor, unsigned hash)
{
    unsigned tablelim = getTableLimit(tablesize);
    if (tablecount>=tablelim)
        expand();

    unsigned vm = doFindNew(hash);
    tablecount++;
    table[vm] = donor;
    onAdd(donor);
}

void SuperHashTable::addNew(void * donor)
{
    addNew(donor, getHashFromElement(donor));
}

void SuperHashTable::doDeleteElement(unsigned v)
{
#ifdef HASHSIZE_POWER2
    unsigned hm = (tablesize - 1);
#endif
    unsigned hs = tablesize;
    unsigned vn = v;
    table[v] = NULL;
    while (1)
    {
        vn++;
        if (vn==hs) vn = 0;
        void *et2 = table[vn];
        if (!et2)
            break;
#ifdef HASHSIZE_POWER2
        unsigned vm = getHashFromElement(et2) & hm;
        if (((vn+hs-vm) & hm)>=((vn+hs-v) & hm))  // diff(vn,vm)>=diff(vn,v)
#else
            unsigned vm = getHashFromElement(et2) % hs;
        if (((vn+hs-vm) % hs)>=((vn+hs-v) % hs))  // diff(vn,vm)>=diff(vn,v)
#endif
        {
            table[v] = et2;
            v = vn;
            table[v] = NULL;
        }
    }
    tablecount--;
}

unsigned SuperHashTable::getTableLimit(unsigned max)        
{ 
    return (max * 3) / 4;
}

bool SuperHashTable::remove(const void *fp)
{
    unsigned v = doFind(fp);
    void * et = table[v];
    if (!et)
        return false;
    doDeleteElement(v);
    onRemove(et);
    return true;
}

bool SuperHashTable::removeExact(void *et)
{
    if (!et)
        return false;
    unsigned v = doFindExact(et);
    if (table[v]!=et)
        return false;
    doDeleteElement(v);
    onRemove(et);
    return true;
}

void *SuperHashTable::findElement(unsigned hash, const void * findEt) const
{
    unsigned vm = doFindElement(hash, findEt);
    void *et = table[vm];
    return et;
}

void *SuperHashTable::findElement(const void * findEt) const
{
    unsigned vm = doFindElement(getHashFromElement(findEt), findEt);
    void *et = table[vm];
    return et;
}

void *SuperHashTable::findExact(const void * findEt) const
{
    unsigned vm = doFindExact(findEt);
    void *et = table[vm];
    return et;
}

void SuperHashTable::doKill(void)
{
    // Check that releaseAll() has been called before doKill()
    // (in particular, derived class destructor must do this)
#ifdef _DEBUG
    // NOTE - don't use an assertex here as exceptions thrown from inside destructors tend to be problematic
    for (unsigned i = 0; i < tablesize; i++)
        if (table[i]) assert(!"SuperHashTable::doKill() : table not empty");
#endif
    free(table);
}

void SuperHashTable::releaseAll(void)
{
    unsigned i;
    for (i = 0; i < tablesize; i++)
    {
        void * et = table[i];
        table[i] = NULL;
        if (et)
            onRemove(et);
    }
}

void SuperHashTable::kill(void)
{
    releaseAll();
    doKill();
    tablesize = InitialTableSize;
    tablecount = 0;
    setCache(0);
    table = (void * *) checked_malloc(InitialTableSize*sizeof(void *),-604);
    memset(table,0,InitialTableSize*sizeof(void *));
}

void *SuperHashTable::addOrFind(void * donor)
{
    unsigned vs = getHashFromElement(donor);
    unsigned vm = doFind(vs, getFindParam(donor));
    void *et = table[vm];
    if(!et)
    {
        unsigned tablelim = getTableLimit(tablesize);
        if (tablecount>=tablelim)
        {
            expand();
            vm = doFind(vs, getFindParam(donor));
        }
        tablecount++;
        table[vm] = donor;
        onAdd(donor);
        return donor;
    } 
    return et;
}

void *SuperHashTable::next(const void *et) const
{
    unsigned i;
    if (!et)
    {
        if (!tablecount)
            return NULL;
        i = (unsigned) -1;
    }
    else
    {
        i = doFindExact(et);
        if (table[i] != et)
        {
            assertex(!"SuperHashTable::Next : start item not found");
            return NULL;
        }
    }
    while (1)
    {
        i++;
        if (i>=tablesize)
            return NULL;
        if (table[i])
            break;
    }
    setCache(i);
    return table[i];
}


bool SuperHashTable::matchesElement(const void *et, const void *searchET) const
{
    assertex(!"SuperHashTable::matchesElement needs to be overridden");
    return false;
}

