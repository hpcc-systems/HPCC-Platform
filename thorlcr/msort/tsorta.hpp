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

// MSort Memory Allocator
#ifndef TSortA_HPP
#define TSortA_HPP


#ifdef _DEBUG
#define _TESTSLAVE
#endif

#include "jlib.hpp"
#include "jio.hpp"
#include "jlzw.hpp"
#include "thbuf.hpp"
#include "thmem.hpp"


class BytePointerArray;
interface ICompare;
interface IRecordSize;
interface ISortKeySerializer;

interface IThorRowSortedLoader: extends IInterface
{
    virtual IRowStream *load(                       // if returns NULL if no overflay
        IRowStream *in,
        IRowInterfaces *rowif,
        ICompare *icompare, 
        bool alldisk, 
        bool &abort, 
        bool &isempty,
        const char *tracename,
        bool isstable,
        unsigned maxcores
    )=0;
    virtual bool hasOverflowed()=0;
    virtual rowcount_t numRows()=0;
    virtual offset_t totalSize()=0;
    virtual unsigned numOverflowFiles()=0;
    virtual unsigned numOverflows()=0;
    virtual unsigned overflowScale()=0;
};

class CThorKeyArray
{
    CActivityBase &activity;
    Linked<IRowInterfaces> rowif;
    Linked<IRowInterfaces> keyif;
    CThorExpandingRowArray keys;
    size32_t maxsamplesize;
    offset_t totalserialsize;
    size32_t serialrowsize;     // 0 when not known
    unsigned index;
    unsigned divisor;
    ISortKeySerializer* keyserializer;
    ICompare *icompare;
    ICompare *ikeycompare;
    ICompare *irowkeycompare;
    OwnedPtr<UnsignedArray> sizes;       // serial sizes (needed if keysize==0)
    OwnedPtr<Int64Array> filepos;
    size32_t filerecsize;
    size32_t filerecnum;
    offset_t totalfilesize;



    void split();
    offset_t findLessEqRowPos(const void * row);
    offset_t findLessRowPos(const void * row);
    int keyRowCompare(unsigned keyidx,const void *row);
    void expandfpos();
    const void *queryKey(unsigned idx) { return keys.query(idx); }
    const void *getRow(unsigned idx);
    int binchopPartition(const void * row,bool lt);
public:

    CThorKeyArray(
        CActivityBase &activity,
        IRowInterfaces *_rowif,
        ISortKeySerializer *_serializer,
        ICompare *_icompare,
        ICompare *_ikeycompare,
        ICompare *_irowkeycompare); 
    void clear();
    void add(const void *row);
    unsigned ordinality() { return keys.ordinality(); }
    void serialize(MemoryBuffer &mb);
    void deserialize(MemoryBuffer &mb,bool append);
    void sort();
    void createSortedPartition(unsigned pn);
    void calcPositions(IFile *file,CThorKeyArray &sample);
    void setSampling(size32_t _maxsamplesize, unsigned _divisor=0);
    int keyCompare(unsigned a,unsigned b);
    offset_t getFilePos(unsigned idx);
    void traceKey(const char *prefix,unsigned idx);
};

extern void traceKey(IOutputRowSerializer *serializer,const char *prefix,const void *key);


#endif



