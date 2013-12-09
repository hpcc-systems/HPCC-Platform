/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    bool needFPosExpand;

    void split();
    offset_t findLessEqRowPos(const void * row);
    offset_t findLessRowPos(const void * row);
    int keyRowCompare(unsigned keyidx,const void *row);
    void expandFPos();
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
    offset_t getFixedFilePos(unsigned i);
    offset_t getFilePos(unsigned idx);
    void traceKey(const char *prefix,unsigned idx);
};

extern void traceKey(IOutputRowSerializer *serializer,const char *prefix,const void *key);


#endif



