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
#include "thmem.hpp"


class BytePointerArray;
interface ICompare;
interface IRecordSize;
interface ISortKeySerializer;

class VarElemArray
{ // simple expanding array for variable sized elements
  // note space not reclaimed and rows stored serialized (if serializer supplied)
  // only intended for relatively small arrays
public:
    VarElemArray(IRowInterfaces *rowif,ISortKeySerializer *_serializer);
    ~VarElemArray();
    void appendLink(const void *row);
    void appendLink(VarElemArray &from,unsigned idx);
    void appendNull();

    void clear();
    void serialize(MemoryBuffer &mb); // mallocs data
    void deserialize(const void *data,size32_t sz,bool append);
    void serializeCompress(MemoryBuffer &mb); // mallocs data
    void deserializeExpand(const void *data,size32_t sz,bool append);
    const byte *item(unsigned i);
    unsigned ordinality();
    void transfer(VarElemArray &from);
    bool equal(ICompare *icmp,VarElemArray &to);
    void sort(ICompare *icmp);
    int compare(ICompare *icmp,unsigned i,unsigned j);
    int compare(ICompare *icmp,unsigned i,VarElemArray &other,unsigned j);
    bool isNull(unsigned idx);
    bool checksorted(ICompare *icmp);
    size32_t totalSize();
private:
    CThorRowArray rows;
    Linked<ICompressor> compressor;
    Linked<IExpander> expander;
    ISortKeySerializer *keyserializer;
    Linked<IEngineRowAllocator> allocator;
    Linked<IOutputRowSerializer> serializer;
    Linked<IOutputRowDeserializer> deserializer;
};


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
        bool isstable
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
    Linked<IRowInterfaces> rowif;
    Linked<IRowInterfaces> keyif;
    CThorRowArray keys;
    size32_t maxsamplesize;
    offset_t totalserialsize;
    size32_t serialrowsize;     // 0 when not known
    unsigned index;
    unsigned divisor;
    ISortKeySerializer* keyserializer;
    ICompare *icompare;
    ICompare *ikeycompare;
    ICompare *irowkeycompare;
    UnsignedArray *sizes;       // serial sizes (needed if keysize==0)
    Int64Array *filepos;         
    size32_t filerecsize;
    size32_t filerecnum;
    offset_t totalfilesize;



    void split();
    offset_t findLessEqRowPos(const void * row);
    offset_t findLessRowPos(const void * row);
    int keyRowCompare(unsigned keyidx,const void *row);
    void expandfpos();
    const void *queryKey(unsigned idx) { return keys.item(idx); }
    const void *getRow(unsigned idx);
    int binchopPartition(const void * row,bool lt);
public:

    CThorKeyArray(
        IRowInterfaces *_rowif,
        ISortKeySerializer *_serializer,
        ICompare *_icompare,
        ICompare *_ikeycompare,
        ICompare *_irowkeycompare); 
    ~CThorKeyArray();
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

IThorRowSortedLoader *createThorRowSortedLoader(CThorRowArray &rows); // NB only contains all rows if hasOverflowed false


#endif



