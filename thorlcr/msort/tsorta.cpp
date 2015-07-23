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

#include "platform.h"
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdarg.h>
#ifdef _WIN32
#include <process.h>
#endif

#include "jexcept.hpp"
#include "jiface.hpp"
#include "jmisc.hpp"
#include "jsort.hpp"
#include "jsorta.hpp"
#include "jmutex.hpp"
#include "jlzw.hpp"
#include "jflz.hpp"
#include "thbufdef.hpp"
#include "thgraph.hpp"
#include "tsorta.hpp"

#include "eclhelper.hpp"
#include "thbuf.hpp"

#ifdef _DEBUG
//#define _FULL_TRACE
#endif


CThorKeyArray::CThorKeyArray(
    CActivityBase &_activity,
    IRowInterfaces *_rowif,
    ISortKeySerializer *_serializer,
    ICompare *_icompare,
    ICompare *_ikeycompare,
    ICompare *_irowkeycompare) : activity(_activity), keys(_activity, _rowif)
{
    rowif.set(_rowif);
    clear();
    maxsamplesize = 0;
    divisor = 1;
    keyserializer = NULL;
    if (_serializer) {
        keyserializer = _serializer;
        keyif.setown(createRowInterfaces(keyserializer->queryRecordSize(), rowif->queryActivityId(), rowif->queryCodeContext()));
    }
    icompare = _icompare;
    ikeycompare = _ikeycompare?_ikeycompare:(_serializer?NULL:_icompare);
    irowkeycompare = _irowkeycompare?_irowkeycompare:(_serializer?NULL:_icompare);
}

void CThorKeyArray::clear()
{
    keys.kill();
    sizes.clear();
    filepos.clear();
    totalserialsize = 0;
    serialrowsize = 0;
    totalfilesize = 0;
    filerecsize = 0;
    filerecnum = 0;
    needFPosExpand = false;
}

void CThorKeyArray::setSampling(size32_t _maxsamplesize, unsigned _divisor)
{
    maxsamplesize = _maxsamplesize;
    serialrowsize = 0;
    sizes.clear();
    assertex(filerecnum==0); // must not have been added to already
    divisor = _divisor?_divisor:1;
}

void CThorKeyArray::expandFPos()
{
    if (!filepos)
    {
        filepos.setown(new Int64Array);
        filepos->ensure(keys.ordinality());
        for (unsigned i=0;i<keys.ordinality();i++)
            filepos->append(getFixedFilePos(i));
        filerecsize = 0;
    }
}

void CThorKeyArray::add(const void *row)
{
    CSizingSerializer ssz;
    rowif->queryRowSerializer()->serialize(ssz,(const byte *)row);
    size32_t recSz = ssz.size();
    totalfilesize += recSz;
    if (filerecnum==0)
        filerecsize=recSz;
    else if (filerecsize!=recSz)
        needFPosExpand = true;
    filerecnum++;

    if (maxsamplesize)
    {
        // only in use after a split()
        if ((filerecnum-1)%divisor != 0)
            return;
    }

    size32_t keySz;
    if (keyserializer)
    {
        RtlDynamicRowBuilder k(keyif->queryRowAllocator());
        keySz = keyserializer->recordToKey(k,row,recSz);
        row = k.finalizeRowClear(keySz);
    }
    else
    {
        keySz = recSz;
        LinkThorRow(row);
    }
    if (maxsamplesize)
    {
        bool splitDone=false;
        while (keys.ordinality()&&(totalserialsize+keySz>maxsamplesize))
        {
            split();
            splitDone = true;
        }
        // if split, current rec may well be candidate to filter out now.
        if (splitDone && ((filerecnum-1)%divisor != 0))
        {
            ReleaseThorRow(row);
            return;
        }
    }
    if (filepos)
        filepos->append(totalfilesize);
    else if (needFPosExpand)
    {
        expandFPos();
        filepos->append(totalfilesize);
    }

    if (sizes)
        sizes->append(keySz);
    else if (keys.ordinality()==0)
        serialrowsize = keySz;
    else if (serialrowsize!=keySz)
    {
        sizes.setown(new UnsignedArray);
        sizes->ensure(keys.ordinality()+1);
        for (unsigned i=0;i<keys.ordinality();i++)
            sizes->append(serialrowsize);
        sizes->append(keySz);
        serialrowsize = 0;
    }
    totalserialsize += keySz;
    keys.append(row);
}

void CThorKeyArray::serialize(MemoryBuffer &mb)
{
    // NB doesn't serialize filepos
    unsigned n = keys.ordinality();
    unsigned i;
    mb.append(n);
    mb.append(serialrowsize);
    if (sizes)
    {
        assertex(n==sizes->ordinality());
        mb.append(n);
        for (i=0;i<n;i++)
            mb.append(sizes->item(i));
    }
    else
        mb.append((unsigned)0);
    mb.append(totalserialsize);
    bool haskeyserializer = keyserializer!=NULL;
    mb.append(haskeyserializer);
    DelayedSizeMarker sizeMark(mb);
    IOutputRowSerializer *serializer = haskeyserializer?keyif->queryRowSerializer():rowif->queryRowSerializer();
    CMemoryRowSerializer msz(mb);
    for (i=0;i<n;i++) 
        serializer->serialize(msz,(const byte *)keys.query(i));
    sizeMark.write();
}

void CThorKeyArray::deserialize(MemoryBuffer &mb,bool append)
{
    // NB doesn't deserialize filepos
    if (!append)
        clear();
    unsigned n;
    unsigned i;
    mb.read(n);
    size32_t rss;
    mb.read(rss);
    unsigned nsz;
    mb.read(nsz);
    if (n&&(rss!=serialrowsize))
    {
        if (rss==0)
        {
            if (nsz)
            {
                sizes.setown(new UnsignedArray);
                for (i=0;i<keys.ordinality();i++)
                    sizes->append(serialrowsize);
                serialrowsize = 0;
            }
        }
        else
        {
            if (!sizes)
                sizes.setown(new UnsignedArray);
            for (i=0;i<n;i++)
                sizes->append(rss);
            rss = 0;
        }
    }
    if (nsz)
    {
        if (!sizes)
            sizes.setown(new UnsignedArray);
        for (i=0;i<nsz;i++)
        {
            unsigned s;
            mb.read(s);
            sizes->append(s);
        }
    }
    serialrowsize = rss;
    offset_t ssz;
    mb.read(ssz);
    totalserialsize += ssz;
    bool haskeyserializer;
    mb.read(haskeyserializer);
    assertex((keyserializer!=NULL)==haskeyserializer);
    IOutputRowDeserializer *deserializer = haskeyserializer?keyif->queryRowDeserializer():rowif->queryRowDeserializer();
    IEngineRowAllocator *allocator = haskeyserializer?keyif->queryRowAllocator():rowif->queryRowAllocator();
    size32_t l;
    mb.read(l);
    CThorStreamDeserializerSource dsz(l,mb.readDirect(l));
    for (i=0;i<n;i++) {
        assertex(!dsz.eos());
        RtlDynamicRowBuilder rowBuilder(allocator);
        size32_t sz = deserializer->deserialize(rowBuilder,dsz);
        keys.append(rowBuilder.finalizeRowClear(sz));
    }
}




static CriticalSection kcsect;
static CThorKeyArray * kcthis;
static int keyCompare(const void *a,const void *b)
{
    return kcthis->keyCompare(*(unsigned*)a,*(unsigned*)b);
}

void CThorKeyArray::sort() 
{
    // bit slow, but I have seen worse
    unsigned n = ordinality();
    unsigned *ra = new unsigned[n];
    unsigned i;
    for (i = 0; i<n; i++)
        ra[i] = i;
    {
        CriticalBlock block(kcsect);
        kcthis = this;
        qsortarray<unsigned>(ra,n,::keyCompare);
    }
    if (sizes)
    {
        OwnedPtr<UnsignedArray> newsizes(new UnsignedArray);
        newsizes->ensure(n);
        for (i = 0; i<n; i++)
            newsizes->append(sizes->item(ra[i]));
        sizes.setown(newsizes.getClear());
    }
    keys.reorder(0,n,ra);
    delete [] ra;
}


void CThorKeyArray::createSortedPartition(unsigned pn) 
{   
    // reduces to pn-1 keys to split into pn equal parts
    if (pn<=1)
    {
        clear();
        return;
    }
    unsigned n = ordinality();
    if (n<pn)
    {
        sort();
        return;
    }
    CriticalBlock block(kcsect);
    kcthis = this;
    unsigned *ra = new unsigned[n];
    unsigned i;
    for (i = 0; i<n; i++)
        ra[i] = i;
    qsortarray<unsigned>(ra, n, ::keyCompare);
    if (sizes)
    {
        OwnedPtr<UnsignedArray> newsizes(new UnsignedArray);
        newsizes->ensure(pn);
        for (i = 1; i<pn; i++) {
            unsigned p = i*n/pn;
            newsizes->append(sizes->item(ra[p]));
        }
        sizes.setown(newsizes.getClear());
    }
    CThorExpandingRowArray newrows(activity, rowif);
    newrows.resize(pn);
    for (i = 1; i<pn; i++)
    {
        unsigned p = i*n/pn;
        newrows.append(keys.get(ra[p]));
    }
    keys.swap(newrows);
}

int CThorKeyArray::binchopPartition(const void * row,bool lt)
{
    int n = (int)ordinality();
    if (n==0)
        return -1;
    int a = 0;
    int b = n;
    int cmp = 0;
#ifdef _TESTING
try {
#endif
    while (a<b)
    {
        unsigned m = (a+b)/2;
        cmp = keyRowCompare((unsigned)m,row);
        if (cmp>0) 
            b = m;
        else
        {
            if (cmp==0)
            {
#ifdef _TESTING
                a = m;
                while ((a<n)&&(keyCompare(m,a)==0))
                    a++;
                if (a<n)
                    assertex(keyRowCompare((unsigned)a,row)>0);
#endif
                while ((m>0)&&(keyCompare(m-1,m)==0))
                    m--;
#ifdef _TESTING
                if (m>0) 
                    assertex(keyRowCompare((unsigned)m-1,row)<0);
#endif
                if (lt)
                    return m-1;
                return m;
            }
            a = m+1;
        }
    }
#ifdef _TESTING
    if (lt)
    {
        if (a<n) 
            assertex(keyRowCompare((unsigned)a,row)>=0);
        if (a>0)
            assertex(keyRowCompare((unsigned)a-1,row)<0);
    }
    else
    {
        if (a<n) 
            assertex(keyRowCompare((unsigned)a,row)>0);
        if (a>0)
            assertex(keyRowCompare((unsigned)a-1,row)<=0);
    }
}
catch (IException *e)
{
    EXCLOG(e,"binchopPartition");
    StringBuffer s("row: ");
    unsigned i;
    for (i=0;i<10;i++)
        s.appendf(" %02x",(int)*((const byte *)row+i));
    PROGLOG("%s",s.str());
    for (i=0;i<(unsigned)n;i++)
    {
        s.clear().appendf("k%d:",i);
        const byte *k=(const byte *)queryKey(i);
        for (unsigned j=0;j<10;j++) 
            s.appendf(" %02x",(int)*(k+j));
        PROGLOG("%s",s.str());
    }
    PROGLOG("a=%d, b=%d, cmp=%d",a,b,cmp);
    throw;
}
#endif
    while (lt&&a&&(keyRowCompare((unsigned)a-1,row)==0))
        a--;
    return a-1;
}

offset_t CThorKeyArray::findLessEqRowPos(const void * row) 
{
    int p = binchopPartition(row,false);
    if (p<0)
        return (offset_t)-1;
    if (filepos)
        return filepos->item(p);
    return getFixedFilePos(p);
}

offset_t CThorKeyArray::findLessRowPos(const void * row) 
{
    int p = binchopPartition(row,true);
    if (p<0)
        return (offset_t)-1;
    if (filepos)
        return filepos->item(p);
    return getFixedFilePos(p);
}

void CThorKeyArray::calcPositions(IFile *file,CThorKeyArray &sample)
{
    // calculates positions based on sample
    // not fast!
    filepos.setown(new Int64Array);
    filepos->ensure(ordinality());
    for (unsigned i0=0;i0<ordinality();i0++)
        filepos->append(-1);
    filerecsize = 0;
    ForEachItemIn(i,*filepos)
    {
        OwnedConstThorRow row = getRow(i);
        offset_t pos = sample.findLessRowPos(row);
        if (pos==(offset_t)-1) 
            pos = 0;
        // should do bin-chop for fixed length but initially do sequential search
        Owned<IRowStream> s = createRowStreamEx(file, rowif, pos);
        loop
        {
            OwnedConstThorRow rowcmp = s->nextRow();
            if (!rowcmp)
                break;
            int cmp = icompare->docompare(rowcmp,row);
            if (cmp>=0) 
                break;
            CSizingSerializer ssz;
            rowif->queryRowSerializer()->serialize(ssz,(const byte *)rowcmp.get());
            pos += ssz.size();
        }
        //PROGLOG("CThorKeyArray::calcPositions %d: initpos = %" I64F "d pos = %" I64F "d",i,initpos,pos);
        filepos->replace(pos,i);
    }
    totalfilesize = sample.totalfilesize;
}

const void *CThorKeyArray::getRow(unsigned idx)
{
    OwnedConstThorRow k;
    k.set(keys.query(idx));
    if (!keyserializer) 
        return k.getClear();
    RtlDynamicRowBuilder r(rowif->queryRowAllocator());
    size32_t rs;
    keyserializer->keyToRecord(r,k,rs);
    return r.finalizeRowClear(rs);
}

int CThorKeyArray::keyCompare(unsigned a,unsigned b)
{
    if (ikeycompare)
        return ikeycompare->docompare(queryKey(a),queryKey(b));
    OwnedConstThorRow cmprow = getRow(b);
    return keyRowCompare(a,cmprow);
}

int CThorKeyArray::keyRowCompare(unsigned keyidx,const void *row)
{
    if (irowkeycompare)
        return -irowkeycompare->docompare(row,queryKey(keyidx));
    OwnedConstThorRow cmprow = getRow(keyidx);
    return icompare->docompare(cmprow,row);
}

void CThorKeyArray::split()
{
    assertex(filerecnum);
    divisor *= 2;
    // not that fast!
    unsigned n = ordinality();
    CThorExpandingRowArray newkeys(activity, rowif);
    newkeys.resize(n);
    OwnedPtr<UnsignedArray> newSizes;
    OwnedPtr<Int64Array> newFilePos;
    if (sizes)
    {
        newSizes.setown(new UnsignedArray);
        newSizes->ensure(n);
    }
    if (filepos)
    {
        newFilePos.setown(new Int64Array);
        newFilePos->ensure(n);
    }
    unsigned newss = 0;
    for (unsigned i=0;i<n;i+=2)
    {
        const void *k = keys.query(i);
        LinkThorRow(k);
        newkeys.append(k);
        size32_t sz = sizes?sizes->item(i):serialrowsize;
        if (newSizes)
            newSizes->append(sz);
        newss += sz;
        if (newFilePos)
            newFilePos->append(filepos->item(i));
    }
    keys.swap(newkeys);
    if (newSizes)
        sizes.setown(newSizes.getClear());
    totalserialsize = newss;
    if (newFilePos)
        filepos.setown(newFilePos.getClear());
}

offset_t CThorKeyArray::getFixedFilePos(unsigned i)
{
    return ((offset_t)i)*((offset_t)divisor)*((offset_t)filerecsize);
}

offset_t CThorKeyArray::getFilePos(unsigned idx)
{
    return idx<ordinality()?(filepos?filepos->item(idx):getFixedFilePos(idx)):totalfilesize;
}

void traceKey(IOutputRowSerializer *serializer, const char *prefix,const void *key)
{
    StringBuffer out;
    getRecordString(key, serializer, prefix, out);
    PROGLOG("%s",out.str());
}

void CThorKeyArray::traceKey(const char *prefix,unsigned idx)
{
    StringBuffer s(prefix);
    s.appendf("[%d]",idx);
    IOutputRowSerializer *serializer = keyserializer?keyif->queryRowSerializer():rowif->queryRowSerializer();
    ::traceKey(serializer,s.str(),queryKey(idx));
}

