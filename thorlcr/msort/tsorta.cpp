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

#include "tsorta.hpp"

#include "eclhelper.hpp"
#include "thmem.hpp"
#include "thbuf.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/msort/tsorta.cpp $ $Id: tsorta.cpp 63605 2011-03-30 10:42:56Z nhicks $");

#ifdef _DEBUG
//#define _FULL_TRACE
#endif


VarElemArray::VarElemArray(IRowInterfaces *rowif,ISortKeySerializer *_keyserializer) 
  : allocator(rowif->queryRowAllocator()),serializer(rowif->queryRowSerializer()),deserializer(rowif->queryRowDeserializer())
{ 
    rows.setSizing(true,true);
    keyserializer = _keyserializer;
}

VarElemArray::~VarElemArray() 
{ 
}


void VarElemArray::appendLink(const void *row)
{
    if (row)
        LinkThorRow(row);
    rows.append(row);
}

void VarElemArray::clear() 
{ 
    rows.clear();
}


bool VarElemArray::checksorted(ICompare *icmp)
{
    unsigned i;
    unsigned n=ordinality();
    for (i=1;i<n;i++) 
        if (compare(icmp,i-1,i)>0)
            return false;
    return true;
}


void VarElemArray::sort(ICompare *icmp)
{
    rows.sort(*icmp,true);
}

size32_t VarElemArray::totalSize()
{ 
    return rows.totalSize();
}



void VarElemArray::serialize(MemoryBuffer &mb)
{
    rows.serialize(serializer,mb,true);
}


void VarElemArray::serializeCompress(MemoryBuffer &mb)
{
    MemoryBuffer exp;
    serialize(exp);
    fastLZCompressToBuffer(mb,exp.length(),exp.toByteArray());
}

void  VarElemArray::deserialize(const void *data, size32_t sz, bool append)
{   
    if (!append)
        clear();
    rows.deserialize(*allocator,deserializer,sz,data,true);

}


void VarElemArray::deserializeExpand(const void *data, size32_t,bool append)
{   
    MemoryBuffer mb;
    fastLZDecompressToBuffer(mb,data);
    deserialize(mb.bufferBase(),mb.length(),append);
}





const byte *VarElemArray::item(unsigned i)
{
    if (i>=rows.ordinality())
        return NULL;
    return (const byte *)rows.item(i);
}


unsigned VarElemArray::ordinality()
{
    return rows.ordinality();
}

void VarElemArray::transfer(VarElemArray &from)
{
    clear();
    ForEachItemIn(i,from) 
        appendLink(from.item(i));
    from.clear();
}

bool VarElemArray::equal(ICompare *icmp,VarElemArray &to)
{
    // slow but better than prev!
    unsigned n = to.ordinality();
    if (n!=ordinality())
        return false;
    for (unsigned i=0;i<n;i++)
        if (compare(icmp,i,to,i)!=0)
            return false;
    return true;
}


int VarElemArray::compare(ICompare *icmp,unsigned i,unsigned j)
{
    const void *p1 = rows.item(i);
    const void *p2 = rows.item(j);
    return  icmp->docompare(p1,p2);
}

void VarElemArray::appendLink(class VarElemArray &from,unsigned int i)
{
    const void *row = from.rows.item(i);
    appendLink(row);
}


int VarElemArray::compare(ICompare *icmp,unsigned i,VarElemArray &other,unsigned j)
{
    const void *p1 = rows.item(i);
    const void *p2 = other.rows.item(j);
    return icmp->docompare(p1,p2);
}

void VarElemArray::appendNull()
{
    rows.append(NULL);
}


bool VarElemArray::isNull(unsigned idx)
{
    if (idx>rows.ordinality())
        return true;
    return rows.item(idx)==NULL;
}


#define TRANSFERBLOCKSIZE 0x100000  // 1MB

class CThorRowSortedLoader : public CSimpleInterface, implements IThorRowSortedLoader
{
    IArrayOf<IRowStream> instrms;
    IArrayOf<IFile> auxfiles;
    CThorRowArray &rows;
    Owned<IOutputRowSerializer> serializer;
    unsigned numrows;
    offset_t totalsize;
    unsigned overflowcount;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorRowSortedLoader(CThorRowArray &_rows)
        : rows(_rows)
    {
        numrows = 0;
        totalsize = 0;
        overflowcount = 0;
    }

    ~CThorRowSortedLoader()
    {
        instrms.kill();
        ForEachItemInRev(i,auxfiles) {
            auxfiles.item(i).remove();
        }
    }

    IRowStream *load(
        IRowStream *in,
        IRowInterfaces *rowif,
        ICompare *icompare, 
        bool alldisk, 
        bool &abort, 
        bool &isempty,
        const char *tracename,
        bool isstable)
    {
        overflowcount = 0;
        unsigned nrecs;
        serializer.set(rowif->queryRowSerializer());
        rows.setSizing(true,false);
#ifdef _FULL_TRACE
        PROGLOG("CThorRowSortedLoader load start");
#endif
        isempty = true;
        while (!abort) {
            rows.clear();
            bool hasoverflowed = false;
            nrecs = rows.load(*in,true,abort,&hasoverflowed);
            if (nrecs)
                isempty = false;
            if (hasoverflowed) 
                overflowcount++;
#ifdef _FULL_TRACE
            PROGLOG("rows loaded overflowed = %s",hasoverflowed?"true":"false");
#endif
            if (nrecs&&icompare)
                rows.sort(*icompare,isstable);
            numrows += nrecs;
            totalsize += rows.totalSize();
            if (!hasoverflowed&&!alldisk) 
                break;
#ifdef _FULL_TRACE
            PROGLOG("CThorRowSortedLoader spilling %u",(unsigned)rows.totalSize());
#endif
            alldisk = true; 
            unsigned idx = newAuxFile();
            Owned<IExtRowWriter> writer = createRowWriter(&auxfiles.item(idx),rowif->queryRowSerializer(),rowif->queryRowAllocator(),false,false,false); 
            rows.save(writer);
            writer->flush();
#ifdef _FULL_TRACE
            PROGLOG("CThorRowSortedLoader spilt");
#endif
            if (!hasoverflowed) 
                break;
        }
        ForEachItemIn(i,auxfiles) {
#ifdef _FULL_TRACE
            PROGLOG("CThorRowSortedLoader spill file(%d) %s %"I64F"d",i,auxfiles.item(i).queryFilename(),auxfiles.item(i).size());
#endif
            instrms.append(*createSimpleRowStream(&auxfiles.item(i),rowif));
        }
        if (alldisk) {
#ifdef _FULL_TRACE
            PROGLOG("CThorRowSortedLoader clearing rows");
#endif
            rows.clear();
        }
        else
            instrms.append(*rows.createRowStream());
#ifdef _FULL_TRACE
        PROGLOG("CThorRowSortedLoader rows gathered %d stream%c",instrms.ordinality(),(instrms.ordinality()==1)?' ':'s');
#endif
        if (instrms.ordinality()==1) 
            return LINK(&instrms.item(0));
        if (icompare)
        {
            Owned<IRowLinkCounter> linkcounter = new CThorRowLinkCounter;
            return createRowStreamMerger(instrms.ordinality(),instrms.getArray(),icompare,false,linkcounter);
        }
        return createConcatRowStream(instrms.ordinality(),instrms.getArray());
    }

    unsigned newAuxFile() // only fixed size records currently supported
    {
        unsigned ret=auxfiles.ordinality();
        StringBuffer tempname;
        GetTempName(tempname,"srtspill",true);
        auxfiles.append(*createIFile(tempname.str()));
        return ret;
    }

    bool hasOverflowed()
    {
        return overflowcount!=0 ;
    }


    rowcount_t numRows()
    {
        return numrows;
    }

    offset_t totalSize()
    {
        return totalsize;
    }

    unsigned numOverflowFiles()
    {
        return auxfiles.ordinality();
    }

    unsigned numOverflows()
    {
        return overflowcount;
    }

    unsigned overflowScale()
    {
        // 1 if no spill
        if (!overflowcount)
            return 1;
        return numOverflowFiles()*2+3; // bit arbitrary
    }

};

IThorRowSortedLoader *createThorRowSortedLoader(CThorRowArray &rows)
{
    return new CThorRowSortedLoader(rows);
}


CThorKeyArray::CThorKeyArray(
    IRowInterfaces *_rowif,
    ISortKeySerializer *_serializer,
    ICompare *_icompare,
    ICompare *_ikeycompare,
    ICompare *_irowkeycompare)
{
    rowif.set(_rowif);
    sizes = NULL;
    filepos = NULL;
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
    keys.clear();
    delete filepos;
    filepos = NULL;
    totalserialsize = 0;
    serialrowsize = 0;
    totalfilesize = 0;
    filerecsize = 0;
    filerecnum = 0;
    index = 0;
    delete sizes;
    sizes = NULL;
}

CThorKeyArray::~CThorKeyArray()
{
    delete filepos;
    delete sizes;
}

void CThorKeyArray::setSampling(size32_t _maxsamplesize, unsigned _divisor)
{
    maxsamplesize = _maxsamplesize;
    serialrowsize = 0;
    sizes = NULL;
    index = 0;
    divisor = _divisor?_divisor:1;
}

void CThorKeyArray::expandfpos()
{
    if (!filepos) {
        filepos = new Int64Array;
        for (unsigned i=0;i<=filerecnum;i++)
            filepos->append(i*(offset_t)filerecsize);
        filerecsize = 0;
    }
}

void CThorKeyArray::add(const void *row)
{
    
    CSizingSerializer ssz;
    rowif->queryRowSerializer()->serialize(ssz,(const byte *)row);
    size32_t recsz = ssz.size();
    totalfilesize += recsz;
    if (filepos)
        filepos->append(totalfilesize);
    else if (filerecnum==0)
        filerecsize=recsz;
    else if (filerecsize!=recsz) {
        expandfpos();
        filepos->append(totalfilesize);
    }
    filerecnum++;
    if (maxsamplesize&&(index%divisor!=(divisor-1))) {
        index++;
        return;
    }
    size32_t sz;
    if (keyserializer) {
        RtlDynamicRowBuilder k(keyif->queryRowAllocator());
        sz = keyserializer->recordToKey(k,row,recsz);
        row = k.finalizeRowClear(sz);
    }
    else {
        sz = recsz;
        LinkThorRow(row);
    }
    if (maxsamplesize) {
        while (keys.ordinality()&&(totalserialsize+sz>maxsamplesize)) 
            split();
    }
    if (sizes)
       sizes->append(sz);
    else if (keys.ordinality()==0) 
        serialrowsize = sz;
    else if (serialrowsize!=sz) {
        sizes = new UnsignedArray;
        for (unsigned i=0;i<keys.ordinality();i++)
            sizes->append(serialrowsize);
       sizes->append(sz);
       serialrowsize = 0;
    }
    totalserialsize += sz;
    keys.append(row);
};

void CThorKeyArray::serialize(MemoryBuffer &mb)
{
    // NB doesn't serialize filepos
    unsigned n = keys.ordinality();
    unsigned i;
    mb.append(n);
    mb.append(serialrowsize);
    if (sizes) {
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
    size32_t pos = mb.length();
    mb.append((size32_t)0);
    IOutputRowSerializer *serializer = haskeyserializer?keyif->queryRowSerializer():rowif->queryRowSerializer();
    CMemoryRowSerializer msz(mb);
    for (i=0;i<n;i++) 
        serializer->serialize(msz,(const byte *)keys.item(i));
    size32_t l = mb.length()-pos-sizeof(size32_t);
    mb.writeDirect(pos,sizeof(l),&l);
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
    if (n&&(rss!=serialrowsize)) {
        if (rss==0) {
            if (nsz) {
                sizes = new UnsignedArray;
                for (i=0;i<keys.ordinality();i++)
                    sizes->append(serialrowsize);
                serialrowsize = 0;
            }
        }
        else {
            if (!sizes)
                sizes = new UnsignedArray;
            for (i=0;i<n;i++)
                sizes->append(rss);
            rss = 0;
        }
    }
    if (nsz) {
        if (!sizes)
            sizes = new UnsignedArray;
        for (i=0;i<nsz;i++) {
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
    if (sizes) {
        UnsignedArray *newsizes = new UnsignedArray;
        for (i = 0; i<n; i++)
            newsizes->append(sizes->item(ra[i]));
        delete sizes;
        sizes = newsizes;
    }
    Int64Array *newpos = new Int64Array;
    for (i = 0; i<n; i++)
        newpos->append(filepos?filepos->item(ra[i]):filerecsize*(offset_t)ra[i]);
    delete filepos;
    filepos = newpos;
    keys.reorder(0,n,ra);
    delete [] ra;
}


void CThorKeyArray::createSortedPartition(unsigned pn) 
{   
    // reduces to pn-1 keys to split into pn equal parts
    if (pn<=1) {
        clear();
        return;
    }
    unsigned n = ordinality();
    if (n<pn) {
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
    if (sizes) {
        UnsignedArray *newsizes = new UnsignedArray;
        for (i = 1; i<pn; i++) {
            unsigned p = i*n/pn;
            newsizes->append(sizes->item(ra[p]));
        }
        delete sizes;
        sizes = newsizes;
    }
    Int64Array *newpos = new Int64Array;
    for (i = 0; i<pn; i++) {
        unsigned p = i*n/pn;
        newpos->append(filepos?filepos->item(ra[p]):filerecsize*(offset_t)ra[p]);
    }
    delete filepos;
    filepos = newpos;
    CThorRowArray newrows;
    for (i = 1; i<pn; i++) {
        unsigned p = i*n/pn;
        const void *r = keys.item(ra[p]);
        LinkThorRow(r);
        newrows.append(r);
    }
    keys.swapWith(newrows);
}

int CThorKeyArray::binchopPartition(const void * row,bool lt)
{
    int n = (int)ordinality();
    if (n==0)
        return -1;
    int a = 0;
    int b = n;
    int cmp;
#ifdef _TESTING
try {
#endif
    while (a<b) {
        unsigned m = (a+b)/2;
        cmp = keyRowCompare((unsigned)m,row);
        if (cmp>0) 
            b = m;
        else {
            if (cmp==0) {
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
    if (lt) {
        if (a<n) 
            assertex(keyRowCompare((unsigned)a,row)>=0);
        if (a>0)
            assertex(keyRowCompare((unsigned)a-1,row)<0);
    }
    else {
        if (a<n) 
            assertex(keyRowCompare((unsigned)a,row)>0);
        if (a>0)
            assertex(keyRowCompare((unsigned)a-1,row)<=0);
    }
}
catch (IException *e) {
    EXCLOG(e,"binchopPartition");
    StringBuffer s("row: ");
    unsigned i;
    for (i=0;i<10;i++)
        s.appendf(" %02x",(int)*((const byte *)row+i));
    PROGLOG("%s",s.str());
    for (i=0;i<(unsigned)n;i++) {
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
    return p*filerecsize;
}

offset_t CThorKeyArray::findLessRowPos(const void * row) 
{
    int p = binchopPartition(row,true);
    if (p<0)
        return (offset_t)-1;
    if (filepos)
        return filepos->item(p);
    return p*filerecsize;
}

void CThorKeyArray::calcPositions(IFile *file,CThorKeyArray &sample)
{
    // calculates positions based on sample
    // not fast!
    delete filepos;
    filepos = new Int64Array;
    for (unsigned i0=0;i0<ordinality();i0++)
        filepos->append(-1);
    filerecsize = 0;
    ForEachItemIn(i,*filepos) {
        OwnedConstThorRow row = getRow(i);
        offset_t pos = sample.findLessRowPos(row);
        if (pos==(offset_t)-1) 
            pos = 0;
        // should do bin-chop for fixed length but initially do sequential search
        Owned<IRowStream> s = createRowStream(file,rowif,pos,(offset_t)-1,RCUNBOUND,false,false);
        loop {
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
        //PROGLOG("CThorKeyArray::calcPositions %d: initpos = %"I64F"d pos = %"I64F"d",i,initpos,pos);
        filepos->replace(pos,i);
    }
    totalfilesize = sample.totalfilesize;
}

const void *CThorKeyArray::getRow(unsigned idx)
{
    OwnedConstThorRow k;
    k.set(keys.item(idx));
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
    divisor *= 2;
    // not that fast!
    unsigned n = ordinality();
    CThorRowArray newkeys;
    UnsignedArray *newsizes = sizes?new UnsignedArray:NULL;
    Int64Array *newfilepos = filepos?new Int64Array:NULL;
    unsigned newss = 0;
    for (unsigned i=0;i<n;i+=2) {
        const void *k = keys.item(i);
        LinkThorRow(k);
        newkeys.append(k);
        size32_t sz = sizes?sizes->item(i):serialrowsize;
        if (newsizes)
            newsizes->append(sz);
        newss += sz;
        if (newfilepos)
            newfilepos->append(filepos->item(i));
    }
    keys.swapWith(newkeys);
    if (newsizes) {
        delete sizes;
        sizes = newsizes;
    }
    totalserialsize = newss;
    if (newfilepos) {
        delete filepos;
        filepos = newfilepos;
    }
}

offset_t CThorKeyArray::getFilePos(unsigned idx)
{
    return idx<ordinality()?(filepos?filepos->item(idx):idx*filerecsize):totalfilesize;
}


void traceKey(IOutputRowSerializer *serializer, const char *prefix,const void *key)
{
    MemoryBuffer mb;
    const byte *k = (const byte *)key;
    size32_t sz = 0;
    if (serializer&&k) {
        CMemoryRowSerializer mbsz(mb);
        serializer->serialize(mbsz,(const byte *)k);
        k = (const byte *)mb.bufferBase();
        sz = mb.length();
    }
    StringBuffer out;
    if (sz)
        out.appendf("%s(%d): ",prefix,sz);
    else {
        out.append(prefix).append(": ");
        if (k)
            sz = 16;
        else
            out.append("NULL");
    }
    bool first=false;
    while (sz) {
        if (first)
            first=false;
        else
            out.append(',');
        if ((sz>=3)&&isprint(k[0])&&isprint(k[1])&&isprint(k[2])) {
            out.append('"');
            do {
                out.append(*k);
                sz--;
                if (sz==0)
                    break;
                if (out.length()>1024)
                    break;
                k++;
            } while (isprint(*k));
            out.append('"');
        }
        if (out.length()>1024) {
            out.append("...");
            break;
        }
        if (sz) {
            out.appendf("%2x",(unsigned)*k);
            k++;
            sz--;
        }
    }
    PROGLOG("%s",out.str());
}

void CThorKeyArray::traceKey(const char *prefix,unsigned idx)
{
    StringBuffer s(prefix);
    s.appendf("[%d]",idx);
    IOutputRowSerializer *serializer = keyserializer?keyif->queryRowSerializer():rowif->queryRowSerializer();
    ::traceKey(serializer,s.str(),queryKey(idx));
}

        
