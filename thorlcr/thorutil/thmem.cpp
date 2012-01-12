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

#include "jmisc.hpp"
#include "jio.hpp"
#include "jsort.hpp"
#include "jsorta.hpp"
#include "jvmem.hpp"

#include "thbufdef.hpp"
#include "thor.hpp"
#include "thormisc.hpp"
#include "eclhelper.hpp"
#include "dautils.hpp"
#include "daclient.hpp"
#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.ipp"
#include "eclrtl.hpp"

#include "thmem.hpp"

#include "thalloc.hpp"

#undef ALLOCATE
#undef CLONE
#undef RESIZEROW
#undef SHRINKROW
#undef MEMACTIVITYTRACESTRING 


#include "thbuf.hpp"
#include "thmem.hpp"

#ifdef _DEBUG
//#define _TESTING
#define ASSERTEX(c) assertex(c)
#else
#define ASSERTEX(c)
#endif
static IThorRowManager *ThorMemoryManager;
static memsize_t MTthreshold=0; 
static CriticalSection MTcritsect;  // held when blocked 
static Owned<ILargeMemLimitNotify> MTthresholdnotify;
static bool MTlocked = false;


class CThorRowArrayException: public CSimpleInterface, public IThorRowArrayException
{
    size32_t sz;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CThorRowArrayException(size32_t _sz) 
    {
//      DebugBreak();
        sz = _sz;
    };
    
    int             errorCode() const { return 101; }
    StringBuffer &  errorMessage(StringBuffer &str) const
    { 
        return str.append("CThorRowArray: Group too large: ").append(sz).append(" bytes");
    }
    MessageAudience errorAudience() const { return MSGAUD_user; }
};

class ThorEngineRowAllocator;

static class CThorRowAllocatorCache : implements IThorRowAllocatorCache
{
    PointerArray ThorRowAllocators; 
    mutable SpinLock ThorMMsect;

public:
    inline ThorEngineRowAllocator &item(unsigned cacheId) const
    {
        SpinBlock block(ThorMMsect);
        ASSERTEX(cacheId<ThorRowAllocators.ordinality());
        ThorEngineRowAllocator *ret = ((ThorEngineRowAllocator *)ThorRowAllocators.item(cacheId));
        ASSERTEX(ret);
        return *ret;
    }

    unsigned append(ThorEngineRowAllocator &a);
    unsigned getActivityId(unsigned cacheId) const;
    StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const;
    void onDestroy(unsigned cacheId, void *row) const;
    void reset();  // resets allocators
    void clear();
    size32_t subSize(unsigned cacheId,const void *row) const;

} ThorAllocatorCache;


void checkMultiThorMemoryThreshold(bool inc)
{
    if (MTthresholdnotify.get())    {
        CriticalBlock block(MTcritsect);
        memsize_t used = ThorMemoryManager->allocated();
        if (MTlocked) {
            if (used<MTthreshold/2) {
                DBGLOG("Multi Thor threshold lock released: %"I64F"d",(offset_t)used);
                MTlocked = false;
                MTthresholdnotify->give(used);
            }
        }
        else if (used>MTthreshold) {
            DBGLOG("Multi Thor threshold  exceeded: %"I64F"d",(offset_t)used);
            if (!MTthresholdnotify->take(used)) {
                throw createOutOfMemException(-9,
                    1024,  // dummy value
                    used);
            }
            DBGLOG("Multi Thor lock taken");
            MTlocked = true;
        }
    }
}

extern graph_decl void setMultiThorMemoryNotify(size32_t size,ILargeMemLimitNotify *notify)
{
    CriticalBlock block(MTcritsect);
    if (MTthresholdnotify.get()&&!notify&&MTlocked) {
        MTlocked = false;
        MTthresholdnotify->give(0);
    }
    MTthreshold = size;
    MTthresholdnotify.set(notify);
    if (notify)
        checkMultiThorMemoryThreshold(true);
}




void CThorRowArray::adjSize(const void *row, bool inc)
{
    if (!row)
        return;
    size32_t size = thorRowMemoryFootprint(row);
    size32_t prevtot = totalsize;
    if (inc) {
        if (raiseexceptions) {
            size32_t tot = size+totalMem()+PERROWOVERHEAD;
            if (tot>maxtotal)
                throw new CThorRowArrayException(tot);
        }
        totalsize += size;
        overhead += PERROWOVERHEAD;  // more - take into acount memory manager granularity?
    }
    else {
        totalsize -= size;
        overhead -= PERROWOVERHEAD;  // more - take into acount memory manager granularity?
    }
    if (!prevtot||!totalsize||(prevtot/0x100000!=totalsize/0x100000)) { // just check on transitions or when 0
        checkMultiThorMemoryThreshold(inc);
    }
}

void CThorRowArray::setNull(unsigned idx)
{
    OwnedConstThorRow row = itemClear(idx);
}

void CThorRowArray::removeRows(unsigned i,unsigned n)
{
    unsigned o = ordinality();
    if (i>=o)
        return;
    if (i+n>o) 
        n = o-i;
    if (n==0)
        return;
    if (n==o) {
        reset(false);
        return;
    }
    const byte **from = ((const byte **)ptrbuf.toByteArray())+i;
    for (unsigned j=0; j<n; j++) {
        if (sizing)
            adjSize(from[j],false);
        ReleaseThorRow(from[j]);
    }
    memmove(&from[0],&from[n],(o-n-i)*sizeof(const void *));
    ptrbuf.setLength(ptrbuf.length()-n*sizeof(const void *));
    numelem -= n;
}

        

unsigned CThorRowArray::load(IRowStream &stream,bool ungrouped)
{
    unsigned n = 0;
    loop {
        OwnedConstThorRow row = stream.nextRow();
        if (!row) {
            if (ungrouped)
                row.setown(stream.nextRow());
            if (!row)
                break;
        }
        append(row.getLink());      // use getLink incase throws exception
        n++;
    }       
    return n;
}

unsigned CThorRowArray::load(IRowStream &stream, bool ungrouped, bool &abort, bool *overflowed)
{
    unsigned n = 0;
    if (overflowed)
        *overflowed = false;
    loop {
        OwnedConstThorRow row = stream.nextRow();
        if (!row) {
            if (ungrouped)
                row.setown(stream.nextRow());
            if (!row)
                break;
        }
        append(row.getLink());      // use getLink incase throws exception
        n++;
        if (overflowed&&isFull()) {
            *overflowed=true; 
            break;
        }
    }       
    return n;
}

unsigned CThorRowArray::load2(IRowStream &stream, bool ungrouped, CThorRowArray &prev, IFile &savefile, IOutputRowSerializer *prevserializer, IEngineRowAllocator *prevallocator, bool &prevsaved, bool &overflowed)
{
    overflowed = false;
    prevsaved = false;
    size32_t prevsz = prev.totalMem();
    unsigned n = 0;
    loop {
        if (totalMem()+prevsz>maxtotal) {
            Owned<IExtRowWriter> writer = createRowWriter(&savefile,prevserializer,prevallocator,false,false,false); 
            prev.save(writer);
            writer->flush();
            prev.clear();
            prevsaved = true;
        }
        OwnedConstThorRow row = stream.nextRow();
        if (!row) {
            if (ungrouped)
                row.setown(stream.nextRow());
            if (!row)
                break;
        }
        append(row.getLink());      // use getLink incase throws exception
        n++;
        if (isFull()) {
            overflowed=true; 
            break;
        }
    }       
    return n;
    
}


void CThorRowArray::transfer(CThorRowArray &from)
{
    clear();
    swapWith(from);

}

void CThorRowArray::swapWith(CThorRowArray &from)
{
    ptrbuf.swapWith(from.ptrbuf);
    unsigned t = numelem;
    numelem = from.numelem;
    from.numelem = t;
    size32_t ts = totalsize;
    totalsize = from.totalsize; 
    from.totalsize = ts;
    ts = overhead;
    overhead = from.overhead;
    from.overhead = ts;
    ts = maxtotal;
    maxtotal = from.maxtotal;
    from.maxtotal = ts;
    IOutputRowSerializer *sz = serializer.getClear();
    serializer.setown(from.serializer.getClear());
    from.serializer.setown(sz);
}


void CThorRowArray::serialize(IOutputRowSerializer *serializer,IRowSerializerTarget &out)
{
    bool warnnull = true;
    assertex(serializer);
    for (unsigned i=0;i<numelem;i++) {
        const void *row = item(i); 
        if (row)
            serializer->serialize(out,(const byte *)row);
        else if (warnnull) {
            WARNLOG("CThorRowArray::serialize ignoring NULL row");
            warnnull = false;
        }

    }

}

void CThorRowArray::serialize(IOutputRowSerializer *serializer,MemoryBuffer &mb,bool hasnulls)
{
    assertex(serializer);
    CMemoryRowSerializer s(mb);
    if (!hasnulls)
        serialize(serializer,s);
    else {
        unsigned short guard = 0x7631;
        mb.append(guard);
        for (unsigned i=0;i<numelem;i++) {
            const void *row = item(i); 
            bool isnull = (row==NULL);
            mb.append(isnull);
            if (!isnull) 
                serializer->serialize(s,(const byte *)row);
        }
    }
}

unsigned CThorRowArray::serializeblk(IOutputRowSerializer *serializer,MemoryBuffer &mb,size32_t dstmax, unsigned idx, unsigned count)
{
    assertex(serializer);
    CMemoryRowSerializer out(mb);
    bool warnnull = true;
    if (idx>=numelem)
        return 0;
    if (numelem-idx<count)
        count = numelem-idx;
    unsigned ret = 0;
    for (unsigned i=0;i<count;i++) {
        size32_t ln = mb.length();
        const void *row = item(i+idx); 
        if (row)
            serializer->serialize(out,(const byte *)row);
        else if (warnnull) {
            WARNLOG("CThorRowArray::serialize ignoring NULL row");
            warnnull = false;
        }
        if (mb.length()>dstmax) {
            if (ln)
                mb.setLength(ln);   // make sure one row
            break;
        }
        ret++;
    }
    return ret;
}


void CThorRowArray::deserializerow(IEngineRowAllocator &allocator,IOutputRowDeserializer *deserializer,IRowDeserializerSource &in)
{
    RtlDynamicRowBuilder rowBuilder(&allocator);
    size32_t sz = deserializer->deserialize(rowBuilder,in);
    append(rowBuilder.finalizeRowClear(sz));
}


void CThorRowArray::deserialize(IEngineRowAllocator &allocator,IOutputRowDeserializer *deserializer,size32_t sz,const void *buf, bool hasnulls)
{
    if (hasnulls) {
        ASSERTEX((sz>=sizeof(short))&&(*(unsigned short *)buf==0x7631)); // check for mismatch
        buf = (const byte *)buf+sizeof(unsigned short);
        sz -= sizeof(unsigned short);
    }
    CThorStreamDeserializerSource d(sz,buf);
    while (!d.eos()) {
        if (hasnulls) {
            bool nullrow;
            d.read(sizeof(bool),&nullrow);
            if (nullrow) {
                append(NULL);
                continue;
            }
        }
        deserializerow(allocator,deserializer,d);
    }
}



IRowStream *CThorRowArray::createRowStream(unsigned start,unsigned num, bool streamowns)
{
    class cStream: public CSimpleInterface, implements IRowStream
    {
    public:
        unsigned pos;
        unsigned num;
        bool owns;
        CThorRowArray* parent;

        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        const void *nextRow()
        {
            if (!num) 
                return NULL;
            num--;
            if (owns)
                return parent->itemClear(pos++);
            const void *ret = parent->item(pos++);
            LinkThorRow(ret);
            return ret;
        }

        void stop()
        {
            num = 0;
            // could remove rest possibly
        }

    } *ret = new cStream();
    if (start>ordinality()) {
        start = ordinality();
        num = 0;
    }
    else if ((num==(unsigned)-1)||(start+num>ordinality()))
        num = ordinality()-start;
    ret->pos = start;
    ret->num = num;
    ret->owns = streamowns;
    ret->parent = this;
    return ret;
}

unsigned CThorRowArray::save(IRowWriter *writer, unsigned pos,unsigned num, bool owns)
{
    if (pos>ordinality()) {
        pos = ordinality();
        num = 0;
    }
    else if ((num==(unsigned)-1)||(pos+num>ordinality()))
        num = ordinality()-pos;
    if (!num) 
        return 0;
    PROGLOG("CThorRowArray::save %d rows",num);
    unsigned ret = 0; 
    while (num--) {
        OwnedConstThorRow row;
        if (owns) 
            row.setown(itemClear(pos++));
        else 
            row.set(item(pos++));
        writer->putRow(row.getClear());
        ret++;
    }
    PROGLOG("CThorRowArray::save done");
    return ret;
}

void CThorRowArray::reorder(unsigned start,unsigned num, unsigned *neworder)
{
    if (start>=ordinality())
        return;
    if (start+num>ordinality())
        num = ordinality()-start;
    if (!num)
        return;
    MemoryAttr ma;
    byte **tmp = (byte **)ma.allocate(num*sizeof(void *));
    byte **p = ((byte **)ptrbuf.toByteArray())+start;
    memcpy(tmp,p,num*sizeof(void *));
    for (unsigned i=0;i<num;i++) 
        p[i] = tmp[neworder[i]];
}

void CThorRowArray::reserve(unsigned n)
{
    size32_t sz = sizeof(const byte *)*n;
    if (raiseexceptions) {
        size32_t tot = sz+totalMem();
        if (tot>maxtotal)
            throw new CThorRowArrayException(tot);
    }
    memset(ptrbuf.reserve(sz),0,sz);
    numelem+=n;
}


void setThorInABox(unsigned num)
{
}


class cMultiThorResourceMutex: public CSimpleInterface, implements ILargeMemLimitNotify, implements IDaliMutexNotifyWaiting
{
    class cMultiThorResourceMutexThread: public Thread
    {
        cMultiThorResourceMutex &parent;
    public:
        cMultiThorResourceMutexThread(cMultiThorResourceMutex &_parent)
            : Thread("cMultiThorResourceMutexThread"),parent(_parent)
        {
        }

        int run() 
        {
            parent.run();
            return 0;
        }
    };
    Owned<cMultiThorResourceMutexThread> thread;
    Owned<IDaliMutex> mutex;
    bool stopping;
    Linked<ICommunicator> clusterComm;
    CSDSServerStatus *status;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    cMultiThorResourceMutex(const char *groupname,CSDSServerStatus *_status)
    {
        status = _status;
        stopping = false;
        clusterComm.set(&queryClusterComm());
        if (clusterComm->queryGroup().rank(queryMyNode())==0) { // master so start thread
            thread.setown(new cMultiThorResourceMutexThread(*this));
            thread->start();
            StringBuffer mname("thorres:");
            mname.append(groupname);
            mutex.setown(createDaliMutex(mname.str()));
        }

    }

    ~cMultiThorResourceMutex()
    {
        stopping = true;
        if (thread) 
            stop();
    }

    void run() // on master
    {
        PROGLOG("cMultiThorResourceMutex thread run");
        try {
            CMessageBuffer mbuf;
            while (!stopping) {
                mbuf.clear();
                rank_t from;
                unsigned timeout = 1000*60*5;
                if (clusterComm->recv(mbuf,RANK_ALL,MPTAG_THORRESOURCELOCK,&from,timeout)) {
                    byte req;
                    mbuf.read(req);
                    if (req==1) {
                        if (mutex) 
                            mutex->enter();
                    }
                    else if (req==0) {
                        if (mutex) 
                            mutex->leave();
                    }
                    clusterComm->reply(mbuf,1000*60*5);
                }
            }
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::run");
        }
    }

    void stop()
    {
        PROGLOG("cMultiThorResourceMutex::stop enter");
        stopping = true;
        if (mutex) 
            mutex->kill();
        try {
            clusterComm->cancel(RANK_ALL,MPTAG_THORRESOURCELOCK);
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::stop");
        }
        if (thread)
            thread->join();
        mutex.clear();
        PROGLOG("cMultiThorResourceMutex::stop leave");
    }

    bool take(memsize_t tot)
    {
        if (stopping)
            return true;
        if (mutex) 
            return mutex->enter();
        if (stopping)
            return false;
        CMessageBuffer mbuf;
        byte req = 1;
        mbuf.append(req);
        try {
            if (!clusterComm->sendRecv(mbuf,0,MPTAG_THORRESOURCELOCK,(unsigned)-1))
                stopping = true;
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::take");
        }
        return !stopping;
    }
                                            // will raise oom exception if false returned
    void give(memsize_t tot)
    {
        if (mutex) {
            mutex->leave();
            return;
        }
        if (stopping)
            return;
        CMessageBuffer mbuf;
        byte req = 0;
        mbuf.append(req);
        try {
            if (!clusterComm->sendRecv(mbuf,0,MPTAG_THORRESOURCELOCK,(unsigned)-1))
                stopping = true;
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::give");
        }

    }

    //IDaliMutexNotifyWaiting
    void startWait()
    {
        if (status)
            status->queryProperties()->setPropInt("@memoryBlocked",1);
    }
    void cycleWait()
    {
        if (status)
            status->queryProperties()->setPropInt("@memoryBlocked",status->queryProperties()->getPropInt("@memoryBlocked")+1);
    }
    void stopWait(bool got)
    {
        if (status)
            status->queryProperties()->setPropInt("@memoryBlocked",0);
    }

};





ILargeMemLimitNotify *createMultiThorResourceMutex(const char *grpname,CSDSServerStatus *_status)
{
    return new cMultiThorResourceMutex(grpname,_status);
}





static class CThorRowCallbackHook : implements IRtlRowCallback
{
public:
    virtual void releaseRow(const void * row) const
    {
        ReleaseThorRow(row);
    }
    virtual void releaseRowset(unsigned count, byte * * rowset) const
    {
        if (rowset)
        {
            /// NB not thread safe!
            if (!isThorRowShared(rowset))
            {
                byte * * finger = rowset;
                while (count--)
                    ReleaseThorRow(*finger++);
            }
            ReleaseThorRow(rowset);
        }
    }
    virtual void * linkRow(const void * row) const
    {
        if (row) 
            LinkThorRow(row);
        return const_cast<void *>(row);
    }
    virtual byte * * linkRowset(byte * * rowset) const
    {
        if (rowset)
            LinkThorRow(rowset);
        return const_cast<byte * *>(rowset);
    }
} ThorRowCallbackHook;

static memsize_t ThorMemoryManagerMaxSize;

void initThorMemoryManager(size32_t szMB, unsigned memtracelevel, unsigned memstatinterval)
{
    ASSERTEX(!ThorMemoryManager);
    ThorMemoryManagerMaxSize = 1024*1024*(memsize_t)szMB;
    ThorMemoryManager = createThorRowManager(ThorMemoryManagerMaxSize, &ThorAllocatorCache, false);
    rtlSetReleaseRowHook(&ThorRowCallbackHook);
}

void resetThorMemoryManager()
{
    ThorAllocatorCache.reset(); // clears cached rows
    if (ThorMemoryManager) {
        ThorMemoryManager->Release();
        ThorMemoryManager = createThorRowManager(ThorMemoryManagerMaxSize, &ThorAllocatorCache, false);
    }
    ThorAllocatorCache.clear(); // do after so that act ids still around
}


#define OUTPUTMETACHILDROW_VERSION 2 // for now, it's only significant that non-zero
class COutputMetaWithChildRow : public CSimpleInterface, implements IOutputMetaData
{
    Linked<IEngineRowAllocator> childAllocator;
    IOutputMetaData *childMeta;
    size32_t extraSz;
    Owned<IOutputRowSerializer> serializer;
    Owned<IOutputRowDeserializer> deserializer;
    Owned<ISourceRowPrefetcher> prefetcher;

    class CSerializer : public CSimpleInterface, implements IOutputRowSerializer
    {
        Owned<IOutputRowSerializer> childSerializer;
        size32_t extraSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CSerializer(IOutputRowSerializer *_childSerializer, size32_t _extraSz) : childSerializer(_childSerializer), extraSz(_extraSz)
        {
        }
        virtual void serialize(IRowSerializerTarget &out, const byte *self)
        {
            out.put(extraSz, self);
            const byte *childRow = *(const byte **)(self+extraSz);
            if (childRow)
            {
                byte b=1;
                out.put(1, &b);
                childSerializer->serialize(out, childRow);
            }
            else
            {
                byte b=0;
                out.put(1, &b);
            }
        }
    };
    class CDeserializer : public CSimpleInterface, implements IOutputRowDeserializer
    {
        Owned<IOutputRowDeserializer> childDeserializer;
        Linked<IEngineRowAllocator> childAllocator;
        size32_t extraSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CDeserializer(IOutputRowDeserializer *_childDeserializer, IEngineRowAllocator *_childAllocator, size32_t _extraSz) : childDeserializer(_childDeserializer), childAllocator(_childAllocator), extraSz(_extraSz)
        {
        }
        virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource &in)
        {
            byte * self = rowBuilder.getSelf();
            in.read(extraSz, self);
            byte b;
            in.read(1, &b);
            const void *fChildRow;
            if (b)
            {
                RtlDynamicRowBuilder childBuilder(childAllocator);
                size32_t sz = childDeserializer->deserialize(childBuilder, in);
                fChildRow = childBuilder.finalizeRowClear(sz);
            }
            else
                fChildRow = NULL;
            memcpy(self+extraSz, &fChildRow, sizeof(const void *));
            return extraSz + sizeof(const void *);
        }
    };

    class CPrefetcher : public CSimpleInterface, implements ISourceRowPrefetcher
    {
        Owned<ISourceRowPrefetcher> childPrefetcher;
        size32_t extraSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CPrefetcher(ISourceRowPrefetcher *_childPrefetcher, size32_t _extraSz) : childPrefetcher(_childPrefetcher), extraSz(_extraSz)
        {
        }
        virtual void readAhead(IRowDeserializerSource &in)
        {
            in.skip(extraSz);
            byte b;
            in.read(1, &b);
            if (b)
                childPrefetcher->readAhead(in);
        }
    };


public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    COutputMetaWithChildRow(IEngineRowAllocator *_childAllocator, size32_t _extraSz) : childAllocator(_childAllocator), extraSz(_extraSz)
    {
        childMeta = childAllocator->queryOutputMeta();
    }
    virtual size32_t getRecordSize(const void *) { return extraSz + sizeof(const void *); }
    virtual size32_t getMinRecordSize() const { return extraSz + sizeof(const void *); }
    virtual size32_t getFixedSize() const { return extraSz + sizeof(const void *); }
    virtual void toXML(const byte * self, IXmlWriter & out) 
    { 
         // ignoring xml'ing extra
        //GH: I think this is what it should do
        childMeta->toXML(*(const byte **)(self+extraSz), out); 
    }
    virtual unsigned getVersion() const { return OUTPUTMETACHILDROW_VERSION; }

//The following can only be called if getMetaDataVersion >= 1, may seh otherwise.  Creating a different interface was too painful
    virtual unsigned getMetaFlags() { return MDFneeddestruct|childMeta->getMetaFlags(); }
    virtual void destruct(byte * self)
    {
        OwnedConstThorRow childRow = *(const void **)(self+extraSz);
    }
    virtual IOutputRowSerializer * createRowSerializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!serializer)
            serializer.setown(new CSerializer(childMeta->createRowSerializer(ctx, activityId), extraSz));
        return LINK(serializer);
    }
    virtual IOutputRowDeserializer * createRowDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!deserializer)
            deserializer.setown(new CDeserializer(childMeta->createRowDeserializer(ctx, activityId), childAllocator, extraSz));
        return LINK(deserializer);
    }
    virtual ISourceRowPrefetcher * createRowPrefetcher(ICodeContext * ctx, unsigned activityId)
    {
        if (!prefetcher)
            prefetcher.setown(new CPrefetcher(childMeta->createRowPrefetcher(ctx, activityId), extraSz));
        return LINK(prefetcher);
    }
    virtual IOutputMetaData * querySerializedMeta() { return this; }
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) 
    {
        //GH: I think this is what it should do, please check
        visitor.visitRow(*(const byte **)(self+extraSz)); 
    }
};

IOutputMetaData *createOutputMetaDataWithChildRow(IEngineRowAllocator *childAllocator, size32_t extraSz)
{
    return new COutputMetaWithChildRow(childAllocator, extraSz);
}


class COutputMetaWithExtra : public CSimpleInterface, implements IOutputMetaData
{
    Linked<IOutputMetaData> meta;
    size32_t metaSz;
    Owned<IOutputRowSerializer> serializer;
    Owned<IOutputRowDeserializer> deserializer;
    Owned<ISourceRowPrefetcher> prefetcher;
    Owned<IOutputMetaData> serializedmeta;

    class CSerialization : public CSimpleInterface, implements IOutputRowSerializer
    {
        Owned<IOutputRowSerializer> serializer;
        size32_t metaSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CSerialization(IOutputRowSerializer *_serializer, size32_t _metaSz) : serializer(_serializer), metaSz(_metaSz)
        {
        }
        virtual void serialize(IRowSerializerTarget &out, const byte *self)
        {
            out.put(metaSz, self);
            serializer->serialize(out, self+metaSz);
        }
    };
    //GH - This code is the same as CPrefixedRowDeserializer
    class CDeserializer : public CSimpleInterface, implements IOutputRowDeserializer
    {
        Owned<IOutputRowDeserializer> deserializer;
        size32_t metaSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CDeserializer(IOutputRowDeserializer *_deserializer, size32_t _metaSz) : deserializer(_deserializer), metaSz(_metaSz)
        {
        }
        virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource &in)
        {
            in.read(metaSz, rowBuilder.getSelf());
            CPrefixedRowBuilder prefixedBuilder(metaSz, rowBuilder);
            size32_t sz = deserializer->deserialize(prefixedBuilder, in);
            return sz+metaSz;
        }
    };

    class CPrefetcher : public CSimpleInterface, implements ISourceRowPrefetcher
    {
        Owned<ISourceRowPrefetcher> childPrefetcher;
        size32_t metaSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CPrefetcher(ISourceRowPrefetcher *_childPrefetcher, size32_t _metaSz) : childPrefetcher(_childPrefetcher), metaSz(_metaSz)
        {
        }
        virtual void readAhead(IRowDeserializerSource &in)
        {
            in.skip(metaSz);
            childPrefetcher->readAhead(in);
        }
    };

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    COutputMetaWithExtra(IOutputMetaData *_meta, size32_t _metaSz) : meta(_meta), metaSz(_metaSz)
    {
    }
    virtual size32_t getRecordSize(const void *rec) 
    {
        size32_t sz = meta->getRecordSize(rec?((byte *)rec)+metaSz:NULL); 
        return sz+metaSz;
    }
    virtual size32_t getMinRecordSize() const 
    { 
        return meta->getMinRecordSize() + metaSz;
    }
    virtual size32_t getFixedSize() const 
    {
        size32_t sz = meta->getFixedSize();
        if (!sz)
            return 0;
        return sz+metaSz;
    }

    virtual void toXML(const byte * self, IXmlWriter & out) { meta->toXML(self, out); }
    virtual unsigned getVersion() const { return meta->getVersion(); }

//The following can only be called if getMetaDataVersion >= 1, may seh otherwise.  Creating a different interface was too painful
    virtual unsigned getMetaFlags() { return meta->getMetaFlags(); }
    virtual void destruct(byte * self) { meta->destruct(self); }
    virtual IOutputRowSerializer * createRowSerializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!serializer)
            serializer.setown(new CSerialization(meta->createRowSerializer(ctx, activityId), metaSz));
        return LINK(serializer);
    }
    virtual IOutputRowDeserializer * createRowDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!deserializer)
            deserializer.setown(new CDeserializer(meta->createRowDeserializer(ctx, activityId), metaSz));
        return LINK(deserializer);
    }
    virtual ISourceRowPrefetcher * createRowPrefetcher(ICodeContext * ctx, unsigned activityId)
    {
        if (!prefetcher)
            prefetcher.setown(new CPrefetcher(meta->createRowPrefetcher(ctx, activityId), metaSz));
        return LINK(prefetcher);
    }
    virtual IOutputMetaData * querySerializedMeta() 
    { 
        IOutputMetaData *sm = meta->querySerializedMeta();
        if (sm==meta.get())
            return this;
        if (!serializedmeta.get())
            serializedmeta.setown(new COutputMetaWithExtra(sm,metaSz));
        return serializedmeta.get();
    } 
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor)
    {
        meta->walkIndirectMembers(self, visitor);
    }
};

IOutputMetaData *createOutputMetaDataWithExtra(IOutputMetaData *meta, size32_t sz)
{
    return new COutputMetaWithExtra(meta, sz);
}

// mirroring MemoryBuffer 
#define FIRST_CHUNK_SIZE     8


class ThorEngineRowAllocator : public CSimpleInterface, implements IThorRowAllocator
{
protected:
    IThorRowManager & rowManager;
    CachedOutputMetaData meta;
    unsigned activityId;
    unsigned allocatorId;
    size32_t minSize;
    size32_t initSize;
    unsigned destructmask;

    void * doFinalizeRow(size32_t newSize, void * row)
    {
        if (newSize) {
#ifdef _DEBUG
            size32_t actualsize = meta.getRecordSize(row);
            if (actualsize!=newSize) {
                PrintStackReport();
                ERRLOG("finalizeRow(%p) actual=%u newSize=%u",row,actualsize,newSize);
                ASSERTEX(actualsize==newSize);
            }
#endif
            unsigned id = allocatorId | ACTIVITY_FLAG_ISREGISTERED | destructmask;
            assertex(newSize>=minSize);
            void * ret = rowManager.finalizeRow(row, newSize, id, meta.isVariableSize()); 
            if ((ret!=row)&&meta.isVariableSize())
                ReleaseThorRow(row);
            return ret;
        }
        if (row) 
            ReleaseThorRow(row);
        return NULL;
    }


public:
    ThorEngineRowAllocator(IThorRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId) 
        : rowManager(_rowManager), meta(_meta) 
    {
        activityId = _activityId;
        allocatorId = ThorAllocatorCache.append(*this);
        initSize = meta.getInitialSize();
        minSize = meta.getMinRecordSize();
        destructmask = meta.needsDestruct()?ACTIVITY_FLAG_NEEDSDESTRUCTOR:0;
    }
    ~ThorEngineRowAllocator() 
    {
    }

    IMPLEMENT_IINTERFACE_USING(CSimpleInterface)

//interface IEngineRowsetAllocator
    virtual byte * * createRowset(unsigned count)
    {
        if (count == 0)
            return NULL;
        return (byte **) rowManager.allocate(count * sizeof(void *), allocatorId | ACTIVITY_FLAG_ISREGISTERED);
    }

    virtual void releaseRowset(unsigned count, byte * * rowset)
    {
        rtlReleaseRowset(count, rowset);
    }

    virtual byte * * linkRowset(byte * * rowset)
    {
        return rtlLinkRowset(rowset);
    }

    virtual byte * * appendRowOwn(byte * * rowset, unsigned newRowCount, void * row)
    {
        if (!rowset)
            rowset = createRowset(newRowCount);
        else
            rowset = (byte * *)rowManager.resizeRow(rowset, (newRowCount-1) * sizeof(void *), newRowCount * sizeof(void *), allocatorId | ACTIVITY_FLAG_ISREGISTERED);

        rowset[newRowCount-1] = (byte *)row;
        return rowset;
    }

    virtual byte * * reallocRows(byte * * rowset, unsigned oldRowCount, unsigned newRowCount)
    {
        if (!rowset)
            rowset = createRowset(newRowCount);
        else
            rowset = (byte * *)rowManager.resizeRow(rowset, oldRowCount * sizeof(void *), newRowCount * sizeof(void *), allocatorId | ACTIVITY_FLAG_ISREGISTERED);

        //New rows (if any) aren't cleared....
        return rowset;
    }
//interface IEngineAnyRowAllocator
    virtual void * createRow()
    {
        return rowManager.allocate(initSize<sizeof(void *)?sizeof(void*):initSize, allocatorId | ACTIVITY_FLAG_ISREGISTERED);
    }

    virtual void * createRow(size32_t & allocatedSize)
    {
        if (meta.isFixedSize())
        {
            allocatedSize = initSize;
            return createRow();
        }
        // extensible row
        return rowManager.allocateExt(initSize, allocatorId | ACTIVITY_FLAG_ISREGISTERED, allocatedSize);
    }

    virtual void releaseRow(const void * row)
    {
        ReleaseThorRow(row);
    }

    virtual void * linkRow(const void * row)
    {
        LinkThorRow(row);
        return const_cast<void *>(row);
    }

    virtual void * finalizeRow(size32_t newSize, void * row, size32_t oldSize)
    {
        return doFinalizeRow(newSize, row);
    }

    virtual void * resizeRow(size32_t newSize, void * row, size32_t & size) // NB in 'size' == max
    {
        // assertex(!meta.isFixedSize()); // JCSMORE - cloneRow calls ensureCapacity->resizeRow....
        // assertex(newSize >= size); // JCSMORE - if variable, this should always be true, but can be called from fixed (cloneRow again)
        // this is used to extend row with slack
        return rowManager.extendRow(row,newSize,allocatorId | ACTIVITY_FLAG_ISREGISTERED,size);
    }

    virtual IOutputMetaData * queryOutputMeta()
    {
        return meta.queryOriginal();
    }
    virtual unsigned queryActivityId()
    {
        return activityId;
    }
    virtual StringBuffer &getId(StringBuffer &idStr)
    {
        return idStr.append(activityId); // MORE - may want more context info in here
    }
    virtual IOutputRowSerializer *createRowSerializer(ICodeContext *ctx)
    {
        return meta.createRowSerializer(ctx, activityId);
    }

    virtual IOutputRowDeserializer *createRowDeserializer(ICodeContext *ctx)
    {
        return meta.createRowDeserializer(ctx, activityId);
    }

};

unsigned CThorRowAllocatorCache::getActivityId(unsigned cacheId) const
{
    return item(cacheId).queryActivityId();
}
StringBuffer & CThorRowAllocatorCache::getActivityDescriptor(unsigned cacheId, StringBuffer &out) const
{
    return item(cacheId).getId(out);
}
void CThorRowAllocatorCache::onDestroy(unsigned cacheId, void *row) const
{
    item(cacheId).queryOutputMeta()->destruct((byte *) row); 
}


size32_t CThorRowAllocatorCache::subSize(unsigned cacheId, const void *row) const
{
    class cRowSubSizer: public IIndirectMemberVisitor
    {
    public:
        size32_t size;
        inline cRowSubSizer()
        {
            size = 0;
        }
        virtual void visitRowset(size32_t count, byte * * rows)
        {
            size += thorRowMemoryFootprint(rows);
            while (count--) {
                size += thorRowMemoryFootprint(*rows);
                rows++;
            }
        }
        virtual void visitRow(const byte * row)
        {
            size += thorRowMemoryFootprint(row);
        }

    } rss;
    item(cacheId).queryOutputMeta()->walkIndirectMembers((const byte *)row,rss);
    return rss.size;
}

unsigned CThorRowAllocatorCache::append(ThorEngineRowAllocator &a)
{
    a.Link();
    SpinBlock block(ThorMMsect);
    unsigned allocatorId = ThorRowAllocators.ordinality();
    assertex(allocatorId<MAX_ACTIVITY_ID);
    ForEachItemIn(i,ThorRowAllocators) 
        if (ThorRowAllocators.item(i)==NULL) {
            allocatorId = i;
            break;
        }
    if (allocatorId==ThorRowAllocators.ordinality())
        ThorRowAllocators.append(&a);
    else
        ThorRowAllocators.replace(&a,allocatorId);
    return allocatorId;
}

void CThorRowAllocatorCache::clear()
{
    SpinBlock block(ThorMMsect);
    ForEachItemIn(i,ThorRowAllocators) {
        ((ThorEngineRowAllocator *)ThorRowAllocators.item(i))->Release();
    }
    ThorRowAllocators.kill();
}

void CThorRowAllocatorCache::reset()
{
}

IThorRowAllocator *createThorRowAllocator(IOutputMetaData * _meta, unsigned _activityId)
{
    assertex(_activityId);
    return new ThorEngineRowAllocator(*ThorMemoryManager,_meta,_activityId);
}


IPerfMonHook *createThorMemStatsPerfMonHook(IPerfMonHook *chain)
{
    return LINK(chain);
}

 memsize_t ThorRowMemoryAvailable()
{
    return ThorMemoryManager->remaining();
 }

void setLCRrowCRCchecking(bool on)
{
    ThorMemoryManager->setLCRrowCRCchecking(on);
}





