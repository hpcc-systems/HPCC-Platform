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
#include <limits.h>
#include <stddef.h>
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jio.hpp"
#include "jlzw.hpp"
#include "jsem.hpp"
#include "jthread.hpp"
#include "jarray.hpp"
#include "jiface.hpp"
#include "jfile.hpp"
#include "jset.hpp"
#include "jqueue.tpp"

#include "thalloc.hpp"
#include "thbuf.hpp"
#include "eclrtl.hpp"
#include "thgraph.hpp"

#ifdef _DEBUG
//#define _FULL_TRACE
//#define TRACE_CREATE

struct SmartBufReenterCheck
{
    bool &check; 
    SmartBufReenterCheck(bool &_check) 
        : check(_check)
    {
        assertex(!check);
        check = true;
    }
    ~SmartBufReenterCheck()
    {
        assertex(check);
        check = false;
    }
};

#define REENTRANCY_CHECK(n) SmartBufReenterCheck recheck(n);
#else
#define REENTRANCY_CHECK(n)
#endif



class CSmartRowBuffer: public CSimpleInterface, implements ISmartRowBuffer, implements IRowWriter
{
    CActivityBase *activity;
    ThorRowQueue *in;
    size32_t insz;
    ThorRowQueue *out;
    Linked<IFile> file;
    Owned<IFileIO> fileio;
    SpinLock lock;
    bool waiting;
    Semaphore waitsem;
    bool waitflush;
    Semaphore waitflushsem;
    size32_t blocksize;
    unsigned numblocks;
    Owned<IBitSet> diskfree;
    UnsignedArray diskin;
    UnsignedArray diskinlen;
    Linked<IOutputRowSerializer> serializer;
    Linked<IOutputRowDeserializer> deserializer;
    Linked<IEngineRowAllocator> allocator;
    bool eoi;
#ifdef _DEBUG
    bool putrecheck;
    bool getrecheck;
#endif

    unsigned allocblk(unsigned nb)
    {
        if (nb<=numblocks) {
            for (unsigned i=0;i<=numblocks-nb;i++) {
                unsigned j;
                for (j=i;j<i+nb;j++) 
                    if (!diskfree->test(j))
                        break;
                if (j==i+nb) {
                    while (j>i)
                        diskfree->set(--j,false);
                    return i;
                }
            }
        }
        unsigned ret = numblocks;
        numblocks += nb;
        return ret;
    }

    void freeblk(unsigned b,unsigned nb)
    {
        for (unsigned i=0;i<nb;i++)
            diskfree->set(i+b,true);
    }

    void diskflush()
    {
        struct cSaveIn {
            ThorRowQueue *rq;
            ThorRowQueue *&in;
            cSaveIn(ThorRowQueue *&_in) 
                : in(_in)
            {
                rq = in;
                in = NULL;
            }
            ~cSaveIn() 
            {
                if (!in)
                    in = rq;
            }
        } csavein(in); // mark as not there so consumer will pause
        if (out && (out->ordinality()==0) && (diskin.ordinality()==0)) {
            in = out;
            out = csavein.rq;
            insz = 0;
            return;
        }
        if (!fileio) {
            SpinUnblock unblock(lock);
            fileio.setown(file->open(IFOcreaterw));
            if (!fileio)
            {
                throw MakeStringException(-1,"CSmartRowBuffer::flush cannot write file %s",file->queryFilename());
            }
        }
        MemoryBuffer mb;
        CMemoryRowSerializer mbs(mb);
        unsigned nb = 0;
        unsigned blk = 0;
        if (!waiting) {
            mb.ensureCapacity(blocksize);
            {
                SpinUnblock unblock(lock);
                unsigned p = 0;
                byte b;
                for (unsigned i=0;i<csavein.rq->ordinality();i++) {
                    if (waiting)
                        break;
                    const void *row = csavein.rq->item(i);
                    if (row) { 
                        b = 1;
                        mb.append(b);
                        serializer->serialize(mbs,(const byte *)row);
                    }
                    else {
                        b = 2; // eog
                        mb.append(b);
                    }
                }
                b = 0;
                mb.append(b);
            }
        }
        if (!waiting) {
            nb = (mb.length()+blocksize-1)/blocksize;
            blk = allocblk(nb);
            SpinUnblock unblock(lock);
            if (mb.length()<nb*blocksize) { // bit overkill!
                size32_t left = nb*blocksize-mb.length();
                memset(mb.reserve(left),0,left);
            }
            fileio->write(blk*(offset_t)blocksize,mb.length(),mb.bufferBase());
            mb.clear();
        }
        if (waiting) {
            // consumer caught up
            freeblk(blk,nb);
            assertex(out->ordinality()==0);
            assertex(diskin.ordinality()==0);
            in = out;
            out = csavein.rq;
        }
        else {
            diskin.append(blk);
            diskinlen.append(nb);
            while (csavein.rq->ordinality()) 
                ReleaseThorRow(csavein.rq->dequeue());
        }
        insz = 0;
    }

    void load()
    {
        ThorRowQueue *rq = out;
        out = NULL; // mark as not there so producer won't fill
        unsigned blk = diskin.item(0);
        unsigned nb = diskinlen.item(0);
        diskin.remove(0);  // better as q but given reading from disk...
        diskinlen.remove(0);  
        {
            SpinUnblock unblock(lock);
            MemoryAttr ma;
            byte *buf = (byte *)ma.allocate(blocksize*nb);
            CThorStreamDeserializerSource ds(blocksize,buf);
            assertex(fileio.get());
            size32_t rd = fileio->read(blk*(offset_t)blocksize,blocksize*nb,buf);
            assertex(rd==blocksize);
            unsigned p = 0;
            loop {
                byte b;
                ds.read(sizeof(b),&b);
                if (!b)
                    break;
                if (b==1) {
                    RtlDynamicRowBuilder rowBuilder(allocator);
                    size32_t sz = deserializer->deserialize(rowBuilder,ds); 
                    rq->enqueue(rowBuilder.finalizeRowClear(sz));
                }
                else if (b==2)
                    rq->enqueue(NULL);

            }
        }
        freeblk(blk,nb);
        out = rq;
    }


public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSmartRowBuffer(CActivityBase *_activity, IFile *_file,size32_t bufsize,IRowInterfaces *rowif)
        : activity(_activity), file(_file), allocator(rowif->queryRowAllocator()), serializer(rowif->queryRowSerializer()), deserializer(rowif->queryRowDeserializer())
    {
#ifdef _DEBUG
        putrecheck = false;
        getrecheck = false;
#endif
        in = new ThorRowQueue;
        out = new ThorRowQueue;
        waiting = false;
        waitflush = false;
        blocksize = ((bufsize/2+0xfffff)/0x100000)*0x100000;
        numblocks = 0;
        insz = 0;
        eoi = false;
        diskfree.setown(createBitSet()); 

#ifdef _FULL_TRACE
        ActPrintLog(activity, "SmartBuffer create %x",(unsigned)(memsize_t)this);
#endif
    }

    ~CSmartRowBuffer()
    {
#ifdef _FULL_TRACE
        ActPrintLog(activity, "SmartBuffer destroy %x",(unsigned)(memsize_t)this);
#endif
        assertex(!waiting);
        assertex(!waitflush);
        // clear in/out contents 
        while (in->ordinality()) 
            ReleaseThorRow(in->dequeue());
        delete in;
        while (out->ordinality()) 
            ReleaseThorRow(out->dequeue());
        delete out;
    }

    void putRow(const void *row)
    {
        REENTRANCY_CHECK(putrecheck)
        size32_t sz = thorRowMemoryFootprint(row);
        SpinBlock block(lock);
        if (eoi) {
            ReleaseThorRow(row);
            
            return;
        }       
        assertex(in);  // reentry check
        if (sz+insz+(in->ordinality()+2)*sizeof(byte)>blocksize) // byte extra per row + end byte
            diskflush();
        in->enqueue(row);
        insz += sz;
        if (waiting) {
            waitsem.signal();
            waiting = false;
        }
    }

    void stop()
    {
#ifdef _FULL_TRACE
        ActPrintLog(activity, "SmartBuffer stop %x",(unsigned)(memsize_t)this);
#endif
        SpinBlock block(lock);
#ifdef _DEBUG
        if (waiting)
        {
            ActPrintLogEx(&activity->queryContainer(), thorlog_null, MCwarning, "CSmartRowBuffer::stop while nextRow waiting");
            PrintStackReport();
        }
#endif
        eoi = true;

        while (out&&out->ordinality()) 
            ReleaseThorRow(out->dequeue());
        while (NULL == in)
        {
            waiting = true;
            SpinUnblock unblock(lock);
            waitsem.wait();
        }
        while (out&&out->ordinality()) 
            ReleaseThorRow(out->dequeue());
        while (in&&in->ordinality()) 
            ReleaseThorRow(in->dequeue());
        diskin.kill();
        if (waiting) {
            waitsem.signal();
            waiting = false;
        }
        if (waitflush) {
            waitflushsem.signal();
            waitflush = false;
        }
    }

    const void *nextRow()
    {
        REENTRANCY_CHECK(getrecheck)
        const void * ret;
        assertex(out);
        assertex(!waiting);  // reentrancy checks
        loop {
            {
                SpinBlock block(lock);
                if (out->ordinality()) {
                    ret = out->dequeue();
                    break;
                }
                if (diskin.ordinality()) {
                    load();
                    ret = out->dequeue();
                    break;
                }
                if (in) {
                    if (in->ordinality()) {
                        ret = in->dequeue();
                        if (ret) {
                            size32_t sz = thorRowMemoryFootprint(ret);
                            assertex(insz>=sz);
                            insz -= sz;
                        }
                        break;
                    }
                    else {
                        if (waitflush) {
                            waitflushsem.signal();
                            waitflush = false;
                        }
                        if (eoi) 
                            return NULL;
                    }
                }
                assertex(!waiting);  // reentrancy check
                waiting = true;
            }
            waitsem.wait();
        }
        return ret;
    }

    void flush()
    {
        // I think flush should wait til all rows read or stopped
#ifdef _FULL_TRACE
        ActPrintLog(activity, "SmartBuffer flush %x",(unsigned)(memsize_t)this);
#endif
        SpinBlock block(lock);
        if (eoi) return;
        loop {
            assertex(in);  // reentry check
            diskflush();
            eoi = true;
            if (waiting) {
                waitsem.signal();
                waiting = false;
            }
            if (out&&!out->ordinality()&&!diskin.ordinality()&&!in->ordinality()) 
                break;
            waitflush = true;
            SpinUnblock unblock(lock);
            while (!waitflushsem.wait(1000*60))
                ActPrintLogEx(&activity->queryContainer(), thorlog_null, MCwarning, "CSmartRowBuffer::flush stalled");
        }
    }

    IRowWriter *queryWriter()
    {
        return this;
    }


};


class CSmartRowInMemoryBuffer: public CSimpleInterface, implements ISmartRowBuffer, implements IRowWriter
{
    // NB must *not* call LinkThorRow or ReleaseThorRow (or Owned*ThorRow) if deallocator set
    CActivityBase *activity;
    ThorRowQueue *in;
    size32_t insz;
    SpinLock lock;
    bool waitingin;
    Semaphore waitinsem;
    bool waitingout;
    Semaphore waitoutsem;
    size32_t blocksize;
    Linked<ISRBRowInterface> srbrowif; // only used for releasing
    bool eoi;
#ifdef _DEBUG
    bool putrecheck;
    bool getrecheck;
#endif

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSmartRowInMemoryBuffer(CActivityBase *_activity, size32_t bufsize,ISRBRowInterface *_srbrowif)
        : activity(_activity), srbrowif(_srbrowif)
    {
#ifdef _DEBUG
        putrecheck = false;
        getrecheck = false;
#endif
        in = new ThorRowQueue;
        waitingin = false;
        waitingout = false;
        blocksize = ((bufsize/2+0xfffff)/0x100000)*0x100000;
        insz = 0;
        eoi = false;
    }

    ~CSmartRowInMemoryBuffer()
    {
        // clear in contents 
        while (in->ordinality()) 
            if (srbrowif)
                srbrowif->releaseRow(in->dequeue());
            else
                ReleaseThorRow(in->dequeue());
        delete in;
    }

    void putRow(const void *row)
    {
        REENTRANCY_CHECK(putrecheck)
        size32_t sz = 0;
        if (row) {
            if (srbrowif)
                sz = srbrowif->rowMemSize(row);
            else
                sz = thorRowMemoryFootprint(row);
#ifdef _DEBUG
            assertex(sz<0x1000000);
#endif
            assertex((int)sz>=0); // trap invalid sizes a bit earlier

#ifdef _TRACE_SMART_PUTGET
            ActPrintLog(activity, "***putRow(%x) %d  {%x}",(unsigned)row,sz,*(const unsigned *)row);
#endif
        }
        {
            SpinBlock block(lock);
            if (!eoi) {
                assertex(in);  // reentry check
                while ((sz+insz>blocksize)&&insz) {
                    waitingin = true;
                    {
                        SpinUnblock unblock(lock);
                        waitinsem.wait();
                    }
                    if (eoi)
                        break;
                }
            }
            if (!eoi) {
                in->enqueue(row);
                insz += sz;
#ifdef _TRACE_SMART_PUTGET
                ActPrintLog(activity, "***putRow2(%x) insize = %d ",insz);
#endif
                if (waitingout) {
                    waitoutsem.signal();
                    waitingout = false;
                }
                return;
            }
        }
        // cancelled
        if (srbrowif)
            srbrowif->releaseRow(row);
        else
            ReleaseThorRow(row);
    }

    const void *nextRow()
    {
        REENTRANCY_CHECK(getrecheck)
        const void * ret;
        SpinBlock block(lock);
        assertex(!waitingout);  // reentrancy checks
        loop {
            if (in->ordinality()) {
                ret = in->dequeue();
                if (ret) {
                    size32_t sz;
                    if (srbrowif)
                        sz = srbrowif->rowMemSize(ret);
                    else
                        sz = thorRowMemoryFootprint(ret);
#ifdef _TRACE_SMART_PUTGET
                    ActPrintLog(activity, "***dequeueRow(%x) %d insize=%d {%x}",(unsigned)ret,sz,insz,*(const unsigned *)ret);
#endif
                    if (insz<sz) {
                        ActPrintLog(activity, "CSmartRowInMemoryBuffer::nextRow(%x) %d insize=%d {%x} ord = %d",(unsigned)(memsize_t)ret,sz,insz,*(const unsigned *)ret,in->ordinality());
                        assertex(insz>=sz);
                    }
                    insz -= sz;
                }
                break;
            }
            else if (eoi) {
                ret = NULL;
                break;
            }
            assertex(!waitingout);  // reentrancy check
            waitingout = true;
            SpinUnblock unblock(lock);
            waitoutsem.wait();
        }
        if (waitingin) {
            waitinsem.signal();
            waitingin = false;
        }
        return ret;
    }

    void stop()
    {
#ifdef _FULL_TRACE
        ActPrintLog(activity, "CSmartRowInMemoryBuffer stop %x",(unsigned)(memsize_t)this);
#endif
        SpinBlock block(lock);
#ifdef _DEBUG
        if (waitingout) {
            ActPrintLogEx(&activity->queryContainer(), thorlog_null, MCwarning, "CSmartRowInMemoryBuffer::stop while nextRow waiting");
            PrintStackReport();
        }
#endif

        eoi = true;
        while (in->ordinality()) 
            if (srbrowif)
                srbrowif->releaseRow(in->dequeue());
            else
                ReleaseThorRow(in->dequeue());
        in->clear();
        if (waitingin) {
            waitinsem.signal();
            waitingin = false;
        }
    }


//  offset_t getPosition()
//  {
//      SpinBlock block(lock);
//      return pos;
//  }

    void flush()
    {
        // I think flush should wait til all rows read
        SpinBlock block(lock);
        eoi = true;
        loop {
            assertex(in);  // reentry check
            if (waitingout) {
                waitoutsem.signal();
                waitingout = false;
            }
            if (!in->ordinality()) 
                break;
            waitingin = true;
            SpinUnblock unblock(lock);
            while (!waitinsem.wait(1000*60))
                ActPrintLogEx(&activity->queryContainer(), thorlog_null, MCwarning, "CSmartRowInMemoryBuffer::flush stalled");
        }
    }

    IRowWriter *queryWriter()
    {
        return this;
    }

};

ISmartRowBuffer * createSmartBuffer(CActivityBase *activity, const char * tempname, size32_t buffsize, IRowInterfaces *rowif) 
{
    Owned<IFile> file = createIFile(tempname);
    return new CSmartRowBuffer(activity,file,buffsize,rowif);
}

ISmartRowBuffer * createSmartInMemoryBuffer(CActivityBase *activity, size32_t buffsize, ISRBRowInterface *srbrowif) 
{
    return new CSmartRowInMemoryBuffer(activity,buffsize,srbrowif);
}


class COverflowableBuffer : public CSimpleInterface, implements IRowWriterMultiReader
{
    IRowInterfaces *rowif;
    CThorRowArray rows;
    bool lastnull;
    rowcount_t total;
    bool eoi;
    Owned<IRowWriter> diskout;
    Owned<IFile> tmpfile;
    unsigned readersInUse;

    void diskSwitch()
    {
        StringBuffer temp;
        GetTempName(temp,"bufovf",true);
        tmpfile.setown(createIFile(temp));
        diskout.setown(createRowWriter(tmpfile,rowif->queryRowSerializer(),rowif->queryRowAllocator(),true,false, false));
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    COverflowableBuffer(IRowInterfaces *_rowif, size32_t sizebuf)
        : rowif(_rowif)
    {
        rows.setSizing(true,false);
        rows.setMaxTotal(sizebuf);
        total = 0;
        eoi = false;
        lastnull = false;
        readersInUse = 0;
    }
    ~COverflowableBuffer()
    {
        assertex(!readersInUse); // readers have link to parent, so shouldn't destruct unless they have released
        doStop();
    }
    void putRow(const void *row)
    {
        assertex(!eoi); 
        if (row==NULL) {
            if (lastnull) {
                flush();
                return;
            }
            lastnull = true;
        }
        else
            lastnull = false;
        if (diskout)
            diskout->putRow(row);
        else {
            rows.append(row);
            if (rows.isFull()) 
                diskSwitch();
        }
        total++;
    }
    void doStop()
    {
        flush(); // unless no readers, will have been already
    }
    void reset()
    {
        rows.clear();
        total = 0;
    }
    void readerStop()
    {
        --readersInUse;
    }
    void flush()
    {
        eoi = true;
        if (diskout) {
            diskout->flush();
            diskout.clear();
        }
    }
    IRowStream *getReader()
    {
        flush();
        class COverflowReader : public CSimpleInterface, implements IRowStream
        {
            Linked<COverflowableBuffer> owner;
            IRowInterfaces *rowIf;
            CThorRowArray &rows;
            IFile *file;
            Owned<IRowStream> diskin;
            rowcount_t pos;
            bool eog;

        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            COverflowReader(COverflowableBuffer *_owner, CThorRowArray &_rows, IFile *_file, IRowInterfaces *_rowIf) : owner(_owner), rows(_rows), file(_file), rowIf(_rowIf)
            {
                if (file)
                    diskin.setown(createRowStream(file,rowIf,0,(offset_t)-1,RCUNBOUND,false,true)); // NH->JCS: always grouped?
                pos = 0;
            }
            ~COverflowReader()
            {
                stop();
            }
            const void *nextRow()
            {
                const void *ret;
                if (pos<rows.ordinality())
                {
                    ret = rows.item((unsigned)(pos++));
                    if (ret)
                        LinkThorRow(ret);
                }
                else if (diskin)
                    ret = diskin->nextRow();
                else
                    return NULL;
                if (ret)
                    return ret;
                else if (eog)
                {
                    // eof
                    diskin.clear();
                    return NULL;
                }
                eog = true;
                return NULL;
            }
            void stop()
            {
                diskin.clear();
                owner->readerStop();
            }
        };
        ++readersInUse;
        return new COverflowReader(this, rows, tmpfile, rowif); // NB: holds link to COverflowReader
    }

//  offset_t getPosition()
//  {
//      offset_t ret = rows.totalSize();
//      if (diskout)
//          ret += diskout->getPosition();
//      return ret;
//  }

    void cancel() {}
};

IRowWriterMultiReader *createOverflowableBuffer(IRowInterfaces *_rowif,size32_t sizebuf)
{
    return new COverflowableBuffer(_rowif,sizebuf);
}


#define VALIDATEEQ(LHS, RHS) if ((LHS)!=(RHS)) { StringBuffer s("FAIL(EQ) - LHS="); s.append(LHS).append(", RHS=").append(RHS); PROGLOG("%s", s.str()); throwUnexpected();  }
#define VALIDATELT(LHS, RHS) if ((LHS)>=(RHS)) { StringBuffer s("FAIL(LT) - LHS="); s.append(LHS).append(", RHS=").append(RHS); PROGLOG("%s", s.str()); throwUnexpected(); }

//#define TRACE_WRITEAHEAD
class CRowSet : public CSimpleInterface
{
    unsigned chunk;
    CThorRowArray rows;
public:
    CRowSet(unsigned _chunk) : chunk(_chunk)
    {
    }
    ~CRowSet()
    {
        rows.clear();
    }
    inline const unsigned queryChunk() { return chunk; }
    inline const unsigned getRowCount() const { return rows.ordinality(); }
    inline void addRow(const void *row) { rows.append(row); }
    inline const void *getRow(unsigned r)
    {
        const void *row = rows.item(r);
        if (row)
            LinkThorRow(row);
        return row;
    }
};

class Chunk : public CInterface
{
public:
    offset_t offset;
    size32_t size;
    Chunk(offset_t _offset, size_t _size) : offset(_offset), size(_size) { }
    Chunk(const Chunk &other) : offset(other.offset), size(other.size) { }
    bool operator==(Chunk const &other) const { return size==other.size && offset==other.offset; }
    Chunk &operator=(Chunk const &other) { offset = other.offset; size = other.size; return *this; }
    offset_t endOffset() { return offset+size; }
};

typedef int (*ChunkCompareFunc)(Chunk *, Chunk *);
int chunkOffsetCompare(Chunk *lhs, Chunk *rhs)
{
    if (lhs->offset>rhs->offset)
        return 1;
    else if (lhs->offset<rhs->offset)
        return -1;
    return 0;
}
int chunkSizeCompare(Chunk **_lhs, Chunk **_rhs)
{
    Chunk *lhs = *(Chunk **)_lhs;
    Chunk *rhs = *(Chunk **)_rhs;
    return (int)lhs->size - (int)rhs->size;
}
int chunkSizeCompare2(Chunk *lhs, Chunk *rhs)
{
    return (int)lhs->size - (int)rhs->size;
}

class CSharedWriteAheadBase : public CSimpleInterface, implements ISharedSmartBuffer
{
    size32_t totalOutChunkSize;
    bool writeAtEof;
    rowcount_t rowsWritten;
    IArrayOf<IRowStream> outputs;
    unsigned readersWaiting;

    inline bool isEof(rowcount_t rowsRead)
    {
        return stopped || writeAtEof && rowsWritten == rowsRead;
    }
    inline void signalReaders()
    {
        if (readersWaiting)
        {
#ifdef TRACE_WRITEAHEAD
            ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "signalReaders: %d", readersWaiting);
#endif
            readersWaiting = 0;
            ForEachItemIn(o, outputs)
                queryCOutput(o).wakeRead(); // if necessary
        }
    }
    inline const rowcount_t &readerWait(const rowcount_t &rowsRead)
    {
        ++readersWaiting;
        return rowsWritten;
    }
    CRowSet *loadMore(unsigned output)
    {
        // needs to be called in crit
        Linked<CRowSet> rowSet;
        unsigned currentChunkNum = queryCOutput(output).currentChunkNum;
        if (currentChunkNum == totalChunksOut)
        {
#ifdef TRACE_WRITEAHEAD
            PROGLOG("output=%d, chunk=%d INMEM", output, currentChunkNum);
#endif
            rowSet.set(inMemRows);
        }
        else
        {
            unsigned whichChunk = queryCOutput(output).currentChunkNum - lowestChunk;
#ifdef TRACE_WRITEAHEAD
            PROGLOG("output=%d, chunk=%d (whichChunk=%d)", output, currentChunkNum, whichChunk);
#endif
            rowSet.setown(readRows(output, whichChunk));
            assertex(rowSet);
        }
        VALIDATEEQ(currentChunkNum, rowSet->queryChunk());
        advanceNextReadChunk(output);
        return rowSet.getClear();
    }
protected:
    class COutput : public CSimpleInterface, implements IRowStream
    {
        CSharedWriteAheadBase &parent;
        unsigned output;
        rowcount_t rowsRead;
        bool eof, readerWaiting;
        Semaphore readWaitSem;
        Owned<CRowSet> outputOwnedRows;
        CRowSet *rowSet;
        unsigned row, rowsInRowSet;

        inline void doStop()
        {
            if (eof) return;
            eof = true;
            parent.stopOutput(output);
        }
        inline const rowcount_t readerWait(const rowcount_t &rowsRead)
        {
            if (rowsRead == parent.rowsWritten)
            {
                if (parent.stopped || parent.writeAtEof)
                    return 0;
                readerWaiting = true;
#ifdef TRACE_WRITEAHEAD
                ActPrintLogEx(&parent.activity->queryContainer(), thorlog_all, MCdebugProgress, "readerWait(%d)", output);
#endif
                const rowcount_t &rowsWritten = parent.readerWait(rowsRead);
                {
                    CriticalUnblock b(parent.crit);
                    readWaitSem.wait();
                }
                if (parent.isEof(rowsRead))
                    return 0;
            }
            return parent.rowsWritten;
        }
        inline void wakeRead()
        {
            if (readerWaiting)
            {
                readerWaiting = false;
                readWaitSem.signal();
            }
        }
    public:
        unsigned currentChunkNum;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        COutput(CSharedWriteAheadBase &_parent, unsigned _output) : parent(_parent), output(_output)
        {
            rowsRead = 0;
            currentChunkNum = 0;
            rowsInRowSet = row = 0;
            readerWaiting = eof = false;
            rowSet = NULL;
        }
        inline CRowSet *queryRowSet() { return rowSet; }
        const void *nextRow()
        {
            if (eof)
                return NULL;
            if (row == rowsInRowSet)
            {
                CriticalBlock b(parent.crit);
                if (!rowSet || (row == (rowsInRowSet = rowSet->getRowCount())))
                {
                    rowcount_t totalRows = readerWait(rowsRead);
                    if (0 == totalRows)
                    {
                        doStop();
                        return NULL;
                    }
                    if (!rowSet || (row == (rowsInRowSet = rowSet->getRowCount()))) // maybe have caught up in same rowSet
                    {
                        Owned<CRowSet> newRows = parent.loadMore(output);
                        if (rowSet)
                            VALIDATEEQ(newRows->queryChunk(), rowSet->queryChunk()+1);
                        outputOwnedRows.setown(newRows.getClear());
                        rowSet = outputOwnedRows.get();
                        rowsInRowSet = rowSet->getRowCount();
                        row = 0;
                    }
                }
            }
            rowsRead++;
            SpinBlock b(parent.spin);
            const void *retrow = rowSet->getRow(row++);
            return retrow;
        }
        virtual void stop()
        {
            CriticalBlock b(parent.crit);
            doStop();
        }
    friend class CSharedWriteAheadBase;
    };
    CActivityBase *activity;
    size32_t minChunkSize;
    unsigned lowestChunk, lowestOutput, outputCount, totalChunksOut;
    bool stopped;
    Owned<CRowSet> inMemRows;
    CriticalSection crit;
    SpinLock spin;
    Linked<IOutputMetaData> meta;

    inline COutput &queryCOutput(unsigned i) { return (COutput &) outputs.item(i); }
    inline unsigned getLowest()
    {
        unsigned o=0;
        offset_t lsf = (unsigned)-1;
        unsigned lsfOutput = (unsigned)-1;
        loop
        {
            if (queryCOutput(o).currentChunkNum < lsf)
            {
                lsf = queryCOutput(o).currentChunkNum; 
                lsfOutput = o;
            }
            if (++o == outputCount)
                break;
        }
        return lsfOutput;
    }
    void advanceNextReadChunk(unsigned output)
    {
        unsigned &currentChunkNum = queryCOutput(output).currentChunkNum;
        unsigned prevChunkNum = currentChunkNum;
        currentChunkNum++;
        if (output == lowestOutput) // might be joint lowest
        {
            lowestOutput = getLowest();
            if (currentChunkNum == queryCOutput(lowestOutput).currentChunkNum) // has new page moved up to join new lowest, in which case it was last.
            {
                if (prevChunkNum != totalChunksOut) // if equal, then prevOffset was mem page and not in pool/disk to free
                    freeOffsetChunk(prevChunkNum);
                else
                {
                    VALIDATEEQ(prevChunkNum, inMemRows->queryChunk()); // validate is current in mem page
                }
                ++lowestChunk;
#ifdef TRACE_WRITEAHEAD
                ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "aNRP: %d, lowestChunk=%d, totalChunksOut=%d", prevChunkNum, lowestChunk, totalChunksOut);
#endif
            }
        }
    }
    void stopOutput(unsigned output)
    {
        // mark stopped output with very high page (as if read all), will not be picked again
        unsigned lowestA = getLowest();
        if (output == lowestA)
        {
            unsigned chunkOrig = queryCOutput(output).currentChunkNum;
            queryCOutput(output).currentChunkNum = (unsigned)-1;
            unsigned lO = getLowest(); // get next lowest
            queryCOutput(output).currentChunkNum = chunkOrig;
            if (((unsigned)-1) == lO) // if last to stop
            {
                markStop();
                return;
            }
#ifdef TRACE_WRITEAHEAD
            ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "Input %d stopped, forcing catchup to free pages", output);
#endif
            while (queryCOutput(output).currentChunkNum < queryCOutput(lO).currentChunkNum)
            {
                Owned<CRowSet> tmpChunk = loadMore(output); // discard
            }
#ifdef TRACE_WRITEAHEAD
            ActPrintLog(activity, "Input %d stopped. Caught up", output);
#endif
            queryCOutput(output).currentChunkNum = (unsigned)-1;
            lowestOutput = getLowest();
        }
        else
            queryCOutput(output).currentChunkNum = (unsigned)-1;
    }
    void stopAll()
    {
        CriticalBlock b(crit);
        markStop();
    }
    virtual void markStop()
    {
        if (stopped) return;
        stopped = true;
    }
    virtual void freeOffsetChunk(unsigned chunk) = 0;
    virtual CRowSet *readRows(unsigned output, unsigned chunk) = 0;
    virtual void flushRows() = 0;
    virtual size32_t rowSize(const void *row) = 0;


    COutput *o1, *o2;
public:

    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSharedWriteAheadBase(CActivityBase *_activity, unsigned _outputCount, IRowInterfaces *rowIf) : activity(_activity), outputCount(_outputCount), meta(rowIf->queryRowMetaData())
    {
        stopped = false;
        writeAtEof = false;
        rowsWritten = 0;
        minChunkSize = 0x2000;
        readersWaiting = 0;
        size32_t minSize = meta->getMinRecordSize();
        if (minChunkSize<minSize*10) // if rec minSize bigish, ensure reasonable minChunkSize
        {
            minChunkSize = minSize*10;
            if (minChunkSize > 0x10000)
                minChunkSize += 2*(minSize+1);
        }
        outputCount = _outputCount;
        unsigned c=0;
        for (; c<outputCount; c++)
        {
            outputs.append(* new COutput(*this, c));

            if (0 == c)
                o1 = &queryCOutput(0);
            else if (1 == c)
                o2 = &queryCOutput(1);
        }
        totalChunksOut = lowestChunk = 0;
        inMemRows.setown(new CRowSet(0));
        lowestOutput = 0;
#ifdef TRACE_WRITEAHEAD
        totalOutChunkSize = sizeof(unsigned);
#else
        totalOutChunkSize = 0;
#endif
    }
    ~CSharedWriteAheadBase()
    {
        ForEachItemIn(o, outputs)
        {
            COutput &output = queryCOutput(o);
            output.outputOwnedRows.clear();
        }
        inMemRows.clear();
    }


    unsigned anyReaderBehind()
    {
        unsigned reader=0;
        loop
        {
            if (reader>=outputCount) // so all waiting, don't need in mem page.
                break;
            if (!queryCOutput(reader).eof && (queryCOutput(reader).rowSet != inMemRows.get())) // if output using this in mem page, then it has linked already, need to find one that is not (i.e. behind)
                return reader;
            ++reader;
        }
        return NotFound;
    }

// ISharedSmartBuffer
    virtual void putRow(const void *row)
    {
        if (stopped)
        {
            ReleaseThorRow(row);
            return;
        }
        unsigned len=rowSize(row);
        CriticalBlock b(crit);
        if (totalOutChunkSize >= minChunkSize) // chunks required to be at least minChunkSize
        {
            unsigned reader=anyReaderBehind();
            if (NotFound != reader)
                flushRows();
            inMemRows.setown(new CRowSet(++totalChunksOut));
#ifdef TRACE_WRITEAHEAD
            totalOutChunkSize = sizeof(unsigned);
#else
            totalOutChunkSize = 0;
#endif
        }
        {
            SpinBlock b(spin);
            inMemRows->addRow(row);
        }

        totalOutChunkSize += len;
        rowsWritten++; // including NULLs(eogs)

        signalReaders();
    }
    virtual void flush()
    {
        CriticalBlock b(crit);
        writeAtEof = true;
        signalReaders();
    }
    virtual offset_t getPosition()
    {
        throwUnexpected();
        return 0;
    }
    virtual IRowStream *queryOutput(unsigned output)
    {
        return &outputs.item(output);
    }
    virtual void cancel()
    {
        CriticalBlock b(crit);
        stopAll();
        signalReaders();
    }
friend class COutput;
};

class CSharedWriteAheadDisk : public CSharedWriteAheadBase
{
    IDiskUsage *iDiskUsage;
    Owned<IFile> spillFile;
    Owned<IFileIO> spillFileIO;
    CIArrayOf<Chunk> freeChunks;
    PointerArrayOf<Chunk> freeChunksSized;
    QueueOf<Chunk, false> savedChunks;
    offset_t highOffset;
    Linked<IEngineRowAllocator> allocator;
    Linked<IOutputRowSerializer> serializer;
    Linked<IOutputRowDeserializer> deserializer;
    IOutputMetaData *serializeMeta;

    struct AddRemoveFreeChunk
    {
        PointerArrayOf<Chunk> &chunkArray;
        Chunk *chunk;
        AddRemoveFreeChunk(PointerArrayOf<Chunk> &_chunkArray, Chunk *_chunk) : chunkArray(_chunkArray), chunk(_chunk)
        {
            chunkArray.zap(chunk);
        }
        ~AddRemoveFreeChunk()
        {
            bool isNew;
            aindex_t sizePos = chunkArray.bAdd(chunk, chunkSizeCompare, isNew);
            if (!isNew) // might match size of another block
                chunkArray.add(chunk, sizePos);
        }
    };
    unsigned findNext(Chunk **base, unsigned arrCount, Chunk *searchChunk, ChunkCompareFunc compareFunc, bool &matched)
    {
        unsigned middle;
        unsigned left = 0;
        int result;
        Chunk *CurEntry;
        unsigned right = arrCount-1;
        /*
        * Loop until we've narrowed down the search range to one or two
        * items.  This ensures that middle != 0, preventing 'right' from wrapping.
        */
        while (right - left > 1)
        {
            /* Calculate the middle entry in the array - NOT the middle offset */
            middle = (right + left) >> 1;

            CurEntry = base[middle];
            result = compareFunc(searchChunk, CurEntry);
            if (result < 0)
                right = middle - 1;
            else if (result > 0)
                left = middle + 1;
            else
            {
                matched = true;
                return middle;
            }
        }
        middle = left;
        /*
        * The range has been narrowed down to 1 or 2 items.
        * Perform an optimal check on these last two elements.
        */
        result = compareFunc(searchChunk, base[middle]);
        if (0 == result)
            matched = true;
        else if (result > 0)
        {
            ++middle;
            if (right == left + 1)
            {
                result = compareFunc(searchChunk, base[middle]);
                if (0 == result)
                    matched = true;
                else
                {
                    matched = false;
                    if (result > 0)
                        ++middle;
                }
            }
            else
                matched = false;
        }
        else
            matched = false;
        if (middle < arrCount)
            return middle;
        return NotFound;
    }
    inline Chunk *getOutOffset(size32_t required)
    {
        unsigned numFreeChunks = freeChunks.ordinality();
        if (numFreeChunks)
        {
            Chunk searchChunk(0, required);
            bool matched;
            unsigned nextPos = findNext(freeChunksSized.getArray(), freeChunksSized.ordinality(), &searchChunk, chunkSizeCompare2, matched);
            if (NotFound != nextPos)
            {
                Linked<Chunk> nextChunk = freeChunksSized.item(nextPos);
                if (nextChunk->size-required >= minChunkSize)
                {
                    Owned<Chunk> chunk = new Chunk(nextChunk->offset, required);
                    decFreeChunk(nextChunk, required);
#ifdef TRACE_WRITEAHEAD
                    ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "getOutOffset: got [free] offset = %"I64F"d, size=%d, but took required=%d", nextChunk->offset, nextChunk->size+required, required);
#endif
                    return chunk.getClear();
                }
#ifdef TRACE_WRITEAHEAD
                ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "getOutOffset: got [free] offset = %"I64F"d, size=%d", diskChunk.offset, diskChunk.size);
#endif
                freeChunksSized.remove(nextPos);
                freeChunks.zap(*nextChunk);
                return nextChunk.getClear(); // if smaller than minChunkSize, return all, don't leave useless fragment
            }
        }
#ifdef TRACE_WRITEAHEAD
        ActPrintLog(activity, "getOutOffset: got new upper offset # = %"I64F"d", highOffset);
#endif
        Chunk *chunk = new Chunk(highOffset, required);
        if (iDiskUsage)
        {
            iDiskUsage->decrease(highOffset);
            highOffset += required; // NB next
            iDiskUsage->increase(highOffset);
        }
        else
            highOffset += required; // NB next
        return chunk;
    }
    inline void mergeFreeChunk(Chunk *dst, const Chunk *src)
    {
        AddRemoveFreeChunk ar(freeChunksSized, dst);
        dst->offset = src->offset;
        dst->size += src->size;
    }
    inline void decFreeChunk(Chunk *chunk, size32_t sz)
    {
        AddRemoveFreeChunk ar(freeChunksSized, chunk);
        chunk->size -= sz;
        chunk->offset += sz;
    }
    inline void incFreeChunk(Chunk *chunk, size32_t sz)
    {
        AddRemoveFreeChunk ar(freeChunksSized, chunk);
        chunk->size += sz;
    }
    inline void addFreeChunk(Chunk *freeChunk, unsigned pos=NotFound)
    {
        bool isNew;
        aindex_t sizePos = freeChunksSized.bAdd(freeChunk, chunkSizeCompare, isNew);
        if (!isNew)
            freeChunksSized.add(freeChunk, sizePos);
        if (NotFound == pos)
            freeChunks.append(*LINK(freeChunk));
        else
            freeChunks.add(*LINK(freeChunk), pos);
    }
    virtual void freeOffsetChunk(unsigned chunk)
    {
        // chunk(unused here) is nominal sequential page #, savedChunks is page # in diskfile
        assertex(savedChunks.ordinality());
        Owned<Chunk> freeChunk = savedChunks.dequeue();
        unsigned nmemb = freeChunks.ordinality();
        if (0 == nmemb)
            addFreeChunk(freeChunk);
        else
        {
            bool matched;
            unsigned nextPos = findNext(freeChunks.getArray(), freeChunks.ordinality(), freeChunk, chunkOffsetCompare, matched);
            assertex(!matched); // should never happen, have found a chunk with same offset as one being freed.
            if (NotFound != nextPos)
            {
                Chunk *nextChunk = &freeChunks.item(nextPos);
                if (freeChunk->endOffset() == nextChunk->offset)
                    mergeFreeChunk(nextChunk, freeChunk);
                else if (nextPos > 0)
                {
                    Chunk *prevChunk = &freeChunks.item(nextPos-1);
                    if (prevChunk->endOffset() == freeChunk->offset)
                        incFreeChunk(prevChunk, freeChunk->size);
                    else
                        addFreeChunk(freeChunk, nextPos);
                }
                else
                    addFreeChunk(freeChunk, nextPos);
            }
            else
                addFreeChunk(freeChunk);
        }
#ifdef TRACE_WRITEAHEAD
        ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "Added chunk, offset %"I64F"d size=%d to freeChunks", freeChunk->offset, freeChunk->size);
#endif
    }
    virtual CRowSet *readRows(unsigned output, unsigned whichChunk)
    {
        assertex(savedChunks.ordinality() > whichChunk);
        unsigned currentChunkNum = queryCOutput(output).currentChunkNum;
        unsigned o=0;
        for (; o<outputCount; o++)
        {
            if (o == output) continue;
            COutput &coutput = queryCOutput(o);
            if (coutput.queryRowSet() && (coutput.queryRowSet()->queryChunk() == currentChunkNum))
            {
#ifdef TRACE_WRITEAHEAD
                ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "Output: %d, readRows found other output %d with matching offset: %d", output, o, currentChunkNum);
#endif
                return LINK(coutput.queryRowSet());
            }
        }
        Chunk &chunk = *savedChunks.item(whichChunk);
        Owned<ISerialStream> stream = createFileSerialStream(spillFileIO, chunk.offset);
#ifdef TRACE_WRITEAHEAD
        unsigned diskChunkNum;
        stream->get(sizeof(diskChunkNum), &diskChunkNum);
        VALIDATEEQ(diskChunkNum, currentChunkNum);
#endif
        CThorStreamDeserializerSource ds(stream);
        Owned<CRowSet> rowSet = new CRowSet(currentChunkNum);
        loop
        {   
            byte b;
            ds.read(sizeof(b),&b);
            if (!b)
                break;
            if (1==b)
            {
                RtlDynamicRowBuilder rowBuilder(allocator);
                size32_t sz = deserializer->deserialize(rowBuilder, ds);
                rowSet->addRow(rowBuilder.finalizeRowClear(sz));
            }
            else if (2==b)
                rowSet->addRow(NULL);
        }
        return rowSet.getClear();
    }
    virtual void flushRows()
    {
        // NB: called in crit
        MemoryBuffer mb;
        mb.ensureCapacity(minChunkSize); // starting size/could be more if variable and bigger
#ifdef TRACE_WRITEAHEAD
        mb.append(inMemRows->queryChunk()); // for debug purposes only
#endif
        CMemoryRowSerializer mbs(mb);
        unsigned r=0;
        for (;r<inMemRows->getRowCount();r++)
        {
            OwnedConstThorRow row = inMemRows->getRow(r);
            if (row)
            {
                mb.append((byte)1);
                serializer->serialize(mbs,(const byte *)row.get());
            }
            else
                mb.append((byte)2); // eog
        }
        mb.append((byte)0);

        size32_t len = mb.length();
        Owned<Chunk> freeChunk = getOutOffset(len); // will find space for 'len', might be bigger if from free list

        spillFileIO->write(freeChunk->offset, len, mb.toByteArray());

        savedChunks.enqueue(freeChunk.getClear());
#ifdef TRACE_WRITEAHEAD
        ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "Flushed chunk = %d (savedChunks pos=%d), writeOffset = %"I64F"d, writeSize = %d", inMemRows->queryChunk(), savedChunks.ordinality()-1, freeChunk->offset, len);
#endif
    }
    virtual size32_t rowSize(const void *row)
    {
        return serializeMeta->getRecordSize(row)+1; // space on disk, +1 = eog marker
    }
public:
    CSharedWriteAheadDisk(CActivityBase *activity, const char *spillName, unsigned outputCount, IRowInterfaces *rowIf, IDiskUsage *_iDiskUsage) : CSharedWriteAheadBase(activity, outputCount, rowIf),
        allocator(rowIf->queryRowAllocator()), serializer(rowIf->queryRowSerializer()), deserializer(rowIf->queryRowDeserializer()), serializeMeta(meta->querySerializedMeta()), iDiskUsage(_iDiskUsage)
    {
        assertex(spillName);
        spillFile.setown(createIFile(spillName));
        spillFile->setShareMode(IFSHnone);
        spillFileIO.setown(spillFile->open(IFOcreaterw));
        highOffset = 0;
    }
    ~CSharedWriteAheadDisk()
    {
        spillFileIO.clear();
        if (spillFile)
            spillFile->remove();

        loop
        {
            Owned<Chunk> chunk = savedChunks.dequeue();
            if (!chunk) break;
        }
        PROGLOG("CSharedWriteAheadDisk: highOffset=%"I64F"d", highOffset);
    }
};

ISharedSmartBuffer *createSharedSmartDiskBuffer(CActivityBase *activity, const char *spillname, unsigned outputs, IRowInterfaces *rowIf, IDiskUsage *iDiskUsage)
{
    return new CSharedWriteAheadDisk(activity, spillname, outputs, rowIf, iDiskUsage);
}

#define MIN_POOL_CHUNKS 10
class CSharedWriteAheadMem : public CSharedWriteAheadBase
{
    QueueOf<CRowSet, false> chunkPool;
    Semaphore poolSem;
    bool writerBlocked;
    unsigned maxPoolChunks;

    virtual void markStop()
    {
        CSharedWriteAheadBase::markStop();
        if (writerBlocked)
        {
            writerBlocked = false;
            poolSem.signal();
        }
    }
    virtual void freeOffsetChunk(unsigned chunk)
    {
        Owned<CRowSet> topRowSet = chunkPool.dequeue();
        VALIDATEEQ(chunk, topRowSet->queryChunk());
#ifdef TRACE_WRITEAHEAD
        ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "freeOffsetChunk: Dequeue chunkPool chunks: %d, chunkPool.ordinality() = %d", topRowSet->queryChunk(), chunkPool.ordinality());
#endif
        topRowSet.clear();
        if (writerBlocked)
        {
            writerBlocked = false;
            poolSem.signal();
        }
    }
    virtual CRowSet *readRows(unsigned output, unsigned whichChunk)
    {
        Linked<CRowSet> rowSet = chunkPool.item(whichChunk);
        VALIDATEEQ(queryCOutput(output).currentChunkNum, rowSet->queryChunk());
        return rowSet.getClear();
    }
    virtual void flushRows()
    {
        // NB: called in crit
        if (chunkPool.ordinality() >= maxPoolChunks)
        {
            writerBlocked = true;
            { CriticalUnblock b(crit);
                poolSem.wait();
                if (stopped) return;
            }
            unsigned reader=anyReaderBehind();
            if (NotFound == reader)
            {
#ifdef TRACE_WRITEAHEAD
                ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "flushRows: caught up whilst blocked to: %d", inMemRows->queryChunk());
#endif
                return; // caught up whilst blocked
            }
            VALIDATELT(chunkPool.ordinality(), maxPoolChunks);
        }
#ifdef TRACE_WRITEAHEAD
        ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "Flushed chunk = %d, chunkPool chunks = %d", inMemRows->queryChunk(), 1+chunkPool.ordinality());
#endif
        chunkPool.enqueue(inMemRows.getClear());
    }
    virtual size32_t rowSize(const void *row)
    {
        return meta->getRecordSize(row); // space in mem.
    }
public:
    CSharedWriteAheadMem(CActivityBase *activity, unsigned outputCount, IRowInterfaces *rowif, unsigned buffSize) : CSharedWriteAheadBase(activity, outputCount, rowif)
    {
        if (((unsigned)-1) == buffSize)
            maxPoolChunks = (unsigned)-1; // no limit
        else
        {
            maxPoolChunks = buffSize / minChunkSize;
            if (maxPoolChunks < MIN_POOL_CHUNKS)
                maxPoolChunks = MIN_POOL_CHUNKS;
        }
        writerBlocked = false;
    }
    ~CSharedWriteAheadMem()
    {
        loop
        {
            Owned<CRowSet> rowSet = chunkPool.dequeue();
            if (!rowSet)
                break;
        }
    }
};

ISharedSmartBuffer *createSharedSmartMemBuffer(CActivityBase *activity, unsigned outputs, IRowInterfaces *rowIf, unsigned buffSize)
{
    return new CSharedWriteAheadMem(activity, outputs, rowIf, buffSize);
}

class CRCFileStream: public CSimpleInterface, implements IFileIOStream
{
    Linked<IFileIOStream> streamin;
public:
    CRC32 &crc;
    bool valid;

    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRCFileStream(IFileIOStream *_stream,CRC32 &_crc)
        : streamin(_stream), crc(_crc)
    {
        valid = true;
    }


    size32_t read(size32_t len, void * data)
    {
        size32_t rd = streamin->read(len,data);
        crc.tally(rd,data);
        return rd;
    }
    
    size32_t write(size32_t len, const void * data)
    {
        throw MakeStringException(-1,"CRCFileStream does not support write");
    }

    void seek(offset_t pos, IFSmode origin)
    {
        offset_t t = streamin->tell();
        streamin->seek(pos,origin);
        if (t!=streamin->tell())  // crc invalid
            if (valid) {
                WARNLOG("CRCFileStream::seek called - CRC will be invalid");
                valid = false;
            }
    }
    offset_t size()
    {
        return streamin->size();
    }

    virtual offset_t tell()
    {
        return streamin->tell();
    }

    void flush()
    {
        // noop as only read
    }

};

