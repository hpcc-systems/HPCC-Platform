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
#include <limits.h>
#include "jlib.hpp"
#include "jflz.hpp"
#include <mpbase.hpp>
#include <mpcomm.hpp>
#include "thorport.hpp"
#include "jsocket.hpp"
#include "jthread.hpp"
#include "thormisc.hpp"
#include "jisem.hpp"
#include "jsort.hpp"
#include "tsorts.hpp"
#include "tsorta.hpp"
#include "tsortm.hpp"
#include "tsortmp.hpp"
#include "thbuf.hpp"
#include "thgraph.hpp"

#define _TRACE

#ifdef _DEBUG
//#define TRACE_UNIQUE
//#define _FULL_TRACE
//#define _FULLMPTRACE
//#define TRACE_PARTITION
//#define TRACE_PARTITION_OVERFLOW
#endif

#define MINISORT_PARALLEL_TRANSFER 16


template <class T>
inline void traceWait(const char *name, T &sem,unsigned interval=60*1000)
{
    while (!sem.wait(interval))
        PROGLOG("Waiting for %s",name);
}


#define TRANSFERBLOCKSIZE 0x100000 // 1MB
#define MINCOMPRESSEDROWSIZE 16
#define MAXCOMPRESSEDROWSIZE  0x2000

#define MPBLOCKTIMEOUT (1000*60*15)             


class CWriteIntercept : public CSimpleInterface
{
    // sampling adapter

    CActivityBase &activity;
    CriticalSection crit;
    IRowInterfaces *rowIf;
    Owned<IFile> dataFile, idxFile;
    Owned<IFileIO> dataFileIO, idxFileIO;
    Owned<ISerialStream> dataFileStream;
    Linked<IFileIOStream> idxFileStream;
    CThorStreamDeserializerSource dataFileDeserializerSource;
    unsigned interval;
    unsigned idx;
    CThorExpandingRowArray sampleRows;
    offset_t overflowsize;
    size32_t fixedsize;
    offset_t lastofs;

    // JCSMORE - writeidxofs is a NOP for fixed size records by the looks of it (at least if serializer writes fixed sizes)
    // bit weird if always true, would look a lot clearer if explictly tested for var length case.
    void writeidxofs(offset_t o)
    {
        // lazy index write

        if (idxFileStream.get())
        {
            idxFileStream->write(sizeof(o),&o);
            return;
        }
        if (lastofs)
        {
            if (fixedsize!=o-lastofs)
            {
                // right create idx
                StringBuffer tempname;
                GetTempName(tempname.clear(),"srtidx",false);
                idxFile.setown(createIFile(tempname.str()));
                idxFileIO.setown(idxFile->open(IFOcreaterw));
                if (!idxFileIO.get())
                {
                    StringBuffer err;
                    err.append("Cannot create ").append(idxFile->queryFilename());
                    LOG(MCerror, thorJob, "%s", err.str());
                    throw MakeActivityException(&activity, -1, "%s", err.str());
                }
                idxFileStream.setown(createBufferedIOStream(idxFileIO,0x100000));
                offset_t s = 0;
                while (s<=lastofs)
                {
                    idxFileStream->write(sizeof(s),&s);
                    s += fixedsize;
                }
                assertex(s==lastofs+fixedsize);
                fixedsize = 0;
                writeidxofs(o);
                return;
            }
        }
        else
            fixedsize = (size32_t)(o-lastofs);
        lastofs = o;
    }
    size32_t _readOverflowPos(rowcount_t pos, unsigned n, offset_t *ofs, bool closeIO)
    {
        if (fixedsize)
        {
            offset_t o = (offset_t)fixedsize*(offset_t)pos;
            for(unsigned i=0;i<n;i++)
            {
                *(ofs++) = o;
                o += fixedsize;
            }
            return n*sizeof(offset_t);
        }
        if (!idxFileIO.get())
        {
            assertex(idxFile);
            idxFileIO.setown(idxFile->open(IFOread));
        }
        size32_t rd = idxFileIO->read((offset_t)pos*(offset_t)sizeof(offset_t),sizeof(*ofs)*n,ofs);
        if (closeIO)
            idxFileIO.clear();
        return rd;
    }

    class CFileOwningStream : public CSimpleInterface, implements IRowStream
    {
        Linked<CWriteIntercept> parent;
        Owned<IRowStream> stream;
        offset_t startOffset;
        rowcount_t max;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        CFileOwningStream(CWriteIntercept *_parent, offset_t _startOffset, rowcount_t _max) : parent(_parent), startOffset(_startOffset), max(_max)
        {
            stream.setown(createRowStreamEx(parent->dataFile, parent->rowIf, startOffset, (offset_t)-1, max));
        }
        virtual const void *nextRow() { return stream->nextRow(); }
        virtual void stop() { stream->stop(); }
    };
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CWriteIntercept(CActivityBase &_activity, IRowInterfaces *_rowIf, unsigned _interval)
        : activity(_activity), rowIf(_rowIf), interval(_interval), sampleRows(activity, rowIf, true)
    {
        interval = _interval;
        idx = 0;
        overflowsize = 0;
        fixedsize = 0;      
        lastofs = 0;
    }
    ~CWriteIntercept()
    {
        closeFiles();
        if (dataFile)
            dataFile->remove();
        if (idxFile)
            idxFile->remove();
    }
    offset_t write(IRowStream *input)
    {
        StringBuffer tempname;
        GetTempName(tempname,"srtmrg",false);
        dataFile.setown(createIFile(tempname.str()));
        Owned<IExtRowWriter> output = createRowWriter(dataFile, rowIf);

        bool overflowed = false;
        ActPrintLog(&activity, "Local Overflow Merge start");
        unsigned ret=0;
        loop
        {
            const void *_row = input->nextRow();
            if (!_row)
                break;
            ret++;

            OwnedConstThorRow row = _row;
            offset_t start = output->getPosition();
            output->putRow(row.getLink());
            idx++;
            if (idx==interval)
            {
                idx = 0;
                if (!sampleRows.append(row.getClear()))
                {
                    // JCSMORE used to check if 'isFull()' here, but only to warn
                    // I think this is bad news, if has run out of room here...
                    // should at least warn in workunit I suspect
                    overflowsize = output->getPosition();
                    if (!overflowed)
                    {
                        WARNLOG("Sample buffer full");
                        overflowed = true;
                    }
                }
            }
            writeidxofs(start);
        }
        output->flush();
        offset_t end = output->getPosition();
        output.clear();
        writeidxofs(end);
        if (idxFileIO)
        {
            idxFileStream->flush();
            idxFileStream.clear();
            idxFileIO.clear();
        }
        if (overflowed)
            WARNLOG("Overflowed by %" I64F "d", overflowsize);
        ActPrintLog(&activity, "Local Overflow Merge done: overflow file '%s', size = %" I64F "d", dataFile->queryFilename(), dataFile->size());
        return end;
    }
    IRowStream *getStream(offset_t startOffset, rowcount_t max)
    {
        return new CFileOwningStream(this, startOffset, max);
    }
    void closeFiles()
    {
        dataFileStream.clear();
        dataFileIO.clear();
        dataFileDeserializerSource.setStream(NULL);
        idxFileIO.clear();
    }
    size32_t readOverflowPos(rowcount_t pos, unsigned n, offset_t *ofs, bool closeIO)
    {
        CriticalBlock block(crit);
        return _readOverflowPos(pos, n, ofs, closeIO);
    }
    const void *getRow(rowcount_t pos)
    {
        CriticalBlock block(crit);
        offset_t ofs[2]; // JCSMORE doesn't really need 2, only to verify read right amount below
        size32_t rd = _readOverflowPos(pos, 2, ofs, false);
        assertex(rd==sizeof(ofs));
        size32_t idxSz = (size32_t)(ofs[1]-ofs[0]);
        if (!dataFileIO)
        {
            dataFileIO.setown(dataFile->open(IFOread));
            dataFileStream.setown(createFileSerialStream(dataFileIO));
            dataFileDeserializerSource.setStream(dataFileStream);
        }
        dataFileStream->reset(ofs[0], idxSz);
        RtlDynamicRowBuilder rowBuilder(rowIf->queryRowAllocator());
        size32_t sz = rowIf->queryRowDeserializer()->deserialize(rowBuilder, dataFileDeserializerSource);
        return rowBuilder.finalizeRowClear(sz);
    }
    void transferRows(CThorExpandingRowArray &rows)
    {
        rows.transferFrom(sampleRows);
    }
};

class CMiniSort
{
    CActivityBase &activity;
    IRowInterfaces &rowIf;
    IOutputRowDeserializer *deserializer;
    IOutputRowSerializer *serializer;
    IEngineRowAllocator *allocator;

    ICommunicator &clusterComm;
    unsigned partNo, numNodes;
    mptag_t mpTag;
    Owned<IThorRowCollector> collector;

    unsigned serialize(IRowStream &stream, MemoryBuffer &mb, size32_t dstMax)
    {
        CMemoryRowSerializer out(mb);
        unsigned ret = 0;
        loop
        {
            size32_t ln = mb.length();
            OwnedConstThorRow row = stream.nextRow();
            if (!row)
                break;
            serializer->serialize(out, (const byte *)row.get());
            ret++;
            if (mb.length() > dstMax)
                break;
        }
        return ret;
    }
    void deserialize(IRowWriter &out, MemoryBuffer &mb)
    {
        CThorStreamDeserializerSource d(mb.length(), mb.bufferBase());
        while (!d.eos())
        {
            RtlDynamicRowBuilder rowBuilder(allocator);
            size32_t sz = deserializer->deserialize(rowBuilder, d);
            out.putRow(rowBuilder.finalizeRowClear(sz));
        }
    }
    void sendToPrimaryNode(CMessageBuffer &mb)
    {
        //compression TBD
        CMessageBuffer mbin;
#ifdef  _FULL_TRACE
        ActPrintLog(&activity, "MiniSort sendToPrimaryNode waiting");
#endif
        clusterComm.recv(mbin,1,mpTag);
#ifdef  _FULL_TRACE
        ActPrintLog(&activity, "MiniSort sendToPrimaryNode continue %u bytes",mbin.length());
#endif
        if (mbin.length()==0) {
            ActPrintLog(&activity, "aborting sendToPrimaryNode");
            // TBD?
            return;
        }
        byte fn;
        mbin.read(fn);
        if (fn!=255)
            throw MakeActivityException(&activity, -1, "MiniSort sendToPrimaryNode: Protocol error(1) %d",(int)fn);
        mb.init(mbin.getSender(),mpTag,mbin.getReplyTag());
#ifdef  _FULL_TRACE
        ActPrintLog(&activity, "MiniSort sendToPrimaryNode %u bytes",mb.length());
#endif
        clusterComm.reply(mb);
    }
    void sendToSecondaryNode(CThorExpandingRowArray &rowArray, rank_t node, unsigned from, unsigned to, size32_t blksize)
    {
        //compression TBD
        CMessageBuffer mbout;
        unsigned done;
        do {
            done = rowArray.serializeBlock(mbout.clear(),from,to-from,blksize,false);
#ifdef  _FULL_TRACE
            ActPrintLog(&activity, "MiniSort serialized %u rows, %u bytes",done,mbout.length());
#endif
            from += done;
#ifdef  _FULL_TRACE
            ActPrintLog(&activity, "MiniSort sendToSecondaryNode(%d) send %u",(int)node,mbout.length());
#endif
            clusterComm.sendRecv(mbout,node,mpTag);
#ifdef  _FULL_TRACE
            ActPrintLog(&activity, "MiniSort sendToSecondaryNode(%d) got %u",(int)node,mbout.length());
#endif
            byte fn;
            mbout.read(fn);
            if (fn!=254)
                throw MakeActivityException(&activity, -1, "MiniSort sendToPrimaryNode: Protocol error(2) %d",(int)fn);
        } while (done!=0);
    }
    void appendFromPrimaryNode(IRowWriter &writer)
    {
#ifdef  _FULL_TRACE
        ActPrintLog(&activity, "MiniSort appending from primary node");
#endif
        CMessageBuffer mbin;
        loop
        {
            mbin.clear();
#ifdef  _FULL_TRACE
            ActPrintLog(&activity, "MiniSort appendFromPrimaryNode waiting");
#endif
            clusterComm.recv(mbin,1,mpTag);
#ifdef  _FULL_TRACE
            ActPrintLog(&activity, "MiniSort appendFromPrimaryNode continue %u bytes",mbin.length());
#endif
            CMessageBuffer mbout;
            mbout.init(mbin.getSender(),mpTag,mbin.getReplyTag());
            byte fn=254;
            mbout.append(fn);
#ifdef  _FULL_TRACE
            ActPrintLog(&activity, "MiniSort appendFromPrimaryNode reply %u bytes",mbout.length());
#endif
            clusterComm.reply(mbout);
#ifdef  _FULL_TRACE
            ActPrintLog(&activity, "MiniSort got from primary node %d",mbin.length());
#endif
            if (mbin.length()==0)
                break;
            deserialize(writer, mbin);
        }
    }
    void appendFromSecondaryNode(IRowWriter &writer, rank_t node, Semaphore &sem)
    {
#ifdef  _FULL_TRACE
        ActPrintLog(&activity, "MiniSort appending from node %d",(int)node);
#endif
        CMessageBuffer mbin;
        bool first = true;
        loop
        {
            mbin.clear();
            byte fn = 255;
            mbin.append(fn);
            clusterComm.sendRecv(mbin,node,mpTag);
#ifdef  _FULL_TRACE
            ActPrintLog(&activity, "MiniSort got %u from node %d",mbin.length(),(int)node);
#endif
            if (first)
            {
                sem.wait();
                first = false;
            }
            if (mbin.length()==0) // nb don't exit before wait!
                break;
            deserialize(writer, mbin);
        }
    }

public:
    CMiniSort(CActivityBase &_activity, IRowInterfaces &_rowIf, ICommunicator &_clusterComm, unsigned _partNo, unsigned _numNodes, mptag_t _mpTag)
        : activity(_activity), rowIf(_rowIf), clusterComm(_clusterComm), partNo(_partNo), numNodes(_numNodes), mpTag(_mpTag)
    {
        collector.setown(createThorRowCollector(activity, &rowIf));
        serializer = rowIf.queryRowSerializer();
        deserializer = rowIf.queryRowDeserializer();
        allocator = rowIf.queryRowAllocator();
    }
    IRowStream *sort(CThorExpandingRowArray &localRows, rowcount_t globalTotal, ICompare &iCompare, bool isStable, rowcount_t &rowCount)
    {
        PrintLog("Minisort started: %s, totalrows=%" RIPF "d", partNo?"seconday node":"primary node", localRows.ordinality());
        size32_t blksize = 0x100000;

        // JCSMORE - at the moment, the localsort set is already sorted
        if (1 == activity.queryJob().querySlaves())
        {
            CThorSpillableRowArray spillableRows(activity, &rowIf);
            rowCount = localRows.ordinality();
            spillableRows.transferFrom(localRows);
            return spillableRows.createRowStream();
        }
        if (partNo)
        {
            CThorSpillableRowArray spillableRows(activity, &rowIf);
            spillableRows.transferFrom(localRows);
            Owned<IRowStream> spillableStream = spillableRows.createRowStream();

            CMessageBuffer mb;
            loop
            {
                unsigned done = serialize(*spillableStream, mb.clear(), blksize);
#ifdef  _FULL_TRACE
                ActPrintLog(&activity, "MiniSort serialized %u rows, %u bytes",done,mb.length());
#endif
                sendToPrimaryNode(mb);
                if (!done)
                    break;
            }
            Owned<IRowWriter> writer = collector->getWriter();
            appendFromPrimaryNode(*writer); // will be sorted, may spill
            writer.clear();
            rowCount = collector->numRows();
            return collector->getStream();
        }
        else
        {
            collector->setup(&iCompare, isStable ? stableSort_earlyAlloc : stableSort_none, rc_allMem, SPILL_PRIORITY_DISABLE); // must not spill
            collector->transferRowsIn(localRows);
            collector->resize((rowidx_t)globalTotal); // pre-expand row array for efficiency

            // JCSMORE - very odd, threaded, but semaphores ensuring sequential writes, why?
            class casyncfor: public CAsyncFor
            {
                CMiniSort &base;
                Semaphore *nextsem;
            public:
                casyncfor(CMiniSort &_base, unsigned numNodes)
                    : base(_base)
                {
                    nextsem = new Semaphore[numNodes];  // 1 extra
                    nextsem[0].signal();
                }
                ~casyncfor()
                {
                    delete [] nextsem;
                }
                void Do(unsigned i)
                {
                    Owned<IRowWriter> writer = base.collector->getWriter();
                    try
                    {
                        base.appendFromSecondaryNode(*writer, (rank_t)(i+2),nextsem[i]); // +2 as master is 0 self is 1
                    }
                    catch (IException *)
                    {
                        // must ensure signal to avoid other threads in this asyncfor deadlocking
                        nextsem[i+1].signal();
                        throw;
                    }
                    nextsem[i+1].signal();
                }
            } afor1(*this, numNodes);
            afor1.For(numNodes-1, MINISORT_PARALLEL_TRANSFER, true);

            // JCSMORE - the blocks sent from other nodes were already sorted..
            // appended to local sorted chunk, and this is resorting the whole lot..
            // It's sorting on remote side, before it knows how big..
            // But shouldn't it merge sort here instead?

            CThorExpandingRowArray globalRows(activity, &rowIf);
            collector->transferRowsOut(globalRows); // will sort in process

#ifdef  _FULL_TRACE
            ActPrintLog(&activity, "MiniSort got %d rows %" I64F "d bytes", globalRows.ordinality(),(unsigned __int64)(globalRows.getMemUsage()));
#endif
            UnsignedArray points;
            globalRows.partition(iCompare, numNodes, points);
#ifdef  _FULL_TRACE
            for (unsigned pi=0;pi<points.ordinality();pi++)
                ActPrintLog(&activity, "points[%d] = %u",pi, points.item(pi));
#endif
            class casyncfor2: public CAsyncFor
            {
                CMiniSort &base;
                CThorExpandingRowArray &globalRows;
                unsigned *points;
                size32_t blksize;
            public:
                casyncfor2(CMiniSort &_base, CThorExpandingRowArray &_globalRows, unsigned *_points, size32_t _blksize)
                    : base(_base), globalRows(_globalRows)
                {
                    points = _points;
                    blksize = _blksize;
                }
                void Do(unsigned i)
                {
                    base.sendToSecondaryNode(globalRows, (rank_t)(i+2), points[i+1], points[i+2], blksize); // +2 as master is 0 self is 1
                }
            } afor2(*this, globalRows,  points.getArray(), blksize);
            afor2.For(numNodes-1, MINISORT_PARALLEL_TRANSFER);
            // get rid of rows sent
            globalRows.removeRows(points.item(1),globalRows.ordinality()-points.item(1));
            // only local rows left
            rowCount = globalRows.ordinality();
            CThorSpillableRowArray spillableRows(activity, &rowIf);
            spillableRows.transferFrom(globalRows);
            return spillableRows.createRowStream();
        }
    }
};

class CThorSorter : public CSimpleInterface, implements IThorSorter, implements ISortSlaveBase, implements ISortSlaveMP,
    private IThreaded
{
    CActivityBase *activity;
    SocketEndpoint myendpoint;
    IDiskUsage *iDiskUsage;
    Linked<ICommunicator> clusterComm;
    mptag_t mpTagRPC;

    CThorExpandingRowArray rowArray;
    CThreaded threaded;

    Owned<CWriteIntercept> intercept;
    Owned<IRowStream> merger;
    Owned<IMergeTransferServer> transferserver;
    Owned<IException> exc, closeexc;
    OwnedConstThorRow partitionrow;

    Linked<IRowInterfaces> rowif, auxrowif;

    unsigned multibinchopnum;
    unsigned overflowinterval; // aka overflowscale
    unsigned partno, numnodes; // JCSMORE - shouldn't be necessary
    rowcount_t totalrows, grandtotal;
    offset_t grandtotalsize;
    rowcount_t *overflowmap, *multibinchoppos;
    bool stopping, gatherdone, nosort, isstable;
    ICompare *rowCompare, *keyRowCompare;
    ICompare *primarySecondaryCompare; // used for co-sort
    ICompare *primarySecondaryUpperCompare; // used in between join
    ISortKeySerializer *keyserializer;      // used on partition calculation
    Owned<IRowInterfaces> keyIf;
    Owned<IOutputRowSerializer> rowToKeySerializer;
    void *midkeybuf;
    Semaphore startgathersem, finishedmergesem, closedownsem;
    InterruptableSemaphore startmergesem;
    size32_t transferblocksize, midkeybufsize;

    class CRowToKeySerializer : public CSimpleInterfaceOf<IOutputRowSerializer>
    {
        ISortKeySerializer *keyConverter;
        IRowInterfaces *rowIf, *keyIf;
    public:
        CRowToKeySerializer(IRowInterfaces *_rowIf, IRowInterfaces *_keyIf, ISortKeySerializer *_keyConverter)
            : rowIf(_rowIf), keyIf(_keyIf), keyConverter(_keyConverter)
        {
        }
        // IOutputRowSerializer impl.
        virtual void serialize(IRowSerializerTarget & out, const byte *row)
        {
            CSizingSerializer ssz;
            rowIf->queryRowSerializer()->serialize(ssz, (const byte *)row);
            size32_t recSz = ssz.size();
            RtlDynamicRowBuilder k(keyIf->queryRowAllocator());
            size32_t keySz = keyConverter->recordToKey(k, row, recSz);
            OwnedConstThorRow keyRow =  k.finalizeRowClear(keySz);
            keyIf->queryRowSerializer()->serialize(out, (const byte *)keyRow.get());
        }
    };
    void main()
    {
        try
        {
            ActPrintLog(activity, "Creating SortSlaveServer on tag %d MP",mpTagRPC);    
            while(SortSlaveMP::marshall(*this,clusterComm,mpTagRPC)&&!stopping)
               ;
            stopping = true;
            ActPrintLog(activity, "Exiting SortSlaveServer on tag %d",mpTagRPC);    
        }
        catch (IJSOCK_Exception *e)
        {
            if (e->errorCode()!=JSOCKERR_cancel_accept)
            {
                PrintExceptionLog(e,"**Exception(9)");
                if (exc.get())
                    e->Release();
                else
                    exc.setown(e);
                e->Release();
            }
        }
        catch (IException *e)
        {
            PrintExceptionLog(e,"**Exception(10)");
            if (exc.get())
                e->Release();
            else
                exc.setown(e);
        }
        stopping = true;
    }
#ifdef _TRACE
    void TraceRow(const char *s, const void *k)
    {
        traceKey(rowif->queryRowSerializer(), s, k);
    }
    void TraceKey(const char *s, const void *k)
    {
        traceKey(keyIf->queryRowSerializer(), s, k);
    }
#else
#define TraceRow(msg, row)
#define TraceKey(msg, key)
#endif

    void stop()
    {
        if (!stopping) {
            stopping = true;
            ActPrintLog(activity,"stopmarshall");
            SortSlaveMP::stopmarshall(clusterComm,mpTagRPC);
            ActPrintLog(activity,"stopmarshalldone");
        }
        if (exc)
            throw exc.getClear();
    }
    ICompare *queryCmpFn(byte cmpfn)
    {
        switch (cmpfn) {
        case CMPFN_NORMAL: return keyRowCompare;
        case CMPFN_COLLATE: return primarySecondaryCompare;
        case CMPFN_UPPER: return primarySecondaryUpperCompare;
        }
        return NULL;
    }
    rowidx_t BinChop(const void *key, bool lesseq, bool firstdup, byte cmpfn)
    {
        rowidx_t n = rowArray.ordinality();
        rowidx_t l=0;
        rowidx_t r=n;
        ICompare* icmp=queryCmpFn(cmpfn);
        while (l<r)
        {
            rowidx_t m = (l+r)/2;
            int cmp = icmp->docompare(key, rowArray.query(m));
            if (cmp < 0)
                r = m;
            else if (cmp > 0)
                l = m+1;
            else
            {
                if (firstdup)
                {
                    while ((m>0)&&(icmp->docompare(key, rowArray.query(m-1))==0))
                        m--;
                }
                else
                {
                    while ((m+1<n)&&(icmp->docompare(key, rowArray.query(m+1))==0))
                        m++;
                }
                return m;
            }
        }
        if (lesseq)
            return l-1;
        return l;
    }
    void doBinChop(CThorExpandingRowArray &keys, rowcount_t * pos, unsigned num, byte cmpfn)
    {
        MemoryBuffer tmp;
        for (unsigned n=0;n<num;n++)
        {
            unsigned i = n;
            loop                                      // adjustment for empty keys
            {
                if (i>=keys.ordinality())
                {
                    pos[n] = rowArray.ordinality();
                    break;
                }
                const void *key = keys.query(i);
                if (key)
                {
                    pos[n] = BinChop(key, false, true, cmpfn);
                    break;
                }
                i++;
            }
        }
    }
    void AdjustOverflow(rowcount_t &apos, const void *key, byte cmpfn)
    {
#ifdef TRACE_PARTITION_OVERFLOW
        ActPrintLog(activity, "AdjustOverflow: in (%" RCPF "d)",apos);
        TraceKey(" ",(byte *)key);
#endif
        rowcount_t pos = (apos+1)*(rowcount_t)overflowinterval;
        if (pos>grandtotal)
            pos = grandtotal;
        assertex(intercept);
        MemoryBuffer bufma;
        while (pos>0)
        {
            OwnedConstThorRow row = intercept->getRow(pos-1);
#ifdef TRACE_PARTITION_OVERFLOW
            ActPrintLog(activity, "Compare to (%" RCPF "d)",pos-1);
            TraceKey(" ",(const byte *)row.get());
#endif
            if (queryCmpFn(cmpfn)->docompare(key, row)>0)
                break;
            pos--;
        }
        intercept->closeFiles();
        apos = pos;
#ifdef TRACE_PARTITION_OVERFLOW
        ActPrintLog(activity, "AdjustOverflow: out (%" RCPF "d)",apos);
#endif
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorSorter(CActivityBase *_activity, SocketEndpoint &ep, IDiskUsage *_iDiskUsage, ICommunicator *_clusterComm, mptag_t _mpTagRPC)
        : activity(_activity), myendpoint(ep), iDiskUsage(_iDiskUsage), clusterComm(_clusterComm), mpTagRPC(_mpTagRPC),
          rowArray(*_activity, _activity), threaded("CThorSorter", this)
    {
        numnodes = 0;
        partno = 0;
        rowCompare = keyRowCompare = NULL;
        nosort = false;
        primarySecondaryCompare = NULL;
        primarySecondaryUpperCompare = NULL;
        midkeybuf = NULL;
        multibinchoppos = NULL;
        transferblocksize = TRANSFERBLOCKSIZE;
        isstable = true;
        stopping = false;
        threaded.start();
    }
    ~CThorSorter()
    {
        stop();
        ActPrintLog(activity, "Joining Sort Slave Server");
        verifyex(threaded.join(10*60*1000));
        myendpoint.set(NULL,0);
        rowArray.kill();
        ActPrintLog(activity, "~CThorSorter");
    }
// ISortSlaveMP
    virtual bool Connect(unsigned _partno, unsigned _numnodes)
    {
        ActPrintLog(activity, "Connected to slave %d of %d",_partno,_numnodes);
        numnodes = _numnodes;
        partno = _partno;
        transferserver.clear();
        transferserver.setown(createMergeTransferServer(this));
        transferserver->start();
        return true; // used to establish link
    }
    virtual void StartGather()
    {
        ActPrintLog(activity, "Start Gather");
        gatherdone = false;
        startgathersem.signal();
    }
    virtual void GetGatherInfo(rowcount_t &numlocal, offset_t &totalsize, unsigned &_overflowscale, bool haskeyserializer)
    {
        if (!gatherdone)
            ERRLOG("GetGatherInfo:***Error called before gather complete");
        if (haskeyserializer != (NULL != keyserializer))
            throwUnexpected();
        numlocal = rowArray.ordinality(); // JCSMORE - this is sample total, why not return actual spill total?
        _overflowscale = overflowinterval;
        totalsize = grandtotalsize; // used by master, if nothing overflowed to see if can MiniSort
    }
    virtual rowcount_t GetMinMax(size32_t &keybufsize,void *&keybuf,size32_t &avrecsize)
    {
        avrecsize = 0;
        if (0 == rowArray.ordinality())
        {
            keybufsize = 0;
            keybuf = NULL;
            return 0;
        }

        MemoryBuffer mb;
        CMemoryRowSerializer msz(mb);

        const void *kp = rowArray.get(0);
        TraceRow("Min =", kp);
        rowToKeySerializer->serialize(msz, (const byte *)kp);

        kp = rowArray.get(rowArray.ordinality()-1);
        TraceRow("Max =", kp);
        rowToKeySerializer->serialize(msz, (const byte *)kp);

        avrecsize = (size32_t)(grandtotalsize/grandtotal);
#ifdef _TRACE
        ActPrintLog(activity, "Ave Rec Size = %u", avrecsize);
#endif
        keybufsize = mb.length();
        keybuf = mb.detach();
        return 2;
    }
    virtual void GetMultiMidPoint(size32_t lbufsize,const void *lkeybuf,
                          size32_t hbufsize,const void *hkeybuf,
                          size32_t &mbufsize,void *&mkeybuf)
    {
        // finds the keys within the ranges specified
        // uses empty keys (0 size) if none found
        CThorExpandingRowArray low(*activity, keyIf, true);
        CThorExpandingRowArray high(*activity, keyIf, true);
        CThorExpandingRowArray mid(*activity, keyIf, true);
        low.deserializeExpand(lbufsize, lkeybuf);
        high.deserializeExpand(hbufsize, hkeybuf);
        unsigned n=low.ordinality();
        assertex(n==high.ordinality());
        unsigned i;
        for (i=0;i<n;i++)
        {
            if (rowArray.ordinality()!=0)
            {
                unsigned p1 = BinChop(low.query(i), false, false, CMPFN_NORMAL);
                if (p1==(unsigned)-1)
                    p1 = 0;
                unsigned p2 = BinChop(high.query(i), true, true, CMPFN_NORMAL);
                if (p2>=rowArray.ordinality()) 
                    p2 = rowArray.ordinality()-1;
                if (p1<=p2)
                {
                    unsigned pm=(p1+p2+1)/2;
                    OwnedConstThorRow kp = rowArray.get(pm);
                    if ((keyRowCompare->docompare(low.query(i), kp)<=0)&&
                        (keyRowCompare->docompare(high.query(i), kp)>=0)) // paranoia
                    {
                        mid.append(kp.getClear());
                    }
                    else
                        mid.append(NULL);
                }
                else
                    mid.append(NULL);
            }
            else
                mid.append(NULL);
        }
        MemoryBuffer mb;
        CMemoryRowSerializer s(mb);
        for (rowidx_t i=0; i<mid.ordinality(); i++)
        {
            const void *row = mid.query(i);
            if (row)
            {
                mb.append(false);
                rowToKeySerializer->serialize(s, (const byte *)row);
            }
            else
                mb.append(true);
        }
        MemoryBuffer compressedMb;
        fastLZCompressToBuffer(compressedMb, mb.length(), mb.toByteArray());
        mbufsize = compressedMb.length();
        mkeybuf = compressedMb.detach();
    }
    virtual void GetMultiMidPointStart(size32_t lbufsize,const void *lkeybuf,
                               size32_t hbufsize,const void *hkeybuf)
    {
        assertex(midkeybuf==NULL); // just incase I ever allow re-entrancy
        GetMultiMidPoint(lbufsize,lkeybuf,hbufsize,hkeybuf,midkeybufsize,midkeybuf);
    }
    virtual void GetMultiMidPointStop(size32_t &mbufsize,void *&mkeybuf)
    {
        assertex(midkeybuf); // just incase I ever allow re-entrancy
        mkeybuf = midkeybuf;
        mbufsize = midkeybufsize;
        midkeybuf = NULL;
    }
    virtual void MultiBinChop(size32_t keybufsize, const byte * keybuf, unsigned num, rowcount_t * pos, byte cmpfn)
    {
        CThorExpandingRowArray keys(*activity, keyIf, true);
        keys.deserialize(keybufsize, keybuf);
        doBinChop(keys, pos, num, cmpfn);
    }
    virtual void MultiBinChopStart(size32_t keybufsize, const byte * keybuf, byte cmpfn)
    {
        CThorExpandingRowArray keys(*activity, keyIf, true);
        keys.deserializeExpand(keybufsize, keybuf);
        assertex(multibinchoppos==NULL); // check for reentrancy
        multibinchopnum = keys.ordinality();
        multibinchoppos = (rowcount_t *)malloc(sizeof(rowcount_t)*multibinchopnum);
        doBinChop(keys, multibinchoppos, multibinchopnum, cmpfn);
    }
    virtual void MultiBinChopStop(unsigned num, rowcount_t * pos)
    {
        assertex(multibinchoppos);
        assertex(multibinchopnum==num);
        memcpy(pos,multibinchoppos,num*sizeof(rowcount_t));
        free(multibinchoppos);
        multibinchoppos = NULL;
    }
    virtual void OverflowAdjustMapStart(unsigned mapsize, rowcount_t * map,
                               size32_t keybufsize, const byte * keybuf, byte cmpfn, bool useaux)
    {
        assertex(intercept);
        overflowmap = (rowcount_t *)malloc(mapsize*sizeof(rowcount_t));
        memcpy(overflowmap,map,mapsize*sizeof(rowcount_t));
        unsigned i;
#ifdef TRACE_PARTITION_OVERFLOW

        ActPrintLog(activity, "OverflowAdjustMap: interval = %d",overflowinterval);
        ActPrintLog(activity, "In: ");
        for (i=0;i<mapsize;i++)
            ActPrintLog(activity, "%" RCPF "d ",overflowmap[i]);
#endif
        CThorExpandingRowArray keys(*activity, keyIf, true);
        keys.deserialize(keybufsize, keybuf);
        for (i=0;i<mapsize-1;i++)
            AdjustOverflow(overflowmap[i], keys.query(i), cmpfn);
        overflowmap[mapsize-1] = grandtotal;
#ifdef TRACE_PARTITION_OVERFLOW
        ActPrintLog(activity, "Out: ");
        for (i=0;i<mapsize;i++)
            ActPrintLog(activity, "%" RCPF "u ",overflowmap[i]);
#endif
    }
    virtual rowcount_t OverflowAdjustMapStop(unsigned mapsize, rowcount_t * map)
    {
        memcpy(map,overflowmap,mapsize*sizeof(rowcount_t));
        free(overflowmap);
        return grandtotal;
    }
    virtual void MultiMerge(unsigned mapsize,rowcount_t *map,
                    unsigned num,SocketEndpoint* endpoints)
    {
        MultiMergeBetween(mapsize,map,NULL,num,endpoints);
    }
    virtual void MultiMergeBetween(unsigned mapsize, rowcount_t * map, rowcount_t * mapupper, unsigned num, SocketEndpoint * endpoints)
    {
        assertex(transferserver.get()!=NULL);
        if (intercept)
            rowArray.kill(); // don't need samples any more. All merged from disk.
        transferserver->merge(mapsize,map,mapupper,num,endpoints,partno);
    }
    virtual void SingleMerge()
    {
        ActPrintLog(activity, "SingleMerge start");
        assertex(numnodes==1);
        totalrows = grandtotal;
        merger.setown(createMergeInputStream(0,totalrows));
        startmergesem.signal();
        traceWait("finishedmergesem(2)",finishedmergesem);
        ActPrintLog(activity, "Merged");
        merger.clear();
        intercept.clear();
    }
    virtual bool FirstRowOfFile(const char *filename,
                        size32_t &rowbufsize, byte * &rowbuf)
    {
        OwnedConstThorRow row;
        MemoryBuffer mb;
        CMemoryRowSerializer msz(mb);
        if (!*filename) // partition row wanted
        {
            if (partitionrow)
                row.set(partitionrow.getClear());
        }
        else
        {
            Owned<IFile> file = createIFile(filename);
            Owned<IExtRowStream> rowstream = createRowStream(file, auxrowif);
            row.setown(rowstream->nextRow());
        }
        if (!row)
        {
            rowbuf = NULL;
            rowbufsize = 0;
            return true;
        }
        rowToKeySerializer->serialize(msz, (const byte *)row.get());
        rowbufsize = mb.length();
        rowbuf = (byte *)mb.detach();
        return true;
    }
    virtual void GetMultiNthRow(unsigned numsplits,size32_t &outbufsize,void *&outkeybuf)
    {
        // actually doesn't get Nth row but numsplits samples distributed evenly through the rows
        assertex(numsplits);
        unsigned numrows = rowArray.ordinality();
        if (0 == numrows)
        {
            outbufsize = 0;
            outkeybuf = NULL;
            return;
        }
        MemoryBuffer mb;
        CMemoryRowSerializer msz(mb);
        for (unsigned i=0;i<numsplits;i++)
        {
            count_t pos = ((i*2+1)*(count_t)numrows)/(2*(count_t)numsplits);
            if (pos>=numrows)
                pos = numrows-1;
            const void *row = rowArray.query((unsigned)pos);
            rowToKeySerializer->serialize(msz, (const byte *)row);
        }
        MemoryBuffer exp;
        fastLZCompressToBuffer(mb, exp.length(), exp.toByteArray());
        outbufsize = exp.length();
        outkeybuf = exp.detach();
    }
    virtual void StartMiniSort(rowcount_t globalTotal)
    {
        // JCSMORE partno and numnodes should be implicit
        CMiniSort miniSort(*activity, *rowif, *clusterComm, partno, numnodes, mpTagRPC);

        Owned<IRowStream> sortedStream;
        try
        {
            sortedStream.setown(miniSort.sort(rowArray, globalTotal, *rowCompare, isstable, totalrows));
        }
        catch (IException *e)
        {
            startmergesem.interrupt(LINK(e));
            throw e;
        }
        merger.setown(sortedStream.getClear());
        startmergesem.signal();
        ActPrintLog(activity, "StartMiniSort output started");
        traceWait("finishedmergesem(2)",finishedmergesem);
        PrintLog("StartMiniSort output done");
        merger.clear();
        intercept.clear();
        PrintLog("StartMiniSort exit");
    }
    virtual void Close()
    {
        ActPrintLog(activity, "Close");
        try {
            if (transferserver)
                transferserver->subjoin(); // need to have finished merge threads 
        }
        catch (IException *e) {
            EXCLOG(e,"CThorSorter");
            if (closeexc.get())
                e->Release();
            else
                closeexc.setown(e);
        }
        intercept.clear();
    }
    virtual void CloseWait()
    {
        ActPrintLog(activity, "Close finished");
        if (closeexc.get())
            throw closeexc.getClear();
        rowArray.kill();
    }
    virtual void Disconnect()
    {
        ActPrintLog(activity, "Disconnecting from slave %d of %d",partno,numnodes);
        if (transferserver) {
            transferserver->stop();
            transferserver.clear();
        }
        ActPrintLog(activity, "Disconnected from slave %d of %d",partno,numnodes);
    }

// ISortSlaveBase
    virtual IRowStream *createMergeInputStream(rowcount_t sstart, rowcount_t snum)
    {
        if (intercept)
        {
            offset_t startofs;  
            size32_t rd = intercept->readOverflowPos(sstart, 1, &startofs, true);
            assertex(rd==sizeof(startofs));
            return intercept->getStream(startofs, snum);
        }
        else
        {
            unsigned _snum = (rowidx_t)snum; // only support 2^32 rows in memory
            assertex(snum==_snum);
            return rowArray.createRowStream((rowidx_t)sstart, _snum, false); // must be false as rows may overlap (between join)
        }
    }
    virtual size32_t getTransferBlockSize()
    {
        return transferblocksize;
    }
    virtual unsigned getTransferPort()
    {
        return myendpoint.port+SOCKETSERVERINC;
    }
    virtual void startMerging(IArrayOf<IRowStream> &readers, rowcount_t _totalrows)
    {
        totalrows = _totalrows;
        if (0 == readers.ordinality())
            merger.setown(createNullRowStream());
        else if (1 == readers.ordinality())
            merger.set(&readers.item(0));
        else
        {
            Owned<IRowLinkCounter> linkcounter = new CThorRowLinkCounter;
            merger.setown(createRowStreamMerger(readers.ordinality(), readers.getArray(), rowCompare, false, linkcounter));
        }
        ActPrintLog(activity, "Global Merger Created: %d streams", readers.ordinality());
        startmergesem.signal();
        traceWait("finishedmergesem",finishedmergesem);
        ActPrintLog(activity, "Global Merged completed");
        merger.clear();
        readers.kill(); // NB: need to be cleared before intercept, which clears up files
        intercept.clear();
        ActPrintLog(activity, "Global Merge exit");
    }

// IThorSorter
    virtual void Gather(
        IRowInterfaces *_rowif,
        IRowStream *in,
        ICompare *_rowCompare,
        ICompare *_primarySecondaryCompare,
        ICompare *_primarySecondaryUpperCompare,
        ISortKeySerializer *_keyserializer,
        const void *_partitionrow,
        bool _nosort,
        bool _unstable,
        bool &abort,
        IRowInterfaces *_auxrowif
        )
    {
        ActPrintLog(activity, "Gather in");
        loop {
            if (abort)
                return;
            if (startgathersem.wait(10000))
                break;
        }
        ActPrintLog(activity, "SORT: Gather");
        assertex(!rowif);
        rowif.set(_rowif);
        rowArray.kill();
        rowArray.setup(rowif);
        dbgassertex(transferserver);
        transferserver->setRowIF(rowif);
        if (_auxrowif&&_auxrowif->queryRowMetaData())
            auxrowif.set(_auxrowif);
        else
        {
            if (_partitionrow)
                ActPrintLog(activity, "SORT: partitionrow passed but no aux serializer");
            auxrowif.set(_rowif);
        }
        keyserializer = _keyserializer;
        rowCompare = _rowCompare;
        if (keyserializer)
        {
            keyIf.setown(createRowInterfaces(keyserializer->queryRecordSize(), activity->queryContainer().queryId(), activity->queryCodeContext()));
            rowToKeySerializer.setown(new CRowToKeySerializer(auxrowif, keyIf, keyserializer));
            keyRowCompare = keyserializer->queryCompareKeyRow();
        }
        else
        {
            ActPrintLog(activity, "No key serializer");
            keyIf.set(auxrowif);
            rowToKeySerializer.set(keyIf->queryRowSerializer());
            keyRowCompare = rowCompare;
        }
        nosort = _nosort;
        if (nosort)
            ActPrintLog(activity, "SORT: Gather not sorting");
        isstable = !_unstable;
        if (_unstable)
            ActPrintLog(activity, "SORT: UNSTABLE");

        if (_primarySecondaryCompare)
            primarySecondaryCompare = _primarySecondaryCompare;
        else
            primarySecondaryCompare = keyRowCompare;
        if (_primarySecondaryUpperCompare)
        {
            if (keyserializer)
                throwUnexpected();
            primarySecondaryUpperCompare = _primarySecondaryUpperCompare;
        }
        else
            primarySecondaryUpperCompare = primarySecondaryCompare;

        Owned<IThorRowLoader> sortedloader = createThorRowLoader(*activity, rowif, nosort?NULL:rowCompare, isstable ? stableSort_earlyAlloc : stableSort_none, rc_allDiskOrAllMem, SPILL_PRIORITY_SELFJOIN);
        Owned<IRowStream> overflowstream;
        memsize_t inMemUsage = 0;
        try
        {
            // if all in memory, rows will be transferred into rowArray when done
            overflowstream.setown(sortedloader->load(in, abort, false, &rowArray, &inMemUsage));
            ActPrintLog(activity, "Local run sort(s) done");
        }
        catch (IException *e)
        {
            PrintExceptionLog(e,"**Exception(2)");
            throw;
        }
        if (!abort)
        {
            transferblocksize = TRANSFERBLOCKSIZE;
            grandtotal = sortedloader->numRows();
            unsigned numOverflows = sortedloader->numOverflows();
            if (_partitionrow)
                partitionrow.set(_partitionrow);

            if (numOverflows) // need to write to file
            {
                assertex(!intercept);
                overflowinterval=sortedloader->overflowScale();
                intercept.setown(new CWriteIntercept(*activity, rowif, overflowinterval));
                grandtotalsize = intercept->write(overflowstream);
                intercept->transferRows(rowArray); // get sample rows
            }
            else // all in memory
            {
                // unspillable at this point, whilst partitioning (or determining if minisort)
                overflowinterval = 1;
                grandtotalsize = inMemUsage;
            }
            ActPrintLog(activity, "Sort done: rows sorted = %" RCPF "d, bytes sorted = %" I64F "d, overflowed to disk %d time%c", grandtotal, grandtotalsize, numOverflows, (numOverflows==1)?' ':'s');
        }
        ActPrintLog(activity, "Gather finished %s",abort?"ABORTED":"");
        gatherdone = true;
    }
    virtual IRowStream * startMerge(rowcount_t &_totalrows)
    {
        ActPrintLog(activity, "SORT Merge Waiting");
        traceWait("startmergesem",startmergesem);
        ActPrintLog(activity, "SORT Merge Start");
        _totalrows = totalrows;
        return merger.getLink();
    }
    virtual void stopMerge()
    {
        ActPrintLog(activity, "Local merge finishing");
        finishedmergesem.signal();
        ActPrintLog(activity, "Local merge finished");
        rowif.clear();
    }
};




//==============================================================================


IThorSorter *CreateThorSorter(CActivityBase *activity, SocketEndpoint &ep,IDiskUsage *iDiskUsage,ICommunicator *clusterComm, mptag_t _mpTagRPC)
{
    return new CThorSorter(activity, ep, iDiskUsage, clusterComm, _mpTagRPC);
}

