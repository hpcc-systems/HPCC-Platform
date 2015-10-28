/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include "jfile.hpp"
#include "thormisc.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "tsorta.hpp"

#include "thorstep.ipp"

#include "thmergeslave.ipp"

#ifdef _DEBUG
//#define _FULL_TRACE
#endif

#define _STABLE_MERGE

class GlobalMergeSlaveActivity : public CSlaveActivity, public CThorDataLink
{
public:
    IArrayOf<IRowStream> streams; 
    IHThorMergeArg *helper;
    Owned<IFile> tmpfile;
    Owned<IRowStream> out;
    mptag_t masterMpTag;
    mptag_t intertag;
    offset_t *partitionpos;
    size32_t chunkmaxsize;
    unsigned width;

    class cRemoteStream : public CSimpleInterface, implements IRowStream
    {
        GlobalMergeSlaveActivity *parent;
        Linked<IOutputRowDeserializer> deserializer;
        ThorRowQueue rows;
        rank_t rank;
        mptag_t tag;
        bool eos;
        Linked<IEngineRowAllocator> allocator;
        offset_t bufpos;
        size32_t bufsize;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        cRemoteStream(IRowInterfaces *rowif, unsigned i,mptag_t _tag, GlobalMergeSlaveActivity *_parent)
            : allocator(rowif->queryRowAllocator()), deserializer(rowif->queryRowDeserializer())
        {
            rank = (rank_t)(i+1);
            tag = _tag;
            parent = _parent;
            eos = false;
            bufpos = 0;
            bufsize = 0;
        }

        ~cRemoteStream()
        {
            while (rows.ordinality()) 
                ReleaseThorRow(rows.dequeue());
        }

        void load()
        {
            bufpos += bufsize;
            if (rank==parent->queryJobChannel().queryMyRank()) {
#ifdef _FULL_TRACE
                ::ActPrintLog(parent, "Merge cRemoteStream::load, get chunk from node %d (local) pos = %" I64F "d",rank,bufpos);
#endif
                bufsize = parent->getRows(rank-1,bufpos,rows);
            }
            else {
#ifdef _FULL_TRACE
                ::ActPrintLog(parent, "Merge cRemoteStream::load, get chunk from node %d tag %d (remote) pos = %" I64F "d",rank,tag,bufpos);
#endif
                CMessageBuffer mb;
                mb.append(bufpos);
                parent->queryContainer().queryJobChannel().queryJobComm().sendRecv(mb, rank, tag);
                bufsize = mb.length();
                CThorStreamDeserializerSource dsz(bufsize,mb.bufferBase());
                while (!dsz.eos()) {
                    RtlDynamicRowBuilder rowBuilder(allocator);
                    size32_t sz=deserializer->deserialize(rowBuilder,dsz);
                    rows.enqueue(rowBuilder.finalizeRowClear(sz));
                }
            }
#ifdef _FULL_TRACE
            ::ActPrintLog(parent, "Merge cRemoteStream::load, got chunk %d",bufsize);
#endif
        }

        const void *nextRow()
        {
            if (!eos) {
                if (rows.ordinality()==0) 
                    load();
                if (rows.ordinality()) 
                    return rows.dequeue();
                eos = true;
            }
            return NULL;
        }
        void stop()
        {
            // no action
        }
    };

    class cProvider: public Thread
    {
        mptag_t tag;
        GlobalMergeSlaveActivity *parent;
        bool stopped;
        Linked<IOutputRowSerializer> serializer;
    public:
        void init(GlobalMergeSlaveActivity *_parent, IOutputRowSerializer *_serializer,mptag_t _tag)
        {
            serializer.set(_serializer);
            parent = _parent;
            stopped = false;
            tag = _tag;
        }
        int run()
        {
            while (!stopped) {
#ifdef _FULL_TRACE
                ::ActPrintLog(parent, "Merge cProvider Receiving request from tag %d",tag);
#endif
                CMessageBuffer mb;
                rank_t sender;
                if (parent->queryJobChannel().queryJobComm().recv(mb, RANK_ALL, tag, &sender)&&!stopped)  {
                    offset_t pos;
                    mb.read(pos);
#ifdef _FULL_TRACE
                    ::ActPrintLog(parent, "Merge cProvider Received request from %d pos = %" I64F "d",sender,pos);
#endif
                    ThorRowQueue rows;
                    size32_t sz = parent->getRows(sender-1, pos, rows);
                    mb.clear().ensureCapacity(sz);
                    CMemoryRowSerializer msz(mb);
                    loop {                                          // there must be a better way than deserializing then serializing!!
                        OwnedConstThorRow row = rows.dequeue();
                        if (!row)
                            break;
                        serializer->serialize(msz,(const byte *)row.get());
                    }
                    if (sz!=mb.length()) {
                        static bool logged=false;
                        if (!logged) {
                            logged = true;
                            WARNLOG("GlobalMergeSlaveActivity mismatch serialize, deserialize (%u,%u)",sz,mb.length());
                        }
                    }
#ifdef _FULL_TRACE
                    ::ActPrintLog(parent, "Merge cProvider replying size %d",mb.length());
#endif
                    if (!stopped)
                        parent->queryJobChannel().queryJobComm().reply(mb);
                }
            }
#ifdef _FULL_TRACE
            ::ActPrintLog(parent, "Merge cProvider exiting",tag);
#endif
            return 0;
        }
        void stop() 
        {
            if (!stopped) {
                stopped = true;
                parent->queryJobChannel().queryJobComm().cancel(RANK_ALL, tag);
                join();
            }
        }

    } provider;

    Owned<CThorRowLinkCounter> linkcounter;

    IRowStream * createPartitionMerger(CThorKeyArray &sample)
    {
        queryJobChannel().queryJobComm().verifyAll();
        CMessageBuffer mb;
        mptag_t replytag = queryMPServer().createReplyTag();
        mptag_t intertag = queryMPServer().createReplyTag();
        serializeMPtag(mb,replytag);
        serializeMPtag(mb,intertag);
        sample.serialize(mb);
        ActPrintLog("MERGE sending samples to master");
        if (!queryJobChannel().queryJobComm().send(mb, (rank_t)0, masterMpTag))
            return NULL;
        ActPrintLog("MERGE receiving partition from master");
        rank_t sender;
        if (!queryJobChannel().queryJobComm().recv(mb, 0, replytag, &sender)) 
            return NULL;
        assertex((unsigned)sender==0);
        ActPrintLog("MERGE received partition from master");
        mb.read(width);
        mptag_t *intertags = new mptag_t[width];
        mb.read(sizeof(mptag_t)*width,intertags);

        CThorKeyArray partition(*this, queryRowInterfaces(this),helper->querySerialize(),helper->queryCompare(),helper->queryCompareKey(),helper->queryCompareRowKey());
        partition.deserialize(mb,false);
        partition.calcPositions(tmpfile,sample);
        partitionpos = new offset_t[width];
        unsigned i;
        for (i=0;i<width;i++) {
            streams.append(*new cRemoteStream(queryRowInterfaces(this),i,intertags[i],this));
            partitionpos[i] = partition.getFilePos(i);
#ifdef _FULL_TRACE
            if (i<width-1) {
                partition.traceKey("MERGE partition key",i);
            }
#endif

            ActPrintLog("Merge: partitionpos[%d] = %" I64F "d",i,partitionpos[i]);
        }
        delete [] intertags;
        provider.init(this,queryRowSerializer(),intertag);
        provider.start();
        if (!streams.ordinality())
            return NULL;
        if (streams.ordinality()==1)
            return &streams.popGet();
        return createRowStreamMerger(streams.ordinality(), streams.getArray(), helper->queryCompare(), helper->dedup(), linkcounter);
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    GlobalMergeSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        partitionpos = NULL;
        linkcounter.setown(new CThorRowLinkCounter);
    }

    ~GlobalMergeSlaveActivity()
    {
        try {
            if (tmpfile) 
                tmpfile->remove();
        }
        catch (IException *e) {
            ActPrintLog(e,"~GlobalMergeSlaveActivity");
            e->Release();
        }
        if (partitionpos)
            delete [] partitionpos;
    }

// IThorSlaveActivity overloaded methods
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorMergeArg *)queryHelper();
        appendOutputLinked(this);
        masterMpTag = container.queryJobChannel().deserializeMPTag(data);
    }

    void abort()
    {
        ActPrintLog("abort");
        CSlaveActivity::abort();
        provider.stop();
    }


// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        ForEachItemIn(i, inputs) {
            IThorDataLink * input = inputs.item(i);
            try {
                startInput(input); 
            }
            catch (CATCHALL) {
                ActPrintLog("MERGE(%" ACTPF "d): Error starting input %d", container.queryId(), i);
                ForEachItemIn(s, streams)
                    streams.item(s).stop();
                throw;
            }
            if (input->isGrouped())
                streams.append(*createUngroupStream(input));
            else
                streams.append(*LINK(input));
        }
#ifndef _STABLE_MERGE
        // shuffle streams otherwise will all be reading in order initially
        unsigned n=streams.ordinality();
        while (n>1) {
            unsigned i = getRandom()%n;
            n--;
            if (i!=n) 
                streams.swap(i,n);
        }
#endif
        if (partitionpos)
        {
            delete [] partitionpos;
            partitionpos = NULL;
        }
        chunkmaxsize = MERGE_TRANSFER_BUFFER_SIZE;
        Owned<IRowStream> merged = createRowStreamMerger(streams.ordinality(), streams.getArray(), helper->queryCompare(),helper->dedup(), linkcounter);
        StringBuffer tmpname;
        GetTempName(tmpname,"merge",true); // use alt temp dir
        tmpfile.setown(createIFile(tmpname.str()));
        Owned<IRowWriter> writer =  createRowWriter(tmpfile, this);
        CThorKeyArray sample(*this, this, helper->querySerialize(), helper->queryCompare(), helper->queryCompareKey(), helper->queryCompareRowKey());
        sample.setSampling(MERGE_TRANSFER_BUFFER_SIZE);
        ActPrintLog("MERGE: start gather");
        loop {
            OwnedConstThorRow row = merged->nextRow();
            if (!row)
                break;
            sample.add(row);
            writer->putRow(row.getClear());
        }
        merged->stop();
        merged.clear();
        streams.kill();
        ActPrintLog("MERGE: gather done");
        writer->flush();
        writer.clear();
        out.setown(createPartitionMerger(sample));

        dataLinkStart();
    }

    virtual void stop()
    {
        if (out)
            out->stop();
        dataLinkStop();
    }

    void kill()
    {
        provider.stop();
        streams.kill();
        CSlaveActivity::kill();
    }

    size32_t getRows(unsigned idx, offset_t pos, ThorRowQueue &out)
    {  // always returns whole rows

        offset_t start = idx?partitionpos[idx-1]:0;
        pos += start;
        offset_t end = partitionpos[idx];
        if (pos>=end)
            return 0;
        Owned<IExtRowStream> rs = createRowStreamEx(tmpfile, queryRowInterfaces(this), pos, end); // this is not good
        offset_t so = rs->getOffset();
        size32_t len = 0;
        size32_t chunksize = chunkmaxsize;
        if (pos+chunksize>end) 
            chunksize = (size32_t)(end-pos);
        do {
            OwnedConstThorRow r = rs->nextRow();
            size32_t l = (size32_t)(rs->getOffset()-so);
            if (!r)
                break;
            if (pos+l>end) {
                ActPrintLogEx(&queryContainer(), thorlog_null, MCwarning, "overrun in GlobalMergeSlaveActivity::getRows(%u,%" I64F "d,%" I64F "d)",l,rs->getOffset(),end);
                break; // don't think should happen
            }
            len = l;
            out.enqueue(r.getClear());
        } while (len<chunksize);
        return len;
    }

    const void * nextRow() 
    {
        if (!abortSoon) {
            if (out) {
                OwnedConstThorRow row = out->nextRow();
                if (row) {
                    dataLinkIncrement();
                    return row.getClear();
                }
            }
        }
        return NULL;
    }

    virtual bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info,inputs.getArray(),inputs.ordinality());
    }
};



class LocalMergeSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IArrayOf<IRowStream> streams; 
    Owned<IRowStream> out;
    IHThorMergeArg *helper;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    LocalMergeSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }

// IThorSlaveActivity overloaded methods
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorMergeArg *)queryHelper();
        appendOutputLinked(this);
    }

    void abort()
    {
        ActPrintLog("abort");
        CSlaveActivity::abort();
    }


// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        ForEachItemIn(i, inputs) {
            IThorDataLink * input = inputs.item(i);
            try { 
                startInput(input); 
            }
            catch (CATCHALL) {
                ActPrintLog("MERGE(%" ACTPF "d): Error starting input %d", container.queryId(), i);
                ForEachItemIn(s, streams)
                    streams.item(s).stop();
                throw;
            }
            if (input->isGrouped())
                streams.append(*createUngroupStream(input));
            else
                streams.append(*LINK(input));
        }
        Owned<IRowLinkCounter> linkcounter = new CThorRowLinkCounter;
        out.setown(createRowStreamMerger(streams.ordinality(), streams.getArray(), helper->queryCompare(), helper->dedup(), linkcounter));

        dataLinkStart();
    }

    virtual void stop()
    {
        out->stop();
        dataLinkStop();
    }

    void kill()
    {
        streams.kill();
        CSlaveActivity::kill();
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!abortSoon) {
            OwnedConstThorRow row = out->nextRow();
            if (row) {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;
    }

    virtual bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info,inputs.getArray(),inputs.ordinality());
    }
};


class CThorStreamMerger : public CStreamMerger
{
    IThorDataLink **inputArray;
public:
    CThorStreamMerger() : CStreamMerger(true) {}

    void initInputs(unsigned _numInputs, IThorDataLink ** _inputArray)
    {
        CStreamMerger::initInputs(_numInputs);
        inputArray = _inputArray;
    }
    virtual bool pullInput(unsigned i, const void *seek, unsigned numFields, const SmartStepExtra *stepExtra)
    {
        const void *next;
        bool matches = true;
        if (seek)
            next = inputArray[i]->nextRowGE(seek, numFields, matches, *stepExtra);
        else
            next = inputArray[i]->ungroupedNextRow();
        pending[i] = (void *)next;
        pendingMatches[i] = matches;
        return (next != NULL);
    }
    virtual void releaseRow(const void *row)
    {
        ReleaseThorRow(row);
    }
};


class CNWayMergeActivity : public CThorNarySlaveActivity, public CThorDataLink, public CThorSteppable
{
    IHThorNWayMergeArg *helper;
    CThorStreamMerger merger;
    CSteppingMeta meta;
    bool initializedMeta;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CNWayMergeActivity(CGraphElementBase *container) : CThorNarySlaveActivity(container), CThorDataLink(this), CThorSteppable(this)
    {
        helper = (IHThorNWayMergeArg *)queryHelper();
        merger.init(helper->queryCompare(), helper->dedup(), helper->querySteppingMeta()->queryCompare());
        initializedMeta = false;
    }
    ~CNWayMergeActivity()
    {
        merger.cleanup();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    void start()
    {
        CThorNarySlaveActivity::start();
        merger.initInputs(expandedInputs.length(), expandedInputs.getArray());
        dataLinkStart();
    }
    void stop()
    {
        merger.done();
        CThorNarySlaveActivity::stop();
        dataLinkStop();
    }
    void reset()
    {
        CThorNarySlaveActivity::reset();
        initializedMeta = false;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        OwnedConstThorRow ret = merger.nextRow();
        if (ret)
        {
            dataLinkIncrement();
            return ret.getClear();
        }
        return NULL;
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities);
        OwnedConstThorRow ret = merger.nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (ret)
        {
            dataLinkIncrement();
            return ret.getClear();
        }
        return NULL;
    }
    virtual bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info,inputs.getArray(),inputs.ordinality());
    }
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx)
    {
        CThorNarySlaveActivity::setInput(index, inputActivity, inputOutIdx);
        CThorSteppable::setInput(index, inputActivity, inputOutIdx);
    }
    virtual IInputSteppingMeta *querySteppingMeta()
    {
        if (expandedInputs.ordinality() == 0)
            return NULL;
        if (!initializedMeta)
        {
            meta.init(helper->querySteppingMeta(), false);
            ForEachItemIn(i, expandedInputs)
            {
                if (meta.getNumFields() == 0)
                    break;
                IInputSteppingMeta *inputMeta = expandedInputs.item(i)->querySteppingMeta();
                meta.intersect(inputMeta);
            }
            initializedMeta = true;
        }
        if (meta.getNumFields() == 0)
            return NULL;
        return &meta;
    }
};


CActivityBase *createNWayMergeActivity(CGraphElementBase *container)
{
    return new CNWayMergeActivity(container);
}


CActivityBase *createLocalMergeSlave(CGraphElementBase *container)
{
    return new LocalMergeSlaveActivity(container);
}

CActivityBase *createGlobalMergeSlave(CGraphElementBase *container)
{
    return new GlobalMergeSlaveActivity(container);
}

