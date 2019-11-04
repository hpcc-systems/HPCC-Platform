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


#include "thprojectslave.ipp"
#include "eclrtl_imp.hpp"


class CProjecStrandProcessor : public CThorStrandProcessor
{
    IHThorProjectArg *helper;
    Owned<IEngineRowAllocator> allocator;

public:
    explicit CProjecStrandProcessor(CThorStrandedActivity &parent, IEngineRowStream *inputStream, unsigned outputId)
        : CThorStrandProcessor(parent, inputStream, outputId)
    {
        helper = static_cast <IHThorProjectArg *> (queryHelper());
        Owned<IRowInterfaces> rowIf = parent.getRowInterfaces();
        allocator.setown(parent.getRowAllocator(rowIf->queryRowMetaData(), (parent.queryHeapFlags()|roxiemem::RHFpacked|roxiemem::RHFunique)));
    }
    STRAND_CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        for (;;)
        {
            if (parent.queryAbortSoon())
                return nullptr;
            OwnedConstThorRow in = inputStream->nextRow();
            if (!in)
            {
                if (numProcessedLastGroup == rowsProcessed)
                    in.setown(inputStream->nextRow());
                if (!in)
                {
                    numProcessedLastGroup = rowsProcessed;
                    return nullptr;
                }
            }

            RtlDynamicRowBuilder rowBuilder(allocator);
            size32_t outSize;
            try
            {
                outSize = helper->transform(rowBuilder, in);
            }
            catch (IException *e)
            {
                parent.ActPrintLog(e, "In helper->transform()");
                throw;
            }
            if (outSize)
            {
                rowsProcessed++;
                return rowBuilder.finalizeRowClear(outSize);
            }
        }
    }
};


//  IThorDataLink needs only be implemented once, since there is only one output,
//  therefore may as well implement it here.

class CProjectSlaveActivity : public CThorStrandedActivity
{
    IHThorProjectArg *helper = nullptr;

public:
    explicit CProjectSlaveActivity(CGraphElementBase *_container) : CThorStrandedActivity(_container)
    {
        helper = static_cast <IHThorProjectArg *> (queryHelper());
        setRequireInitData(false);
        appendOutputLinked(this);
    }

    virtual CThorStrandProcessor *createStrandProcessor(IEngineRowStream *instream) override
    {
        return new CProjecStrandProcessor(*this, instream, 0);
    }
    virtual CThorStrandProcessor *createStrandSourceProcessor(bool inputOrdered) override { throwUnexpected(); }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.fastThrough = true; // ish
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info, queryInput(0));
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
};


class CPrefetchProjectSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorPrefetchProjectArg *helper;
    rowcount_t numProcessedLastGroup = 0;
    bool eof = false;
    Owned<IEngineRowAllocator> allocator;
    IThorChildGraph *child = nullptr;
    bool parallel = false;
    unsigned preload = 0;

    class PrefetchInfo : public CSimpleInterface
    {
    public:
        inline PrefetchInfo(IHThorPrefetchProjectArg &helper, const void *_in, unsigned __int64 _recordCount)
        {
            if (helper.preTransform(extract, _in, _recordCount))
            {
                in.setown(_in);
                recordCount = _recordCount;
            }
            else
            {
                ReleaseThorRow(_in);
                recordCount = 0;
            }
        }
        OwnedConstThorRow in;
        unsigned __int64 recordCount;
        rtlRowBuilder extract;
    };
    class CPrefetcher : public CSimpleInterface, implements IThreaded
    {
        CPrefetchProjectSlaveActivity &parent;
        CThreadedPersistent threaded;
        rowcount_t recordCount = 0;
        bool full = false;
        bool blocked = false;
        bool stopped = true;
        bool eog = true;
        bool eoi = true;
        bool eoq = true;
        QueueOf<PrefetchInfo, true> prefetchQueue;
        CriticalSection crit;
        Semaphore blockedSem, fullSem;

    public:
        CPrefetcher(CPrefetchProjectSlaveActivity &_parent) : threaded("CPrefetcher", this), parent(_parent)
        {
            recordCount = 0; full = blocked = eoq = eoi = stopped = false; eog = true;
        }
        ~CPrefetcher() { stop(); }
        PrefetchInfo *pullRecord()
        {
            OwnedConstThorRow row = parent.inputStream->nextRow();
            if (row)
            {
                eog = false;
                return new PrefetchInfo(*parent.helper, row.getClear(), ++recordCount);
            }
            else if (!eog)
            {
                eog = true;
                return NULL;
            }
            eoi = true;
            return NULL;
        }
        void start() { recordCount = 0; full = blocked = eoq = eoi = stopped = false; eog = true; threaded.start(); }
        void stop()
        {
            stopped = true;
            fullSem.signal();
            threaded.join(INFINITE, false);
            while (prefetchQueue.ordinality())
                ::Release(prefetchQueue.dequeue());
        }
        void abort()
        {
            blockedSem.signal(); // reader might be stuck
        }
        virtual void threadmain() override
        {
            for (;;)
            {
                Owned<PrefetchInfo> fetchRow = pullRecord();
                CriticalBlock b(crit);
                if (!eoi)
                    prefetchQueue.enqueue(fetchRow.getClear());
                if (blocked)
                {
                    blocked = false;
                    blockedSem.signal();
                }
                if (eoi)
                    break;
                if (prefetchQueue.ordinality() >= parent.preload)
                {
                    full = true;
                    CriticalUnblock b(crit);
                    fullSem.wait();
                    if (stopped)
                        break;
                }
            }
        }
        PrefetchInfo *getPrefetchRow()
        {
            if (eoq)
                return NULL;
            CriticalBlock b(crit);
            for (;;)
            {
                if (prefetchQueue.ordinality())
                {
                    if (full)
                    {
                        full = false;
                        fullSem.signal();
                    }
                    return prefetchQueue.dequeue();
                }
                else
                {
                    if (eoi)
                    {
                        eoq = true;
                        return NULL;
                    }
                    blocked = true;
                    CriticalUnblock b(crit);
                    blockedSem.wait();
                }
            }
        }
    } prefetcher;

    PrefetchInfo *readNextRecord()
    {
        if (!parallel)
            return prefetcher.pullRecord();
        else
            return prefetcher.getPrefetchRow();
    }

public:
    CPrefetchProjectSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), prefetcher(*this)
    {
        helper = (IHThorPrefetchProjectArg *) queryHelper();
        parallel = 0 != (helper->getFlags() & PPFparallel);
        setRequireInitData(false);
        allocator.set(queryRowAllocator());
        appendOutputLinked(this);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        preload = helper->getLookahead();
        if (!preload)
            preload = 10; // default
        child = helper->queryChild();
        numProcessedLastGroup = getDataLinkGlobalCount();
        eof = !helper->canMatchAny();
        if (parallel)
            prefetcher.start();
    }
    virtual void stop() override
    {
        if (parallel)
            prefetcher.stop();
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eof)
            return NULL;
        for (;;)
        {
            Owned<PrefetchInfo> prefetchRow = readNextRecord();
            if (!prefetchRow)
            {
                if (numProcessedLastGroup == getDataLinkGlobalCount())
                    prefetchRow.setown(readNextRecord());
                if (!prefetchRow)
                {
                    numProcessedLastGroup = getDataLinkGlobalCount();
                    eof = true;
                    return NULL;
                }
            }
            if (prefetchRow->in)
            {
                RtlDynamicRowBuilder out(allocator);
                Owned<IEclGraphResults> results;
                if (child)
                    results.setown(child->evaluate(prefetchRow->extract.size(), prefetchRow->extract.getbytes()));
                size32_t outSize = helper->transform(out, prefetchRow->in, results, prefetchRow->recordCount);
                if (outSize)
                {
                    dataLinkIncrement();
                    return out.finalizeRowClear(outSize);
                }
            }
        }
    }
    void abort()
    {
        CSlaveActivity::abort();
        prefetcher.abort();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info, queryInput(0));
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
};


CActivityBase *createPrefetchProjectSlave(CGraphElementBase *container)
{
    return new CPrefetchProjectSlaveActivity(container);
}

CActivityBase *createProjectSlave(CGraphElementBase *container)
{
    return new CProjectSlaveActivity(container);
}


