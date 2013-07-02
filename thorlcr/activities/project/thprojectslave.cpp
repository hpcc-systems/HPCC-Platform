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


#include "thprojectslave.ipp"
#include "eclrtl_imp.hpp"

//  IThorDataLink needs only be implemented once, since there is only one output,
//  therefore may as well implement it here.

class CProjectSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorProjectArg * helper;
    bool anyThisGroup;
    IThorDataLink *input;
    Owned<IEngineRowAllocator> allocator;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CProjectSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorProjectArg *> (queryHelper());
        anyThisGroup = false;
        allocator.set(queryRowAllocator());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);
        startInput(input);
        dataLinkStart();
    }
    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        loop {
            OwnedConstThorRow row = input->nextRow();
            if (!row && !anyThisGroup)
                row.setown(input->nextRow());
            if (!row||abortSoon)
                break;
            RtlDynamicRowBuilder ret(allocator);
            size32_t sz;
            try {
                sz = helper->transform(ret, row);
            }
            catch (IException *e) 
            { 
                ActPrintLog(e, "In helper->transform()");
                throw; 
            }
            catch (CATCHALL)
            { 
                ActPrintLog("PROJECT: Unknown exception in helper->transform()"); 
                throw;
            }
            if (sz) {
                dataLinkIncrement();
                anyThisGroup = true;
                return ret.finalizeRowClear(sz);
            }
        }
        anyThisGroup = false;
        return NULL;
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = true; // ish
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info,inputs.item(0));
    }

    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
};


class CPrefetchProjectSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorPrefetchProjectArg *helper;
    rowcount_t numProcessedLastGroup;
    bool eof;
    IThorDataLink *input;
    Owned<IEngineRowAllocator> allocator;
    IThorChildGraph *child;
    bool parallel;
    unsigned preload;

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
                ReleaseThorRow(_in);
        }
        OwnedConstThorRow in;
        unsigned __int64 recordCount;
        rtlRowBuilder extract;
    };
    class CPrefetcher : public CSimpleInterface, implements IThreaded
    {
        CPrefetchProjectSlaveActivity &parent;
        CThreadedPersistent threaded;
        rowcount_t recordCount;
        bool full, blocked, stopped, eoi, eog, eoq;
        QueueOf<PrefetchInfo, true> prefetchQueue;
        CriticalSection crit;
        Semaphore blockedSem, fullSem;

    public:
        CPrefetcher(CPrefetchProjectSlaveActivity &_parent) : threaded("CPrefetcher", this), parent(_parent)
        {
        }
        ~CPrefetcher() { stop(); }
        PrefetchInfo *pullRecord()
        {
            OwnedConstThorRow row = parent.input->nextRow();
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
            threaded.join();
            while (prefetchQueue.ordinality())
                ::Release(prefetchQueue.dequeue());
        }
        void abort()
        {
            blockedSem.signal(); // reader might be stuck
        }
        void main()
        {
            loop
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
            loop
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
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CPrefetchProjectSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this), prefetcher(*this)
    {
        helper = (IHThorPrefetchProjectArg *) queryHelper();
        parallel = 0 != (helper->getFlags() & PPFparallel);
        preload = helper->getLookahead();
        if (!preload)
            preload = 10; // default
        child = helper->queryChild();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        allocator.set(queryRowAllocator());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);
        startInput(input);

        numProcessedLastGroup = getDataLinkGlobalCount();
        eof = !helper->canMatchAny();
        if (parallel)
            prefetcher.start();
        dataLinkStart();
    }
    void stop()
    {
        if (parallel)
            prefetcher.stop();
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eof)
            return NULL;
        loop
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
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
};


CActivityBase *createPrefetchProjectSlave(CGraphElementBase *container)
{
    return new CPrefetchProjectSlaveActivity(container);
}


CActivityBase *createProjectSlave(CGraphElementBase *container)
{
    return new CProjectSlaveActivity(container);
}


