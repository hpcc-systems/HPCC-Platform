/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#include "jexcept.hpp"
#include "jthread.hpp"
#include "jqueue.hpp"
#include "roxiemem.hpp"
#include "thorstrand.hpp"
#include <atomic>

#define DEFAULT_ROWBLOCK_SIZE 500

static const byte endOfSectionMarker = 0;
const void * queryEndOfSectionMarker() { return &endOfSectionMarker; }

//---------------------------------------------------------------------------------------------------------------------

class CStrandJunction : public CInterfaceOf<IStrandJunction>
{
public:
    explicit CStrandJunction(unsigned _numProducers, unsigned _numStrands)
    : numProducers(_numProducers), numStrands(_numStrands), stopping(false), started(false)
    {
        assertex(numProducers);
    }

    virtual void reset()
    {
        producerStopSem.reinit(0);
        producerStoppedSem.reinit(0);
        stopping.store(false, std::memory_order_relaxed);
        started = false;
    }

    inline bool isStopping() const
    {
        return stopping.load(std::memory_order_relaxed);
    }

    inline void noteStarted()
    {
        started = true;
    }

    void startProducerThread(IThreaded & main)
    {
        CThreaded * thread = new CThreaded("ReadAheadThread", &main);
        threads.append(*thread);
        thread->start();
    }

    void processConsumerStop()
    {
        if (started)
        {
            //Ensure only one producer triggers stopping on the inputs
            if (!stopping.exchange(true, std::memory_order_acq_rel))
            {
                stopActiveProducers();
                for (unsigned i=0; i < numProducers; i++)
                    producerStoppedSem.wait();
            }
        }
        else
            stopInactiveProducers();
    }

    void waitForStop()
    {
        producerStopSem.wait();
    }

    void notifyStopped()
    {
        producerStoppedSem.signal();
    }

    inline unsigned getNumProducers() const { return numProducers; }

protected:
    //Stop producers that have already been started()
    virtual void stopActiveProducers()
    {
        producerStopSem.signal(numProducers);
        ForEachItemIn(i, threads)
            threads.item(i).join();
        threads.kill();
    }

    //Stop producers that have never been started()
    virtual void stopInactiveProducers() = 0;

protected:
    const unsigned numStrands;

private:
    const unsigned numProducers;
    Semaphore producerStopSem;
    Semaphore producerStoppedSem;
    CIArrayOf<CThreaded> threads;
    std::atomic<bool> stopping;
    bool started;
};


class OneToOneJunction : public CInterfaceOf<IStrandJunction>
{
public:
    OneToOneJunction() : stream(NULL) {}

    virtual IEngineRowStream * queryOutput(unsigned n)
    {
        assertex(n == 0);
        assertex(stream);
        return stream;
    }
    virtual void setInput(unsigned n, IEngineRowStream * _stream)
    {
        assertex(n == 0);
        stream = _stream;
    }
    virtual void start()
    {
    }
    virtual void reset()
    {
    }
    virtual void abort()
    {
    }

protected:
    IEngineRowStream * stream;
};

//---------------------------------------------------------------------------------------------------------------------

RoxieRowBlock::~RoxieRowBlock()
{
    releaseRows();
}

bool RoxieRowBlock::empty() const
{
    return (readPos >= writePos) && !exception;
}

bool RoxieRowBlock::readFromStream(IRowStream * stream)
{
    bool done = false;
    try
    {
        loop
        {
            const void * row = stream->nextRow();
            if (!row)
            {
                done = true;
                break;
            }
            else if (row == &endOfSectionMarker)
            {
                setEndOfChunk();
                break;
            }
            if (addRowNowFull(row))
                break;
        }
    }
    catch (IException * e)
    {
        setExceptionOwn(e);
        done = true;
    }
    return done;
}

void RoxieRowBlock::releaseRows()
{
    while (readPos < writePos)
        ReleaseRoxieRow(rows[readPos++]);
}

void RoxieRowBlock::throwAnyPendingException()
{
    if (exception)
        throw exception.getClear();
}

void RoxieRowBlock::operator delete (void * ptr)
{
    ReleaseRoxieRow(ptr);
}

//---------------------------------------------------------------------------------------------------------------------

RowBlockAllocator::RowBlockAllocator(roxiemem::IRowManager & rowManager, size32_t minRowsPerBlock) : rowsPerBlock(0)
{
    assertex(minRowsPerBlock);

    size_t classSize = sizeof(RoxieRowBlock) - RoxieRowBlock::numDummyDynamicRows * sizeof(void *);
    size_t requestedSize = classSize + minRowsPerBlock * sizeof(void*);
    unsigned heapFlags = roxiemem::RHFunique;
    heap.setown(rowManager.createFixedRowHeap(requestedSize, 0, heapFlags, 0));
    rowsPerBlock = (rowManager.getExpectedCapacity(requestedSize, heapFlags) - classSize ) / sizeof(void*);
    assertex(rowsPerBlock >= minRowsPerBlock);
}

//A bit of an experimental implementation - other options could include a list like the allocators
RoxieRowBlock * RowBlockAllocator::newBlock()
{
    return new (heap->allocate()) RoxieRowBlock(rowsPerBlock);
}


//---------------------------------------------------------------------------------------------------------------------

static void resetBlockQueue(IRowQueue * queue)
{
    queue->reset();

    loop
    {
        const void * next;
        if (!queue->tryDequeue(next))
            break;
        RoxieRowBlock * curBlock = (RoxieRowBlock *)next;
        if (curBlock)
            curBlock->releaseBlock();
    }
}


class StreamToBlockQueueThread : public CInterface, implements IThreaded
{
public:
    StreamToBlockQueueThread(CStrandJunction & _junction, IRowQueue * _queue, RowBlockAllocator & _allocator)
    : junction(_junction), queue(_queue), stream(NULL), allocator(_allocator) {}

    virtual void main()
    {
        bool done = false;
        while (!done)
        {
            RoxieRowBlock * block = allocator.newBlock();
            done = block->readFromStream(stream);
            if (junction.isStopping() || block->empty() || !queue->enqueue(block))
            {
                block->releaseBlock();
                break;
            }
        }
        queue->noteWriterStopped();
        junction.waitForStop();
        stream->stop();
        junction.notifyStopped();
    }

    void setInput(IEngineRowStream * _input)
    {
        stream = _input;
    }

    void setQueue(IRowQueue * _queue)
    {
        queue = _queue;
    }

    void stopInput()
    {
        stream->stop();
    }

protected:
    CStrandJunction & junction;
    RowBlockAllocator & allocator;
    IEngineRowStream * stream;
    IRowQueue * queue;
};

class StreamFromBlockQueue : public CInterfaceOf<IEngineRowStream>
{
public:
    StreamFromBlockQueue(CStrandJunction & _junction, IRowQueue & _queue) : junction(_junction), queue(_queue)
    {
        curBlock = NULL;
    }
    ~StreamFromBlockQueue()
    {
        reset();
    }

    virtual const void *nextRow()
    {
        const void * ret;
        loop
        {
            if (curBlock)
            {
                if (curBlock->nextRow(ret))
                    return ret;
                if (!pendingException)
                    pendingException.setown(curBlock->getClearException());
                curBlock->releaseBlock();
                curBlock = NULL;
            }
            const void * next;
            if (!queue.dequeue(next))
            {
                //If inputs are unordered, process exceptions last of all
                if (pendingException)
                    throw pendingException.getClear();
                return NULL;
            }
            curBlock = (RoxieRowBlock *)next;
        }
    }

    virtual void stop()
    {
        junction.processConsumerStop();
    }

    virtual void resetEOF()
    {
        throwUnexpectedX("resetEOF called on BlockedReader");
    }

    void reset()
    {
        if (curBlock)
            curBlock->releaseBlock();
        curBlock = NULL;
        pendingException.clear();
    }

protected:
    CStrandJunction & junction;
    IRowQueue & queue;
    RoxieRowBlock * curBlock;
    Owned<IException> pendingException;
};

//---------------------------------------------------------------------------------------------------------------------

class BlockedManyToOneJunction : public CStrandJunction
{
public:
    BlockedManyToOneJunction(roxiemem::IRowManager & _rowManager, unsigned _numStrands, unsigned blockSize, IRowQueue * _queue)
    : CStrandJunction(_numStrands, _numStrands), queue(_queue), allocator(_rowManager, blockSize), consumer(*this, *_queue)
    {
        producers = new StreamToBlockQueueThread * [numStrands];
        for (unsigned i=0; i < numStrands; i++)
            producers[i] = new StreamToBlockQueueThread(*this, queue, allocator);
    }
    ~BlockedManyToOneJunction()
    {
        for (unsigned i=0; i < numStrands; i++)
            producers[i]->Release();
        delete [] producers;
    }

    virtual IEngineRowStream * queryOutput(unsigned n)
    {
        assertex(n == 0);
        return &consumer;
    }
    virtual void setInput(unsigned n, IEngineRowStream * _stream)
    {
        assertex(n < numStrands);
        producers[n]->setInput(_stream);
    }
    virtual void abort()
    {
        queue->abort();
    }
    virtual void reset()
    {
        consumer.reset();
        resetBlockQueue(queue);
        CStrandJunction::reset();
    }
    virtual void start()
    {
        for (unsigned i=0; i < numStrands; i++)
            startProducerThread(*producers[i]);
        noteStarted();
    }
    static BlockedManyToOneJunction * create(roxiemem::IRowManager & rowManager, unsigned numStrands, unsigned blockSize)
    {
        const unsigned maxQueued = numStrands * 4;
        Owned<IRowQueue> queue = createRowQueue(1, numStrands, maxQueued, 0);
        return new BlockedManyToOneJunction(rowManager, numStrands, blockSize, queue.getClear());
    }

protected:
    virtual void stopActiveProducers()
    {
        queue->abort();
        CStrandJunction::stopActiveProducers();
    }
    virtual void stopInactiveProducers()
    {
        for (unsigned i=0; i < numStrands; i++)
            producers[i]->stopInput();
    }


protected:
    Owned<IRowQueue> queue;
    RowBlockAllocator allocator;
    StreamToBlockQueueThread * * producers;
    StreamFromBlockQueue consumer;
};

//---------------------------------------------------------------------------------------------------------------------


class BlockedOneToManyJunction : public CStrandJunction
{
public:
    BlockedOneToManyJunction(roxiemem::IRowManager & _rowManager, unsigned _numStrands, unsigned _maxQueueItems, unsigned _blockSize)
    : CStrandJunction(1, _numStrands), allocator(_rowManager, _blockSize), producer(*this, NULL, allocator)
    {
        queue.setown(createRowQueue(numStrands, 1, _maxQueueItems, 0));
        producer.setQueue(queue);

        consumers = new StreamFromBlockQueue * [numStrands];
        for (unsigned i=0; i < numStrands; i++)
            consumers[i] = new StreamFromBlockQueue(*this, *queue);
    }
    ~BlockedOneToManyJunction()
    {
        for (unsigned i=0; i < numStrands; i++)
            consumers[i]->Release();
        delete [] consumers;
    }

    virtual IEngineRowStream * queryOutput(unsigned n)
    {
        assertex(n < numStrands);
        return consumers[n];
    }
    virtual void setInput(unsigned n, IEngineRowStream * _stream)
    {
        assertex(n == 0);
        producer.setInput(_stream);
    }
    virtual void abort()
    {
        queue->abort();
    }
    virtual void reset()
    {
        resetBlockQueue(queue);
        for (unsigned i=0; i < numStrands; i++)
            consumers[i]->reset();
        CStrandJunction::reset();
    }
    virtual void start()
    {
        startProducerThread(producer);
        noteStarted();
    }

protected:
    virtual void stopActiveProducers()
    {
        queue->abort();
        CStrandJunction::stopActiveProducers();
    }

    virtual void stopInactiveProducers()
    {
        for (unsigned i=0; i < numStrands; i++)
            producer.stopInput();
    }

protected:
    Owned<IRowQueue> queue;
    RowBlockAllocator allocator;
    StreamToBlockQueueThread producer;
    StreamFromBlockQueue * * consumers;
};

//---------------------------------------------------------------------------------------------------------------------

//Trivial single element queue
class SingleItemBlockQueue
{
public:
    SingleItemBlockQueue() : avail(0U), space(1U)
    {
    }

    ~SingleItemBlockQueue()
    {
        if (value)
            value->releaseBlock();
    }

    void abort()
    {
        abortSoon = true;
        avail.signal();
        space.signal();
    }

    void reset()
    {
        avail.reinit(0);
        space.reinit(1);
        if (value)
        {
            value->releaseBlock();
            value = NULL;
        }
        abortSoon = false;
        finishedWriting = false;
    }

    bool enqueue(RoxieRowBlock * next)
    {
        if (abortSoon)
            return false;
        space.wait();
        if (abortSoon)
            return false;
        value = next;
        avail.signal();
        return true;
    }

    void noteWriterStopped()
    {
        if (abortSoon)
            return;
        space.wait();
        finishedWriting = true;
        avail.signal();
    }

    bool dequeue(RoxieRowBlock * & ret)
    {
        if (abortSoon)
            return false;
        avail.wait();
        if (abortSoon)
            return false;

        if (finishedWriting)
        {
            avail.signal();
            return false;
        }

        ret = value;
        value = NULL;
        space.signal();
        return true;
    }

protected:
    RoxieRowBlock * value = nullptr;
    bool abortSoon = false;
    bool finishedWriting = false;
    Semaphore space __attribute__((aligned(CACHE_LINE_SIZE)));
    Semaphore avail __attribute__((aligned(CACHE_LINE_SIZE)));
};
//may replace with
typedef SingleItemBlockQueue OrderedJunctionBlockQueue;


class OrderedReadAheadThread : public CInterface, implements IThreaded
{
    //friend class OrderedManyToOneJunction;
public:
    OrderedReadAheadThread(CStrandJunction & _junction, RowBlockAllocator & _allocator) : junction(_junction), stream(NULL), allocator(_allocator)
    {
        finished = false;
        alive = true;
    }

    virtual void main()
    {
        bool done = false;
        while (!done)
        {
            RoxieRowBlock * block = allocator.newBlock();
            done = block->readFromStream(stream);
            if (block->empty() || !queue.enqueue(block))
            {
                block->releaseBlock();
                break;
            }
        }
        noteWriterStopped();
        junction.waitForStop();
        stream->stop();
        junction.notifyStopped();
    }

    void abort()
    {
        queue.abort();
    }

    void noteFinished()
    {
        assertex(finished);
        assertex(alive);
        alive = false;
    }

    void reset()
    {
        queue.reset();
        finished = false;
        alive = true;
    }

    void stopInput()
    {
        stream->stop();
    }

    void setInput(IEngineRowStream * _input)
    {
        stream = _input;
    }

    void noteWriterStopped()
    {
        finished = true;
        queue.noteWriterStopped();
    }

    inline bool isAlive() const { return alive; }
    inline OrderedJunctionBlockQueue & queryQueue() { return queue; }

protected:
    CStrandJunction & junction;
    RowBlockAllocator & allocator;
    IEngineRowStream * stream;
    OrderedJunctionBlockQueue queue;
    bool finished;
    bool alive;
};


//This class primarily supports multi-stranded source activities which immediately feed into a single-stranded activity.
class OrderedManyToOneJunction : public CStrandJunction, implements IEngineRowStream
{
public:
    IMPLEMENT_IINTERFACE_USING(CStrandJunction)

    OrderedManyToOneJunction(roxiemem::IRowManager & _rowManager, unsigned _numStrands, unsigned blockSize)
    : CStrandJunction(_numStrands, _numStrands), allocator(_rowManager, blockSize)
    {
        producers = new OrderedReadAheadThread * [numStrands];
        for (unsigned i=0; i < numStrands; i++)
            producers[i] = new OrderedReadAheadThread(*this, allocator);
        curBlock = NULL;
        curStrand = 0;
        numActiveStrands = numStrands;
    }
    ~OrderedManyToOneJunction()
    {
        for (unsigned i=0; i < numStrands; i++)
            producers[i]->Release();
        delete [] producers;
    }

    virtual IEngineRowStream * queryOutput(unsigned n)
    {
        assertex(n == 0);
        return this;
    }
    virtual void setInput(unsigned n, IEngineRowStream * _stream)
    {
        assertex(n < numStrands);
        producers[n]->setInput(_stream);
    }
    virtual void abort()
    {
        for (unsigned i=0; i < numStrands; i++)
            producers[i]->abort();
    }
    virtual void reset()
    {
        if (curBlock)
        {
            curBlock->releaseBlock();
            curBlock = NULL;
        }
        for (unsigned strand=0; strand < numStrands; strand++)
            producers[strand]->reset();
        curStrand = 0;
        numActiveStrands = numStrands;
        CStrandJunction::reset();
    }
    virtual void start()
    {
        for (unsigned i=0; i < numStrands; i++)
            startProducerThread(*producers[i]);
        noteStarted();
    }
    virtual const void *nextRow()
    {
        if (numActiveStrands == 0)
            return NULL;

        loop
        {
            if (curBlock)
            {
                const void * ret;
                if (curBlock->nextRow(ret))
                    return ret;
                curBlock->throwAnyPendingException();
                bool isEnd = curBlock->isEndOfChunk();
                curBlock->releaseBlock();
                curBlock = NULL;
                if (isEnd)
                    nextStrand();
            }

            loop
            {
                OrderedReadAheadThread & curProducer = *(producers[curStrand]);
                OrderedJunctionBlockQueue & queue = curProducer.queryQueue();
                if (!queue.dequeue(curBlock))
                {
                    //Abort requested
                    numActiveStrands = 0;
                    return NULL;
                }

                //DBGLOG("active(%d) strand(%d)", numActiveStrands, curStrand);
                if (curBlock)
                    break;

                curProducer.noteFinished();
                if (--numActiveStrands == 0)
                    return NULL;
                nextStrand();
            }
        }
    }

    virtual void stop()
    {
        processConsumerStop();
    }

    virtual void resetEOF()
    {
        throwUnexpectedX("resetEOF called on OrderedManyToOneJunction");
    }

protected:
    void nextStrand()
    {
        do
        {
            curStrand++;
            if (curStrand == numStrands)
                curStrand = 0;
        } while (!producers[curStrand]->isAlive());
    }

    virtual void stopActiveProducers()
    {
        for (unsigned i=0; i < numStrands; i++)
            producers[i]->abort();
        CStrandJunction::stopActiveProducers();
    }

    virtual void stopInactiveProducers()
    {
        for (unsigned i=0; i < numStrands; i++)
            producers[i]->stopInput();
    }


protected:
    unsigned numActiveStrands;
    RowBlockAllocator allocator;
    OrderedReadAheadThread * * producers;
    RoxieRowBlock * curBlock;
    unsigned curStrand;
};

//---------------------------------------------------------------------------------------------------------------------

IStrandJunction * createStrandJunction(roxiemem::IRowManager & rowManager, unsigned numInputs, unsigned numOutputs, unsigned blockSize, bool isOrdered)
{
    if ((numInputs == 1) && (numOutputs == 1))
        return new OneToOneJunction();
    if (blockSize == 0)
        blockSize = DEFAULT_ROWBLOCK_SIZE;
    if (numOutputs == 1)
    {
        if (isOrdered)
            return new OrderedManyToOneJunction(rowManager, numInputs, blockSize);
        return BlockedManyToOneJunction::create(rowManager, numInputs, blockSize);
    }
    if (numInputs == 1)
    {
        unsigned maxQueueItems = numOutputs * 2;
        return new BlockedOneToManyJunction(rowManager, numOutputs, maxQueueItems, blockSize);
    }

    //More: We could implement M:N using the existing base classes if there was a need
    UNIMPLEMENTED_X("createStrandJunction M:N");
}

void clearRowQueue(IRowQueue * queue)
{
    const void * next;
    while (queue->tryDequeue(next))
        ReleaseRoxieRow(next);
}


//---------------------------------------------------------------------------------------------------------------------

//Class for managing processing on a single ordered strand
class OrderedStrandRowBlockInput : public CInterfaceOf<IEngineRowStream>
{
    friend class OrderedManyToOneJunction;

public:
    OrderedStrandRowBlockInput(CStrandJunction & _splitJunction, IOrderedOutputCallback & _callback)
    : splitJunction(_splitJunction), callback(_callback)
    {
        curInputBlock = nullptr;
    }

 //interface IEngineRowStream for the rows being supplied to the strand.
    virtual const void *nextRow()
    {
        loop
        {
            if (!curInputBlock)
            {
                if (!inputQueue.dequeue(curInputBlock))
                {
                    callback.noteEndOfInput();
                    return NULL;
                }
            }

            const void * row;
            if (curInputBlock->nextRow(row))
                return row;
            curInputBlock->throwAnyPendingException();
            if (curInputBlock->isEndOfChunk())
            {
                if (!callback.noteEndOfInputChunk())
                    inputQueue.abort();
            }
            curInputBlock->releaseBlock();
            curInputBlock = NULL;
        }
    }

    virtual void stop()
    {
        splitJunction.processConsumerStop();
    }

    virtual void resetEOF()
    {
        throwUnexpectedX("resetEOF called on OrderedStrand");
    }

    void abort()
    {
        inputQueue.abort();
    }

    void reset()
    {
        inputQueue.reset();
        if (curInputBlock)
            curInputBlock->releaseBlock();
        curInputBlock = nullptr;
    }

    void noteWriterStopped()
    {
        queryInputQueue().noteWriterStopped();
    }

    inline OrderedJunctionBlockQueue & queryInputQueue() { return inputQueue; }

protected:
    CStrandJunction & splitJunction;
    IOrderedOutputCallback & callback;
    OrderedJunctionBlockQueue inputQueue;
    RoxieRowBlock * curInputBlock;
};


class OrderedInputJunction : public CStrandJunction, implements IThreaded
{
public:
    OrderedInputJunction(roxiemem::IRowManager & _rowManager, unsigned _numStrands, unsigned _blockSize, bool _isGrouped, const OrderedCallbackArray & callbacks)
    : CStrandJunction(1, _numStrands), inputBlockAllocator(_rowManager, _blockSize), isGrouped(_isGrouped)
    {
        strands = new OrderedStrandRowBlockInput * [numStrands];
        for (unsigned i=0; i < numStrands; i++)
            strands[i] = new OrderedStrandRowBlockInput(*this, *callbacks.item(i));

        blockSize = inputBlockAllocator.maxRowsPerBlock();
        minGroupBlockSize = (blockSize * 7 + 4) / 8;  // Fill with groups until at least 7/8 filled.
        assertex(minGroupBlockSize != 0);
    }
    ~OrderedInputJunction()
    {
        for (unsigned i=0; i < numStrands; i++)
            delete strands[i];
        delete [] strands;
    }
    virtual IEngineRowStream * queryOutput(unsigned n) { assertex(n < numStrands); return strands[n]; }
    virtual void setInput(unsigned n, IEngineRowStream * _stream)
    {
        assertex(n==0);
        input = _stream;
    }
    virtual void start()
    {
        startProducerThread(*this);
        noteStarted();
    }
    virtual void abort()
    {
        for (unsigned i=0; i < numStrands; i++)
            strands[i]->abort();
    }
    virtual void reset()
    {
        for (unsigned i=0; i < numStrands; i++)
            strands[i]->reset();
        CStrandJunction::reset();
    }
    virtual void stopActiveProducers()
    {
        abort();
        CStrandJunction::stopActiveProducers();
    }
    virtual void stopInactiveProducers()
    {
        input->stop();
    }

    virtual void main()
    {
        unsigned curStrand = 0;
        bool done = false;
        size32_t endChunkSize = minGroupBlockSize-1;
        const void * prev = nullptr;    // only checked if non null, never dereferenced
        while (!done)
        {
            RoxieRowBlock * block = inputBlockAllocator.newBlock();
            bool isEndOfChunk = !isGrouped;
            try
            {
                if (isGrouped)
                {
                    loop
                    {
                        const void * row = input->nextRow();
                        if (!row)
                        {
                            if (!prev)
                            {
                                done = true;
                                isEndOfChunk = true;
                            }
                            else if (block->numRows() >= endChunkSize)
                                isEndOfChunk = true;
                        }
                        prev = row;
                        if (block->addRowNowFull(row) || isEndOfChunk)
                            break;
                    }
                }
                else
                {
                    //MORE: This could more efficiently loop 0..blockSize-1 and remove the test in addRowNowFull()
                    loop
                    {
                        const void * row = input->nextRow();
                        if (unlikely(!row))
                        {
                            //Some activities rely on two successive nulls to indicate end of file, so read another row and check it
                            row = input->nextRow();
                            assertex(!row);
                            done = true;
                            break;
                        }
                        else
                        {
                            if (block->addRowNowFull(row))
                                break;
                        }

                        if (isStopping())
                            break;
                    }
                }
            }
            catch (IException * e)
            {
                //MORE: Protect against exceptions, ensure exception is fed and processed by the strand.  (Otherwise read ahead may cause
                //premature failure...
                block->setExceptionOwn(e);
                done = true;
            }

            if (isEndOfChunk)
            {
                block->setEndOfChunk();
                endChunkSize = minGroupBlockSize-1;
            }
            else
                endChunkSize = 0;   // Switch to the next strand as soon as an end of group is encountered

            if (block->empty() || !strands[curStrand]->queryInputQueue().enqueue(block))
            {
                block->releaseBlock();
                break;
            }

            curStrand = curStrand+1;
            if (curStrand == numStrands)
                curStrand = 0;
        }

        for (unsigned i=0; i < numStrands; i++)
        {
            strands[curStrand]->noteWriterStopped();
            curStrand = curStrand+1;
            if (curStrand == numStrands)
                curStrand = 0;
        }

        waitForStop();
        input->stop();
        notifyStopped();
    }

protected:
    RowBlockAllocator inputBlockAllocator;
    OrderedStrandRowBlockInput * * strands;
    IEngineRowStream * input = nullptr;
    unsigned blockSize;
    unsigned minGroupBlockSize;
    bool isGrouped;
};

//---------------------------------------------------------------------------------------------------------------------

//Class for reading input from a streaming source activity.
class OrderedStrandStreamInput : public CInterfaceOf<IEngineRowStream>
{
    friend class OrderedManyToOneJunction;

public:
    OrderedStrandStreamInput(CStrandJunction & _splitJunction, IOrderedOutputCallback & _callback)
    : splitJunction(_splitJunction), callback(_callback)
    {
    }

    void setInput(IEngineRowStream * _input)
    {
        stream = _input;
    }

 //interface IEngineRowStream for the rows being supplied to the strand.
    virtual const void *nextRow()
    {
        loop
        {
            if (eof)
                return NULL;

            const void * row = stream->nextRow();
            if (likely(row != &endOfSectionMarker))
                return row;

            if (!callback.noteEndOfInputChunk())
                eof = true;
        }
    }

    virtual void stop()
    {
        stream->stop();
    }

    virtual void resetEOF()
    {
        throwUnexpectedX("resetEOF called on OrderedStrand");
    }

    void abort()
    {
        eof = true;
        //MORE: provide a callback for notifying the source?
    }

    void reset()
    {
        eof = false;
    }

protected:
    CStrandJunction & splitJunction;
    IOrderedOutputCallback & callback;
    IRowStream * stream = nullptr;
    bool eof = false;
};


class OrderedSourceJunction : public CStrandJunction
{
public:
    OrderedSourceJunction(unsigned _numStrands, const OrderedCallbackArray & callbacks)
    : CStrandJunction(_numStrands, _numStrands)
    {
        strands = new OrderedStrandStreamInput * [numStrands];
        for (unsigned i=0; i < numStrands; i++)
            strands[i] = new OrderedStrandStreamInput(*this, *callbacks.item(i));
    }
    ~OrderedSourceJunction()
    {
        for (unsigned i=0; i < numStrands; i++)
            delete strands[i];
        delete [] strands;
    }
    virtual IEngineRowStream * queryOutput(unsigned n)
    {
        assertex(n < numStrands);
        return strands[n];
    }
    virtual void reset()
    {
        for (unsigned i=0; i < numStrands; i++)
            strands[i]->reset();
        CStrandJunction::reset();
    }
    virtual void setInput(unsigned n, IEngineRowStream * _stream)
    {
        assertex(n < numStrands);
        strands[n]->setInput(_stream);
    }
    virtual void start()
    {
        noteStarted();
    }
    virtual void abort()
    {
        for (unsigned i=0; i < numStrands; i++)
            strands[i]->abort();
    }
    virtual void stopActiveProducers()
    {
        throwUnexpected();
    }
    virtual void stopInactiveProducers()
    {
        throwUnexpected();
    }

protected:
    OrderedStrandStreamInput * * strands;
};

//---------------------------------------------------------------------------------------------------------------------

class OrderedStrandRowBlockOutput : public CInterface, implements IThreaded, implements IOrderedOutputCallback
{
    friend class OrderedManyToOneJunction;

public:
    OrderedStrandRowBlockOutput(CStrandJunction & _joinJunction, RowBlockAllocator & _allocator)
    : joinJunction(_joinJunction), allocator(_allocator)
    {
    }

//IThreaded - main function used to read rows from the strand and add to the output
    virtual void main()
    {
        bool done = false;
        while (!done)
        {
            try
            {
                loop
                {
                    const void * row = strand->nextRow();
                    //NB: Need to be check the final eog isn't lost when processing sequentially
                    if (!row && eoi)
                    {
                        done = true;
                        break;
                    }

                    //curOutputBlock may be modified within the call to strand->nextRow() above
                    //(but not by any other threads)
                    if (!curOutputBlock)
                        curOutputBlock = allocator.newBlock();

                    if (curOutputBlock->addRowNowFull(row))
                        break;
                }
            }
            catch (IException * e)
            {
                if (!curOutputBlock)
                    curOutputBlock = allocator.newBlock();
                curOutputBlock->setExceptionOwn(e);
                done = true;
            }

            if (curOutputBlock)
            {
                if (curOutputBlock->empty() || !outputQueue.enqueue(curOutputBlock))
                {
                    curOutputBlock->releaseBlock();
                    curOutputBlock = NULL;
                    break;
                }
                curOutputBlock = NULL;
            }
        }
        finished = true;
        outputQueue.noteWriterStopped();
        joinJunction.waitForStop();
        strand->stop();
        joinJunction.notifyStopped();
    }

    virtual bool noteEndOfInputChunk()
    {
        if (!curOutputBlock)
            curOutputBlock = allocator.newBlock();
        curOutputBlock->setEndOfChunk();
        bool success = true;
        if (!outputQueue.enqueue(curOutputBlock))
        {
            curOutputBlock->releaseBlock();
            success = false;
        }
        curOutputBlock = NULL;
        return success;
    }

    virtual void noteEndOfInput()
    {
        eoi = true;
    }

    void abort()
    {
        outputQueue.abort();
    }

    void reset()
    {
        outputQueue.reset();
        if (curOutputBlock)
            curOutputBlock->releaseBlock();
        curOutputBlock = nullptr;
        finished = false;
        alive = true;
        eoi = false;
    }

    void setStrand(IEngineRowStream * _input)
    {
        strand = _input;
    }

    void stopStrand()
    {
        strand->stop();
    }

    inline OrderedJunctionBlockQueue & queryOutputQueue() { return outputQueue; }

protected:
    CStrandJunction & joinJunction;
    RowBlockAllocator & allocator;
    IEngineRowStream * strand = nullptr; // the stream that executes in parallel processing the stream of rows
    OrderedJunctionBlockQueue outputQueue;
    RoxieRowBlock * curOutputBlock = nullptr;
    bool finished = false;
    bool alive = true;
    bool eoi = false;
};

class OrderedOutputJunction : public CStrandJunction, implements IEngineRowStream
{
public:
    OrderedOutputJunction(roxiemem::IRowManager & _rowManager, unsigned _numStrands, unsigned _blockSize)
    : CStrandJunction(_numStrands, _numStrands), outputBlockAllocator(_rowManager, _blockSize)
    {
        strands = new OrderedStrandRowBlockOutput * [numStrands];
        for (unsigned i=0; i < numStrands; i++)
            strands[i] = new OrderedStrandRowBlockOutput(*this, outputBlockAllocator);
    }
    ~OrderedOutputJunction()
    {
        for (unsigned i=0; i < numStrands; i++)
            delete strands[i];
        delete [] strands;
    }
    IMPLEMENT_IINTERFACE_USING(CStrandJunction)

    virtual IEngineRowStream * queryOutput(unsigned n) { assertex(n==0); return this; }
    virtual void setInput(unsigned n, IEngineRowStream * _stream) { strands[n]->setStrand(_stream); }
    virtual void start()
    {
        for (unsigned i=0; i < numStrands; i++)
            startProducerThread(*strands[i]);
        noteStarted();
    }
    virtual void abort()
    {
        for (unsigned i=0; i < numStrands; i++)
            strands[i]->abort();
    }
    virtual void stopActiveProducers()
    {
        for (unsigned i=0; i < numStrands; i++)
            strands[i]->abort();
        CStrandJunction::stopActiveProducers();
    }
    virtual void reset()
    {
        curOutputStrand = 0;
        if (curOutputBlock)
            curOutputBlock->releaseBlock();
        curOutputBlock = nullptr;
        for (unsigned i=0; i < numStrands; i++)
            strands[i]->reset();
        CStrandJunction::reset();
    }
    virtual void stopInactiveProducers()
    {
        for (unsigned i=0; i < numStrands; i++)
            strands[i]->stopStrand();
    }
    void gatherCallbacks(OrderedCallbackArray & callbacks)
    {
        for (unsigned i=0; i < numStrands; i++)
            callbacks.append(strands[i]);
    }

//implementation of IEngineRowStream
     virtual const void *nextRow()
     {
         loop
         {
             if (likely(curOutputBlock))
             {
                 const void * result;
                 if (curOutputBlock->nextRow(result))
                     return result;
                 curOutputBlock->throwAnyPendingException();
                 if (curOutputBlock->isEndOfChunk())
                 {
                     curOutputStrand++;
                     if (curOutputStrand == numStrands)
                         curOutputStrand = 0;
                 }
                 curOutputBlock->releaseBlock();
                 curOutputBlock = NULL;
             }

             if (!strands[curOutputStrand]->queryOutputQueue().dequeue(curOutputBlock))
             {
                 //If there is no more output on the next strand, then all the strands will have finished processing.
                 return NULL;
             }
         }
     }

     virtual void stop()
     {
         processConsumerStop();
     }

     virtual void resetEOF()
     {
         throwUnexpectedX("resetEOF called on OrderedStrandBranch");
     }

protected:
    RowBlockAllocator outputBlockAllocator;
    OrderedStrandRowBlockOutput * * strands;
    RoxieRowBlock * curOutputBlock = nullptr;
    unsigned curOutputStrand = 0;
};

//---------------------------------------------------------------------------------------------------------------------

class CStrandBranch : public CInterfaceOf<IStrandBranch>
{
public:
    CStrandBranch(IStrandJunction * _input, IStrandJunction * _output) : input(_input), output(_output)
    {
    }

    virtual IStrandJunction * queryInputJunction()
    {
        return input;
    }

    virtual IStrandJunction * queryOutputJunction()
    {
        return output;
    }

protected:
    Linked<IStrandJunction> input;
    Linked<IStrandJunction> output;
};

//---------------------------------------------------------------------------------------------------------------------

extern THORHELPER_API IStrandBranch * createStrandBranch(roxiemem::IRowManager & rowManager, unsigned numStrands, unsigned blockSize, bool isOrdered, bool isGrouped, bool inputIsStreamed)
{
    Linked<IStrandJunction> input;
    Linked<IStrandJunction> output;

    //Slightly inefficient to go via a junction, but makes the testing code simpler!
    assertex(numStrands);
    if (numStrands == 1)
    {
        input.setown(new OneToOneJunction());
        output.setown(new OneToOneJunction());
    }
    else if (isOrdered || isGrouped)
    {
        //MORE To allow parallel sinks allow callbacks to be provided and have a dummy output junction or none
        //But the number will not be known until the branch is created, so the callback will need to create them
        OrderedCallbackArray callbacks;
        if (true)
        {
            Owned<OrderedOutputJunction> outputJunction = new OrderedOutputJunction(rowManager, numStrands, blockSize);
            outputJunction->gatherCallbacks(callbacks);
            output.setown(outputJunction.getClear());
        }
        else
        {
            //Create and gather the appropriate number of callbacks from the sink.
        }

        if (inputIsStreamed)
        {
            input.setown(new OrderedSourceJunction(numStrands, callbacks));
        }
        else
        {
            input.setown(new OrderedInputJunction(rowManager, numStrands, blockSize, isGrouped, callbacks));
        }
    }
    else
    {
        input.setown(createStrandJunction( rowManager, 1, numStrands, blockSize, false));
        output.setown(createStrandJunction( rowManager, numStrands, 1, blockSize, false));
    }
    return new CStrandBranch(input, output);
}

//---------------------------------------------------------------------------------------------------------------------


class BlockedRowStreamWriter : public CInterfaceOf<IRowWriterEx>
{
public:
    BlockedRowStreamWriter(IRowQueue * _queue, RowBlockAllocator & _allocator) : queue(_queue), allocator(_allocator)
    {
        curBlock = NULL;
    }

    virtual void putRow(const void *row)
    {
        if (!curBlock)
            curBlock = allocator.newBlock();
        if (curBlock->addRowNowFull(row))
        {
            if (!queue->enqueue(curBlock))
                curBlock->releaseBlock();
            curBlock = NULL;
        }
    }

    virtual void flush()
    {
        if (curBlock)
        {
            if (!queue->enqueue(curBlock))
                curBlock->releaseBlock();
            curBlock = NULL;
        }
    }

    virtual void noteStopped()
    {
        flush();
        queue->noteWriterStopped();
    }

protected:
    IRowQueue * queue;
    RowBlockAllocator & allocator;
    RoxieRowBlock * curBlock;
};

class UnorderedManyToOneRowStream : public CInterfaceOf<IManyToOneRowStream>
{
public:
    UnorderedManyToOneRowStream(roxiemem::IRowManager & _rowManager, unsigned _numStrands, unsigned blockSize) : numStrands(_numStrands), allocator(_rowManager, blockSize)
    {
        const unsigned maxQueued = numStrands * 4;
        queue.setown(createRowQueue(1, numStrands, maxQueued, 0));

        producers = new BlockedRowStreamWriter * [numStrands];
        for (unsigned i=0; i < numStrands; i++)
            producers[i] = new BlockedRowStreamWriter(queue, allocator);
        curBlock = NULL;
    }
    ~UnorderedManyToOneRowStream()
    {
        for (unsigned i=0; i < numStrands; i++)
            producers[i]->Release();
        delete [] producers;
    }

    virtual void abort()
    {
        queue->abort();
    }
    virtual void stop()
    {
        //MORE: What should this do?
        queue->abort();
    }
    virtual void reset()
    {
        queue->reset();

        loop
        {
            if (curBlock)
                curBlock->releaseBlock();
            const void * next;
            if (!queue->tryDequeue(next))
                break;
            curBlock = (RoxieRowBlock *)next;
        }
        curBlock = NULL;
    }
    virtual const void *nextRow()
    {
        const void * ret;
        loop
        {
            if (curBlock)
            {
                if (curBlock->nextRow(ret))
                    return ret;
                curBlock->releaseBlock();
                curBlock = NULL;
            }
            const void * next;
            if (!queue->dequeue(next))
                return NULL;
            curBlock = (RoxieRowBlock *)next;
        }
    }
    virtual IRowWriterEx * getWriter(unsigned n)
    {
        return LINK(producers[n]);
    }

protected:
    unsigned numStrands;
    Owned<IRowQueue> queue;
    RowBlockAllocator allocator;
    BlockedRowStreamWriter * * producers;
    RoxieRowBlock * curBlock;
};

//---------------------------------------------------------------------------------------------------------------------


extern THORHELPER_API IManyToOneRowStream * createManyToOneRowStream(roxiemem::IRowManager & rowManager, unsigned numInputs, unsigned blockSize, bool isOrdered)
{
    if (!isOrdered)
        return new UnorderedManyToOneRowStream(rowManager, numInputs, blockSize);
    return NULL;
}
