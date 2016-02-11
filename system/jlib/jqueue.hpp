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



#ifndef __JQUEUE__
#define __JQUEUE__

#include "jlib.hpp"
#include <atomic>
#include <utility>
#include "jatomic.hpp"

// A generalised queue interface.

template <typename ELEMENT>
interface IQueueOf : extends IInterface
{
public:
    virtual bool enqueue(const ELEMENT item) = 0;
    virtual bool dequeue(ELEMENT & result) = 0;
    virtual bool tryDequeue(ELEMENT & result) = 0;
    virtual void noteWriterStopped() = 0;
    virtual void abort() = 0;
    virtual void reset() = 0;
};

typedef IQueueOf<const void *> IRowQueue;

extern jlib_decl IRowQueue * createRowQueue(unsigned numReaders, unsigned numWriters, unsigned maxItems, unsigned maxSlots);

/*
 * The ReaderWriterQueue is a bounded inter-thread queue that aims to be lock free when adding an item to the queue
 * if there is space, and removing items from a non empty queue.
 * Normally lock free implementations may be unbounded - which can cause memory to be exhausted if the producers
 * are faster than the consumer.  They also tend to spin until they can proceed - but this performs poorly if the
 * consumer and producer are not perfectly balanced.  The high level of contention can also cause cache issues.
 *
 * This implementations will wait on a semaphore if there is no room, or if the queue is empty.
 * It uses a single state field which combines information about the queue state and the number of waiting readers/writers.
 *
 * The queue also has support for
 *   aborting - causing all consumers and producers to fail to enqueue/dequeue
 *   noting writers have completed.  This is particularly useful consumers of a M:1 queue knowing there will be no more items.
 */

//NOTE: maxSlotBits * 2 + writerBits * 2 + readerBits <= sizeof(ELEMENT)*8)
template <typename ELEMENT, typename state_t, unsigned readerBits, unsigned writerBits, unsigned maxSlotBits, unsigned fixedSlotBits=0>
class ReaderWriterQueue
{
    //This uses one spare slot in the array to ensure the count field cannot overflow.
    //state has [dequeue-pos][reader-count][writer-count][num-items] in a single field.
    //num-items is the least significant field (so does not need to be shifted)
    //dequeue-pos it the most significant field, so there is no need to worry about wrapping into other fields.
    //dequeue-pos must have enough extra bits to disambiguate N writers all waiting to write to the same slot
    class BufferElement
    {
    public:
        ELEMENT value;
        std::atomic<unsigned> sequence;
    };

    // max readers = 2^readerBits-1
    // max writers = 2^writerBits-1
    // max queuesize = 2^(sequenceBits-1)-1

    //Derived constants
    const static unsigned extraSequenceBits = writerBits;       // Possibly this could be reduced to min(readerBits, writerBits)
    const static unsigned stateBits = (unsigned)(sizeof(state_t)*8);
    const static unsigned padBits = stateBits - (extraSequenceBits + writerBits + 2 * maxSlotBits + readerBits);
    const static unsigned countBits = maxSlotBits + padBits; // ensure the sequence wraps as expected in the top bits.
    const static unsigned sequenceBits = extraSequenceBits+maxSlotBits;
    const static unsigned countShift = 0;
    const static unsigned writerShift = countBits;
    const static unsigned readerShift = countBits + writerBits;
    const static unsigned dequeueShift = countBits + writerBits + readerBits;
    const static state_t readerMask = ((state_t)1 << dequeueShift) - ((state_t)1 << readerShift);
    const static state_t writerMask = ((state_t)1 << readerShift) - ((state_t)1 << writerShift);
    const static state_t sequenceMask = ((state_t)1 << sequenceBits) - 1;
    const static state_t countMask = ((state_t)1 << countBits) - 1;;
    const static state_t dequeueMask = (sequenceMask << dequeueShift);
    const static unsigned maxSlots = (1U << maxSlotBits) - 1;
    const static unsigned initialSpinsBeforeWait = 2000;
    const static unsigned slotUnavailableSpins = 50;    // If not available for a short time then the thread must have been rescheduled

    const static state_t fixedSlotMask = (1U << fixedSlotBits) - 1;

public:
    ReaderWriterQueue(unsigned _maxWriters, unsigned _maxItems) : maxItems(_maxItems), maxWriters(_maxWriters)
    {
        //printf("element(%u) pad(%u) write(%u), read(%u) slot(%u) count(%u) max(%u)\n", stateBits, padBits, writerBits, readerBits, maxSlotBits, countBits, maxItems);
        //Check all the bits are used, and none of the bits overlap.
        assertex(padBits < stateBits);
        assertex(countBits >= maxSlotBits);
        assertex(sequenceBits + readerBits + writerBits + countBits == stateBits);
        assertex((readerMask | writerMask | countMask | dequeueMask) == (state_t)-1);
        assertex((readerMask | writerMask | countMask | dequeueMask) == (readerMask ^ writerMask ^ countMask ^ dequeueMask));

        // Reserve at least one free entry to ensure the count bitfield does not overflow.
        const unsigned minSpace = 1;
        assertex(maxItems != 0);
        assertex(maxItems + minSpace <= maxSlots);

        unsigned numSlots;
        if (fixedSlotBits == 0)
        {
            //Ensure the array is a power of two, so the sequence can be mapped to an item using an AND
            numSlots = 1;
            while (numSlots < maxItems + minSpace)
                numSlots += numSlots;
            dynamicSlotMask = numSlots - 1;
        }
        else
        {
            numSlots = fixedSlotMask + 1;
            dynamicSlotMask = fixedSlotMask;
        }

        activeWriters.store(maxWriters, std::memory_order_relaxed);
        aborted.store(false, std::memory_order_relaxed);
        state.store(0, std::memory_order_relaxed);
        values = new BufferElement[numSlots];
        for (unsigned i=0; i < numSlots; i++)
            values[i].sequence.store(i, std::memory_order_relaxed);
    }
    ~ReaderWriterQueue()
    {
        delete [] values;
    }

    //Should possibly have the following functions instead for correct C++11 integration...
    //void enqueue(const ELEMENT & value);
    //void enqueue(ELEMENT && value);
    bool enqueue(const ELEMENT value)
    {
        if (aborted.load(std::memory_order_relaxed))
            return false;

        dbgassertex(!allWritersStopped());

        //Note, compare_exchange_weak updates curState when it fails, so don't read inside the main loop
        unsigned numSpins = initialSpinsBeforeWait;
        state_t curState = state.load(std::memory_order_acquire);
        loop
        {
            unsigned curCount = (curState & countMask);
            if (curCount == maxItems)
            {
                if (--numSpins != 0) // likely
                {
                    curState = state.load(std::memory_order_acquire);
                    continue;
                }
                numSpins = initialSpinsBeforeWait;

                //The list is currently full, increment the number of writers waiting.
                //This can never overflow...
                const state_t nextState = curState + ((state_t)1 << writerShift);
                if (state.compare_exchange_weak(curState, nextState, std::memory_order_relaxed))
                {
                    if (aborted.load(std::memory_order_acquire))
                        return false;
                    writers.wait();
                    if (aborted.load(std::memory_order_acquire))
                        return false;
                    curState = state.load(std::memory_order_acquire);
                }
            }
            else
            {
                //Increment the number of items (which can never overflow), and possibly decrease readers
                state_t nextState = (curState + 1);

                //If a reader is waiting then decrement the count, and signal later..
                //Note, this test is a constant folded
                if (readerBits == 1)
                {
                    //More efficient to perform an unconditional mask
                    nextState &= ~readerMask;
                }
                else
                {
                    if ((curState & readerMask) != 0)
                        nextState -= (1 << readerShift);
                }
                if (state.compare_exchange_weak(curState, nextState, std::memory_order_relaxed))
                {
                    unsigned slotMask = (fixedSlotBits ? fixedSlotMask : dynamicSlotMask);
                    unsigned curDequeueSeq = (curState >> dequeueShift); // No need to mask since the top field
                    unsigned curEnqueueSeq = (curDequeueSeq + curCount) & sequenceMask;
                    unsigned filledSeq = (curEnqueueSeq + 1) & sequenceMask;
                    unsigned curEnqueueSlot = curEnqueueSeq & slotMask;
                    BufferElement & cur = values[curEnqueueSlot];

                    //MORE: Another producer has been interrupted while writing to the same slot
                    //or the consumer has not yet read from the slot.
                    //spin until that has been consumed.
                    unsigned spins = 0;
                    while (cur.sequence.load(std::memory_order_acquire) != curEnqueueSeq)
                    {
                        if (slotUnavailableSpins != 0 && ++spins == slotUnavailableSpins)
                        {
                            ThreadYield();
                            spins = 0;
                        }
                        else
                            spinPause();
                    }

                    //enqueue takes ownership of the object -> use std::move
                    cur.value = std::move(value);
                    cur.sequence.store(filledSeq, std::memory_order_release);
                    if ((curState & readerMask) != 0)
                        readers.signal();
                    return true;
                }
            }
        }
    }

    bool dequeue(ELEMENT & result, bool returnIfEmpty)
    {
        if (aborted.load(std::memory_order_relaxed))
            return false;

        unsigned numSpins = initialSpinsBeforeWait;
        //Note, compare_exchange_weak updates curState when it fails, so don't read inside the main loop
        state_t curState = state.load(std::memory_order_acquire);
        loop
        {
            unsigned curCount = (curState & countMask);
            //Check if the queue is empty
            if (curCount == 0)
            {
                if (returnIfEmpty)
                    return false;

                //If all writers have finished then no more items will be enqueued
                if (allWritersStopped())
                {
                    curState = state.load(std::memory_order_acquire);

                    //Check that nothing has been added since the previous load. (Very small window.)
                    if ((curState & countMask) == 0)
                        return false;
                    continue;
                }

                //We must check numSpins before we try and increment the number of readers
                if (--numSpins != 0)
                {
                    curState = state.load(std::memory_order_acquire);
                    continue;
                }
                numSpins = initialSpinsBeforeWait;

                //The list is currently empty, increment the number of readers waiting.
                //This can never overflow...
                state_t nextState = curState + (1 << readerShift);
                if (state.compare_exchange_weak(curState, nextState, std::memory_order_relaxed))
                {
                    if (aborted.load(std::memory_order_acquire))
                        return false;

                    //If no longer any active writers it may have happened before the cas, so the semaphore may not
                    //have been signalled. Either new items have been added, or the loop will terminate - so loop again.
                    if (!allWritersStopped())
                    {
                        readers.wait();
                        if (aborted.load(std::memory_order_acquire))
                            return false;
                    }
                    curState = state.load(std::memory_order_acquire);
                }
            }
            else
            {
                //Increase the dequeue position (which will harmlessly wrap), and decrement the count.
                state_t nextState = (curState + ((state_t)1 << dequeueShift) - 1);

                //If a reader is waiting then decrement the count, and signal later..
                if (writerBits == 1)
                {
                    //More efficient to perform an unconditional mask
                    //NOTE: check assembler to ensure it is merged with previous mask above
                    nextState &= ~writerMask;
                }
                else
                {
                    if ((curState & writerMask) != 0)
                        nextState -= (1 << writerShift);
                }
                if (state.compare_exchange_weak(curState, nextState, std::memory_order_relaxed))
                {
                    unsigned curDequeueSeq = (curState >> dequeueShift); // No need to mask since the top field
                    unsigned slotMask = (fixedSlotBits ? fixedSlotMask : dynamicSlotMask);
                    unsigned expectedSeq = (curDequeueSeq + 1) & sequenceMask;
                    unsigned curDequeueSlot = (curDequeueSeq & slotMask);
                    BufferElement & cur = values[curDequeueSlot];
                    unsigned spins = 0;
                    loop
                    {
                        unsigned sequence = cur.sequence.load(std::memory_order_acquire);
                        if (sequence == expectedSeq)
                            break;
                        //possibly yield every n iterations?
                        if (slotUnavailableSpins != 0 && ++spins == slotUnavailableSpins)
                        {
                            ThreadYield();
                            spins = 0;
                        }
                        else
                            spinPause();
                    }

                    result = std::move(cur.value);
                    const unsigned numSlots = slotMask + 1;
                    unsigned nextSeq = (curDequeueSeq + numSlots) & sequenceMask;
                    cur.sequence.store(nextSeq, std::memory_order_release);
                    if ((curState & writerMask) != 0)
                        writers.signal();
                    return true;
                }
            }
        }
    }

    virtual void reset()
    {
        activeWriters.store(maxWriters, std::memory_order_relaxed);
        aborted.store(false, std::memory_order_relaxed);
        readers.reinit(0);
        writers.reinit(0);
    }
    virtual void noteWriterStopped()
    {
        //MORE: If this reduces activeProducers to 0 then it may need to wake up any waiting threads.
        if (--activeWriters <= 0)
        {
            state_t curState = state.load(std::memory_order_acquire);
            unsigned readersWaiting = (unsigned)((curState & readerMask) >> readerShift);
            readers.signal(readersWaiting);
        }
    }
    virtual void abort()
    {
        //readers and writers may enqueue/dequeue another row before this takes effect
        aborted.store(true, std::memory_order_release);
        state_t curState = state.load(std::memory_order_acquire);
        unsigned readersWaiting = (unsigned)((curState & readerMask) >> readerShift);
        unsigned writersWaiting = (unsigned)((curState & writerMask) >> writerShift);
        readers.signal(readersWaiting);
        writers.signal(writersWaiting);
    }
    inline bool allWritersStopped() const { return activeWriters.load(std::memory_order_acquire) <= 0; }

protected:
    BufferElement * values;
    unsigned dynamicSlotMask;
    unsigned maxItems;
    unsigned maxWriters;
    std::atomic<int> activeWriters;
    std::atomic<bool> aborted;
    Semaphore readers __attribute__((aligned(CACHE_LINE_SIZE)));
    Semaphore writers __attribute__((aligned(CACHE_LINE_SIZE)));
    //Ensure the state is not on the same cache line as anything else, especially anything that is modified.
    std::atomic<state_t> state __attribute__((aligned(CACHE_LINE_SIZE)));
};

#endif
