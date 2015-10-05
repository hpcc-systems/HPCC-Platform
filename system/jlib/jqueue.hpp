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
#include "jatomic.hpp"

template <typename ELEMENT>
interface IQueueOf : extends IInterface
{
public:
    virtual void enqueue(const ELEMENT item) = 0;
    virtual unsigned enqueue(size_t count, ELEMENT * items) = 0;
    virtual ELEMENT dequeue() = 0;
};


/*
 This uses one spare slot in the array to differentiate between a full queue and an empty queue.
 This ensures that writer cannot overtake the readers and create an ABA problem
 i) The writer only fills in a blank entry
 ii) the reader sets to null after reading
 */

//NOTE: maxSlotBits * 2 + writerBits * 2 + readerBits <= sizeof(ELEMENT)*8)
template <typename ELEMENT, typename state_t, unsigned readerBits, unsigned writerBits, unsigned maxSlotBits, unsigned fixedSlotBits=0>
class ReaderWriterQueue
{
    //state has [dequeue-pos][reader-count][writer-count][num-items] in a single field.
    //num-items is the least significant field (so does not need to be shifted)
    //dequeue-pos it the most significant field, so there is no need to worry about wrapping into other fields.
    //dequeue-pos must have enough extra bits to disambiguate N writers all
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
    const static unsigned initialSpinsBeforeWait = 20000;
    const static state_t fixedSlotMask = (1U << fixedSlotBits) - 1;

public:
    ReaderWriterQueue(unsigned _maxItems) : maxItems(_maxItems)
    {
        //printf("element(%u) pad(%u) write(%u), read(%u) slot(%u) count(%u) max(%u)\n", stateBits, padBits, writerBits, readerBits, maxSlotBits, countBits, maxItems);
        //Check all the bits are used, and none of the bits overlap.
        assertex(padBits < stateBits);
        assertex(countBits >= maxSlotBits);
        assertex(sequenceBits + readerBits + writerBits + countBits == stateBits);
        assertex((readerMask | writerMask | countMask | dequeueMask) == (state_t)-1);
        assertex((readerMask | writerMask | countMask | dequeueMask) == (readerMask ^ writerMask ^ countMask ^ dequeueMask));

        // Reserve at least one free entry to be able to tell when the queue is full.
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
            dynamicSlotMask = numSlots -1;
        }
        else
        {
            numSlots = fixedSlotMask + 1;
            dynamicSlotMask = fixedSlotMask;
        }

        state.store(0, std::memory_order_relaxed);
        values = new BufferElement[numSlots];
        for (unsigned i=0; i < numSlots; i++)
            values[i].sequence.store(i, std::memory_order_relaxed);
    }
    ~ReaderWriterQueue()
    {
        delete [] values;
    }

    void enqueue(const ELEMENT value)
    {
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
                    writers.wait();
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
                    while (cur.sequence.load(std::memory_order_acquire) != curEnqueueSeq)
                    {
                        spinPause();
                        //more: option to back off and yield.
                    }

                    cur.value = value;
                    cur.sequence.store(filledSeq, std::memory_order_release);
                    if ((curState & readerMask) != 0)
                        readers.signal();
                    return;
                }
            }
        }
    }

    //Enqueue up to n items, if there is no space, block.  Otherwise enqueue as many as possible, and return the number enqueued
    //MORE: Should this block until all queued, or return after adding some?  Two functions?
    unsigned enqueue(unsigned num, ELEMENT * value)
    {
        //Note, compare_exchange_weak updates curState when it fails, so don't read inside the main loop
        unsigned numSpins = initialSpinsBeforeWait;
        state_t curState = state.load(std::memory_order_acquire);
        loop
        {
            unsigned curCount = (curState & countMask);
            unsigned spaces = maxItems - curCount;
            if (spaces == 0)
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
                    writers.wait();
                    curState = state.load(std::memory_order_acquire);
                }
            }
            else
            {
                unsigned numChunk = (num > spaces) ? spaces : num;
                //Increment the number of items (which can never overflow), and possibly decrease readers
                state_t nextState = (curState + numChunk);

                //If a reader is waiting then decrement the count, and signal later..
                //Note, this test is a constant folded
                unsigned wakeUp;
                if (readerBits == 1)
                {
                    //More efficient to perform an unconditional mask
                    nextState &= ~readerMask;
                    wakeUp = (curState & readerMask) ? 1 : 0;
                }
                else
                {
                    unsigned readersWaiting = (curState & readerMask) >> readerShift;
                    wakeUp = (readersWaiting > numChunk) ? numChunk : readersWaiting;
                    nextState -= (wakeUp << readerShift);
                }
                if (state.compare_exchange_weak(curState, nextState, std::memory_order_relaxed))
                {
                    unsigned slotMask = (fixedSlotBits ? fixedSlotMask : dynamicSlotMask);
                    unsigned curDequeueSeq = (curState >> dequeueShift); // No need to mask since the top field

                    for (unsigned i=0; i < numChunk; i++)
                    {
                        unsigned curEnqueueSeq = (curDequeueSeq + curCount + i) & sequenceMask;
                        unsigned filledSeq = (curEnqueueSeq + 1) & sequenceMask;
                        unsigned curEnqueueSlot = curEnqueueSeq & slotMask;
                        BufferElement & cur = values[curEnqueueSlot];

                        //MORE: Another producer has been interrupted while writing to the same slot
                        //or the consumer has not yet read from the slot.
                        //spin until that has been consumed.
                        while (cur.sequence.load(std::memory_order_acquire) != curEnqueueSeq)
                        {
                            spinPause();
                            //more: option to back off and yield.
                        }

                        cur.value = value[i];
                        cur.sequence.store(filledSeq, std::memory_order_release);
                    }
                    while (wakeUp--)
                        readers.signal();
                    return numChunk;
                }
            }
        }
    }

    ELEMENT dequeue()
    {
        unsigned numSpins = initialSpinsBeforeWait;
        //Note, compare_exchange_weak updates curState when it fails, so don't read inside the main loop
        state_t curState = state.load(std::memory_order_acquire);
        loop
        {
            unsigned curCount = (curState & countMask);
            //Check if the queue is empty
            if (curCount == 0)
            {
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
                    readers.wait();
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
                    loop
                    {
                        unsigned sequence = cur.sequence.load(std::memory_order_acquire);
                        if (sequence == expectedSeq)
                            break;
                        //possibly yield every n iterations?
                        spinPause();
                    }

                    ELEMENT ret = cur.value;
                    const unsigned numSlots = slotMask + 1;
                    unsigned nextSeq = (curDequeueSeq + numSlots) & sequenceMask;
                    cur.sequence.store(nextSeq, std::memory_order_release);
                    if ((curState & writerMask) != 0)
                        writers.signal();
                    return ret;
                }
            }
        }
    }

protected:
    BufferElement * values;
    unsigned dynamicSlotMask;
    unsigned maxItems;
    Semaphore readers;
    Semaphore writers;
    //Ensure the state is not on the same cache line as anything else, especially anything that is modified.
    char pad[64-sizeof(state_t)];
    std::atomic<state_t> state;
};

typedef IQueueOf<const void *> IRowQueue;
extern IRowQueue * createRowQueue(unsigned numReaders, unsigned numWriters, unsigned maxItems, unsigned maxSlots);

#endif
