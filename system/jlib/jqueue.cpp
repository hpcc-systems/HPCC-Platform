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


#include "platform.h"
#include "jmutex.hpp"
#include "jqueue.hpp"

//---------------------------------------------------------------------------------------------------------------------
template <typename state_t, unsigned readerBits, unsigned writerBits, unsigned maxSlotBits, unsigned slotBits>
class CRowQueue : implements CInterfaceOf<IRowQueue>
{
public:
    CRowQueue(unsigned _maxItems, unsigned _numProducers) : queue(_numProducers, _maxItems), numProducers(_numProducers)
    {
    }

    virtual bool enqueue(const void * const item)
    {
        return queue.enqueue(item);
    }
    virtual bool dequeue(const void * & result)
    {
        return queue.dequeue(result, false);
    }
    virtual bool tryDequeue(const void * & result)
    {
        return queue.dequeue(result, true);
    }
    virtual void reset()
    {
        //This resets the state, but does not clear the elements, they must be dequeued and released.
        queue.reset();
        //How clean up the queue and ensure the elements are disposed of?
    }
    virtual void noteWriterStopped()
    {
        queue.noteWriterStopped();
    }
    virtual void abort()
    {
        queue.abort();
    }

private:
    ReaderWriterQueue<const void *, state_t, readerBits, writerBits, maxSlotBits, slotBits> queue;
    const unsigned numProducers;
};


IRowQueue * createRowQueue(unsigned numReaders, unsigned numWriters, unsigned maxItems, unsigned maxSlots)
{
    //Ideally if the numberOfReaders or writers is 1 then ideally, supply 1 for the relevant values.
    //
    assertex(maxSlots == 0 || maxItems < maxSlots);

    if ((numReaders == 1) && (numWriters == 1) && (maxItems < 256))
        return new CRowQueue<unsigned, 1, 1, 8, 8>(maxItems, numWriters);

    if ((numReaders == 1) && (numWriters == 1) && (maxItems < 0x4000))
        return new CRowQueue<unsigned, 1, 1, 14, 0>(maxItems, numWriters);

    if ((numReaders == 1) && (numWriters <= 127) && (maxItems < 256))
        return new CRowQueue<unsigned, 1, 7, 8, 0>(maxItems, numWriters);

    if ((numWriters == 1) && (numReaders <= 255) && (maxItems < 2048))
        return new CRowQueue<unsigned, 8, 1, 11, 0>(maxItems, numWriters);

    if ((numReaders <= 31) && (numWriters <= 31) && (maxItems < 128))
        return new CRowQueue<unsigned, 6, 6, 7, 0>(maxItems, numWriters);

    assertex((numReaders < 0x1000) && (numWriters < 0x400));
    return new CRowQueue<unsigned __int64, 12, 10, 16, 0>(maxItems, numWriters);
}

//MORE:
//use likely()/unlikely() - they might improve the code
//Adaptive spin counts - separate variables for reader/writer.  reduce number of spins if a producer/consumer blocks.
//Add options to indicate spinning for readers or writers is ok
//Use a traits class instead of multiple parameters?
//If readers or writers spin, then there is no need to keep track of waiting counts in the state.
//Base the blog on an earlier simpler example - without the full templatization
