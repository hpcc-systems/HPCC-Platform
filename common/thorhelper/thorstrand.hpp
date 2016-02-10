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

#ifndef THORSTRAND_HPP
#define THORSTRAND_HPP

#include "jqueue.hpp"
#include "thorhelper.hpp"
#include "roxiestream.hpp"
#include "roxiemem.hpp"

class IStrandJunction : extends IInterface
{
public:
    virtual IEngineRowStream * queryOutput(unsigned n) = 0;
    virtual void setInput(unsigned n, IEngineRowStream * _stream) = 0;
    virtual void start() = 0;
    virtual void reset() = 0;
    virtual void abort() = 0;
};

inline void startJunction(IStrandJunction * junction) { if (junction) junction->start(); }
inline void resetJunction(IStrandJunction * junction) { if (junction) junction->reset(); }

interface IManyToOneRowStream : extends IRowStream
{
public:
    virtual IRowWriterEx * getWriter(unsigned n) = 0;
    virtual void abort() = 0;
};

interface IStrandBranch : extends IInterface
{
    virtual IStrandJunction * queryInputJunction() = 0;
    virtual IStrandJunction * queryOutputJunction() = 0;
};

extern THORHELPER_API IStrandJunction * createStrandJunction(roxiemem::IRowManager & _rowManager, unsigned numInputs, unsigned numOutputs, unsigned blockSize, bool isOrdered);
extern THORHELPER_API IStrandBranch * createStrandBranch(roxiemem::IRowManager & _rowManager, unsigned numStrands, unsigned blockSize, bool isOrdered, bool isGrouped);
extern THORHELPER_API void clearRowQueue(IRowQueue * queue);

extern THORHELPER_API IManyToOneRowStream * createManyToOneRowStream(roxiemem::IRowManager & _rowManager, unsigned numInputs, unsigned blockSize, bool isOrdered);

//---------------------------------------------------------------------------------------------------------------------

class RowBlockAllocator;
class THORHELPER_API RoxieRowBlock
{
public:
    const static unsigned numDummyDynamicRows = 1;
    explicit RoxieRowBlock(unsigned _maxRows) noexcept : maxRows(_maxRows)
    {
        readPos = 0;
        writePos = 0;
        endOfChunk = false;
    }
    ~RoxieRowBlock();

    inline bool addRowNowFull(const void * row)
    {
        dbgassertex(writePos < maxRows);
        rows[writePos] = row;
        return (++writePos == maxRows);
    }

    bool empty() const;
    IException * getClearException()
    {
         return exception.getClear();
    }
    inline bool isEndOfChunk() const { return endOfChunk; }
    inline bool nextRow(const void * & row)
    {
        if (readPos >= writePos)
            return false;
        row = rows[readPos++];
        return true;
    }
    inline size32_t numRows() const { return writePos - readPos; }

    bool readFromStream(IRowStream * stream);
    inline void releaseBlock()
    {
        //This function is called instead of directly calling delete in case a cache is introduced later.
        delete this;
    }
    void releaseRows();

    inline void setEndOfChunk() { endOfChunk = true; }
    inline void setExceptionOwn(IException * e) { exception.setown(e); }

    void throwAnyPendingException();

    static void operator delete (void * ptr);

protected:
    Owned<IException> exception;
    const size32_t maxRows;
    size32_t readPos;
    size32_t writePos;
    bool endOfChunk;
    const void * rows[numDummyDynamicRows];        // Actually multiple rows.  Memory is allocated by the RowBlockAllocator.
};


class THORHELPER_API RowBlockAllocator
{
public:
    RowBlockAllocator(roxiemem::IRowManager & _rowManager, unsigned rowsPerBlock);
    RoxieRowBlock * newBlock();

    size32_t maxRowsPerBlock() const { return rowsPerBlock; }

public:
    size32_t rowsPerBlock;
    Owned<roxiemem::IFixedRowHeap> heap;
};


//---------------------------------------------------------------------------------------------------------------------

typedef IQueueOf<RoxieRowBlock *> IRowBlockQueue;


//MORE:  This implementation should be improved!  Directly use the correct queue implementation??
class CRowBlockQueue : implements CInterfaceOf<IRowBlockQueue>
{
public:
    CRowBlockQueue(unsigned numReaders, unsigned numWriters, unsigned maxItems, unsigned maxSlots)
    {
        queue.setown(createRowQueue(numReaders, numWriters, maxItems, maxSlots));
    }

    virtual bool enqueue(RoxieRowBlock * const item)
    {
        return queue->enqueue(reinterpret_cast<const void *>(item));
    }
    virtual bool dequeue(RoxieRowBlock * & result)
    {
        const void * tempResult;
        bool ok = queue->dequeue(tempResult);
        result = const_cast<RoxieRowBlock *>(reinterpret_cast<const RoxieRowBlock *>(tempResult));
        return ok;
    }
    virtual bool tryDequeue(RoxieRowBlock * & result)
    {
        const void * tempResult;
        bool ok = queue->tryDequeue(tempResult);
        result = const_cast<RoxieRowBlock *>(reinterpret_cast<const RoxieRowBlock *>(tempResult));
        return ok;
    }
    virtual void reset()
    {
        queue->reset();
    }
    virtual void noteWriterStopped()
    {
        queue->noteWriterStopped();
    }
    virtual void abort()
    {
        queue->abort();
    }

private:
    Owned<IRowQueue> queue;
};





#endif // THORSTRAND_HPP
