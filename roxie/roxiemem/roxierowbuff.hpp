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

#ifndef _ROXIEROWBUFF_INCL
#define _ROXIEROWBUFF_INCL

#include "roxiemem.hpp"

namespace roxiemem {

/*
This base class provides an array of row pointers that can be used to implement a buffer.
It is designed to be thread safe, but reduce critical section calls on the common cases.

There are 3 ranges
  0..firstRow - pointers that are no longer considered part of the array.
  firstRow..commitRows - rows that need to be accessed within a critical section, and can be spilled/read as a block.
  commitRows..maxRows - a range of rows that can be written without requiring a lock

Rows and row arrays are allocated from the rowmanager.

The class is not intended to be used for reading rows - they are tranferred to a InputRowArray instance instead.
In the future better read support might be added - it needs more thought.

A derived implementation resizes the array if necessary.

*/

//This allows up to 32Gb of pointers to rows on a 64bit machine.  That is likely to be enough for the next few months.
typedef size32_t rowidx_t;

class roxiemem_decl RoxieOutputRowArray
{
public:
    RoxieOutputRowArray(IRowManager * _rowManager, rowidx_t _initialSize, size32_t _commitDelta);
    inline ~RoxieOutputRowArray() { kill(); }

    //The following can be called from the writer, without any need to lock first.
    inline bool append(const void * row)
    {
        if (numRows >= maxRows)
        {
            if (!ensure(numRows+1))
            {
                flush();
                if (numRows >= maxRows)
                    return false;
            }
        }

        rows[numRows++] = row;
        if (numRows >= commitRows + commitDelta)
            flush();
        return true;
    }
    void flush();

    //The following can be accessed from the reader without any need to lock
    const void * query(rowidx_t i) const;
    const void * getClear(rowidx_t i);
    const void * get(rowidx_t i) const;

    //A thread calling the following functions must own the lock, or guarantee no other thread will access
    const void * * getBlock(rowidx_t readRows);
    void noteSpilled(rowidx_t spilledRows);
    void clearRows();
    void kill();
    void transferRows(rowidx_t & outNumRows, const void * * & outRows);

    //The block returned is only valid until the critical section is released

    inline rowidx_t firstCommitted() const { return firstRow; }
    inline rowidx_t numCommitted() const { return commitRows - firstRow; }

    //Locking functions - use the RoxieOutputRowArrayLock class below.
    inline void lock() const { cs.enter(); }
    inline void unlock() const { cs.leave(); }

protected:
    virtual bool ensure(rowidx_t requiredRows) { return false; }

protected:
    IRowManager * rowManager;
    const void * * rows;
    rowidx_t maxRows;  // Number of rows that can fit in the allocated memory.
    rowidx_t firstRow; // Only rows firstRow..numRows are considered initialized.  Only read/write within cs.
    rowidx_t numRows;  // rows that have been added can only be updated by writing thread.
    rowidx_t commitRows;  // can only be updated by writing thread within a critical section
    const size32_t commitDelta;  // How many rows need to be written before they are added to the committed region?
    mutable CriticalSection cs;
};

class roxiemem_decl DynamicRoxieOutputRowArray : public RoxieOutputRowArray
{
public:
    DynamicRoxieOutputRowArray(IRowManager * _rowManager, rowidx_t _initialSize, size32_t _commitDelta)
        : RoxieOutputRowArray(_rowManager, _initialSize, _commitDelta) {}

protected:
    virtual bool ensure(rowidx_t requiredRows);
};

class RoxieOutputRowArrayLock
{
public:
    RoxieOutputRowArrayLock(const RoxieOutputRowArray & _rows) : rows(_rows) { rows.lock(); }
    ~RoxieOutputRowArrayLock() { rows.unlock(); }
private:
    RoxieOutputRowArrayLock(RoxieOutputRowArrayLock &);
    const RoxieOutputRowArray & rows;
};

//---------------------------------------------------------------------------------------------------------------------

//A very simple buffer for reading values from.  Not really useful for multi threaded access.
class roxiemem_decl RoxieSimpleInputRowArray
{
public:
    RoxieSimpleInputRowArray() { rows = NULL; firstRow = 0; numRows = 0; }
    RoxieSimpleInputRowArray(const void * * _rows, size32_t _numRows);
    inline ~RoxieSimpleInputRowArray() { kill(); }

    //The following are threadsafe, if only think called.
    inline const void * query(rowidx_t i) const
    {
        if ((i < firstRow) || (i >= numRows))
            return NULL;
        return rows[i];
    }
    const void * get(rowidx_t i) const
    {
        if ((i < firstRow) || (i >= numRows))
            return NULL;
        const void * row = rows[i];
        if (row)
            LinkRoxieRow(row);
        return row;
    }

    //only thread safe if no other thread accesses the same element
    const void * getClear(rowidx_t i)
    {
        if ((i < firstRow) || (i >= numRows))
            return NULL;
        const void * row = rows[i];
        rows[i] = NULL;
        return row;
    }

    //The following are not thread safe.
    inline const void * dequeue()
    {
        if (firstRow >= numRows)
            return NULL;
        return rows[firstRow++];
    }

    void kill();
    void transferFrom(RoxieOutputRowArray & donor);

protected:
    const void * * rows;
    rowidx_t firstRow; // Only rows firstRow..numRows are considered initialized.
    rowidx_t numRows;
};


}

#endif
