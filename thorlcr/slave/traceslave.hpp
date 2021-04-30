/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

#ifndef TRACESLAVE_HPP
#define TRACESLAVE_HPP

#include <eclhelper.hpp>
#include <jatomic.hpp>
#include <jbuff.hpp>
#include <jdebug.hpp>
#include <jiface.hpp>
#include <jlog.hpp>
#include <jscm.hpp>
#include <jstats.h>
#include <jstring.hpp>
#include <platform.h>
#include <stddef.h>
#include <thmem.hpp>
#include <thorxmlwrite.hpp>
#include <rtlformat.hpp>

interface IThorDebug : extends IInterface
{
    virtual void debugRequest(MemoryBuffer &mb) = 0;
};

class CTracingStream : public CSimpleInterfaceOf<IEngineRowStream>, implements IThorDebug
{
private:
    class CThorRowHistory
    {
        OwnedConstThorRow row;
        __int64 cycleStamp;

    public:
        inline CThorRowHistory() : cycleStamp(0) { }

        inline void set(const void * _row, __int64 _cycleStamp)
        {
            cycleStamp = _cycleStamp;
            row.set(_row);
        }
        inline void getTimeStamp(StringBuffer &retTimeStamp) const
        {
            unsigned __int64 nowUSec = getTimeStampNowValue();
            __int64 diffUSec = cycle_to_microsec(get_cycles_now() - cycleStamp);
            unsigned __int64 adjustedTime = nowUSec - diffUSec;
            formatTimeStampAsLocalTime(retTimeStamp, adjustedTime);
        }
        inline const void * get() const      { return row.get(); }
        inline void clear()                  { row.clear(); }
        inline __int64 getCycleStamp() const { return cycleStamp; }
    };
    class CTraceQueue
    {
    public:
        CThorRowHistory *rowBuffer;
        unsigned rowBufferSize;
        unsigned rowTailp;

        CTraceQueue() : rowBuffer(NULL), rowBufferSize(0), rowTailp(0) { }

        ~CTraceQueue()
        {
            delete [] rowBuffer;
        }
        void init(unsigned _bufsize)
        {
            if (!rowBuffer && _bufsize)
            {
                rowBufferSize = _bufsize;
                rowBuffer = new CThorRowHistory[rowBufferSize];
                rowTailp = 0;
            }
        }
        inline void enqueue(const void *row, __int64 _cycleStamp)
        {
            // NOTE - updates then sets rowHeadp (atomically) so that the code
            // below (ignoring oldest record) is safe
            rowBuffer[rowTailp].set(row, _cycleStamp);
            unsigned nextTailp = rowTailp + 1;
            if (nextTailp==rowBufferSize)
                nextTailp = 0;
            rowTailp = nextTailp;
        }
        inline void enqueue(const CThorRowHistory &_rowBuffer)
        {
            enqueue(_rowBuffer.get(),_rowBuffer.getCycleStamp());
        }
        // Move items in current queue into a second queue (clear out 'this' queue)
        // NOTE - set skipLast to true when the oldest item must be skipped.  This should
        // be used where the queue COULD have been updated after the buffers are swapped and
        // the enqueueRowForTrace overwriting the oldest entry.
        void queueOut(CTraceQueue &traceOutQueue, bool skipOldestEntry)
        {
            const unsigned numberRowsToSkip = skipOldestEntry ? 1 : 0;
            unsigned rowPos = rowTailp + numberRowsToSkip;
            if (rowPos==rowBufferSize) rowPos = 0;
            for (unsigned n=numberRowsToSkip; n<rowBufferSize;++n)
            {
                if (rowBuffer[rowPos].get())
                {
                    traceOutQueue.enqueue(rowBuffer[rowPos]);
                    rowBuffer[rowPos].clear();
                }
                if (++rowPos==rowBufferSize) rowPos = 0;
            }
        }
        // Dump items to MemoryBuffer (without clearing items from queue)
        void dump(MemoryBuffer &mb, IHThorArg *helper) const
        {
            CommonXmlWriter xmlwrite(XWFnoindent|XWFtrim);
            IOutputMetaData *meta = helper->queryOutputMeta();
            unsigned rowPos = rowTailp;
            for (unsigned n=0; n<rowBufferSize;++n)
            {
                const CThorRowHistory &rowCurrent = rowBuffer[rowPos];
                if (rowCurrent.get())
                {
                    xmlwrite.outputBeginNested("Row", true);
                    xmlwrite.outputInt(meta->getRecordSize(rowCurrent.get()), sizeof(size32_t), "@size");
                    StringBuffer timeStamp;
                    rowCurrent.getTimeStamp(timeStamp);
                    xmlwrite.outputString(timeStamp.length(), timeStamp.str(), "@timestamp");
                    try
                    {
                        meta->toXML((const byte*)(rowCurrent.get()), xmlwrite);
                    }
                    catch (IException *e)
                    {
                        EXCLOG(e, "CTracingThorDataLink::dump");
                        e->Release();
                    }
                    xmlwrite.outputEndNested("Row", true);
                    mb.append(xmlwrite.str());
                    xmlwrite.clear();
                }
                if (++rowPos==rowBufferSize) rowPos = 0;
            }
        }
    };
    const unsigned traceQueueSize;
    CTraceQueue buffers[2];
    CTraceQueue rowBufferLogCache;
    std::atomic<unsigned> rowBufInUse;
    IThorDataLink *thorDataLink;
    IEngineRowStream *inputStream;
    IHThorArg *helper;

    inline void enqueueRowForTrace(const void *row)
    {
        buffers[rowBufInUse].enqueue(row, thorDataLink->queryEndCycles());
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterfaceOf<IEngineRowStream>);

    virtual void debugRequest(MemoryBuffer &mb) override
    {
        // NOTE - cannot be called by more than one thread
        buffers[1].init(traceQueueSize+1);
        rowBufferLogCache.init(traceQueueSize);
        int bufToDump = rowBufInUse;
        int inactiveBuf = 1 - bufToDump;

        // Queue any remaining rows from inactive buffer as the oldest row is skipped
        // when queue out from active buffer to avoid race condition issues (this inactive buffer
        // may have been formerly an active buffer)
        buffers[inactiveBuf].queueOut(rowBufferLogCache, false);

        rowBufInUse = inactiveBuf;// Swap Active & inactiveBuf
        buffers[bufToDump].queueOut(rowBufferLogCache, true);
        rowBufferLogCache.dump(mb, helper);
    }

// IEngineRowStream
    virtual void resetEOF() override { inputStream->resetEOF(); }
    virtual const void *nextRow() override
    {
        const void *row = inputStream->nextRow();
        enqueueRowForTrace(row);
        return row;
    }
    virtual void stop() override
    {
        inputStream->stop();
    }

    CTracingStream(IThorDataLink *_thorDataLink, IEngineRowStream *_inputStream, IHThorArg *_helper, unsigned _traceQueueSize)
        : thorDataLink(_thorDataLink), inputStream(_inputStream), helper(_helper), traceQueueSize(_traceQueueSize)
    {
        rowBufInUse = 0;
        buffers[0].init(traceQueueSize+1);
    }
};

#endif
