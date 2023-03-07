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

#ifndef CCDQUEUE_IPP_INCL
#define CCDQUEUE_IPP_INCL

#include <udplib.hpp>

extern IpAddress multicastBase;
extern IpAddress multicastLast;

// RoxieOutputQueueManager is really just managing the global communicator

//=================================================================================

interface IPendingCallback : public IInterface
{
    virtual bool wait(unsigned timeoutMsecs) = 0;
    virtual MemoryBuffer &queryData() = 0;
};


class CDummyMessagePacker : implements IMessagePacker, public CInterface
{
protected:
    unsigned lastput;
    const IOutputMetaData *outputMeta = nullptr;
    unsigned activityId = 0;

public:
    MemoryBuffer data;

    IMPLEMENT_IINTERFACE;
    CDummyMessagePacker(const IOutputMetaData *_outputMeta, unsigned _activityId) : outputMeta(_outputMeta), activityId(_activityId)
    {
        lastput = 0;
    }

    virtual void *getBuffer(unsigned len, bool variable) override
    {
        if (variable)
        {
            char *ret = (char *) data.ensureCapacity(len + sizeof(RecordLengthType));
            return ret + sizeof(RecordLengthType);
        }
        else
        {
            return data.ensureCapacity(len);
        }
    }

    virtual void putBuffer(const void *buf, unsigned len, bool variable) override
    {
        assertex(!outputMeta);
        if (variable)
        {
            buf = ((char *) buf) - sizeof(RecordLengthType);
            *(RecordLengthType *) buf = len;
            len += sizeof(RecordLengthType);
        }
        data.setWritePos(lastput + len);
        lastput += len;
    }

    virtual void flush() override { }
    virtual void sendMetaInfo(const void *buf, unsigned len) override { throwUnexpected(); }
    virtual unsigned size() const override { return lastput; }

// The implementation of IEngineRowAllocator allows us to construct rows directly into the buffer that is going to be used to return them
// In localAgent mode we could actually go one better and construct rows directly in the corresponding caller's allocator, I would have thought...
// but for the standard case constructing directly into the return buffers could be quite advantageous (we already do for fixed size).
// Many of these methods are not needed, since anything involoving child rows is constructed in a standard row allocator then serialized.

    virtual const byte * * createRowset(unsigned _numItems) override { throwUnexpected(); };
    virtual const byte * * linkRowset(const byte * * rowset) override { throwUnexpected(); };
    virtual void releaseRowset(unsigned count, const byte * * rowset) override { throwUnexpected(); };
    virtual const byte * * appendRowOwn(const byte * * rowset, unsigned newRowCount, void * row) override { throwUnexpected(); };
    virtual const byte * * reallocRows(const byte * * rowset, unsigned oldRowCount, unsigned newRowCount) override { throwUnexpected(); };
    virtual void * linkRow(const void * row) override { throwUnexpected(); };

    virtual void * createRow() override { size32_t dummy; return createRow(dummy); };
    virtual void releaseRow(const void * row) override { };

//Used for dynamically sizing rows.
    virtual void * createRow(size32_t & allocatedSize) override 
    {
        assertex(outputMeta);
        unsigned lenSize = outputMeta->isFixedSize() ? 0 : sizeof(RecordLengthType);
        char *ret = (char *) data.ensureCapacity(outputMeta->getMinRecordSize() + lenSize);
        allocatedSize = data.capacity() - lenSize;
        return ret + lenSize;
    };
    virtual void * createRow(size32_t initialSize, size32_t & allocatedSize) override { UNIMPLEMENTED; };
    virtual void * resizeRow(size32_t newSize, void * row, size32_t & size) override 
    {
        assertex(outputMeta);
        unsigned lenSize = outputMeta->isFixedSize() ? 0 : sizeof(RecordLengthType);
        char *ret = (char *) data.ensureCapacity(newSize + lenSize);
        size = data.capacity() - lenSize;
        return ret + lenSize;
    };
    virtual void * finalizeRow(size32_t newSize, void * row, size32_t oldSize) override 
    {
        assertex(outputMeta);
        unsigned lenSize = outputMeta->isFixedSize() ? 0 : sizeof(RecordLengthType);
        assert(row == (char *) data.reserve(0)+lenSize);
        if (lenSize)
            ((RecordLengthType *) row)[-1] = newSize;
        data.setWritePos(lastput + newSize + lenSize);
        lastput += newSize + lenSize;
        // MORE - could truncate - any point?
        return row;
    };
    virtual IOutputMetaData * queryOutputMeta() override { return const_cast<IOutputMetaData *>(outputMeta); };
    virtual unsigned queryActivityId() const override { return activityId; };
    virtual StringBuffer &getId(StringBuffer &idStr) override { return idStr.append(activityId); };

    virtual IOutputRowSerializer *createDiskSerializer(ICodeContext *ctx = NULL) override { throwUnexpected(); };
    virtual IOutputRowDeserializer *createDiskDeserializer(ICodeContext *ctx) override { throwUnexpected(); };
    virtual IOutputRowSerializer *createInternalSerializer(ICodeContext *ctx = NULL) override { throwUnexpected(); };
    virtual IOutputRowDeserializer *createInternalDeserializer(ICodeContext *ctx) override { throwUnexpected(); };
    virtual IEngineRowAllocator *createChildRowAllocator(const RtlTypeInfo *childtype) override { throwUnexpected(); };
    virtual void gatherStats(CRuntimeStatisticCollection & stats) override { throwUnexpected(); };
    virtual void releaseAllRows() override { throwUnexpected(); };

};

interface IPacketDiscarder : public IInterface
{
    virtual void start() = 0;
    virtual void stop() = 0;
};

extern IRoxieOutputQueueManager *createOutputQueueManager(unsigned numWorkers, bool encrypted);
extern IReceiveManager *createLocalReceiveManager();
extern IPacketDiscarder *createPacketDiscarder();
extern void startPingTimer();
extern void stopPingTimer();
extern void closeMulticastSockets();
extern void sendUnloadMessage(hash64_t hash, const char *id, const IRoxieContextLogger &logctx);

extern unsigned getReplicationLevel(unsigned channel);

#endif
