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

interface IRoxieOutputQueueManager : public IInterface
{
    virtual void sendPacket(IRoxieQueryPacket *x, const IRoxieContextLogger &logctx) = 0;
    virtual void sendIbyti(RoxiePacketHeader &header, const IRoxieContextLogger &logctx) = 0;
    virtual void sendAbort(RoxiePacketHeader &header, const IRoxieContextLogger &logctx) = 0;
    virtual void sendAbortCallback(const RoxiePacketHeader &header, const char *lfn, const IRoxieContextLogger &logctx) = 0;
    virtual IMessagePacker *createOutputStream(RoxiePacketHeader &x, bool outOfBand, const IRoxieContextLogger &logctx) = 0;
    virtual bool replyPending(RoxiePacketHeader &x) = 0;
    virtual bool abortCompleted(RoxiePacketHeader &x) = 0;
    virtual bool suspendChannel(unsigned channel, bool suspend, const IRoxieContextLogger &logctx) = 0;
    virtual bool checkSuspended(const RoxiePacketHeader &header, const IRoxieContextLogger &logctx) = 0;

    virtual unsigned getHeadRegionSize() const = 0;
    virtual void setHeadRegionSize(unsigned newsize) = 0;

    virtual void start() = 0;
    virtual void stop() = 0;
    virtual void join() = 0;
    virtual IReceiveManager *queryReceiveManager() = 0;

    virtual IPendingCallback *notePendingCallback(const RoxiePacketHeader &header, const char *lfn) = 0;
    virtual void removePendingCallback(IPendingCallback *x) = 0;
};

class CDummyMessagePacker : public CInterface, implements IMessagePacker
{
protected:
    unsigned lastput;

public:
    MemoryBuffer data;

    IMPLEMENT_IINTERFACE;
    CDummyMessagePacker()
    {
        lastput = 0;
    }

    virtual bool dataQueued() 
    {
        return false;
    }


    virtual void *getBuffer(unsigned len, bool variable)
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

    virtual void putBuffer(const void *buf, unsigned len, bool variable)
    {
        if (variable)
        {
            buf = ((char *) buf) - sizeof(RecordLengthType);
            *(RecordLengthType *) buf = len;
            len += sizeof(RecordLengthType);
        }
        data.setWritePos(lastput + len);
        lastput += len;
    }

    virtual void flush(bool last_message) { }
    virtual void sendMetaInfo(const void *buf, unsigned len) { throwUnexpected(); }
    virtual unsigned size() const { return lastput; }
};

interface IPacketDiscarder : public IInterface
{
    virtual void start() = 0;
    virtual void stop() = 0;
};

extern IRoxieOutputQueueManager *ROQ;
extern IRoxieOutputQueueManager *createOutputQueueManager(unsigned snifferChannel, unsigned numWorkers);
extern IReceiveManager *createLocalReceiveManager();
extern IPacketDiscarder *createPacketDiscarder();
extern void startPingTimer();
extern void stopPingTimer();
extern void closeMulticastSockets();
extern void sendUnloadMessage(hash64_t hash, const char *id, const IRoxieContextLogger &logctx);

#endif
