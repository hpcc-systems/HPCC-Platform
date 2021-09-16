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

public:
    MemoryBuffer data;

    IMPLEMENT_IINTERFACE;
    CDummyMessagePacker()
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
