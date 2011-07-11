/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
        data.setLength(lastput);
        if (variable)
        {
            char *ret = (char *) data.reserve(len + sizeof(RecordLengthType));
            return ret + sizeof(RecordLengthType);
        }
        else
        {
            return data.reserve(len);
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
        data.setLength(lastput + len);
        lastput += len;
    }

    virtual void flush(bool last_message) { data.setLength(lastput); }
    virtual void abort() {}
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

#endif
