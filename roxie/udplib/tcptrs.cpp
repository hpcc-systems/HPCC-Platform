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

#include "udplib.hpp"
#include "udpsha.hpp"
#include "udptrs.hpp"
#include "udpipmap.hpp"

#include "jsocket.hpp"
#include "jlog.hpp"
#include "jencrypt.hpp"
#include "jsecrets.hpp"
#include "jiouring.hpp"
#include "roxie.hpp"
#ifdef _WIN32
#include <winsock.h>
#else
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif
#include <math.h>
#include <atomic>
#include <algorithm>

#include "socketutils.hpp"

using roxiemem::DataBuffer;

class DataBufferTcpSender : public CTcpSender
{
public:
    DataBufferTcpSender(bool _lowLatency) : CTcpSender(_lowLatency) {}

protected:
    virtual void releaseBuffer(void * buffer) override
    {
        DataBuffer * cast = static_cast<DataBuffer *>(buffer);
        cast->Release();
    };
};

class CTcpSendManager : implements ISendManager, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    static constexpr bool lowLatency = false; // Do not set to true because there are often multiple packets sent in quick succession (e.g. stats then data)
    CTcpSendManager(int server_flow_port, int data_port, int client_flow_port, int q_size, int _numQueues, const IpAddress &_myIP, TokenBucket *_bucket, bool _encrypted, bool useIOUring)
        : sender(lowLatency), numQueues(_numQueues), myIP(_myIP), bucket(_bucket), dataPort(data_port), encrypted(_encrypted)
    {
        myId = myIP.getHostText(myIdStr).str();

        if (useIOUring)
        {
            Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring poll='1'/>");
            Owned<IAsyncProcessor> asyncSender = createURingProcessor(config, true);
            if (asyncSender)
            {
                sender.setAsyncProcessor(asyncSender);

                //MORE: I am not sure if this is actually worthwhile - need to perform some performance tests
                roxiemem::registerRoxieMemory(*asyncSender);
            }
        }
    }

    ~CTcpSendManager()
    {
    }

    // Interface ISendManager

    virtual void writeOwn(IUdpReceiverEntry &receiver, DataBuffer *buffer, unsigned length, unsigned queue) override
    {
        // NOTE: takes ownership of the DataBuffer
        assert(queue < numQueues);
        assert(buffer);
        if (encrypted && !udpEncryptOnSendThread)
        {
            UdpPacketHeader *header = (UdpPacketHeader*) buffer->data;
            length -= sizeof(UdpPacketHeader);
            const MemoryAttr &udpkey = getSecretUdpKey(true);
            size_t encryptedLength = aesEncryptInPlace(udpkey.get(), udpkey.length(), buffer->data + sizeof(UdpPacketHeader), length, DATA_PAYLOAD - sizeof(UdpPacketHeader));
            length = encryptedLength + sizeof(UdpPacketHeader);
            header->length = length;
            assertex(length <= DATA_PAYLOAD);
        }

        if (bucket)
            bucket->wait((length / 1024)+1);

        // IUdpReceiverEntry is an opaque interface - cast to the unrelated actual class.  Better would be to derive
        // CSocketTarget from IUdpReceiverEntry, but that requires refactoring to resolve dll dependencies
        CSocketTarget * socket = reinterpret_cast<CSocketTarget *>(&receiver);
        socket->writeAsync(buffer->data, length, buffer);
    }

    virtual IMessagePacker *createMessagePacker(ruid_t ruid, unsigned sequence, const void *messageHeader, unsigned headerSize, const ServerIdentifier &destNode, int queue) override
    {
        // Resolve the ip to a class instance for sending data to that node, use reinterpret class to cast to an unrelated opaque interface
        SocketEndpoint ep(dataPort, destNode.getIpAddress());
        IUdpReceiverEntry * receiver = reinterpret_cast<IUdpReceiverEntry *>(sender.queryWorkerSocket(ep));
        return ::createMessagePacker(ruid, sequence, messageHeader, headerSize, *this, *receiver, myIP, getNextMessageSequence(), queue, encrypted);
    }

    virtual bool dataQueued(ruid_t ruid, unsigned msgId, const ServerIdentifier &destNode) override
    {
        return false;
    }

    virtual bool abortData(ruid_t ruid, unsigned msgId, const ServerIdentifier &destNode)
    {
        return false;
    }

    virtual void abortAll(const ServerIdentifier &destNode)
    {
    }

    virtual bool allDone()
    {
        return true;
    }

protected:
    inline unsigned getNextMessageSequence()
    {
        unsigned res;
        do
        {
            res = ++msgSeq;
        } while (unlikely(!res));
        return res;
    }

protected:
    DataBufferTcpSender sender;
    unsigned numQueues;
    IpAddress myIP;
    StringBuffer myIdStr;
    const char *myId;
    Linked<TokenBucket> bucket;
    unsigned dataPort;
    bool encrypted;
    std::atomic<unsigned> msgSeq{0};
};

ISendManager *createTcpSendManager(int server_flow_port, int data_port, int client_flow_port, int queue_size_pr_server, int queues_pr_server, const IpAddress &_myIP, TokenBucket *rateLimiter, bool encryptionInTransit, bool useIOUring)
{
    assertex(!_myIP.isNull());
    return new CTcpSendManager(server_flow_port, data_port, client_flow_port, queue_size_pr_server, queues_pr_server, _myIP, rateLimiter, encryptionInTransit, useIOUring);
}
