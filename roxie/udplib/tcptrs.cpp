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

class TcpSendRequest final : public IAsyncCallback
{
public:
    TcpSendRequest(IAsyncProcessor * _processor, CSocketTarget * _socket, DataBuffer * _buffer, size32_t _length)
        : processor(_processor), socket(_socket), buffer(_buffer), length(_length)
    {
    }

    void submit()
    {
        socket->writeAsync(processor, length, buffer->data, *this);
    }

    virtual void onAsyncComplete(int result) override
    {
        if (result < 0)
        {
            switch (-result)
            {
            case EAGAIN:
                //Not sure this can ever happen, but here as an indication of what should be done
                submit();
                return;
            }

            //Force an asynchronous reconnect
            socket->connectAsync(processor);
        }
        buffer->Release();
        // This object is no longer needed - free it up.  This is slightly dangerous.... but the object should not be
        // used after this point.
        delete this;
    }

private:
    IAsyncProcessor * processor;
    CSocketTarget * socket;
    DataBuffer * buffer;
    unsigned length;
};

class CTcpSendManager : implements ISendManager, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    static constexpr bool lowLatency = false; // Do not set to true because there after often multiple packets sent in quick succession (e.g. stats then data)
    CTcpSendManager(int server_flow_port, int data_port, int client_flow_port, int q_size, int _numQueues, const IpAddress &_myIP, TokenBucket *_bucket, bool _encrypted, bool useIOUring)
        : sender(lowLatency), numQueues(_numQueues), myIP(_myIP), bucket(_bucket), dataPort(data_port), encrypted(_encrypted)
    {
        myId = myIP.getHostText(myIdStr).str();

        if (useIOUring)
        {
            Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring poll='1'/>");
            dbglogXML(config);
            asyncSender.setown(createURingProcessor(config, false));
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
        if (asyncSender)
        {
            TcpSendRequest * request = new TcpSendRequest(asyncSender, socket, buffer, length);
            request->submit();
        }
        else
        {
            socket->write(buffer->data, length);
            buffer->Release();
        }
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
    CTcpSender sender;
    unsigned numQueues;
    IpAddress myIP;
    StringBuffer myIdStr;
    const char *myId;
    Linked<TokenBucket> bucket;
    unsigned dataPort;
    bool encrypted;
    std::atomic<unsigned> msgSeq{0};
    Owned<IAsyncProcessor> asyncSender;
};

ISendManager *createTcpSendManager(int server_flow_port, int data_port, int client_flow_port, int queue_size_pr_server, int queues_pr_server, const IpAddress &_myIP, TokenBucket *rateLimiter, bool encryptionInTransit, bool useIOUring)
{
    assertex(!_myIP.isNull());
    return new CTcpSendManager(server_flow_port, data_port, client_flow_port, queue_size_pr_server, queues_pr_server, _myIP, rateLimiter, encryptionInTransit, useIOUring);
}
