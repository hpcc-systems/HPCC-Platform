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

#ifndef UDPMSGPK_INCL
#define UDPMSGPK_INCL
#include "jiface.hpp"
#include "jstring.hpp"
#include "jisem.hpp"
#include "jsocket.hpp"
#include "roxiemem.hpp"

#ifdef UDPLIB_EXPORTS
#define UDPLIB_API DECL_EXPORT
#else
#define UDPLIB_API DECL_IMPORT
#endif

typedef unsigned ruid_t;   // at 1000/sec recycle every 49 days
#define RUIDF   "0x%.8x"
#define RUID_PING 0
#define RUID_DISCARD 1
#define RUID_FIRST 2

typedef unsigned RecordLengthType;
#define MAX_RECORD_LENGTH 0xffffffff

class UDPLIB_API ServerIdentifier
{
private:
    IpAddress serverIp;  // MORE - should really be an endpoint?
public:
    ServerIdentifier() : serverIp() { }
    ServerIdentifier(const ServerIdentifier &from) : serverIp(from.serverIp) { }
    ServerIdentifier(const IpAddress &from) : serverIp(from) { }
    const IpAddress &getNodeAddress() const;
    const ServerIdentifier & operator=(const ServerIdentifier &from)
    {
        serverIp = from.serverIp;
        return *this;
    }
    bool operator==(const ServerIdentifier &from) const
    {
        return serverIp.ipequals(from.serverIp);
    }
    unsigned hash() const
    {
        return serverIp.iphash(0);
    }
    inline void setIp(const IpAddress &_ip)
    {
        serverIp = _ip;
    }
    StringBuffer &getTraceText(StringBuffer &s) const
    {
        return serverIp.getIpText(s);
    }
};

extern UDPLIB_API ServerIdentifier myNode;

interface IMessagePacker : extends IInterface
{
    virtual void *getBuffer(unsigned len, bool variable) = 0;
    virtual void putBuffer(const void *buf, unsigned len, bool variable) = 0;
    virtual void flush() = 0;
    virtual void sendMetaInfo(const void *buf, unsigned len) = 0;

    virtual unsigned size() const = 0;  // Total amount written via putBuffer plus any overhead from record length prefixes
    
    // Notes: 
    // 1. Parameters to putBuffer must have been returned from getBuffer. 
    // 2  putBuffer must me called before any call to flush 
    // 3. There is an implicit abort if is released without final flush
    // 4  This interface is single threaded
    // 5. call to getbuffer, without call to makes the previous call to getBuffer and NULL operation
    // 6. if flush has been called with last_message true, then any calls to getBuffer or putBuffer is undefined.
};

interface IException;

interface IMessageUnpackCursor : extends IInterface
{
    virtual const void *getNext(int length) = 0;
    virtual bool atEOF() const = 0;
    virtual bool isSerialized() const = 0;
    //    if one tries to read past the last record then NULL will be returned, 
    //    if one asks for more data than available then throws exception.
};

interface IMessageResult : extends IInterface
{
    virtual IMessageUnpackCursor *getCursor(roxiemem::IRowManager *rowMgr) const = 0;
    virtual const void *getMessageHeader(unsigned &length) const = 0;
    virtual const void *getMessageMetadata(unsigned &length) const = 0;
    virtual void discard() const = 0;
};

interface IMessageCollator : extends IInterface
{
    virtual IMessageResult *getNextResult(unsigned time_out, bool &anyActivity) = 0;
    virtual void interrupt(IException *E = NULL) = 0;
    virtual ruid_t queryRUID() const = 0;
    virtual unsigned queryBytesReceived() const = 0;
};

interface IReceiveManager : extends IInterface 
{
    virtual IMessageCollator *createMessageCollator(roxiemem::IRowManager *rowManager, ruid_t ruid) = 0;
    virtual void detachCollator(const IMessageCollator *collator) = 0;
};

// Opaque data structure that SendManager gives to message packer describing how to talk to a particular target node
interface IUdpReceiverEntry
{
};

interface ISendManager : extends IInterface 
{
    virtual IMessagePacker *createMessagePacker(ruid_t id, unsigned sequence, const void *messageHeader, unsigned headerSize, const ServerIdentifier &destNode, int queue) = 0;
    virtual void writeOwn(IUdpReceiverEntry &receiver, roxiemem::DataBuffer *buffer, unsigned len, unsigned queue) = 0;
    virtual bool dataQueued(ruid_t ruid, unsigned sequence, const ServerIdentifier &destNode) = 0;
    virtual bool abortData(ruid_t ruid, unsigned sequence, const ServerIdentifier &destNode) = 0;
    virtual bool allDone() = 0;
};

extern UDPLIB_API IReceiveManager *createReceiveManager(int server_flow_port, int data_port, int client_flow_port, int sniffer_port, const IpAddress &sniffer_multicast_ip, int queue_size, unsigned maxSlotsPerSender);
extern UDPLIB_API ISendManager *createSendManager(int server_flow_port, int data_port, int client_flow_port, int sniffer_port, const IpAddress &sniffer_multicast_ip, int queue_size_pr_server, int queues_pr_server, TokenBucket *rateLimiter);

extern UDPLIB_API void setAeronProperties(const IPropertyTree *config);
extern UDPLIB_API IReceiveManager *createAeronReceiveManager(const SocketEndpoint &ep);
extern UDPLIB_API ISendManager *createAeronSendManager(unsigned dataPort, unsigned numQueues, const IpAddress &myIP);

extern UDPLIB_API RelaxedAtomic<unsigned> unwantedDiscarded;

extern UDPLIB_API unsigned udpTraceLevel;
extern UDPLIB_API unsigned udpTraceCategories;
extern UDPLIB_API unsigned udpOutQsPriority;
extern UDPLIB_API void queryMemoryPoolStats(StringBuffer &memStats);

extern UDPLIB_API unsigned multicastTTL;

#if defined( __linux__) || defined(__APPLE__)
extern UDPLIB_API void setLinuxThreadPriority(int level);
#endif

extern UDPLIB_API unsigned udpFlowSocketsSize;
extern UDPLIB_API unsigned udpLocalWriteSocketSize;
extern UDPLIB_API unsigned udpMaxRetryTimedoutReqs;
extern UDPLIB_API unsigned udpRequestToSendTimeout;
extern UDPLIB_API unsigned udpRequestToSendAckTimeout;

extern UDPLIB_API unsigned udpRetryBusySenders;
extern UDPLIB_API bool udpSnifferEnabled;
extern UDPLIB_API unsigned udpSnifferReadThreadPriority;
extern UDPLIB_API unsigned udpSnifferSendThreadPriority;

extern UDPLIB_API void stopAeronDriver();

#endif
