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

#ifndef UDPMSGPK_INCL
#define UDPMSGPK_INCL
#include "jiface.hpp"
#include "jstring.hpp"
#include "jisem.hpp"
#include "jsocket.hpp"
#include "roxiemem.hpp"

#ifdef _WIN32
#ifdef UDPLIB_EXPORTS
#define UDPLIB_API __declspec(dllexport)
#else
#define UDPLIB_API __declspec(dllimport)
#endif
#else
#define UDPLIB_API
#endif

typedef unsigned ruid_t;   // at 1000/sec recycle every 49 days
#define RUIDF   "0x%.8x"
#define RUID_PING 0
#define RUID_DISCARD 1
#define RUID_FIRST 2

typedef unsigned RecordLengthType;
#define MAX_RECORD_LENGTH 0xffffffff

interface IMessagePacker : extends IInterface
{
    virtual void *getBuffer(unsigned len, bool variable) = 0;
    virtual void putBuffer(const void *buf, unsigned len, bool variable) = 0;
    virtual void flush(bool last_message = false) = 0;
    virtual bool dataQueued() = 0;
    virtual void abort() = 0;
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

    virtual bool add_package(roxiemem::DataBuffer *dataBuff) = 0;
};

interface IReceiveManager : extends IInterface 
{
    virtual IMessageCollator *createMessageCollator(roxiemem::IRowManager *rowManager, ruid_t ruid) = 0;
    virtual void detachCollator(const IMessageCollator *collator) = 0;
    virtual void setDefaultCollator(IMessageCollator *collator) = 0;
};

interface ISendManager : extends IInterface 
{
    virtual IMessagePacker *createMessagePacker(ruid_t id, unsigned sequence, const void *messageHeader, unsigned headerSize, unsigned destNodeIndex, int queue) = 0;
    virtual bool dataQueued(ruid_t ruid, unsigned sequence, unsigned destNodeIndex) = 0;
    virtual bool abortData(ruid_t ruid, unsigned sequence, unsigned destNodeIndex) = 0;
    virtual bool allDone() = 0;
};

extern UDPLIB_API IReceiveManager *createReceiveManager(int server_flow_port, int data_port, int client_flow_port, int sniffer_port, const IpAddress &sniffer_multicast_ip, int queue_size, unsigned maxSlotsPerSender, unsigned myNodeIndex);
extern UDPLIB_API ISendManager *createSendManager(int server_flow_port, int data_port, int client_flow_port, int sniffer_port, const IpAddress &sniffer_multicast_ip, int queue_size_pr_server, int queues_pr_server, unsigned maxRetryData, TokenBucket *rateLimiter, unsigned myNodeIndex);

extern UDPLIB_API const IpAddress &getNodeAddress(unsigned index);
extern UDPLIB_API unsigned addRoxieNode(const char *ipString);
extern UDPLIB_API unsigned getNumNodes();

extern UDPLIB_API atomic_t unwantedDiscarded;
extern UDPLIB_API atomic_t packetsRetried;
extern UDPLIB_API atomic_t packetsAbandoned;

extern UDPLIB_API unsigned udpTraceLevel;
extern UDPLIB_API unsigned udpTraceCategories;
extern UDPLIB_API unsigned udpOutQsPriority;
extern UDPLIB_API void queryMemoryPoolStats(StringBuffer &memStats);

#ifdef __linux__
extern UDPLIB_API void setLinuxThreadPriority(int level);
#endif

extern UDPLIB_API bool enableSocketMaxSetting;
extern UDPLIB_API unsigned udpFlowSocketsSize;
extern UDPLIB_API unsigned udpLocalWriteSocketSize;
extern UDPLIB_API unsigned udpMaxRetryTimedoutReqs;
extern UDPLIB_API unsigned udpRequestToSendTimeout;
extern UDPLIB_API unsigned udpRetryBusySenders;
extern UDPLIB_API unsigned udpInlineCollationPacketLimit;
extern UDPLIB_API bool udpInlineCollation;
extern UDPLIB_API bool udpSnifferEnabled;
extern UDPLIB_API bool udpSendCompletedInData;
#endif
