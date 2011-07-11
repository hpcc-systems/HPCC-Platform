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


#ifndef JBROADCAST_IPP
#define JBROADCAST_IPP

#include "jarray.hpp"
#include "jbuff.hpp"
#include "jsuperhash.hpp"

#include "jbroadcast.hpp"

enum MCPacket_Cmd { MCPacket_None, MCPacket_Poll, MCPacket_Stop } ;

#define MC_PACKET_SIZE 64000
#define MC_ACK_PACKET_SIZE 64000

struct MCPacketHeader
{
    bctag_t tag;        // filters to different client receivers
    unsigned jobId;     // sequenced per broadcast to identify new streams from old resent packets in old streams
    unsigned id;        // sequence id
    unsigned length;    // length of data in packet
    unsigned offset;    // offset of data in stream
    unsigned total;     // total # of packets in stream
    byte cmd;           // optional cmd (e.g. poll)
};

class CDataPacket : public CInterface
{
public:
    CDataPacket()
    {
        header = (MCPacketHeader *)mb.reserveTruncate(MC_PACKET_SIZE);
        pktData = ((byte*)header)+sizeof(MCPacketHeader);
    }

    MCPacketHeader *header;
    void *queryData() { return pktData; }
    MCPacketHeader *detachPacket() { (MCPacketHeader *) mb.detach(); }

private:
    void *pktData;
    MemoryBuffer mb;
};

struct MCAckPacketHeader
{
    unsigned node;
    bctag_t tag;
    unsigned jobId;
    bool ackDone;
};

class CUIntValue : public CInterface
{
    unsigned value;
public:
    CUIntValue(unsigned _value) : value(_value) { }
    const unsigned &queryValue() { return value; }
    const void *queryFindParam() const { return &value; }
};

typedef OwningSimpleHashTableOf<CUIntValue, unsigned> CUIntTable;

class CMCastReceiver : public CInterface, implements IBroadcastReceiver
{
public:
    IMPLEMENT_IINTERFACE;

    CMCastReceiver(bctag_t _tag);
    ~CMCastReceiver();

    bool packetReceived(CDataPacket &dataPacket, bool &complete);
    bool buildNack(MCAckPacketHeader *ackPacket, size32_t &sz, unsigned total=0);
    void reset();
    const void *queryFindParam() const { return &tag; }

// IBroadcastReceiver impl.
    virtual bool eos();
    virtual bool read(MemoryBuffer &mb);
    virtual void stop();

private:
    CIArrayOf<CDataPacket> dataPackets;
    UnsignedArray pktsReceived;
    Semaphore receivedSem;
    bctag_t tag;
    unsigned nextPacket;
    CriticalSection crit;
    bool aborted, eosHit;
    CTimeMon logTmRecv, logTmCons;
};

class CMCastRecvServer : public Thread
{
    CUIntTable oldJobIds; // could expire these after a short time.
    StringAttr broadcastRoot;
    bool stopped;
    Owned<ISocket> sock, ackSock;
    SimpleHashTableOf<CMCastReceiver, bctag_t> receivers;
    CIArrayOf<CDataPacket> dataPackets;
    CriticalSection receiversCrit;
    unsigned groupMember;
    SocketEndpoint mcastEp;
    unsigned ackPort;

public:
    IMPLEMENT_IINTERFACE;

    CMCastRecvServer(const char *_broadcastRoot, unsigned groupMember, SocketEndpoint &mcastEp, unsigned broadcastAckPort);

    CMCastReceiver *getReceiver(bctag_t tag);
    void registerReceiver(CMCastReceiver &receiver);
    void deregisterReceiver(CMCastReceiver &receiver);
    void stop();
    virtual int run();
};

class CUIntTableItem : public CUIntTable
{
    unsigned pkt;
public:
    CUIntTableItem(unsigned _pkt) : pkt(_pkt) { }
    const void *queryFindParam() const { return &pkt; }
    const unsigned &queryPacket() { return pkt; }
};

typedef OwningSimpleHashTableOf<CUIntTableItem, unsigned> CPktNodeTable;

class CCountedItem : public CUIntValue
{
public:
    unsigned count;
    CCountedItem(unsigned value) : CUIntValue(value) { count = 0; }
};

class CCountTable : public OwningSimpleHashTableOf<CCountedItem, unsigned>
{
public:
    inline unsigned incItem(unsigned v)
    {
        CCountedItem *c = find(v);
        if (!c) c = new CCountedItem(v);
        add(* c);
        c->count++;
        return c->count;
    }
};

#endif
