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

#ifdef _WIN32
#pragma warning( disable : 4786)
#pragma warning( disable : 4018)
#endif

#include "platform.h"
#include <string>
#include <map>
#include <queue>

#include "jthread.hpp"
#include "jlog.hpp"
#include "jisem.hpp"
#include "udplib.hpp"
#include "udptrr.hpp"
#include "udptrs.hpp"
#include "udpmsgpk.hpp"
#include "roxiemem.hpp"
#include "roxie.hpp"

using roxiemem::DataBuffer;
using roxiemem::IRowManager;

RelaxedAtomic<unsigned> unwantedDiscarded;

// PackageSequencer ====================================================================================
//
typedef DataBuffer * data_buffer_ptr;

int g_sequence_compare(const void *arg1, const void *arg2 ) 
{
    DataBuffer *dataBuff1 =  *(DataBuffer **) arg1;
    DataBuffer *dataBuff2 =  *(DataBuffer **) arg2;
    UdpPacketHeader *pktHdr1 = (UdpPacketHeader*) dataBuff1->data;
    UdpPacketHeader *pktHdr2 = (UdpPacketHeader*) dataBuff2->data;
    if (pktHdr1->pktSeq < pktHdr2->pktSeq) return -1;
    if (pktHdr1->pktSeq > pktHdr2->pktSeq) return 1;
    return 0;
}


class PackageSequencer : public CInterface, implements IInterface
{
    DataBuffer *firstPacket;
    DataBuffer *lastContiguousPacket;
    unsigned metaSize;
    unsigned headerSize;
    const void *header;

    MemoryBuffer metadata;
    InterruptableSemaphore dataAvailable; // MORE - need to work out when to interrupt it!

public:
    IMPLEMENT_IINTERFACE;

    PackageSequencer() 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: PackageSequencer::PackageSequencer this=%p", this);
        metaSize = 0;
        headerSize = 0;
        header = NULL;
        firstPacket = NULL;
        lastContiguousPacket = NULL;
    }

    ~PackageSequencer() 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: PackageSequencer::~PackageSequencer this=%p", this);
        DataBuffer *finger = firstPacket;
        while (finger)
        {
            DataBuffer *goer = finger;
            finger = finger->msgNext;
            goer->Release();
        }
    }
    
    DataBuffer *next(DataBuffer *after)
    {
        dataAvailable.wait(); // MORE - when do I interrupt? Should I time out? Will potentially block indefinitely if sender restarts (leading to an abandoned packet) or stalls.
        DataBuffer *ret;
        if (after)
            ret = after->msgNext;
        else
            ret = firstPacket;
        if (checkTraceLevel(TRACE_MSGPACK, 5))
        {
            if (ret)
            {
                StringBuffer s;
                UdpPacketHeader *pktHdr = (UdpPacketHeader*) ret->data;
                DBGLOG("UdpCollator: PackageSequencer::next returns ruid=" RUIDF " id=0x%.8X mseq=%u pkseq=0x%.8X node=%s dataBuffer=%p this=%p",
                        pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->node.getTraceText(s).str(), ret, this);
            }
            else
                DBGLOG("UdpCollator: PackageSequencer::next returns NULL this=%p", this);
        }
        return ret;
    }

    bool insert(DataBuffer *dataBuff)  // returns true if message is complete.
    {
        bool res = false;
        assert(dataBuff->msgNext == NULL);
        UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;

        if (checkTraceLevel(TRACE_MSGPACK, 5))
        {
            StringBuffer s;
            DBGLOG("UdpCollator: PackageSequencer::insert ruid=" RUIDF " id=0x%.8X mseq=%u pkseq=0x%.8X node=%s dataBuffer=%p this=%p",
                    pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->node.getTraceText(s).str(), dataBuff, this);
        }

        DataBuffer *finger;
        DataBuffer *prev;
        if (lastContiguousPacket)
        {
            UdpPacketHeader *oldHdr = (UdpPacketHeader*) lastContiguousPacket->data;
            if (pktHdr->pktSeq <= oldHdr->pktSeq)
            {
                // discard duplicated incoming packet - should be uncommon unless we requested a resend for a packet already in flight
                if (checkTraceLevel(TRACE_MSGPACK, 5))
                    DBGLOG("UdpCollator: Discarding duplicate incoming packet");
                dataBuff->Release();
                return false;
            }
            finger = lastContiguousPacket->msgNext;
            prev = lastContiguousPacket;
        }
        else
        {
            finger = firstPacket; 
            prev = NULL;
        }
        while (finger)
        {
            UdpPacketHeader *oldHdr = (UdpPacketHeader*) finger->data;
            if (pktHdr->pktSeq == oldHdr->pktSeq)
            {
                // discard duplicated incoming packet - should be uncommon unless we requested a resend for a packet already in flight
                if (checkTraceLevel(TRACE_MSGPACK, 5))
                    DBGLOG("UdpCollator: Discarding duplicate incoming packet");
                dataBuff->Release();
                return false;
            }
            else if (pktHdr->pktSeq < oldHdr->pktSeq)
            {
                break;
            }
            else
            {
                prev = finger;
                finger = finger->msgNext;
            }
        }
        if (prev)
        {
            assert(prev->msgNext == finger);
            prev->msgNext = dataBuff;
        }
        else
            firstPacket = dataBuff;
        dataBuff->msgNext = finger;
        if (prev == lastContiguousPacket)
        {
            unsigned prevseq;
            if (lastContiguousPacket)
            {
                UdpPacketHeader *lastHdr  = (UdpPacketHeader*) lastContiguousPacket->data;
                prevseq = lastHdr->pktSeq & UDP_PACKET_SEQUENCE_MASK;
                finger = lastContiguousPacket->msgNext;
            }
            else
            {
                prevseq = (unsigned) -1;
                finger = firstPacket;
            }
            while (finger)
            {
                UdpPacketHeader *fingerHdr  = (UdpPacketHeader*) finger->data;
                unsigned pktseq = fingerHdr->pktSeq & UDP_PACKET_SEQUENCE_MASK;
                if (pktseq == prevseq+1)
                {
                    unsigned packetDataSize = fingerHdr->length - fingerHdr->metalength - sizeof(UdpPacketHeader);
                    assert(packetDataSize < roxiemem::DATA_ALIGNMENT_SIZE);
                    if (pktseq == 0)
                    {
                        // MORE - Is this safe - header lifetime is somewhat unpredictable without a copy of it...
                        // Client header is at the start of packet 0
                        headerSize = *(unsigned short *)(finger->data + sizeof(UdpPacketHeader));
                        header = finger->data + sizeof(UdpPacketHeader) + sizeof(unsigned short);
                        packetDataSize -= headerSize + sizeof(unsigned short);
                    }
                    if (fingerHdr->metalength)
                    {
                        // MORE - may be worth taking the effort to avoid copy of metadata unless it's split up
                        metaSize += fingerHdr->metalength;
                        metadata.append(fingerHdr->metalength, finger->data + fingerHdr->length - fingerHdr->metalength);
                    }

                    lastContiguousPacket = finger;
                    dataAvailable.signal();
                    if (fingerHdr->pktSeq & UDP_PACKET_COMPLETE)
                    {
                        res = true;
                        dataAvailable.signal(); // allowing us to read the NULL that signifies end of message. May prefer to use the flag to stop?
                    }
                }
                else
                    break;
                finger = finger->msgNext;
                prevseq = pktseq;
            }
        }
        return res;
    }

    inline const void *getMetaData(unsigned &length)
    {
        length = metadata.length();
        return metadata.toByteArray();
    }

    inline const void *getMessageHeader()
    {
        return header;
    }

    inline unsigned getHeaderSize()
    {
        return headerSize;
    }

};

// MessageResult ====================================================================================
//
class CMessageUnpackCursor: implements IMessageUnpackCursor, public CInterface
{
    PackageSequencer *pkSequencer;
    DataBuffer *dataBuff;
    unsigned current_pos;
    Linked<IRowManager> rowMgr;

public:
    IMPLEMENT_IINTERFACE;

    CMessageUnpackCursor(PackageSequencer *_pkSqncr, IRowManager *_rowMgr) : rowMgr(_rowMgr)
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: CMessageUnpackCursor::CMessageUnpackCursor this=%p", this);
        pkSequencer = _pkSqncr; 
        dataBuff = pkSequencer->next(NULL);
        UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
        current_pos = sizeof(UdpPacketHeader) + pkSequencer->getHeaderSize() + sizeof(unsigned short); // YUK - code neads cleaning!
        unsigned packetDataLimit = pktHdr->length - pktHdr->metalength;
        while (current_pos >= packetDataLimit)
        {
            current_pos = sizeof(UdpPacketHeader);
            dataBuff = pkSequencer->next(dataBuff); // NULL, we expect, unless packing was really weird.
            if (dataBuff)
            {
                pktHdr = (UdpPacketHeader*) dataBuff->data;
                packetDataLimit = pktHdr->length - pktHdr->metalength;
            }
            else
                break;
        }
    }
    
    ~CMessageUnpackCursor() 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: CMessageUnpackCursor::~CMessageUnpackCursor this=%p", this);
        pkSequencer->Release();
    }

    virtual bool atEOF() const
    {
        return dataBuff == NULL;
    }

    virtual bool isSerialized() const
    {
        return true;
    }

    virtual const void *getNext(int length) 
    {
        // YUK horrid code! Though packer is even more horrid
        void        *res = 0;
        if (dataBuff) 
        {   
            UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
            if (checkTraceLevel(TRACE_MSGPACK, 4))
            {
                StringBuffer s;
                DBGLOG("UdpCollator: CMessageUnpackCursor::getNext(%u) pos=%u pktLength=%u metaLen=%u ruid=" RUIDF " id=0x%.8X mseq=%u pkseq=0x%.8X node=%s dataBuff=%p this=%p",
                    length, current_pos, pktHdr->length, pktHdr->metalength,
                    pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, 
                    pktHdr->node.getTraceText(s).str(), dataBuff, this);
            }
            unsigned packetDataLimit = pktHdr->length - pktHdr->metalength;
            if ((packetDataLimit  - current_pos) >= (unsigned) length)
            {
                // Simple case - no need to copy
                res = &dataBuff->data[current_pos];
                current_pos += length;
                while (current_pos >= packetDataLimit)
                {
                    dataBuff = pkSequencer->next(dataBuff);
                    current_pos = sizeof(UdpPacketHeader);
                    if (dataBuff)
                    {
                        pktHdr = (UdpPacketHeader*) dataBuff->data;
                        packetDataLimit = pktHdr->length - pktHdr->metalength;
                    }
                    else
                        break;
                }
                LinkRoxieRow(res);
                return res;
            }   
            char *currResLoc = (char*)rowMgr->allocate(length, 0);
            res = currResLoc;
            while (length && dataBuff) 
            {
                // Spans more than one block - allocate and copy
                unsigned cpyLen = packetDataLimit - current_pos;
                if (cpyLen > (unsigned) length) cpyLen = length;
                memcpy(currResLoc, &dataBuff->data[current_pos], cpyLen);
                length -= cpyLen;
                currResLoc += cpyLen;
                current_pos += cpyLen;
                while (current_pos >= packetDataLimit)
                {
                    dataBuff = pkSequencer->next(dataBuff);
                    if (dataBuff)
                    {
                        current_pos = sizeof(UdpPacketHeader);
                        pktHdr = (UdpPacketHeader*) dataBuff->data;
                        packetDataLimit = pktHdr->length - pktHdr->metalength;
                    }
                    else
                    {
                        current_pos = 0;
                        pktHdr = NULL;
                        packetDataLimit = 0;
                        break;
                    }
                }
            }
            assertex(!length);  // fail if not enough data available
        }
        else
            res = NULL;
        return res;
    }

};

 
class CMessageResult : public IMessageResult, CInterface {
    PackageSequencer    *pkSequencer;
    mutable MemoryBuffer metaInfo;
    mutable CriticalSection metaCrit;
    
public:
    IMPLEMENT_IINTERFACE;

    CMessageResult(PackageSequencer *_pkSqncr) 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: CMessageResult::CMessageResult pkSqncr=%p, this=%p", _pkSqncr, this);
        pkSequencer = _pkSqncr; 
    }
    
    ~CMessageResult() 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: CMessageResult::~CMessageResult this=%p", this);
        pkSequencer->Release();
    }

    virtual IMessageUnpackCursor *getCursor(IRowManager *rowMgr) const 
    {
        return new CMessageUnpackCursor(LINK(pkSequencer), rowMgr);
    }

    virtual const void *getMessageHeader(unsigned &length) const 
    {
        length = pkSequencer->getHeaderSize();
        return pkSequencer->getMessageHeader();
    }

    virtual const void *getMessageMetadata(unsigned &length) const 
    {
        return pkSequencer->getMetaData(length);
    }

    virtual void discard() const 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 2))
            DBGLOG("UdpCollator: CMessageResult - Roxie server discarded a packet");
        unwantedDiscarded++;
    }

};

 


// MessageCollator ====================================================================================
//

PUID GETPUID(DataBuffer *dataBuff)
{
    UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
    unsigned ip4 = pktHdr->node.getIp4();
    return (((PUID) ip4) << 32) | (PUID) pktHdr->msgSeq;
}

CMessageCollator::CMessageCollator(IRowManager *_rowMgr, unsigned _ruid) : rowMgr(_rowMgr), ruid(_ruid)
{
    if (checkTraceLevel(TRACE_MSGPACK, 3))
        DBGLOG("UdpCollator: CMessageCollator::CMessageCollator rowMgr=%p this=%p ruid=" RUIDF "", _rowMgr, this, ruid);
    memLimitExceeded = false;
    activity = false;
    totalBytesReceived = 0;
}

CMessageCollator::~CMessageCollator()
{
    if (checkTraceLevel(TRACE_MSGPACK, 3))
        DBGLOG("UdpCollator: CMessageCollator::~CMessageCollator ruid=" RUIDF ", this=%p", ruid, this);
    while (!queue.empty())
    {
        PackageSequencer *pkSqncr = queue.front();
        queue.pop();
        pkSqncr->Release();
    }
}

unsigned CMessageCollator::queryBytesReceived() const
{
    return totalBytesReceived; // Arguably should lock, but can't be bothered. Never going to cause an issue in practice.
}

bool CMessageCollator::attach_databuffer(DataBuffer *dataBuff)
{
    activity = true;
    if (memLimitExceeded || roxiemem::memPoolExhausted())
    {
        DBGLOG("UdpCollator: mem limit exceeded");
        return false;
    }
    if (!dataBuff->attachToRowMgr(rowMgr))
    {
        memLimitExceeded = true;
        DBGLOG("UdpCollator: mem limit exceeded");
        return(false);
    }
    collate(dataBuff);
    return true;
}

bool CMessageCollator::attach_data(const void *data, unsigned len)
{
    // Simple code can allocate databuffer, copy data in, then call attach_databuffer
    // MORE - we can attach as we create may be more sensible (and simplify roxiemem rather if it was the ONLY way)
    activity = true;
    if (memLimitExceeded || roxiemem::memPoolExhausted())
    {
        DBGLOG("UdpCollator: mem limit exceeded");
        return false;
    }
    DataBuffer *dataBuff = bufferManager->allocate();
    assertex(len <= DATA_PAYLOAD);
    memcpy(dataBuff->data, data, len);
    if (!dataBuff->attachToRowMgr(rowMgr))
    {
        memLimitExceeded = true;
        DBGLOG("UdpCollator: mem limit exceeded");
        dataBuff->Release();
        return(false);
    }
    collate(dataBuff);
    return true;
}

void CMessageCollator::collate(DataBuffer *dataBuff)
{
    UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
    totalBytesReceived += pktHdr->length;

    PUID puid = GETPUID(dataBuff);
    // MORE - I think we leak a PackageSequencer for messages that we only receive parts of - maybe only an issue for "catchall" case
    PackageSequencer *pkSqncr = mapping.getValue(puid);
    if (!pkSqncr)
    {
        pkSqncr = new PackageSequencer;
        mapping.setValue(puid, pkSqncr);
        pkSqncr->Release();
    }
    bool isComplete = pkSqncr->insert(dataBuff);
    if (isComplete)
    {
        pkSqncr->Link();
        mapping.remove(puid);
        queueCrit.enter();
        queue.push(pkSqncr);
        sem.signal();
        queueCrit.leave();
    }
}

IMessageResult *CMessageCollator::getNextResult(unsigned time_out, bool &anyActivity)
{
    if (checkTraceLevel(TRACE_MSGPACK, 3))
        DBGLOG("UdpCollator: CMessageCollator::getNextResult() timeout=%.8X ruid=%u rowMgr=%p this=%p", time_out, ruid, (void*) rowMgr, this);

    if (memLimitExceeded)
    {
        DBGLOG("UdpCollator: CMessageCollator::getNextResult() throwing memory limit exceeded exception - rowMgr=%p this=%p", (void*) rowMgr, this);
        throw MakeStringException(0, "memory limit exceeded");
    }
    else if (roxiemem::memPoolExhausted())
    {
        DBGLOG("UdpCollator: CMessageCollator::getNextResult() throwing memory pool exhausted exception - rowMgr=%p this=%p", (void*)rowMgr, this);
        throw MakeStringException(0, "memory pool exhausted");
    }
    if (sem.wait(time_out))
    {
        queueCrit.enter();
        PackageSequencer *pkSqncr = queue.front();
        queue.pop();
        queueCrit.leave();
        anyActivity = true;
        activity = false;
        return new CMessageResult(pkSqncr);
    }
    anyActivity = activity;
    activity = false;
    if (!anyActivity && ruid>=RUID_FIRST && checkTraceLevel(TRACE_MSGPACK, 1)) // suppress the tracing for pings where we expect the timeout...
    {
        DBGLOG("UdpCollator: CMessageCollator::GetNextResult timeout");
    }
    return 0;
}

void CMessageCollator::interrupt(IException *E)
{
    sem.interrupt(E);
}

// ====================================================================================
//

extern CMessageCollator *createCMessageCollator(IRowManager *rowManager, ruid_t ruid)
{
    return new CMessageCollator(rowManager, ruid);
}
