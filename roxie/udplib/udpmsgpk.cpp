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
#include "jencrypt.hpp"
#include "jsecrets.hpp"

#include "udplib.hpp"
#include "udptrr.hpp"
#include "udptrs.hpp"
#include "udpmsgpk.hpp"
#include "roxiemem.hpp"
#include "roxie.hpp"
#include "ccd.hpp"

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
    DataBuffer *tail = nullptr;
    unsigned metaSize;
    unsigned headerSize;
    const RoxiePacketHeader *header;
    unsigned maxSeqSeen = 0;
#ifdef _DEBUG
    unsigned numPackets = 0;
#endif
    bool outOfBand = false;

    MemoryBuffer metadata;
    InterruptableSemaphore dataAvailable; // MORE - need to work out when to interrupt it!
    bool encrypted = false;

public:
    IMPLEMENT_IINTERFACE;

    PackageSequencer(bool _encrypted) : encrypted(_encrypted)
    {
        metaSize = 0;
        headerSize = 0;
        header = NULL;
        firstPacket = NULL;
        lastContiguousPacket = NULL;
    }

    ~PackageSequencer() 
    {
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
        if (after)
        {
            UdpPacketHeader * hdr = (UdpPacketHeader*) after->data;
            if (hdr->pktSeq & UDP_PACKET_COMPLETE)
                return nullptr;
        }

        dataAvailable.wait(); // MORE - when do I interrupt? Should I time out? Will potentially block indefinitely if sender restarts (leading to an abandoned packet) or stalls.
        DataBuffer *ret;
        if (after)
            ret = after->msgNext;
        else
            ret = firstPacket;
        return ret;
    }

    bool insert(DataBuffer *dataBuff, std::atomic<unsigned> &duplicates, std::atomic<unsigned> &resends)  // returns true if message is complete.
    {
        bool res = false;
        assert(dataBuff->msgNext == NULL);
        UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
        unsigned pktseq = pktHdr->pktSeq;
        if ((pktseq & UDP_PACKET_SEQUENCE_MASK) > maxSeqSeen)
            maxSeqSeen = pktseq & UDP_PACKET_SEQUENCE_MASK;
        if (pktseq & UDP_PACKET_RESENT)
        {
            pktseq &= ~UDP_PACKET_RESENT;
            pktHdr->pktSeq = pktseq;
            resends++;
        }

        // Optimize the (very) common case where I need to add to the end
        DataBuffer *finger;
        DataBuffer *prev;
        if (tail && (pktseq > ((UdpPacketHeader*) tail->data)->pktSeq))
        {
            assert(tail->msgNext==nullptr);
            finger = nullptr;
            prev = tail;
            tail = dataBuff;
        }
        else
        {
            // This is an insertion sort - YUK!
            if (lastContiguousPacket)
            {
                UdpPacketHeader *oldHdr = (UdpPacketHeader*) lastContiguousPacket->data;
                if (pktHdr->pktSeq <= oldHdr->pktSeq)
                {
                    // discard duplicated incoming packet - should be uncommon unless we requested a resend for a packet already in flight
                    dataBuff->Release();
                    duplicates++;
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
        }
        if (prev)
        {
            assert(prev->msgNext == finger);
            prev->msgNext = dataBuff;
        }
        else
        {
            firstPacket = dataBuff;
            if (!tail)
                tail = dataBuff;
        }
        dataBuff->msgNext = finger;
#ifdef _DEBUG
        numPackets++;
#endif
        // Now that we know we are keeping it, decrypt it
        // MORE - could argue that we would prefer to wait even longer - until we know consumer wants it - but that might be complex
        if (encrypted)
        {
            const MemoryAttr &udpkey = getSecretUdpKey(true);
            size_t decryptedSize = aesDecryptInPlace(udpkey.get(), udpkey.length(), pktHdr+1, pktHdr->length-sizeof(UdpPacketHeader));
            pktHdr->length = decryptedSize + sizeof(UdpPacketHeader);
        }

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
                    [[maybe_unused]] unsigned packetDataSize = fingerHdr->length - fingerHdr->metalength - sizeof(UdpPacketHeader);
                    assert(packetDataSize < roxiemem::DATA_ALIGNMENT_SIZE);
                    if (pktseq == 0)
                    {
                        // MORE - Is this safe - header lifetime is somewhat unpredictable without a copy of it...
                        // Client header is at the start of packet 0
                        headerSize = *(unsigned short *)(finger->data + sizeof(UdpPacketHeader));
                        header = (const RoxiePacketHeader *) (finger->data + sizeof(UdpPacketHeader) + sizeof(unsigned short));
                        outOfBand = (header->overflowSequence & OUTOFBAND_SEQUENCE) != 0;
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

    inline const RoxiePacketHeader *getMessageHeader()
    {
        return header;
    }
    inline unsigned getHeaderSize()
    {
        return headerSize;
    }
    inline bool isOutOfBand()
    {
        return outOfBand;
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

    const void *getNext(int length) 
    {
        // YUK horrid code! Though packer is even more horrid
        void        *res = 0;
        if (dataBuff) 
        {   
            UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
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

    virtual RecordLengthType *getNextLength() override
    {
        if (!dataBuff) 
            return nullptr;
        UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
        unsigned packetDataLimit = pktHdr->length - pktHdr->metalength;
        assertex ((packetDataLimit  - current_pos) >= sizeof(RecordLengthType));
        RecordLengthType *res = (RecordLengthType *) &dataBuff->data[current_pos];
        current_pos += sizeof(RecordLengthType);
        // Note that length is never separated from data... but length can be zero so still need to do this...
        // MORE - could common up this code with getNext above
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
        pkSequencer = _pkSqncr; 
    }
    
    ~CMessageResult() 
    {
        pkSequencer->Release();
    }

    virtual IMessageUnpackCursor *getCursor(IRowManager *rowMgr) const 
    {
        return new CMessageUnpackCursor(LINK(pkSequencer), rowMgr);
    }

    virtual const RoxiePacketHeader *getMessageHeader(unsigned &length) const override
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

CMessageCollator::CMessageCollator(IRowManager *_rowMgr, unsigned _ruid, bool _encrypted) : rowMgr(_rowMgr), encrypted(_encrypted), ruid(_ruid)
{
    memLimitExceeded = false;
    activity = false;
    totalBytesReceived = 0;
}

CMessageCollator::~CMessageCollator()
{
    while (!queue.empty())
    {
        PackageSequencer *pkSqncr = queue.front();
        queue.pop_front();
        pkSqncr->Release();
    }
}

void CMessageCollator::noteDuplicate(bool isResend)
{
    totalDuplicates++;
    if (isResend)
        totalResends++;
}


unsigned CMessageCollator::queryBytesReceived() const
{
    return totalBytesReceived;
}

unsigned CMessageCollator::queryDuplicates() const
{
    return totalDuplicates;
}

unsigned CMessageCollator::queryResends() const
{
    return totalResends;
}

bool CMessageCollator::attach_databuffer(DataBuffer *dataBuff)
{
    UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
    totalBytesReceived += pktHdr->length;
    if (pktHdr->node.isNull())   // Indicates a packet that has been identified as a duplicate to be logged and discarded
    {
        noteDuplicate((pktHdr->pktSeq & UDP_PACKET_RESENT) != 0);
        dataBuff->Release();
        return true;
    }
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
    totalBytesReceived += len;
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
    // Special case for single packet messages - if the sequence number is 0 and the complete flag is set
    // No need to check in the hash table - even if a retry.
    // In the future this could also use a simpler package sequencer
    PackageSequencer *pkSqncr;
    if ((pktHdr->pktSeq & (UDP_PACKET_SEQUENCE_MASK|UDP_PACKET_COMPLETE)) == UDP_PACKET_COMPLETE)
    {
        pkSqncr = new PackageSequencer(encrypted);
        bool isComplete = pkSqncr->insert(dataBuff, totalDuplicates, totalResends);
        assertex(isComplete);
    }
    else
    {
        PUID puid = GETPUID(dataBuff);
        // MORE - we leak (at least until query terminates) a PackageSequencer for messages that we only receive parts of - maybe only an issue for "catchall" case
        pkSqncr = mapping.getValue(puid);
        bool isNew = false;
        if (!pkSqncr)
        {
            pkSqncr = new PackageSequencer(encrypted);
            isNew = true;
        }
        bool isComplete = pkSqncr->insert(dataBuff, totalDuplicates, totalResends);
        if (!isComplete)
        {
            if (isNew)
            {
                mapping.setValue(puid, pkSqncr);
                pkSqncr->Release();
            }
            return;
        }

        if (!isNew)
        {
            pkSqncr->Link();
            mapping.remove(puid);
        }
    }

    queueCrit.enter();
    if (pkSqncr->isOutOfBand())
        queue.push_front(pkSqncr);
    else
        queue.push_back(pkSqncr);
    queueCrit.leave();
    sem.signal();
}

IMessageResult *CMessageCollator::getNextResult(unsigned time_out, bool &anyActivity)
{
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
        queue.pop_front();
        queueCrit.leave();
        anyActivity = true;
        activity = false;
        return new CMessageResult(pkSqncr);
    }
    anyActivity = activity;
    activity = false;
    return 0;
}

void CMessageCollator::interrupt(IException *E)
{
    sem.interrupt(E);
}

// ====================================================================================
//

