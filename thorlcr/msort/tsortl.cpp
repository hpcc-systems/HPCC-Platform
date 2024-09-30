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

#include "platform.h"
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdarg.h>
#ifdef _WIN32
#include <process.h>
#endif

#include "mpcomm.hpp"
#include "jfile.hpp"
#include "jio.hpp"
#include "jsocket.hpp"

#include "tsorts.hpp"
#include "thbuf.hpp"
#include "thmem.hpp"

#include "securesocket.hpp"

#ifdef _DEBUG
//#define _FULL_TRACE
#endif

#ifdef _MSC_VER
#pragma warning( disable : 4355 )
#endif

class CREcheck { 
    bool &busy;
public:
    CREcheck(bool &_busy) : busy(_busy)
    { 
        assertex(!busy); 
        busy = true; 
    }
    ~CREcheck()
    {
        busy = false;
    }
};

#define RECHECK(b) CREcheck checkRE(b)

// unique/random pattern used to validate header is of expected protocol on connect
static constexpr unsigned __int64 transferStreamSignature = 0x0ea614193fb99496;

struct TransferStreamHeader
{
    unsigned __int64 hdrSig = transferStreamSignature; // used to validate hdr is of expected protocol on connect
    rowcount_t numrecs;
    rowcount_t pos;
    unsigned id;
    unsigned crc = 0;
    TransferStreamHeader(rowcount_t _pos, rowcount_t _numrecs, unsigned _id)
        : pos(_pos), numrecs(_numrecs), id(_id)
    {
        crc = getCrc();
    }
    TransferStreamHeader() {}
    unsigned getCrc() const
    {
        unsigned retCrc = crc32((const char *)&numrecs, sizeof(numrecs), 0);
        retCrc = crc32((const char *)&pos, sizeof(pos), retCrc);
        retCrc = crc32((const char *)&id, sizeof(id), retCrc);
        return retCrc;
    }
};


class CSocketRowStream: public CSimpleInterface, implements IRowStream
{
    MemoryBuffer inbuf;
    Linked<IEngineRowAllocator> allocator;
    Linked<IOutputRowDeserializer> deserializer;
    Linked<ISocket> socket;
    Owned<ISerialStream> bufferStream;
    CThorStreamDeserializerSource dsz;
    unsigned id;
    bool stopped;
    bool busy; // for reenter check
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CSocketRowStream(unsigned _id,IEngineRowAllocator *_allocator,IOutputRowDeserializer *_deserializer,ISocket *_socket)
        : allocator(_allocator), deserializer(_deserializer), socket(_socket), dsz(0,NULL)
    {
        bufferStream.setown(createMemoryBufferSerialStream(inbuf));
        dsz.setStream(bufferStream);

        id = _id;
        socket->set_block_mode(BF_SYNC_TRANSFER_PULL,0,DEFAULTTIMEOUT*1000);
        busy = false;
        stopped = false;
    }

    const void *nextRow()
    {
        RECHECK(busy);
        if (!socket.get())
            return NULL;
        if (dsz.eos()) {
            inbuf.clear();
#ifdef _FULL_TRACE
            LOG(MCthorDetailedDebugInfo, "CSocketRowStream.nextRow recv (%d,%x)",id,(unsigned)(memsize_t)socket.get());
#endif
            size32_t sz = socket->receive_block_size();
#ifdef _FULL_TRACE
            LOG(MCthorDetailedDebugInfo, "CSocketRowStream.nextRow(%d,%x,%d)",id,(unsigned)(memsize_t)socket.get(),sz);
#endif
            if (sz==0) {
                // eof so terminate (no need to confirm)
                stopped = true;
                return NULL;
            }

            void *buf = inbuf.reserve(sz);
            socket->receive_block(buf,sz);
            assertex(!dsz.eos());
#ifdef _FULL_TRACE
            LOG(MCthorDetailedDebugInfo, "CSocketRowStream.nextRow got (%d,%x,%d)",id,(unsigned)(memsize_t)socket.get(),sz);
#endif
        }
        RtlDynamicRowBuilder rowBuilder(allocator);
        size32_t sz = deserializer->deserialize(rowBuilder,dsz);
        return rowBuilder.finalizeRowClear(sz);
    }


    void stop()
    {
        RECHECK(busy);
        if (!stopped) {
            stopped = true;
            try {
#ifdef _FULL_TRACE
                LOG(MCthorDetailedDebugInfo, "CSocketRowStream.stop(%x)",(unsigned)(memsize_t)socket.get());
#endif
                bool eof = true;
                socket->write(&eof,sizeof(eof)); // confirm stop
#ifdef _FULL_TRACE
                LOG(MCthorDetailedDebugInfo, "CSocketRowStream.stopped(%x)",(unsigned)(memsize_t)socket.get());
#endif
            }
            catch (IException *e) {
                IERRLOG(e,"CSocketRowStream::stop");
                e->Release();
            }
        }
    }
};

class CSocketRowWriter: public CSimpleInterface, implements ISocketRowWriter
{
    IOutputRowSerializer* serializer;
    Linked<IThorRowInterfaces> rowif;
    Linked<ISocket> socket;
    MemoryBuffer outbuf;
    CMemoryRowSerializer rsz;
    size32_t bufsize;
    offset_t pos;
    bool stopped;
    size32_t preallocated;
    bool initbuf;
    unsigned id;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CSocketRowWriter(unsigned _id, IThorRowInterfaces *_rowif,ISocket *_socket,size32_t _bufsize)
        : rowif(_rowif), socket(_socket), rsz(outbuf)
    {
        id = _id;
        assertex(rowif);
        serializer = rowif->queryRowSerializer();
        bufsize = _bufsize;
        pos = 0;
        socket->set_block_mode(BF_SYNC_TRANSFER_PULL,0,DEFAULTTIMEOUT*1000);
        stopped = false;
        initbuf = true;
        size32_t initSize = rowif->queryRowMetaData()->querySerializedDiskMeta()->getMinRecordSize();

        if (initSize>bufsize)
            preallocated = initSize;
        else if (initSize>bufsize/4)
            preallocated = bufsize+bufsize/4;
        else
            preallocated = bufsize+initSize;

#ifdef _FULL_TRACE
        LOG(MCthorDetailedDebugInfo, "CSocketRowWriter(%d,%x) preallocated = %d",id,(unsigned)(memsize_t)socket.get(),preallocated);
#endif
    }

    ~CSocketRowWriter()
    {
        if (!stopped) {
            IWARNLOG("CSocketRowWriter:: releasing before stopped");
            stop();
        }
    }

    void stop()
    {
        if (stopped)
            return;
        stopped = true;
        if (outbuf.length())
            flush();
        try {
#ifdef _FULL_TRACE
            LOG(MCthorDetailedDebugInfo, "CSocketRowWriter.stop(%x)",(unsigned)(memsize_t)socket.get());
#endif
            socket->send_block(NULL,0);
#ifdef _FULL_TRACE
            LOG(MCthorDetailedDebugInfo, "CSocketRowWriter.stopped(%x)",(unsigned)(memsize_t)socket.get());
#endif
        }
        catch (IJSOCK_Exception *e) { // already gone!
            if ((e->errorCode()!=JSOCKERR_broken_pipe)&&(e->errorCode()!=JSOCKERR_graceful_close))
                throw;
            e->Release();
        }
    }

    void putRow(const void *row)
    {
        if (row==NULL) 
            stop();
        else if (!stopped) {
            if (initbuf) {
                outbuf.ensureCapacity(preallocated);
                initbuf = false;
            }
            serializer->serialize(rsz,(const byte *)row);
            if (outbuf.length()>bufsize) 
                flush();
        }
        ReleaseThorRow(row);
    }
    void flush()
    {
        size32_t l = outbuf.length();
#ifdef _FULL_TRACE
        LOG(MCthorDetailedDebugInfo, "CSocketRowWriter.flush(%d,%x,%d)",id,(unsigned)(memsize_t)socket.get(),l);
#endif
        if (l) {
            if (!socket->send_block(outbuf.bufferBase(),l)) {
                LOG(MCthorDetailedDebugInfo, "CSocketRowWriter remote stop");
                stopped = true;
            }
            pos += l;
            outbuf.resetBuffer();
            if (l>preallocated)
                preallocated = l;  // assume the worst
            initbuf = true;
        }
    }
    virtual offset_t getPosition()
    {
        return pos;
    }

    virtual bool bufferSent()
    {
        return initbuf;  // will be true at start
    }
};


IRowStream *ConnectMergeRead(unsigned id, IThorRowInterfaces *rowif,SocketEndpoint &nodeaddr,rowcount_t startrec,rowcount_t numrecs, ISocket *socket)
{
    TransferStreamHeader hdr(startrec, numrecs, id);
#ifdef _FULL_TRACE
    StringBuffer s;
    nodeaddr.getEndpointHostText(s);
    LOG(MCthorDetailedDebugInfo, "ConnectMergeRead(%d,%s,%x,%" RCPF "d,%" RCPF "u)",id,s.str(),(unsigned)(memsize_t)socket.get(),startrec,numrecs);
#endif
    socket->write(&hdr,sizeof(hdr));
    return new CSocketRowStream(id,rowif->queryRowAllocator(),rowif->queryRowDeserializer(),socket);
}


ISocketRowWriter *ConnectMergeWrite(IThorRowInterfaces *rowif,ISocket *socket,size32_t bufsize,rowcount_t &startrec,rowcount_t &numrecs)
{
    TransferStreamHeader hdr;
    unsigned remaining = sizeof(hdr);
    byte *dst = (byte *)&hdr;

    /*
     * A client has connected at this stage, the hdr should be sent swiftly.
     * A generous 1 minute timeout between reads, if longer, timeout exception,
     * will be thrown, and the connection ignored.
     */
    while (true)
    {
        size32_t read;
        socket->readtms(dst, 1, remaining, read, 60*1000); // 1 min timeout
        if (read == remaining)
            break;
        remaining -= read;
        dst += read;
    }
    if (hdr.hdrSig != transferStreamSignature)
    {
        char name[100];
        int port = socket->peer_name(name,sizeof(name));
        StringBuffer hdrRaw;
        hexdump2string((byte const *)&hdr, sizeof(hdr), hdrRaw);
        throw makeStringExceptionV(TE_SortConnectProtocolErr, "SORT connection protocol mismatch from: %s:%u, raw hdr = %s", name, port, hdrRaw.str());
    }
    if (hdr.getCrc() != hdr.crc)
    {
        char name[100];
        int port = socket->peer_name(name,sizeof(name));
        StringBuffer hdrRaw;
        hexdump2string((byte const *)&hdr, sizeof(hdr), hdrRaw);
        throw makeStringExceptionV(TE_SortConnectCrcErr, "SORT connection failed crc check from: %s:%u, raw hdr = %s", name, port, hdrRaw.str());
    }
    startrec = hdr.pos;
    numrecs = hdr.numrecs;
#ifdef _FULL_TRACE
    char name[100];
    int port = socket->peer_name(name,sizeof(name));
    LOG(MCthorDetailedDebugInfo, "ConnectMergeWrite(%d,%s:%d,%x,%" RCPF "d,%" RCPF "u)",hdr.id,name,port,(unsigned)(memsize_t)socket,startrec,numrecs);
#endif
    return new CSocketRowWriter(hdr.id,rowif,socket,bufsize);
}



