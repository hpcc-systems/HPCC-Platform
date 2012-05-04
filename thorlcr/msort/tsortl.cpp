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

#include "platform.h"
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdarg.h>
#ifdef _WIN32
#include <process.h>
#endif

#include "jfile.hpp"
#include "jio.hpp"
#include "jsocket.hpp"

#include "tsorts.hpp"
#include "thbuf.hpp"
#include "thmem.hpp"

#ifdef _DEBUG
//#define _FULL_TRACE
#endif

#define DEFAULTTIMEOUT 3600 // 60 minutes 

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

struct TransferStreamHeader
{
    rowmap_t numrecs;
    rowcount_t pos;
    size32_t   recsize;
    unsigned id;
    TransferStreamHeader(rowcount_t _pos, rowmap_t _numrecs, unsigned _recsize, unsigned _id) 
        : pos(_pos), numrecs(_numrecs), recsize(_recsize), id(_id)
    {
    }
    TransferStreamHeader() {}
    void winrev() { _WINREV(pos); _WINREV(numrecs); _WINREV(recsize);  _WINREV(id); }
};


static ISocket *DoConnect(SocketEndpoint &nodeaddr)
{
    return ISocket::connect_wait(nodeaddr,DEFAULTTIMEOUT);
}

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
            PROGLOG("CSocketRowStream.nextRow recv (%d,%x)",id,(unsigned)(memsize_t)socket.get());
#endif
            size32_t sz = socket->receive_block_size();
#ifdef _FULL_TRACE
            PROGLOG("CSocketRowStream.nextRow(%d,%x,%d)",id,(unsigned)(memsize_t)socket.get(),sz);
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
            PROGLOG("CSocketRowStream.nextRow got (%d,%x,%d)",id,(unsigned)(memsize_t)socket.get(),sz);
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
                PROGLOG("CSocketRowStream.stop(%x)",(unsigned)(memsize_t)socket.get());
#endif
                bool eof = true;
                socket->write(&eof,sizeof(eof)); // confirm stop
#ifdef _FULL_TRACE
                PROGLOG("CSocketRowStream.stopped(%x)",(unsigned)(memsize_t)socket.get());
#endif
            }
            catch (IException *e) {
                EXCLOG(e,"CSocketRowStream::stop");
                e->Release();
            }
        }
    }
};

class CSocketRowWriter: public CSimpleInterface, implements ISocketRowWriter
{
    IOutputRowSerializer* serializer;
    Linked<IRowInterfaces> rowif;
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
    CSocketRowWriter(unsigned _id,IRowInterfaces *_rowif,ISocket *_socket,size32_t _bufsize)
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
        size32_t maxsz = rowif->queryRowMetaData()->querySerializedMeta()->getRecordSize(NULL);
        // bit of a guess - TBD better (using ISerialStream?)
        if (maxsz<rowif->queryRowMetaData()->querySerializedMeta()->getMinRecordSize())
            maxsz=rowif->queryRowMetaData()->querySerializedMeta()->getMinRecordSize();

        if (maxsz>bufsize)
            preallocated = maxsz;
        else if (maxsz>bufsize/4)
            preallocated = bufsize+bufsize/4;
        else
            preallocated = bufsize+maxsz;

#ifdef _FULL_TRACE
        PROGLOG("CSocketRowWriter(%d,%x) preallocated = %d",id,(unsigned)(memsize_t)socket.get(),preallocated);
#endif
    }

    ~CSocketRowWriter()
    {
        if (!stopped) {
            WARNLOG("CSocketRowWriter:: releasing before stopped");
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
            PROGLOG("CSocketRowWriter.stop(%x)",(unsigned)(memsize_t)socket.get());
#endif
            socket->send_block(NULL,0);
#ifdef _FULL_TRACE
            PROGLOG("CSocketRowWriter.stopped(%x)",(unsigned)(memsize_t)socket.get());
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
        PROGLOG("CSocketRowWriter.flush(%d,%x,%d)",id,(unsigned)(memsize_t)socket.get(),l);
#endif
        if (l) {
            if (!socket->send_block(outbuf.bufferBase(),l)) {
                PROGLOG("CSocketRowWriter remote stop");
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


IRowStream *ConnectMergeRead(unsigned id,IRowInterfaces *rowif,SocketEndpoint &nodeaddr,rowcount_t startrec,rowmap_t numrecs)
{
    Owned<ISocket> socket = DoConnect(nodeaddr);
    TransferStreamHeader hdr(startrec,numrecs,0,id);
#ifdef _FULL_TRACE
    StringBuffer s;
    nodeaddr.getUrlStr(s);
    PROGLOG("ConnectMergeRead(%d,%s,%x,%"RCPF"d,%"RMF"u)",id,s.str(),(unsigned)(memsize_t)socket.get(),startrec,numrecs);
#endif
    hdr.winrev();
    socket->write(&hdr,sizeof(hdr));
    return  new CSocketRowStream(id,rowif->queryRowAllocator(),rowif->queryRowDeserializer(),socket);
}


ISocketRowWriter *ConnectMergeWrite(IRowInterfaces *rowif,ISocket *socket,size32_t bufsize,rowcount_t &startrec,rowmap_t &numrecs)
{
    TransferStreamHeader hdr;
    socket->read(&hdr,sizeof(hdr));
    hdr.winrev();
    startrec = hdr.pos;
    numrecs = hdr.numrecs;
#ifdef _FULL_TRACE
    char name[100];
    int port = socket->peer_name(name,sizeof(name));
    PROGLOG("ConnectMergeWrite(%d,%s:%d,%x,%"RCPF"d,%"RMF"u)",hdr.id,name,port,(unsigned)(memsize_t)socket,startrec,numrecs);
#endif
    return new CSocketRowWriter(hdr.id,rowif,socket,bufsize);
}



