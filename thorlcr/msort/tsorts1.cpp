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
#include <limits.h>

#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "thorport.hpp"

#include "tsorts.hpp"
#include "thcrc.hpp"
#include "thmem.hpp"


#define _TRACE

#ifdef _DEBUG
//#define TRACE_UNIQUE
#define _FULL_TRACE
//#define _FULLMPTRACE
//#define TRACE_PARTITION
//#define TRACE_PARTITION_OVERFLOW
#endif


// This contains the original global merge method

class CSortMerge;
MAKEPointerArray(CSortMerge,CSortMergeArray);

class CMergeReadStream : public CSimpleInterface, public IRowStream
{
protected:  
    IRowStream *stream;
    SocketEndpoint endpoint;
    void eos()
    {
        if (stream) {
            stream->Release();
            stream = NULL;
        }
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CMergeReadStream(IRowInterfaces *rowif, unsigned streamno,SocketEndpoint &targetep, rowcount_t startrec, rowmap_t numrecs)
    {
        endpoint = targetep;
#ifdef _TRACE
        char url[100];
        targetep.getUrlStr(url,sizeof(url));
        PrintLog("SORT Merge READ: Stream(%u) %s, pos=%"RCPF"d len=%"RCPF"u",streamno,url,startrec,numrecs);
#endif
        SocketEndpoint mergeep = targetep;
        mergeep.port+=SOCKETSERVERINC; 
        stream = ConnectMergeRead(streamno,rowif,mergeep,startrec,numrecs);
#ifdef _TRACE
        PrintLog("SORT Merge READ: Stream(%u) connected to %s",streamno,url);
#endif
    }
    virtual ~CMergeReadStream()
    {
        if (stream) {
            char url[100];
            endpoint.getUrlStr(url,sizeof(url));
            PrintLog("SORT Merge READ: EOS via destructor for %s",url);
            stream->stop();
        }
        eos();
    }
    
    const void *nextRow()
    { 
        if (stream) {
            OwnedConstThorRow row = stream->nextRow();
            if (row)
                return row.getClear();
#ifdef _FULL_TRACE
            char url[100];
            endpoint.getUrlStr(url,sizeof(url));
            PrintLog("SORT Merge READ: EOS for %s",url);
#endif
            eos();
        }
        return NULL;
    }

    virtual void stop()
    {
        if (stream) {
#ifdef _FULL_TRACE
            char url[100];
            endpoint.getUrlStr(url,sizeof(url));
            PrintLog("SORT Merge READ: stop for %s",url);
#endif
            stream->stop();
            eos();
        }
    }

};


class CSortTransferServerThread;



class CSortMerge: public CSimpleInterface, implements ISocketSelectNotify
{
    Owned<IRowStream> iseq;
    StringBuffer url;
    ISortSlaveBase &src;
    Owned<ISocketRowWriter> out;
    rowcount_t poscount;
    rowmap_t numrecs;
//  unsigned pos;
//  unsigned endpos;
    unsigned ndone;
    bool started;
    CSortTransferServerThread *parent;
    bool done;
    bool closing;
    Semaphore donesem;
    Owned<IException> exception;
    ISocketSelectHandler *selecthandler;
protected:
    Owned<ISocket> socket;
    CriticalSection crit;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSortMerge(CSortTransferServerThread *_parent,ISocket* _socket,ISocketRowWriter *_out,rowcount_t _poscount,rowmap_t _numrecs,ISocketSelectHandler *_selecthandler);
    ~CSortMerge()
    {
#ifdef _FULL_TRACE
        PROGLOG("~CSortMerge in");
#endif
        if (started)
            closedown();
#ifdef _FULL_TRACE
        PROGLOG("~CSortMerge out");
#endif
    }
    void init()
    {
        CriticalBlock block(crit);
        started = true;
#ifdef _TRACE
        char name[64];
        int port = socket->peer_name(name,sizeof(name));
        url.append(name).append(':').append(port);
        PrintLog("SORT Merge WRITE: start %s, pos=%"RCPF"d, len=%"RCPF"d",url.str(),poscount,numrecs);
#endif
        rowmap_t pos=(rowmap_t)poscount;
        assertex(pos==poscount);
        try {
            iseq.setown(src.createMergeInputStream(pos,numrecs));
        }
        catch (IException *e) {
            PrintExceptionLog(e,"**Exception(4a)");
            throw;
        }
    }

    void closedown();

    bool processRows()
    {
        // NB sends a single buffer
        CriticalBlock block(crit);
        bool sent = false;
        try {
            if (!socket)
                return false;
            if (!started)
                init();
            loop {
                OwnedConstThorRow row = iseq->nextRow();
                if (!row) {
                    if (sent)
                        out->flush();
                    break;
                }
                out->putRow(row.getClear());     
                ndone ++;
                sent = true;
                if (out->bufferSent())
                    return true;
            }
        }
        catch (IException *e) {
            PrintExceptionLog(e,"CSortMergeBase processRows");
            throw;
        }
        return sent;
    }
    void waitdone()
    {
        char peer[16];
        if (socket) {
            socket->peer_name(peer,sizeof(peer)-1);
            PROGLOG("waitdone %s",peer);
        }
        else
            peer[0] = 0;
        while (!done)
            donesem.wait();
        if (exception)
            throw exception.getClear();
        if (peer[0])
            PROGLOG("waitdone exit");
    }
    bool notifySelected(ISocket *sock,unsigned selected)
    {
        while (!done) {
            try {
                if (closing) {
                    closing = false;
#ifdef _FULL_TRACE
                    PROGLOG("notifySelected calling closedown");
#endif
                    closedown();
#ifdef _FULL_TRACE
                    PROGLOG("notifySelected called closedown");
#endif
                    done = true;
                    donesem.signal();
                    return false;
                }
            }
            catch (IException *e) {
                EXCLOG(e,"CSortMerge notifySelected.1");
                exception.setown(e);
                done = true;
                donesem.signal();
                return false;
            }
            try {
                if (processRows()) 
                    return false;   // false correct here
            }
            catch (IException *e) {
                EXCLOG(e,"CSortMerge notifySelected.2");
                exception.setown(e);
            }
            closing = true;
            CriticalBlock block(crit);
            if (!sock||!socket)
                break;
            if (sock->avail_read()==0)
                break;
        }
        return false; // false correct here
    }
};


class CSortTransferServerThread: protected Thread, implements IMergeTransferServer
{
protected: friend class CSortMerge;
    ISortSlaveBase &slave;
    bool term;
    Owned<ISocket> server;
    CriticalSection childsect;
    CSortMergeArray children;
    Owned<ISocketSelectHandler> selecthandler;
    Linked<IRowInterfaces> rowif;
    CriticalSection rowifsect;
    Semaphore rowifsem;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface)

    void start() 
    { 
        Thread::start(); 
    }

    CSortTransferServerThread(ISortSlaveBase &in) 
        : slave(in), Thread("SortTransferServer") 
    {
        unsigned port = in.getTransferPort();
        server.setown(ISocket::create(port));
        term = false;
    }

    void setRowIF(IRowInterfaces *_rowif)
    {
        // bit of a kludge
        CriticalBlock block(rowifsect);
        rowif.set(_rowif);
        rowifsem.signal();
    }

    void waitRowIF()
    {
        // bit of a kludge
        CriticalBlock block(rowifsect);
        while (!rowif&&!term) {
            PROGLOG("CSortTransferServerThread waiting for row interface");
            CriticalUnblock unblock(rowifsect);
            rowifsem.wait(60*1000);
        }
    }

    void stop()
    {
        PrintLog("CSortTransferServerThread::stop");
        term = true;
        try {
            server->cancel_accept();
        }
        catch (IJSOCK_Exception *e) {
            PrintExceptionLog(e,"CSortTransferServerThread:stop");
        }
        verifyex(join(10*60*1000));
        PrintLog("CSortTransferServerThread::stopped");
    }

    int run() 
    {
        PrintLog("CSortTransferServerThread started port %d",slave.getTransferPort());
        unsigned numretries = 10;
        try {
            while (!term) {
                ISocket* socket = server->accept(true);
                if (!socket) {
                    break;
                }

                rowcount_t poscount=0;
                rowmap_t numrecs=0;
                ISocketRowWriter *strm=NULL;
                try {
                    waitRowIF();
                    strm = ConnectMergeWrite(rowif,socket,0x100000,poscount,numrecs);   
                }
                catch (IJSOCK_Exception *e) { // retry if failed
                    PrintExceptionLog(e,"WARNING: Exception(ConnectMergeWrite)");
                    if (--numretries==0)
                        throw e;
                    e->Release();
                    continue;
                }
                CriticalBlock block(childsect);
                add(strm,socket,poscount,numrecs);
            }
        }
        catch (IJSOCK_Exception *e)
        {
            if (e->errorCode()!=JSOCKERR_cancel_accept) {
                PrintExceptionLog(e,"CSortTransferServerThread Exception");
                // Ignore for now
            }
            e->Release();
        }
        try {
            server->close();
        }
        catch (IJSOCK_Exception *e)
        {
            if (e->errorCode()!=JSOCKERR_cancel_accept) {
                PrintExceptionLog(e,"CSortTransferServerThread closing");
                // Ignore for now
            }
            e->Release();
        }
        subjoin();
        PrintLog("CSortTransferServerThread finished");
        return 0;
    }

    void subjoin()
    {
        CriticalBlock proc(childsect);
        ForEachItemIn(i,children)
        {
            CSortMerge &c = children.item(i);
            CriticalUnblock unblock(childsect);
            c.waitdone();
        }
        if (selecthandler) {
            selecthandler->stop(true);  
            selecthandler.clear();
        }
        ForEachItemIn(i2,children)
        {
            CSortMerge &c = children.item(i2);
            c.Release();
        }
        children.kill();
        rowif.clear();

    }

    void add(ISocketRowWriter *strm,ISocket *socket,rowcount_t poscount,rowmap_t numrecs) // takes ownership of sock
    {
        CriticalBlock proc(childsect);
        if (!selecthandler) {
            selecthandler.setown(createSocketSelectHandler("SORT"));
            selecthandler->start();
        }
        CSortMerge *sub = new CSortMerge(this,socket,strm,poscount,numrecs,selecthandler);
        children.append(*sub);
        selecthandler->add(socket,SELECTMODE_READ,sub);

    }

    void remove(ISocket *socket)
    {
        CriticalBlock proc(childsect);
        if (selecthandler) 
            selecthandler->remove(socket);
    }



    rowmap_t merge(unsigned mapsize,rowmap_t *map,rowmap_t *mapupper,
                   unsigned numnodes,SocketEndpoint* endpoints,
                   unsigned partno)
    {

    // map format is an array numnodes*numnodes 
    //     with columns corresponding to split pos (final column is size)
    //     and rows indicating node number

        waitRowIF();

#define vMAPL(node,col) (((int)col>=0)?map[node*numnodes+col]:0)
#define vMAPU(node,col) (mapupper?(((int)col>=0)?mapupper[node*numnodes+col]:0):vMAPL(node,col))

        assertex(mapsize==numnodes*numnodes);
        unsigned i;
        unsigned j;
#ifdef TRACE_PARTITION
        unsigned k = 0;
        for (i=0;i<numnodes;i++) {
            char url[100];
            endpoints[i].getUrlStr(url,sizeof(url));
            PrintLog("  %s",url);
            for (j=0;j<numnodes;j++) {
                PrintLog("  %u,",map[k]);
                k++;
            }
        }
#endif  
        rowmap_t resnum=0;
        for (i=0;i<numnodes;i++) 
            resnum += vMAPU(i,partno)-vMAPL(i,partno-1);
        // calculate start position
        unsigned __int64 respos=0;
        for(i=0;i<partno;i++) 
            for (j=0;j<numnodes;j++) 
                respos += vMAPL(j,i)-vMAPL(j,i-1);      // note we are adding up all of the lower as we want start

        rowmap_t totalrows = resnum;
        PrintLog("Output start = %"RCPF"d, num = %"RCPF"u",respos,resnum);

        IArrayOf<IRowStream> readers;
        IException *exc = NULL;
        try {
            for (j=0;j<numnodes;j++) {
                unsigned i=j;
                rowmap_t sstart=vMAPL(i,partno-1);
                rowmap_t snum=vMAPU(i,partno)-sstart; 
                if (snum>0) {
                    if (i==partno) {
                        PrintLog("SORT Merge READ: Stream(%u) local, pos=%"RCPF"u len=%"RCPF"u",i,sstart,snum);
                        readers.append(*slave.createMergeInputStream(sstart,snum));
                    }
                    else
                        readers.append(*new CMergeReadStream(rowif,i,endpoints[i], sstart, snum));
                }
            }
        }
        catch (IException *e)
        {
            PrintExceptionLog(e,"**MultiMerge");
            exc = e;
        }
        if (!exc) {
            try {
                slave.startMerging(readers, totalrows);
            }
            catch (IException *e)
            {
                PrintExceptionLog(e,"**MultiMerge.2");
                exc = e;
            }
        }
        if (exc)
            throw exc;
        return totalrows;
    }
};

CSortMerge::CSortMerge(CSortTransferServerThread *_parent,ISocket* _socket,ISocketRowWriter *_out,rowcount_t _poscount,rowmap_t _numrecs,ISocketSelectHandler *_selecthandler)
    : src(_parent->slave),socket(_socket),out(_out)
{
    parent = _parent;
    poscount = _poscount;
    numrecs = _numrecs;
    ndone = 0;
    started = false;
    selecthandler = _selecthandler;
    done = false;
    closing = false;
}

void CSortMerge::closedown()
{
    CriticalBlock block(crit);
#ifdef _FULL_TRACE
    PrintLog("SORT Merge: closing %s",url.str());
#endif
    if (!socket)
        return;
    try {
        iseq.clear();
    }
    catch (IException *e) {
        PrintExceptionLog(e,"**Exception(4b)");
        throw;
    }
    try {
        if (out)
            out->stop();
    }
    catch (IException *e) {
        PrintExceptionLog(e,"**Exception(4c)");
        throw;
    }
    try {
        out.clear();
    }
    catch (IException *e) {
        PrintExceptionLog(e,"**Exception(5)");
        throw;
    }
    parent->remove(socket);
    try {
        socket.clear();
    }
    catch (IException *e) {
        PrintExceptionLog(e,"**Exception(5b)");
        throw;
    }
    started = false;
#ifdef _TRACE
    PrintLog("SORT Merge: finished %s, %d rows merged",url.str(),ndone);
#endif
}

IMergeTransferServer *createMergeTransferServer(ISortSlaveBase *parent)
{
    return new CSortTransferServerThread(*parent);
}
