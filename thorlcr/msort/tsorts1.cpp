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
#include <limits.h>

#include "jlib.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "thorport.hpp"

#include "tsorts.hpp"
#include "thmem.hpp"


#ifdef _DEBUG
//#define TRACE_UNIQUE
#define _FULL_TRACE
//#define _FULLMPTRACE
//#define TRACE_PARTITION
//#define TRACE_PARTITION_OVERFLOW
#endif


// This contains the original global merge method

class CSortMerge;
typedef CopyReferenceArrayOf<CSortMerge> CSortMergeArray;

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
    CMergeReadStream(IThorRowInterfaces *rowif, unsigned streamno,SocketEndpoint &targetep, rowcount_t startrec, rowcount_t numrecs)
    {
        endpoint = targetep;
        char url[100];
        targetep.getUrlStr(url,sizeof(url));
        LOG(MCthorDetailedDebugInfo, thorJob, "SORT Merge READ: Stream(%u) %s, pos=%" RCPF "d len=%" RCPF "u",streamno,url,startrec,numrecs);
        SocketEndpoint mergeep = targetep;
        mergeep.port+=SOCKETSERVERINC; 
        stream = ConnectMergeRead(streamno,rowif,mergeep,startrec,numrecs);
        LOG(MCthorDetailedDebugInfo, thorJob, "SORT Merge READ: Stream(%u) connected to %s",streamno,url);
    }
    virtual ~CMergeReadStream()
    {
        if (stream) {
            char url[100];
            endpoint.getUrlStr(url,sizeof(url));
            DBGLOG("SORT Merge READ: EOS via destructor for %s",url);
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
            LOG(MCthorDetailedDebugInfo, thorJob, "SORT Merge READ: EOS for %s",url);
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
            LOG(MCthorDetailedDebugInfo, thorJob, "SORT Merge READ: stop for %s",url);
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
    rowcount_t numrecs;
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

    CSortMerge(CSortTransferServerThread *_parent,ISocket* _socket,ISocketRowWriter *_out,rowcount_t _poscount,rowcount_t _numrecs,ISocketSelectHandler *_selecthandler);
    ~CSortMerge()
    {
#ifdef _FULL_TRACE
        LOG(MCthorDetailedDebugInfo, thorJob, "~CSortMerge in");
#endif
        if (started)
            closedown();
#ifdef _FULL_TRACE
        LOG(MCthorDetailedDebugInfo, thorJob, "~CSortMerge out");
#endif
    }
    void init()
    {
        CriticalBlock block(crit);
        started = true;
        char name[64];
        int port = socket->peer_name(name,sizeof(name));
        url.append(name).append(':').append(port);
        LOG(MCthorDetailedDebugInfo, thorJob, "SORT Merge WRITE: start %s, pos=%" RCPF "d, len=%" RCPF "d",url.str(),poscount,numrecs);
        rowcount_t pos=poscount;
        try
        {
            iseq.setown(src.createMergeInputStream(pos,numrecs));
        }
        catch (IException *e)
        {
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
            for (;;) {
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
            LOG(MCthorDetailedDebugInfo, thorJob, "waitdone %s",peer);
        }
        else
            peer[0] = 0;
        while (!done)
            donesem.wait();
        if (exception)
            throw exception.getClear();
        if (peer[0])
            LOG(MCthorDetailedDebugInfo, thorJob, "waitdone exit");
    }
    bool notifySelected(ISocket *sock,unsigned selected)
    {
        while (!done) {
            try {
                if (closing) {
                    closing = false;
#ifdef _FULL_TRACE
                    LOG(MCthorDetailedDebugInfo, thorJob, "notifySelected calling closedown");
#endif
                    closedown();
#ifdef _FULL_TRACE
                    LOG(MCthorDetailedDebugInfo, thorJob, "notifySelected called closedown");
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
    Linked<IThorRowInterfaces> rowif;
    CriticalSection rowifsect;
    Semaphore rowifsem;
public:
    IMPLEMENT_IINTERFACE_USING(Thread)

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

    void setRowIF(IThorRowInterfaces *_rowif)
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
        DBGLOG("CSortTransferServerThread::stop");
        term = true;
        try {
            server->cancel_accept();
        }
        catch (IJSOCK_Exception *e) {
            PrintExceptionLog(e,"CSortTransferServerThread:stop");
        }
        verifyex(join(10*60*1000));
        DBGLOG("CSortTransferServerThread::stopped");
    }

    int run() 
    {
        DBGLOG("CSortTransferServerThread started port %d",slave.getTransferPort());
        unsigned numretries = 10;
        try {
            while (!term)
            {
                Owned<ISocket> socket = server->accept(true);
                if (!socket)
                    break;

                rowcount_t poscount=0;
                rowcount_t numrecs=0;
                ISocketRowWriter *strm=NULL;
                try
                {
                    waitRowIF();
                    strm = ConnectMergeWrite(rowif,socket,0x100000,poscount,numrecs);
                }
                catch (IJSOCK_Exception *e) // retry if failed
                {
                    PrintExceptionLog(e, "WARNING: Exception(ConnectMergeWrite)");
                    if (--numretries==0)
                        throw;
                    e->Release();
                    continue;
                }
                catch (IException *e) // only retry if serialization check failed, indicating possible foreign client connect
                {
                    PrintExceptionLog(e, "WARNING: Exception(ConnectMergeWrite)");
                    if (TE_InvalidSortConnect != e->errorCode() || (--numretries==0))
                        throw;
                    e->Release();
                    continue;
                }
                CriticalBlock block(childsect);
                add(strm, socket.getClear(), poscount, numrecs);
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
        DBGLOG("CSortTransferServerThread finished");
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

    void add(ISocketRowWriter *strm,ISocket *socket,rowcount_t poscount,rowcount_t numrecs) // takes ownership of sock
    {
        CriticalBlock proc(childsect);
        if (!selecthandler) {
            selecthandler.setown(createSocketSelectHandler("SORT"));
            selecthandler->start();
        }
        CSortMerge *sub = new CSortMerge(this,socket,strm,poscount,numrecs,selecthandler); // NB: takes ownership of 'socket'
        children.append(*sub);
        selecthandler->add(socket,SELECTMODE_READ,sub);

    }

    void remove(ISocket *socket)
    {
        CriticalBlock proc(childsect);
        if (selecthandler) 
            selecthandler->remove(socket);
    }



    rowcount_t merge(unsigned mapsize,rowcount_t *map,rowcount_t *mapupper,
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
            DBGLOG("  %s",url);
            for (j=0;j<numnodes;j++) {
                DBGLOG("  %u,",map[k]);
                k++;
            }
        }
#endif  
        rowcount_t resnum=0;
        for (i=0;i<numnodes;i++) 
            resnum += vMAPU(i,partno)-vMAPL(i,partno-1);
        // calculate start position
        unsigned __int64 respos=0;
        for(i=0;i<partno;i++) 
            for (j=0;j<numnodes;j++) 
                respos += vMAPL(j,i)-vMAPL(j,i-1);      // note we are adding up all of the lower as we want start

        rowcount_t totalrows = resnum;
        LOG(MCthorDetailedDebugInfo, thorJob, "Output start = %" RCPF "d, num = %" RCPF "u",respos,resnum);

        IArrayOf<IRowStream> readers;
        IException *exc = NULL;
        try
        {
            for (j=0;j<numnodes;j++)
            {
                unsigned i=j;
                rowcount_t sstart=vMAPL(i,partno-1);
                rowcount_t snum=vMAPU(i,partno)-sstart;
                if (snum>0)
                {
                    if (i==partno)
                    {
                        LOG(MCthorDetailedDebugInfo, thorJob, "SORT Merge READ: Stream(%u) local, pos=%" RCPF "u len=%" RCPF "u",i,sstart,snum);
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

CSortMerge::CSortMerge(CSortTransferServerThread *_parent,ISocket* _socket,ISocketRowWriter *_out,rowcount_t _poscount,rowcount_t _numrecs,ISocketSelectHandler *_selecthandler)
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
    LOG(MCthorDetailedDebugInfo, thorJob, "SORT Merge: closing %s",url.str());
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
    LOG(MCthorDetailedDebugInfo, thorJob, "SORT Merge: finished %s, %d rows merged",url.str(),ndone);
}

IMergeTransferServer *createMergeTransferServer(ISortSlaveBase *parent)
{
    return new CSortTransferServerThread(*parent);
}
