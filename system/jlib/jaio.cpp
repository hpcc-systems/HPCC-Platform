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
#include <assert.h>
#include <stdio.h>
#include "jaio.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/system/jlib/jaio.cpp $ $Id: jaio.cpp 62376 2011-02-04 21:59:58Z sort $");

#ifdef _WIN32
#define ASYNC_READ_TIMEOUT  5000        // milliseconds
#define ASYNC_MAX_TIMEOUTS  10          // avoid 'deadlock' if something goes very wrong
#endif


#if defined(__linux__) 
struct aio_result_t
{
    int aio_return;
    int aio_errno;
    aio_result_t *next;
    aiocb *cb;          // for use by posix style aio.
};
#define AIO_INPROGRESS (-2)

class AsyncRequest
{
public:
    aio_result_t  result; // must be first
    int ready;
    char *        buffer;    
    AsyncRequest()
    {
        result.cb = (aiocb *) calloc(1, sizeof(aiocb));
        result.cb->aio_reqprio = 0; // JCSMORE-Solaris man pages say that this should be set to 0 for portability...?
        result.cb->aio_sigevent.sigev_signo = SIGUSR1;
        result.cb->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
    }
    ~AsyncRequest() { free(result.cb); }
};

AsyncBlockReader::AsyncBlockReader()
{
    cur = new AsyncRequest;
    next = new AsyncRequest;
    eof = 1;
}

AsyncBlockReader::~AsyncBlockReader()
{
    finish();
    delete cur;
    delete next;
}

__declspec (thread) aio_result_t *readyTail = NULL;
__declspec (thread) aio_result_t *ready = NULL;

void aioSigHandler(int sig, siginfo_t *info, void *pContext)
{
    aio_result_t *resultp = (aio_result_t *) info->si_value.sival_ptr;
    if (aio_error(resultp->cb) == EINPROGRESS)
        resultp->aio_return = -1;
    else
    {
        resultp->aio_return = aio_return(resultp->cb);
    
        if (NULL == ready)
            ready = readyTail = resultp;
        else
        {
            readyTail->next = resultp;
            readyTail = resultp;
        }
    }
    resultp->aio_errno = errno;
}

int aioread64(int fildes, char *bufp, int bufs, offset_t offset, int whence, aio_result_t *resultp)
{
    assertex(whence == FILE_BEGIN); // JCSMORE

    struct aiocb *cb = resultp->cb;
    resultp->cb->aio_fildes = fildes;
    resultp->cb->aio_buf = bufp;
    resultp->cb->aio_nbytes = bufs;
    resultp->cb->aio_offset = offset;
    cb->aio_sigevent.sigev_value.sival_ptr = (void *) resultp;

    if (-1 == aio_read(cb))
        resultp->aio_return = -1;
    resultp->aio_errno = errno;
    return 0;
};

aio_result_t *aiowait(void *)
{
    aio_result_t * r;
    while (NULL == ready)        // JCSMORE - isn't there a race condition here? As with aiowait()?
        pause();
    assertex(errno == EINTR);
    r = ready;
    ready = ready->next;
    if (NULL == ready)
        readyTail = NULL;
    return r;
}


void AsyncBlockReader::waitblk()
{
    assertex(next);
    AsyncRequest *req;
    while(!next->ready) {
        req = (AsyncRequest *)aiowait(NULL);
        req->ready = 1;
    }
    req = next;
    next = cur;
    cur = req;
}

void AsyncBlockReader::enqueue(AsyncRequest *req)
{
    if (offset==insize) {
        req->result.aio_return = 0;
        req->result.aio_errno = 0;
        req->ready = 1;
        return;
    }
    assertex(req!=NULL);
    assertex(req->ready);
    req->result.aio_return = AIO_INPROGRESS;
    req->result.aio_errno = 0;
    req->ready = 0;
    int rd = blksize;
    if (insize-offset<blksize)
        rd = (int)(insize-offset);
    if (aioread64(infile, req->buffer, rd, offset, FILE_BEGIN, &req->result)==-1) 
    {
        throw MakeOSException("async failed ");
    }
    offset+=rd;
}

void AsyncBlockReader::init(int file,offset_t st,size32_t blocksize,void *buf1,void *buf2)
{
    struct sigaction action;
    sigemptyset(&action.sa_mask);
    action.sa_sigaction = aioSigHandler;
    action.sa_flags = SA_SIGINFO;
    sigaction(SIGUSR1, &action, NULL);
    finish();
    infile = file;
    struct _stat sb;
    if(_fstat(infile, &sb)==-1) 
        assertex(!"Illegal input file");    
    insize = sb.st_size;
    blksize = blocksize;
    eof = 0;
    cur->buffer = (char *)buf1;
    cur->ready = 1;
    next->buffer = (char *)buf2;
    next->ready = 1;
    start = st;
    offset = start;
    pos = start;
    enqueue(next);
}

void *AsyncBlockReader::readnext(size32_t &ret)
{
    if(eof) {
        ret = 0;
        return 0;
    }
    waitblk();
    if(cur->result.aio_return == -1) {
        errno = cur->result.aio_errno;
        eof = 1;
    }
    else if(offset==insize) {
        eof = 1;
    }
    else
        enqueue(next);
    ret = cur->result.aio_return;
    pos += ret;
    return cur->buffer;
}

void AsyncBlockReader::finish()
{
    if (!eof) {
        waitblk();
    }
}

void AsyncBlockReader::getinfo(offset_t &of,offset_t &p,offset_t &sz)
{
    of = start;
    p = pos;
    sz = insize;
}


#endif




#ifdef _WIN32



class CW32AsyncRequest 
{

public:
    char * buffer;

    CW32AsyncRequest() {buffer = NULL; }

    void setBuffer(void * src) {buffer = static_cast <char *> (src); }
};






CW32AsyncBlockReader::CW32AsyncBlockReader()
{
    currentRequest = new CW32AsyncRequest();
    nextRequest = new CW32AsyncRequest();
    eof = true;
    pending = false;
    overlapped.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
}


CW32AsyncBlockReader::~CW32AsyncBlockReader()
{
    finish();
    CloseHandle(overlapped.hEvent);
    delete nextRequest;
    delete currentRequest;
}


void CW32AsyncBlockReader::finish()
{
    // CancelIo(hfile);             // not supported in Win95
    if(!eof && pending)
    {
        waitblk();
        eof = true;         
    }
}


void CW32AsyncBlockReader::init(HANDLE file, offset_t _start, size32_t _blockSize, void * buffer1, void * buffer2)
{
    hfile = file;
    start = _start; 
    blockSize = _blockSize;

    DWORD sizeHi, sizeLo;   
    sizeLo = GetFileSize(hfile, &sizeHi);
    insize.set(sizeLo, sizeHi);

    currentRequest->setBuffer(buffer1);
    nextRequest->setBuffer(buffer2);

    reset();
}


void CW32AsyncBlockReader::reset()
{
    finish();
    eof = false;
    offset = start;
    offset.get(overlapped.Offset, overlapped.OffsetHigh);   
    enqueue();
}


void CW32AsyncBlockReader::enqueue()    // reads next buffer 
{
    if (offset < insize)
    {
        DWORD bytesRead;
        if(!ReadFile(hfile, nextRequest->buffer, blockSize, &bytesRead, &overlapped))
        {                       
            switch(GetLastError())
            {
            case ERROR_IO_PENDING:
                pending = true;
                break;
            default:
                pending = false;
                eof = true;             // effectively
            }           
        }   
        else    // cached operations will usually complete immediately, but "pending" is still true
        {
            pending = true;
        }
    }
    else
    {
        pending = false;
        eof = true;             
    }
}


DWORD CW32AsyncBlockReader::waitblk()
{
    assertex(pending == true);

    DWORD bytesRead;
    BOOL _WaitForSingleObject; 
    int attempts = ASYNC_MAX_TIMEOUTS;

    do
    {
        switch(WaitForSingleObject(overlapped.hEvent, ASYNC_READ_TIMEOUT))
        {
        case WAIT_OBJECT_0:
            pending = false;                // pending operation complete
            if(GetOverlappedResult(hfile, &overlapped, &bytesRead, FALSE))
            {
                offset += bytesRead;
                offset.get(overlapped.Offset, overlapped.OffsetHigh);

                CW32AsyncRequest * r = nextRequest;
                nextRequest = currentRequest;
                currentRequest = r;
            }
            else
            {
                assertex(false);
                bytesRead = 0;
                eof = true;
            }           
            _WaitForSingleObject = FALSE;
            break;
        case WAIT_TIMEOUT:
            --attempts;
            if(!attempts)                       // ran out of attempts
            {
                pending = false;
                _WaitForSingleObject = FALSE;
                bytesRead = 0;
                eof = true;
            }
            else
            {
                _WaitForSingleObject = TRUE;
            }
            break;                          
        default:            
            assertex(false);                        // overlapped structure probably corrupt
            pending = false;
            eof = true;                         
            bytesRead = 0;
            _WaitForSingleObject = FALSE;
        }
    } 
    while (_WaitForSingleObject);
    return bytesRead;
}


void * CW32AsyncBlockReader::readnext(size32_t &readLength)
{
    if (eof)
    {
        readLength = 0; 
        return NULL;    
    }
    readLength = waitblk();
    if (!eof)
        enqueue();
    return currentRequest->buffer;
}


void CW32AsyncBlockReader::getinfo(offset_t &of, offset_t &p, offset_t &sz)
{
    of = start;
    p = offset.get();
    sz = insize.get();
}


#endif

