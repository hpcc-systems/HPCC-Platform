/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "jexcept.hpp"
#include "thorherror.h"
#include "roxiehelper.hpp"
#include "roxielmj.hpp"

#include "jmisc.hpp"
#include "jfile.hpp"
#include "mpbase.hpp"
#include "dafdesc.hpp"
#include "dadfs.hpp"

unsigned traceLevel = 0;

//OwnedRowArray
void OwnedRowArray::clear()
{
    ForEachItemIn(idx, buff)
        ReleaseRoxieRow(buff.item(idx));
    buff.kill();
}

void OwnedRowArray::clearPart(aindex_t from, aindex_t to)
{
    aindex_t idx;
    for(idx = from; idx < to; idx++)
        ReleaseRoxieRow(buff.item(idx));
    buff.removen(from, to-from);
}

void OwnedRowArray::replace(const void * row, aindex_t pos)
{
    ReleaseRoxieRow(buff.item(pos));
    buff.replace(row, pos);
}

//=========================================================================================

//CRHRollingCacheElem copied/modified from THOR
CRHRollingCacheElem::CRHRollingCacheElem()
{
    row = NULL;
    cmp = INT_MIN; 
}
CRHRollingCacheElem::~CRHRollingCacheElem()
{
    if (row)
        ReleaseRoxieRow(row);
}
void CRHRollingCacheElem::set(const void *_row)
{
    if (row)
        ReleaseRoxieRow(row);
    row = _row;
}


//CRHRollingCache copied/modified from THOR CRollingCache
CRHRollingCache::~CRHRollingCache()
{
    loop 
    {  
        CRHRollingCacheElem *e = cache.dequeue();  
        if (!e)  
            break;  
        delete e;  
    }  
}

void CRHRollingCache::init(IInputBase *_in, unsigned _max)
{
    max = _max;
    in =_in;
    cache.clear();
    cache.reserve(max);
    eos = false;
    while (cache.ordinality()<max/2)
        cache.enqueue(NULL);
    while (!eos && (cache.ordinality()<max))
        advance();
}

#ifdef TRACEROLLING
void CRHRollingCache::PrintCache()
{
    for (unsigned i = 0;i<max;i++) {
        CRHRollingCacheElem *e = cache.item(i);
        if (i==0)
            DBGLOG("RC==============================");
        int ii = 0;
        if (e && e->row)
            ii = isalpha(*((char*)e->row)) ? 0 : 4;
        chas sz[100];
        sprintf(sz,"%c%d: %s",(i==max/2)?'>':' ',i,e?(const char *)e->row+ii:"-----");
        for (int xx=0; sz[xx] != NULL; xx++)
        {
            if (!isprint(sz[xx]))
            {
                sz[xx] = NULL;
                break;
            }
        }
        DBGLOG(sz);
        if (i == max-1)
            DBGLOG("RC==============================");
    }
}
#endif

CRHRollingCacheElem * CRHRollingCache::mid(int rel)
{
    return cache.item((max/2)+rel); // relies on unsigned wrap
}

void CRHRollingCache::advance()
{
    CRHRollingCacheElem *e = (cache.ordinality()==max)?cache.dequeue():NULL;    //cache full, remove head element
    if (!eos) {
        if (!e)
            e = new CRHRollingCacheElem();
        const void * nextrec = in->nextInGroup();//get row from CRHCRHDualCache::cOut, which gets from CRHCRHDualCache, which gets from input
        if (!nextrec)
            nextrec = in->nextInGroup();
        if (nextrec) {
            e->set(nextrec);
            cache.enqueue(e);
#ifdef TRACEROLLING
            PrintCache();
#endif
            return;
        }
        else
            eos = true;
    }
    delete e;
    cache.enqueue(NULL);
#ifdef TRACEROLLING
    PrintCache();
#endif
}

//=========================================================================================

//CRHDualCache copied from THOR, and modified to get input from IInputBase instead 
//of IReadSeqVar and to manage rows as OwnedRoxieRow types
CRHDualCache::CRHDualCache()
{
    strm1 = NULL;
    strm2 = NULL;
}

CRHDualCache::~CRHDualCache()
{
    ::Release(strm1);
    ::Release(strm2);
    loop 
    {  
        CRHRollingCacheElem *e = cache.dequeue();  
        if (!e)  
            break;  
        delete e;  
    }  
}

void CRHDualCache::init(IInputBase * _in)
{
    in = _in;
    cache.clear();
    eos = false;
    base = 0;
    posL = 0;
    posR = 0;
    strm1 = new cOut(this,posL);
    strm2 = new cOut(this,posR) ;
}

#ifdef TRACEROLLING
void CRHDualCache::PrintCache()
{
    for (unsigned i = 0;i<cache.ordinality();i++) {
        CRHRollingCacheElem *e = cache.item(i);
        if (i==0)
        {
            DBGLOG("DC=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-BASE:%d,posL=%d,posR=%d %s", base, posL,posR, eos?"EOS":"");
        }
        
        DBGLOG("%c%d: %s",(i==cache.ordinality()/2)?'>':' ',i,e?(const char *)e->row:"-----");
        if (i == cache.ordinality()-1)
            DBGLOG("DC=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-");
    }
}
#endif

bool CRHDualCache::get(unsigned n, CRHRollingCacheElem *&out)
{
    // take off any no longer needed
    CRHRollingCacheElem *e=NULL;
    while ((base<posL) && (base<posR)) {
        delete e;
        e = cache.dequeue();
        base++;
    }
    assertex(n>=base);
    while (!eos && (n-base>=cache.ordinality())) //element already in cache?
    {
        if (!e)
            e = new CRHRollingCacheElem;
        const void * nextrec = in->nextInGroup();   //get from activity
        if (!nextrec)
            nextrec = in->nextInGroup();
        if (!nextrec) {
            eos = true;
            break;
        }

        e->set(nextrec);

        cache.enqueue(e);
        e = NULL;
#ifdef TRACEROLLING
        PrintCache();
#endif
    }
    delete e;
    if (n-base>=cache.ordinality())
        return false;
    out = cache.item(n-base);
    return true;
}

size32_t CRHDualCache::getRecordSize(const void *ptr)
{
    return in->queryOutputMeta()->getRecordSize(ptr);
}

size32_t CRHDualCache::getFixedSize() const
{
    return in->queryOutputMeta()->getFixedSize();
}

size32_t CRHDualCache::getMinRecordSize() const
{
    return in->queryOutputMeta()->getMinRecordSize();
}

CRHDualCache::cOut::cOut(CRHDualCache *_parent, unsigned &_pos) 
: pos(_pos)
{
    parent = _parent;
    stopped = false;
}

const void * CRHDualCache::cOut::nextInGroup()
{
    CRHRollingCacheElem *e;
    if (stopped || !parent->get(pos,e))
        return NULL;   //no more data
    LinkRoxieRow(e->row);
    pos++;
    return e->row;
}

IOutputMetaData * CRHDualCache::cOut::queryOutputMeta() const
{
    return parent->input()->queryOutputMeta();
}

void CRHDualCache::cOut::stop()
{
    pos = (unsigned)-1;
    stopped = true;
}

//=========================================================================================

IRHLimitedCompareHelper *createRHLimitedCompareHelper()
{
    return new CRHLimitedCompareHelper();
}

//CRHLimitedCompareHelper
void CRHLimitedCompareHelper::init( unsigned _atmost,
                                 IInputBase *_in,
                                 ICompare * _cmp,
                                 ICompare * _limitedcmp )
{
    atmost = _atmost;
    cache.setown(new CRHRollingCache());
    cache->init(_in,(atmost+1)*2);
    cmp = _cmp;
    limitedcmp = _limitedcmp;
}

bool CRHLimitedCompareHelper::getGroup(OwnedRowArray &group, const void *left)
{
    // this could be improved!
    
    // first move 'mid' forwards until mid>=left
    int low = 0;
    loop 
    {
        CRHRollingCacheElem * r = cache->mid(0);
        if (!r)
            break; // hit eos              
        int c = cmp->docompare(left,r->row);
        if (c == 0) 
        {
            r->cmp = limitedcmp->docompare(left,r->row);
            if (r->cmp <= 0) 
                break;
        }
        else if (c < 0) 
        {
            r->cmp = -1;
            break;
        }
        else
            r->cmp = 1;
        cache->advance();
        if (cache->mid(low-1))  // only if haven't hit start
            low--;
    }
    // now scan back (note low should be filled even at eos)
    loop 
    {
        CRHRollingCacheElem * pr = cache->mid(low-1);
        if (!pr)
            break; // hit start 
        int c = cmp->docompare(left,pr->row);
        if (c == 0) 
        {
            pr->cmp = limitedcmp->docompare(left,pr->row);
            if (pr->cmp==1) 
                break;
        }
        else 
        {
            pr->cmp = 1;  
            break;
        }
        low--;
    }
    int high = 0;
    if (cache->mid(0)) // check haven't already hit end
    { 
        // now scan fwd
        loop 
        {
            high++;
            CRHRollingCacheElem * nr = cache->mid(high);
            if (!nr) 
                break;
            int c = cmp->docompare(left,nr->row);
            if (c==0) 
            {
                nr->cmp = limitedcmp->docompare(left,nr->row);
                if (nr->cmp==-1) 
                    break;
            }
            else 
            {
                nr->cmp = -1;
                break;
            }
        }
    }
    while (high-low>(int)atmost) 
    {
        int vl = iabs(cache->mid(low)->cmp);
        int vh = iabs(cache->mid(high-1)->cmp);
        int v;
        if (vl==0) 
        {
            if (vh==0)  // both ends equal
                return false;
            v = vh;
        }
        else if (vh==0)
            v = vl;
        else
            v = imin(vl,vh);
        // remove worst match from either end
        while ((low<high)&&(iabs(cache->mid(low)->cmp)==v))
            low++;
        while ((low<high)&&(iabs(cache->mid(high-1)->cmp)==v))
            high--;
        if (low>=high) 
            return true;   // couldn't make group;
    }
    for (int i=low;i<high;i++) 
    {
        CRHRollingCacheElem *r = cache->mid(i);
        LinkRoxieRow(r->row);
        group.append(r->row);   
    }
    return group.ordinality()>0;
}
//========================================================================================= 

CSafeSocket::CSafeSocket(ISocket *_sock)
{
    httpMode = false;
    mlFmt = MarkupFmt_Unknown;
    sent = 0; 
    heartbeat = false; 
    sock.setown(_sock);
}

CSafeSocket::~CSafeSocket()
{
    sock.clear();
    ForEachItemIn(idx, queued)
    {
        free(queued.item(idx));
    }
    queued.kill();
    lengths.kill();
}

unsigned CSafeSocket::bytesOut() const
{
    return sent;
}

bool CSafeSocket::checkConnection() const
{
    if (sock)
        return sock->check_connection();
    else
        return false;
}

size32_t CSafeSocket::write(const void *buf, size32_t size, bool takeOwnership)
{
    CriticalBlock c(crit); // NOTE: anyone needing to write multiple times without interleave should have already locked this. We lock again for the simple cases.
    OwnedMalloc<void> ownedBuffer;
    if (takeOwnership)
        ownedBuffer.setown((void *) buf);
    if (!size)
        return 0;
    try
    {
        if (httpMode)
        {
            if (!takeOwnership)
            {
                ownedBuffer.setown(malloc(size));
                if (!ownedBuffer)
                    throw MakeStringException(THORHELPER_INTERNAL_ERROR, "Out of memory in CSafeSocket::write (requesting %d bytes)", size);
                memcpy(ownedBuffer, buf, size);
            }
            queued.append(ownedBuffer.getClear());
            lengths.append(size);
            return size;
        }
        else
        {
            sent += size;
            size32_t written = sock->write(buf, size);
            return written;
        }
    }
    catch(...)
    {
        heartbeat = false;
        throw;
    }
}

bool CSafeSocket::readBlock(MemoryBuffer &ret, unsigned timeout, unsigned maxBlockSize)
{
    // MORE - this is still not good enough as we could get someone else's block if there are multiple input datasets
    CriticalBlock c(crit);
    try
    {
        unsigned bytesRead;
        unsigned len;
        try
        {
            sock->read(&len, sizeof (len), sizeof (len), bytesRead, timeout);
        }
        catch (IJSOCK_Exception *E)
        {
            if (E->errorCode()==JSOCKERR_graceful_close)
            {
                E->Release();
                return false;
            }
            throw;
        }
        assertex(bytesRead == sizeof(len));
        _WINREV(len);
        if (len & 0x80000000)
            len ^= 0x80000000;
        if (len > maxBlockSize)
            throw MakeStringException(THORHELPER_DATA_ERROR, "Maximum block size (%d bytes) exceeded (missing length prefix?)", maxBlockSize);
        if (len)
        {
            unsigned bytesRead;
            sock->read(ret.reserveTruncate(len), len, len, bytesRead, timeout);
        }
        return len != 0;
    }
    catch(...)
    {
        heartbeat = false;
        throw;
    }
}

bool CSafeSocket::readBlock(StringBuffer &ret, unsigned timeout, HttpHelper *pHttpHelper, bool &continuationNeeded, bool &isStatus, unsigned maxBlockSize)
{
    continuationNeeded = false;
    isStatus = false;
    CriticalBlock c(crit);
    try
    {
        unsigned bytesRead;
        unsigned len = 0;
        try
        {
            sock->read(&len, sizeof (len), sizeof (len), bytesRead, timeout);
        }
        catch (IJSOCK_Exception *E)
        {
            if (E->errorCode()==JSOCKERR_graceful_close)
            {
                E->Release();
                return false;
            }
            throw;
        }
        assertex(bytesRead == sizeof(len));
        unsigned left = 0;
        char *buf;

        if (pHttpHelper != NULL && strncmp((char *)&len, "POST", 4) == 0)
        {
#define MAX_HTTP_HEADERSIZE 8000
            char header[MAX_HTTP_HEADERSIZE + 1]; // allow room for \0
            sock->read(header, 1, MAX_HTTP_HEADERSIZE, bytesRead, timeout);
            header[bytesRead] = 0;
            char *payload = strstr(header, "\r\n\r\n");
            if (payload)
            {
                *payload = 0;
                payload += 4;
                char *str;

                // capture authentication token
                if ((str = strstr(header, "Authorization: Basic ")) != NULL)
                    pHttpHelper->setAuthToken(str+21);

                // capture content type
                if ((str = strstr(header, "Content-Type: ")) != NULL)
                    pHttpHelper->setContentType(str+14);

                // determine payload length
                str = strstr(header, "Content-Length: ");
                if (str)
                {
                    len = atoi(str + strlen("Content-Length: "));
                    buf = ret.reserveTruncate(len);
                    left = len - (bytesRead - (payload - header));
                    if (len > left)
                        memcpy(buf, payload, len - left); 
                }
                else
                    left = len = 0;
            }
            else
                left = len = 0;

            pHttpHelper->setIsHttp(true);
            if (!len)
                throw MakeStringException(THORHELPER_DATA_ERROR, "Badly formed HTTP header");
        }
        else if (strnicmp((char *)&len, "STAT", 4) == 0)
            isStatus = true;
        else
        {
            _WINREV(len);
            if (len & 0x80000000)
            {
                len ^= 0x80000000;
                continuationNeeded = true;
            }
            if (len > maxBlockSize)
                throw MakeStringException(THORHELPER_DATA_ERROR, "Maximum block size (%d bytes) exceeded (missing length prefix?)", maxBlockSize);
            left = len;

            if (len)
                buf = ret.reserveTruncate(len);
        }

        if (left)
        {
            sock->read(buf + (len - left), left, left, bytesRead, timeout);
        }

        return len != 0;
    }
    catch (...)
    {
        heartbeat = false;
        throw;
    }
}

void CSafeSocket::setHttpMode(const char *queryName, bool arrayMode, TextMarkupFormat _mlfmt)
{
    CriticalBlock c(crit); // Should not be needed
    httpMode = true;
    mlFmt = _mlfmt;
    heartbeat = false;
    assertex(contentHead.length()==0 && contentTail.length()==0);
    if (mlFmt==MarkupFmt_JSON)
    {
        contentHead.append("{");
        contentTail.append("}");
    }
    else
    {
        contentHead.append(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
            "<soap:Body>");
        if (arrayMode)
        {
            contentHead.append("<").append(queryName).append("ResponseArray>");
            contentTail.append("</").append(queryName).append("ResponseArray>");
        }
        contentTail.append("</soap:Body></soap:Envelope>");
    }
}

void CSafeSocket::setHeartBeat()
{
    CriticalBlock c(crit);
    heartbeat = true;
}

bool CSafeSocket::sendHeartBeat(const IContextLogger &logctx)
{
    if (heartbeat)
    {
        StringBuffer s;
        bool rval = false;

        unsigned replyLen = 5;
        unsigned rev = replyLen | 0x80000000; // make it a blocked msg
        _WINREV(rev);
        s.append(sizeof(rev), (char *) &rev);
        s.append('H');
        rev = (unsigned) time(NULL);
        _WINREV(rev);
        s.append(sizeof(rev), (char *) &rev);
        
        try
        {
            CriticalBlock c(crit);
            sock->write(s.str(), replyLen + sizeof(rev));
            rval = true;
        }
        catch (IException * E)
        {
            StringBuffer error("HeartBeat write failed with exception: ");
            E->errorMessage(error);
            logctx.CTXLOG("%s", error.str());
            E->Release();
        }
        catch(...)
        {
            logctx.CTXLOG("HeartBeat write failed (Unknown exception)");
        }
        return rval;
    }
    else
        return true;
};

void CSafeSocket::flush()
{
    if (httpMode)
    {
        unsigned length = contentHead.length() + contentTail.length();
        ForEachItemIn(idx, lengths)
            length += lengths.item(idx);

        StringBuffer header;
        header.append("HTTP/1.0 200 OK\r\n");
        header.append("Content-Type: ").append(mlFmt == MarkupFmt_JSON ? "application/json" : "text/xml").append("\r\n");
        header.append("Content-Length: ").append(length).append("\r\n\r\n");


        CriticalBlock c(crit); // should not be anyone writing but better to be safe
        if (traceLevel > 5)
            DBGLOG("Writing HTTP header length %d to HTTP socket", header.length());
        sock->write(header.str(), header.length());
        sent += header.length();
        if (traceLevel > 5)
            DBGLOG("Writing content head length %d to HTTP socket", contentHead.length());
        sock->write(contentHead.str(), contentHead.length());
        sent += contentHead.length();
        ForEachItemIn(idx2, queued)
        {
            unsigned length = lengths.item(idx2);
            if (traceLevel > 5)
                DBGLOG("Writing block length %d to HTTP socket", length);
            sock->write(queued.item(idx2), length);
            sent += length;
        }
        if (traceLevel > 5)
            DBGLOG("Writing content tail length %d to HTTP socket", contentTail.length());
        sock->write(contentTail.str(), contentTail.length());
        sent += contentTail.length();
        if (traceLevel > 5)
            DBGLOG("Total written %d", sent);
    }
}

void CSafeSocket::sendException(const char *source, unsigned code, const char *message, bool isBlocked, const IContextLogger &logctx)
{
    try
    {
        FlushingStringBuffer response(this, isBlocked, MarkupFmt_XML, false, httpMode, logctx);
        response.startDataset("Exception", NULL, (unsigned) -1);
        response.appendf("<Source>%s</Source><Code>%d</Code>", source, code);
        response.append("<Message>");
        response.encodeXML(message);
        response.append("</Message>");
    }
    catch(IException *EE)
    {
        StringBuffer error("While reporting exception: ");
        EE->errorMessage(error);
        logctx.CTXLOG("%s", error.str());
        EE->Release();
    }
#ifndef _DEBUG
    catch(...) {}
#endif
}

//==============================================================================================================

#define RESULT_FLUSH_THRESHOLD 10000u

#ifdef _DEBUG
#define HTTP_SPLIT_THRESHOLD 100u
#define HTTP_SPLIT_RESERVE 200u
#else
#define HTTP_SPLIT_THRESHOLD 64000u
#define HTTP_SPLIT_RESERVE 65535u
#endif
interface IXmlStreamFlusher;

//==============================================================================================================

bool FlushingStringBuffer::needsFlush(bool closing)
{
    if (isBlocked || closing) // can't flush unblocked. MORE - may need to break it up though.... 
    {
        size32_t len = s.length() - emptyLength;
        return len > (closing ? 0 : RESULT_FLUSH_THRESHOLD);
    }
    else
        return false; // MORE - if there is a single result, it can be flushed (actually, can flush anytime all prior results have been closed)
}

void FlushingStringBuffer::startBlock()
{
    size32_t len = 0;
    s.clear();
    if (!isHttp)
        append(sizeof(size32_t), (char *) &len);
    rowCount = 0;
    if (isBlocked)
    {
        s.append('R');
        unsigned rev = sequenceNumber++;
        _WINREV(rev);
        s.append(sizeof(rev), (char *) &rev);
        rev = rowCount;
        _WINREV(rev);
        s.append(sizeof(rev), (char *) &rev); // NOTE -  need to patch up later. At this point it is 0.
        s.append(strlen(name)+1, name);
    }
    emptyLength = s.length();
    // MORE - should probably pre-reserve string at RESULT_FLUSH_THRESHOLD plus a bit
}

FlushingStringBuffer::FlushingStringBuffer(SafeSocket *_sock, bool _isBlocked, TextMarkupFormat _mlFmt, bool _isRaw, bool _isHttp, const IContextLogger &_logctx)
  : sock(_sock), isBlocked(_isBlocked), mlFmt(_mlFmt), isRaw(_isRaw), isHttp(_isHttp), logctx(_logctx)
{
    sequenceNumber = 0;
    rowCount = 0;
    isSoap = false;
    isEmpty = true;
    extend = false;
    trim = false;
    emptyLength = 0;
    tagClosed = true;
}

FlushingStringBuffer::~FlushingStringBuffer()
{
    try
    {
        flush(true);
    }
    catch (IException *E)
    {
        // Ignore any socket errors that we get at termination - nothing we can do about them anyway...
        E->Release();
    }
    catch(...)
    {
    }
    ForEachItemIn(idx, queued)
    {
        free(queued.item(idx));
    }
}

//void FlushingStringBuffer::append(char data)
//{
    //append(1, &data);
//}

void FlushingStringBuffer::append(const char *data)
{
    append(strlen(data), data);
}

void FlushingStringBuffer::append(unsigned len, const char *data)
{
    try
    {
        CriticalBlock b(crit);
        s.append(len, data);
    }
    catch (IException *E)
    {
        logctx.logOperatorException(E, __FILE__, __LINE__, "FlushingStringBuffer::append");
        throw;
    }
}

void FlushingStringBuffer::appendf(const char *format, ...)
{
    StringBuffer t;
    va_list args;
    va_start(args, format);
    t.valist_appendf(format, args);
    va_end(args);
    append(t.length(), t.str());
}

void FlushingStringBuffer::encodeXML(const char *x, unsigned flags, unsigned len, bool utf8)
{
    StringBuffer t;
    ::encodeXML(x, t, flags, len, utf8);
    append(t.length(), t.str());
}

void FlushingStringBuffer::addPayload(StringBuffer &s, unsigned int reserve)
{
    if (!s.length())
        return;
    lengths.append(s.length());
    queued.append(s.detach());
    if (reserve)
        s.ensureCapacity(reserve);
}

void FlushingStringBuffer::flushXML(StringBuffer &current, bool isClosing)
{
    CriticalBlock b(crit);
    if (isHttp) // we don't do any chunking for non-HTTP yet
    {
        if (isClosing || current.length() > HTTP_SPLIT_THRESHOLD)
        {
            addPayload(s, HTTP_SPLIT_RESERVE);
            addPayload(current, isClosing ? 0 : HTTP_SPLIT_RESERVE);
        }
    }
    else if (isClosing)
        append(current.length(), current.str());
}

void FlushingStringBuffer::flush(bool closing)
{
    CriticalBlock b(crit);
    if (closing && tail.length())
    {
        s.append(tail);
        tail.clear();
    }
    if (isHttp)
    {
        if (!closing && s.length() > HTTP_SPLIT_THRESHOLD)
            addPayload(s, HTTP_SPLIT_RESERVE);
    }
    else if (needsFlush(closing))
    {
        // MORE - if not blocked we can get very large blocks.
        assertex(s.length() > sizeof(size32_t));
        unsigned replyLen = s.length() - sizeof(size32_t);
        unsigned revLen = replyLen | ((isBlocked)?0x80000000:0);
        _WINREV(revLen);
        if (logctx.queryTraceLevel() > 1)
        {
            if (isBlocked)
                logctx.CTXLOG("Sending reply: Sending blocked %s data", getFormatName(mlFmt));
            else
#ifdef _DEBUG
                logctx.CTXLOG("Sending reply length %d: %.1024s", (unsigned) (s.length() - sizeof(size32_t)), s.str()+sizeof(size32_t));
#else
                logctx.CTXLOG("Sending reply length %d: %.40s", (unsigned) (s.length() - sizeof(size32_t)), s.str()+sizeof(size32_t));
#endif
        }
        *(size32_t *) s.str() = revLen;
        if (isBlocked)
        {
            unsigned revRowCount = rowCount;
            _WINREV(revRowCount);
            *(size32_t *) (s.str()+9) = revRowCount;
        }
        if (logctx.queryTraceLevel() > 9)
            logctx.CTXLOG("writing block size %d to socket", replyLen);
        try
        {
            if (sock)
            {
                if (isHttp)
                    sock->write(s.str()+sizeof(revLen), replyLen);
                else
                    sock->write(s.str(), replyLen + sizeof(revLen));
            }
            else
                fwrite(s.str()+sizeof(revLen), replyLen, 1, stdout);
        }
        catch (...)
        {
            if (logctx.queryTraceLevel() > 9)
                logctx.CTXLOG("Exception caught FlushingStringBuffer::flush");

            s.clear();
            emptyLength = 0;
            throw;
        }

        if (logctx.queryTraceLevel() > 9)
            logctx.CTXLOG("wrote block size %d to socket", replyLen);

        if (closing)
        {
            s.clear();
            emptyLength = 0;
        }
        else
            startBlock();
    }
}

void *FlushingStringBuffer::getPayload(size32_t &length)
{
    assertex(isHttp);
    CriticalBlock b(crit);
    if (queued.ordinality())
    {
        length = lengths.item(0);
        void *ret = queued.item(0);
        queued.remove(0);
        lengths.remove(0);
        return ret;
    }
    length = s.length();
    return length ? s.detach() : NULL;
}

void FlushingStringBuffer::startDataset(const char *elementName, const char *resultName, unsigned sequence, bool _extend, IProperties *xmlns)
{
    CriticalBlock b(crit);
    extend = _extend;
    if (isEmpty || !extend)
    {
        name.clear().append(resultName ? resultName : elementName);
        sequenceNumber = 0;
        startBlock();
        if (!isBlocked)
        {
            if (mlFmt==MarkupFmt_XML)
            {
                s.append('<').append(elementName);
                if (isSoap && (resultName || (sequence != (unsigned) -1)))
                {
                    s.append(" xmlns=\'urn:hpccsystems:ecl:").appendLower(queryName.length(), queryName.sget()).append(":result:");
                    if (resultName && *resultName)
                        s.appendLower(strlen(resultName), resultName).append('\'');
                    else
                        s.append("result_").append(sequence+1).append('\'');
                    if (xmlns)
                    {
                        Owned<IPropertyIterator> it = xmlns->getIterator();
                        ForEach(*it)
                        {
                            const char *name = it->getPropKey();
                            s.append(" xmlns:").append(name).append("='").append(xmlns->queryProp(name)).append("'");
                        }
                    }
                }
                if (resultName && *resultName)
                    s.appendf(" name='%s'",resultName);
                else if (sequence != (unsigned) -1)
                    s.appendf(" name='Result %d'",sequence+1);
                s.append(">\n");
                tail.clear().appendf("</%s>\n", elementName);
            }
        }
        isEmpty = false;
    }
}

void FlushingStringBuffer::startScalar(const char *resultName, unsigned sequence)
{
    CriticalBlock b(crit);
    assertex(!s.length());
    name.clear().append(resultName ? resultName : "Dataset");

    sequenceNumber = 0;
    startBlock();
    if (!isBlocked)
    {
        if (mlFmt==MarkupFmt_XML)
        {
            tail.clear();
            s.append("<Dataset");
            if (isSoap && (resultName || (sequence != (unsigned) -1)))
            {
                s.append(" xmlns=\'urn:hpccsystems:ecl:").appendLower(queryName.length(), queryName.sget()).append(":result:");
                if (resultName && *resultName)
                    s.appendLower(strlen(resultName), resultName).append('\'');
                else
                    s.append("result_").append(sequence+1).append('\'');
            }
            if (resultName && *resultName)
                s.appendf(" name='%s'>\n",resultName);
            else
                s.appendf(" name='Result %d'>\n",sequence+1);
            s.append(" <Row>");
            if (resultName && *resultName)
            {
                s.appendf("<%s>", resultName);
                tail.appendf("</%s>", resultName);
            }
            else
            {
                s.appendf("<Result_%d>", sequence+1);
                tail.appendf("</Result_%d>", sequence+1);
            }
            tail.appendf("</Row>\n</Dataset>\n");
        }
        else if (!isRaw)
        {
            tail.clear().append('\n');
        }
    }
}

void FlushingStringBuffer::incrementRowCount()
{
    CriticalBlock b(crit);
    rowCount++;
}

void FlushingJsonBuffer::encodeXML(const char *x, unsigned flags, unsigned len, bool utf8)
{
    CriticalBlock b(crit);
    appendJSONStringValue(s, NULL, len, x, true);
}

void FlushingJsonBuffer::startDataset(const char *elementName, const char *resultName, unsigned sequence, bool _extend)
{
    CriticalBlock b(crit);
    extend = _extend;
    if (isEmpty || !extend)
    {
        name.clear().append(resultName ? resultName : elementName);
        sequenceNumber = 0;
        startBlock();
        if (!isBlocked)
        {
            StringBuffer seqName;
            if (!resultName || !*resultName)
                resultName = seqName.appendf("result_%d", sequence+1).str();
            appendJSONName(s, resultName).append('{');
            tail.set("}");
        }
        isEmpty = false;
    }
}

void FlushingJsonBuffer::startScalar(const char *resultName, unsigned sequence)
{
    CriticalBlock b(crit);
    assertex(!s.length());
    name.set(resultName ? resultName : "Dataset");

    sequenceNumber = 0;
    startBlock();
    if (!isBlocked)
    {
        StringBuffer seqName;
        if (!resultName || !*resultName)
            resultName = seqName.appendf("Result_%d", sequence+1).str();
        appendJSONName(s, resultName).append('{');
        appendJSONName(s, "Row").append("[{");
        appendJSONName(s, resultName);
        tail.set("}]}");
    }
}
//=====================================================================================================

ClusterWriteHandler::ClusterWriteHandler(char const * _logicalName, char const * _activityType)
    : logicalName(_logicalName), activityType(_activityType)
{
    makePhysicalPartName(logicalName.get(), 1, 1, physicalName, false);
    splitFilename(physicalName, &physicalDir, &physicalDir, &physicalBase, &physicalBase);
}

void ClusterWriteHandler::addCluster(char const * cluster)
{
    Owned<IGroup> group = queryNamedGroupStore().lookup(cluster);
    if (!group)
        throw MakeStringException(0, "Unknown cluster %s while writing file %s", cluster, logicalName.get());
    if (group->isMember())
    {
        if (localCluster)
            throw MakeStringException(0, "Cluster %s occupies node already specified while writing file %s", cluster,
                    logicalName.get());
        localClusterName.set(cluster);
        localCluster.set(group);
    }
    else
    {
        ForEachItemIn(idx, remoteNodes)
        {
            Owned<INode> other = remoteNodes.item(idx).getNode(0);
            if (group->isMember(other))
                throw MakeStringException(0, "Cluster %s occupies node already specified while writing file %s",
                        cluster, logicalName.get());
        }
        remoteNodes.append(*group.getClear());
        remoteClusters.append(cluster);
    }
}

void ClusterWriteHandler::getLocalPhysicalFilename(StringAttr & out) const
{
    if(localCluster.get())
        out.set(physicalName.str());
    else
        getTempFilename(out);
    PROGLOG("%s(CLUSTER) for logical filename %s writing to local file %s", activityType.get(), logicalName.get(), out.get());
}

void ClusterWriteHandler::splitPhysicalFilename(StringBuffer & dir, StringBuffer & base) const
{
    dir.append(physicalDir);
    base.append(physicalBase);
}

void ClusterWriteHandler::getTempFilename(StringAttr & out) const
{
    // Should be implemented by more derived (platform-specific) class, if needed
    throwUnexpected();
}

void ClusterWriteHandler::copyPhysical(IFile * source, bool noCopy) const
{
    RemoteFilename rdn, rfn;
    rdn.setLocalPath(physicalDir.str());
    rfn.setLocalPath(physicalName.str());
    ForEachItemIn(idx, remoteNodes)
    {
        rdn.setEp(remoteNodes.item(idx).queryNode(0).endpoint());
        rfn.setEp(remoteNodes.item(idx).queryNode(0).endpoint());
        Owned<IFile> targetdir = createIFile(rdn);
        Owned<IFile> target = createIFile(rfn);
        PROGLOG("%s(CLUSTER) for logical filename %s copying %s to %s", activityType.get(), logicalName.get(), source->queryFilename(), target->queryFilename());
        if(noCopy)
        {
            WARNLOG("Skipping remote copy due to debug option");
        }
        else
        {
            targetdir->createDirectory();
            copyFile(target, source);
        }
    }
}

void ClusterWriteHandler::setDescriptorParts(IFileDescriptor * desc, char const * basename, IPropertyTree * attrs) const
{
    if(!localCluster.get()&&(remoteNodes.ordinality()==0))
        throw MakeStringException(0, "Attempting to write file to no clusters");
    ClusterPartDiskMapSpec partmap; // will get this from group at some point
    desc->setNumParts(1);
    desc->setPartMask(basename);
    if (localCluster) 
        desc->addCluster(localClusterName,localCluster, partmap);
    ForEachItemIn(idx,remoteNodes) 
        desc->addCluster(remoteClusters.item(idx),&remoteNodes.item(idx), partmap);
    if (attrs) {
        // need to set part attr
        IPartDescriptor *partdesc = desc->queryPart(0);
        IPropertyTree &pprop = partdesc->queryProperties();
        // bit of a kludge (should really set properties *after* creating part rather than passing prop tree in)
        Owned<IAttributeIterator> ai = attrs->getAttributes();
        ForEach(*ai) 
            pprop.setProp(ai->queryName(),ai->queryValue());
    }
}
void ClusterWriteHandler::finish(IFile * file) const
{
    if(!localCluster.get())
    {
        PROGLOG("%s(CLUSTER) for logical filename %s removing temporary file %s", activityType.get(), logicalName.get(), file->queryFilename());
        file->remove();
    }
}

void ClusterWriteHandler::getClusters(StringArray &clusters) const
{
    if(localCluster)
        clusters.append(localClusterName);
    ForEachItemIn(c, remoteClusters)
        clusters.append(remoteClusters.item(c));
}

//=====================================================================================================

class COrderedOutputSerializer : implements IOrderedOutputSerializer, public CInterface
{
    class COrderedResult : public CInterface
    {
        bool closed;
        StringBuffer sb;
    public:
        IMPLEMENT_IINTERFACE;
        COrderedResult() : closed(false) {}
        bool flush(FILE * outFile, bool onlyClosed)
        {
            if (closed || !onlyClosed)
            {
                if (sb.length())
                {
                    ::fwrite(sb.str(), sb.length(), 1, outFile);
                    sb.clear();
                }
            }
            return closed;
        }
        size32_t printf(const char *format, va_list args)
        {
            if (closed)
                throw MakeStringException(0, "Attempting to append to previously closed result in COrderedResult::printf");
            int prevLen = sb.length();
            sb.valist_appendf(format, args);
            return sb.length() - prevLen;
        }
        size32_t fwrite(const void * data, size32_t size, size32_t count)
        {
            if (closed)
                throw MakeStringException(0, "Attempting to append to previously closed result in COrderedResult::fwrite");
            size32_t len = size * count;
            sb.append(len, (const char *)data);
            return len;
        }
        void close(bool nl)
        {
            if (closed)
                throw MakeStringException(0, "Attempting to reclose result in COrderedResult::close");
            if (nl)
                sb.append('\n');
            closed = true;
        }
    };

    CIArrayOf<COrderedResult> COrderedResultArr;
    int lastSeqFlushed;
    FILE * outFile;
    CriticalSection crit;

    COrderedResult * getResult(size32_t seq)
    {
        while ((int)COrderedResultArr.ordinality() < (seq+1))
            COrderedResultArr.append(*(new COrderedResult()));
        return &COrderedResultArr.item(seq);
    }

    void flushCurrent()//stream current sequence
    {
        COrderedResult &res = COrderedResultArr.item(lastSeqFlushed + 1);
        res.flush(outFile,false);
        fflush(outFile);
    }

    void flushCompleted(bool onlyClosed)//flush completed sequence(s)
    {
        int lastSeq = (int)COrderedResultArr.ordinality()-1;
        for (; lastSeqFlushed < lastSeq; lastSeqFlushed++)
        {
            COrderedResult &res = COrderedResultArr.item(lastSeqFlushed + 1);
            if (!res.flush(outFile,onlyClosed) && onlyClosed)
                break;
        }
        fflush(outFile);
    }
public:
    IMPLEMENT_IINTERFACE;
    COrderedOutputSerializer(FILE* _outFile) : lastSeqFlushed(-1), outFile(_outFile) {}
    ~COrderedOutputSerializer()
    {
        if (lastSeqFlushed != (COrderedResultArr.ordinality()-1))
            flushCompleted(false);
        COrderedResultArr.kill();
    }

    //IOrderedOutputSerializer
    size32_t fwrite(int seq, const void * data, size32_t size, size32_t count)  
    { 
        CriticalBlock c(crit);
        size32_t ret = getResult(seq)->fwrite(data,size, count);
        if (seq == (lastSeqFlushed + 1))
            flushCurrent();
        return ret;
    }
    size32_t printf(int seq, const char *format, ...) __attribute__((format(printf, 3, 4)))
    { 
        CriticalBlock c(crit);
        va_list args;
        va_start(args, format);
        int ret = getResult(seq)->printf(format, args);
        va_end(args);
        if (seq == (lastSeqFlushed + 1))
            flushCurrent();
        return ret;
    }
    void close(int seq, bool nl)
    {
        CriticalBlock c(crit);
        getResult(seq)->close(nl);
        if ( seq == (lastSeqFlushed+1) )
            flushCompleted(true);
    }
};

IOrderedOutputSerializer * createOrderedOutputSerializer(FILE * _outFile)
{
    return new COrderedOutputSerializer(_outFile);
}

//=====================================================================================================

StringBuffer & mangleHelperFileName(StringBuffer & out, const char * in, const char * wuid, unsigned int flags)
{
    out = in;
    if (flags & (TDXtemporary | TDXjobtemp))
        out.append("__").append(wuid);
    return out;
}

StringBuffer & mangleLocalTempFilename(StringBuffer & out, char const * in)
{
    char const * start = in;
    while(true)
    {
        char const * end = strstr(start, "::");
        if(end)
        {
            out.append(end-start, start).append("__scope__");
            start = end + 2;
        }
        else
        {
            out.append(start);
            break;
        }
    }
    return out;
}

StringBuffer & expandLogicalFilename(StringBuffer & logicalName, const char * fname, IConstWorkUnit * wu, bool resolveLocally)
{
    if (fname[0]=='~')
        logicalName.append(fname+1);
    else if (resolveLocally)
    {
        StringBuffer sb(fname);
        sb.replaceString("::",PATHSEPSTR);
        makeAbsolutePath(sb.str(), logicalName.clear());
    }
    else
    {
        SCMStringBuffer lfn;
        if (wu)
        {
            wu->getScope(lfn);
            if(lfn.length())
                logicalName.append(lfn.s).append("::");
        }
        logicalName.append(fname);
    }
    return logicalName;
}
