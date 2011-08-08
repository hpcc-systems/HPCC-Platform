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

#include "jliball.hpp"
#include "jqueue.tpp"
#include "jisem.hpp"

#include "thorxmlread.hpp"
#include "thorxmlwrite.hpp"

#include "thorcommon.ipp"
#include "thorsoapcall.hpp"

#include "securesocket.hpp"

#include "eclrtl.hpp"

#ifndef _WIN32
#include <stdexcept>
#endif

#define CONTENT_LENGTH "Content-Length: "

unsigned soapTraceLevel = 1;

#define WSCBUFFERSIZE 0x10000
#define MAXWSCTHREADS 50    //Max Web Service Call Threads

interface IReceivedRoxieException : extends IException
{
public:
    virtual const void *errorRow() = 0;
};

#define EXCEPTION_PREFIX "ReceivedRoxieException:"

class ReceivedRoxieException: public CInterface, public IReceivedRoxieException
{
public:
    IMPLEMENT_IINTERFACE;

    ReceivedRoxieException(int code, const char *_msg, const void *_row=NULL) : errcode(code), msg(_msg), row(_row) { };
    int errorCode() const { return (errcode); };
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        if (strncmp(str.str(), EXCEPTION_PREFIX, strlen(EXCEPTION_PREFIX)) == 0)
            str.append(msg);
        else
            str.append(EXCEPTION_PREFIX).append(" (").append(msg).append(")");

        return str;
    };
    MessageAudience errorAudience() const { return (MSGAUD_user); };
    const void *errorRow() { return (row); };

private:
    int errcode;
    StringAttr msg;
    const void *row;
};

//=================================================================================================

class Url : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    StringBuffer method;
    StringBuffer host;
    unsigned port;
    StringBuffer path;
    StringBuffer userPasswordPair;

    StringBuffer &getUrlString(StringBuffer &url) const
    {
        return url.append(method).append("://").append(host).append(":").append(port).append(path);
    }

    IException *getUrlException(IException *e) const
    {
        StringBuffer url;
        StringBuffer text;
        e->errorMessage(text);
        rtlAddExceptionTag(text, "url", getUrlString(url).str());
        if (text.length() <= 1024)
            return MakeStringException(e->errorCode(), text.str());
        else
            return MakeStringExceptionDirect(e->errorCode(), text.str());
    }

    Url()
    {
    }

private:
    char translateHex(char hex) {
        if(hex >= 'A')
            return (hex & 0xdf) - 'A' + 10;
        else
            return hex - '0';
    }

    void userPassword_decode(const char* userpass, StringBuffer& result)
    {
        if(!userpass || !*userpass)
            return;

        const char *finger = userpass;
        while (*finger)
        {
            char c = *finger++;
            if (c == '%')
            {
                if(*finger != '\0')
                {
                    c = translateHex(*finger);
                    finger++;
                }
                if(*finger != '\0')
                {
                    c = (char)(c*16 + translateHex(*finger));
                    finger++;
                }
            }
            result.append(c);
        }

        return;
    }

public:
    Url(char *urltext)
    {
        char *p;

        if (p = strstr(urltext, "://"))
        {
            *p = 0;
            p += 3; // skip past the colon-slash-slash
            method.append(urltext);
            urltext = p;
        }
        else
            throw MakeStringException(-1, "Malformed URL");

        if (p = strchr(urltext, '@'))
        {
            // extract username & password
            *p = 0;
            p++;

            userPassword_decode(urltext, userPasswordPair);
            urltext = p;
        }

        if (p = strchr(urltext, ':'))
        {
            // extract the port
            *p = 0;
            p++;

            port = atoi(p);

            host.append(urltext);

            if (p = strchr(p, '/'))
                path.append(p);
            else
                path.append("/");
        }
        else
        {
            // no port - look at method for port
            if (stricmp(method.str(), "https") == 0)
                port = 443;
            else if (stricmp(method.str(), "http") == 0)
                port = 80;
            else
                throw MakeStringException(-1, "Unsupported access method");

            if (p = strchr(urltext, '/'))
            {
                *p = 0;
                p++;

                host.append(urltext);
                path.append("/").append(p);
            }
            else
            {
                host.append(urltext);
                path.append("/");
            }
        }
    }
};

typedef IArrayOf<Url> UrlArray;

//=================================================================================================

class UrlListParser
{
public:
    UrlListParser(const char * text)
    {
        fullText = strdup(text);
    }

    ~UrlListParser()
    {
        free(fullText);
    }

    unsigned getUrls(UrlArray &array)
    {
        char *copyFullText = strdup(fullText);

        char *saveptr;
        char *url = strtok_r(copyFullText, "|", &saveptr);
        while (url != NULL)
        {
            array.append(*new Url(url));
            url = strtok_r(NULL, "|", &saveptr);
        }

        free(copyFullText);
        return array.ordinality();
    }

private:
    char *fullText;
};

//=================================================================================================

#define BLACKLIST_RETRIES 10
#define ROXIE_ABORT_EVENT  1407
#define TIMELIMIT_EXCEEDED 1408

class BlackLister : public CInterface, implements IThreadFactory
{
    SocketEndpointArray list;
    Owned<IThreadPool> pool;
    CriticalSection crit;

private:
    inline void checkRoxieAbortMonitor(IRoxieAbortMonitor * roxieAbortMonitor)
    {
        if (roxieAbortMonitor)
        {
            try 
            {
                roxieAbortMonitor->checkForAbort();//throws
            }
            catch (IException *e)
            {
                StringBuffer s;
                throw MakeStringException(ROXIE_ABORT_EVENT, e->errorMessage(s).str());
            }
        }
    }

public:
    bool lookup(SocketEndpoint &ep, const IContextLogger &logctx)
    {
        CriticalBlock b(crit);
        if (list.find(ep)!=NotFound)
        {
            if (soapTraceLevel > 3)
            {
                StringBuffer s;
                logctx.CTXLOG("socket %s is blacklisted", ep.getUrlStr(s).str());
            }
            return true;
        }
        return false;

    }
    void blacklist(SocketEndpoint &ep, const IContextLogger &logctx)
    {
        CriticalBlock b(crit);
        if (list.find(ep)==NotFound)
        {
            if (soapTraceLevel > 0)
            {
                StringBuffer s;
                logctx.CTXLOG("Blacklisting socket %s", ep.getUrlStr(s).str());
            }
            list.append(ep);
            pool->start(&ep);
        }
    }
    void deblacklist(SocketEndpoint &ep)
    {
        CriticalBlock b(crit);
        unsigned idx = list.find(ep);
        if (idx!=NotFound)
        {
            if (soapTraceLevel > 0)
            {
                StringBuffer s;
                DBGLOG("De-blacklisting socket %s", ep.getUrlStr(s).str());
            }
            list.remove(idx);
        }
    }

public:
    IMPLEMENT_IINTERFACE;

    BlackLister()
    {
        pool.setown(createThreadPool("SocketBlacklistPool", this, NULL, 0, 0));
    }

    ISocket* connect(SocketEndpoint &ep,
                     const IContextLogger &logctx,
                     unsigned retries,
                     unsigned timeout,
                     IRoxieAbortMonitor * roxieAbortMonitor)
    {
        if (lookup(ep, logctx))
        {
            StringBuffer s;
            ep.getUrlStr(s);
            throw MakeStringException(-1, "blacklisted socket %s", s.str());
        }
        Owned<IException> exc;
        try
        {
            checkRoxieAbortMonitor(roxieAbortMonitor);
            Owned<ISocket> sock;
            Owned<ISocketConnectWait> scw = nonBlockingConnect(ep, timeout == WAIT_FOREVER ? 60000 : timeout*retries*1000);
            loop
            {
                sock.setown(scw->wait(1000));//throws if connect fails or timeout
                checkRoxieAbortMonitor(roxieAbortMonitor);
                if (sock)
                    return sock.getLink();
            }
        }

        catch (IJSOCK_Exception *e)
        {
            EXCLOG(e,"BlackLister::connect");
            if (exc)
                e->Release();
            else
                exc.setown(e);
        }

        blacklist(ep, logctx);
        if (exc->errorCode()==JSOCKERR_connection_failed) {
            StringBuffer s;
            ep.getUrlStr(s);
            throw MakeStringException(JSOCKERR_connection_failed, "connection failed %s", s.str());
        }
        throw exc.getClear();
        return NULL;
    }

    bool blacklisted (unsigned short port, char const* host)
    {
        SocketEndpoint ep(host, port);
        return (list.find(ep)!=NotFound);
    }

    ISocket* connect(unsigned short port,
                     char const* host,
                     const IContextLogger &logctx,
                     unsigned retries,
                     unsigned timeout,
                     IRoxieAbortMonitor * roxieAbortMonitor )
    {
        SocketEndpoint ep(host, port);
        return connect(ep, logctx, retries, timeout, roxieAbortMonitor);
    }

    virtual IPooledThread *createNew()
    {
        class SocketDeblacklister : public CInterface, implements IPooledThread
        {
            SocketEndpoint ep;
            BlackLister &parent;
            Semaphore stopped;
        public:
            IMPLEMENT_IINTERFACE;
            SocketDeblacklister(BlackLister &_parent): parent(_parent)
            {
            }
            ~SocketDeblacklister()
            {
            }

            virtual void init(void *param)
            {
                ep.set(*(SocketEndpoint *) param);
            }
            virtual void main()
            {
                unsigned delay = 5000;
                for (unsigned i = 0; i < BLACKLIST_RETRIES; i++)
                {
                    try
                    {
                        Owned<ISocket> s = ISocket::connect_timeout(ep, 10000);
                        s->close();
                        break;
                    }
                    catch (IJSOCK_Exception *E)
                    {
                        // EXCLOG(E, "While updating socket blacklist");  // MORE - may need to downgrade if this fires traps
                        E->Release();
                        if (stopped.wait(delay))
                            return;
                        delay += delay;
                    }
                }
                parent.deblacklist(ep);
            }
            virtual bool stop()
            {
                stopped.signal();
                return true;
            }
            virtual bool canReuse() { return true; }
        };
        return new SocketDeblacklister(*this);
    }

    void stop()
    {
        pool->stopAll();
    }
} *blacklist;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    blacklist = new BlackLister;

    return true;
}

MODULE_EXIT()
{
    blacklist->stop();
    delete blacklist;
}

//=================================================================================================

class ColumnProvider : public CInterface, public IColumnProvider
{
public:
    ColumnProvider(unsigned _callLatencyMs) : callLatencyMs(_callLatencyMs) {}
    IMPLEMENT_IINTERFACE;
    virtual bool        getBool(const char * path) { return base->getBool(path); }
    virtual void        getData(size32_t len, void * text, const char * path) { base->getData(len, text, path); }
    virtual void        getDataX(size32_t & len, void * & text, const char * path) { base->getDataX(len, text, path); }
    virtual __int64     getInt(const char * path)
    {
        __int64 ret = base->getInt(path);
        if((ret==0) && path && *path=='_')
        {
            if (stricmp(path, "_call_latency_ms")==0)
                ret = callLatencyMs;
            else if (stricmp(path, "_call_latency")==0)
                ret = (callLatencyMs + 500) / 1000;
        }
        return ret;
    }
    virtual void        getQString(size32_t len, char * text, const char * path) { base->getQString(len, text, path); }
    virtual void        getString(size32_t len, char * text, const char * path) { base->getString(len, text, path); }
    virtual void        getStringX(size32_t & len, char * & text, const char * path) { base->getStringX(len, text, path); }
    virtual void        getUnicodeX(size32_t & len, UChar * & text, const char * path) { base->getUnicodeX(len, text, path); }
    virtual bool        getIsSetAll(const char * path) { return base->getIsSetAll(path); }
    virtual IColumnProviderIterator * getChildIterator(const char * path) { return base->getChildIterator(path); }
    virtual void        getUtf8X(size32_t & len, char * & text, const char * path) { base->getUtf8X(len, text, path); }

    virtual bool        readBool(const char * path, bool _default) { return base->readBool(path, _default); }
    virtual void        readData(size32_t len, void * text, const char * path, size32_t _lenDefault, const void * _default) { base->readData(len, text, path, _lenDefault, _default); }
    virtual void        readDataX(size32_t & len, void * & text, const char * path, size32_t _lenDefault, const void * _default) { base->readDataX(len, text, path, _lenDefault, _default); }
    virtual __int64     readInt(const char * path, __int64 _default)
    {
        if(path && *path=='_')
        {
            if (stricmp(path, "_call_latency_ms")==0)
                return callLatencyMs;
            else if (stricmp(path, "_call_latency")==0)
                return (callLatencyMs + 500) / 1000;
        }
        return base->readInt(path, _default);
    }
    virtual void        readQString(size32_t len, char * text, const char * path, size32_t _lenDefault, const char * _default) { base->readQString(len, text, path, _lenDefault, _default); }
    virtual void        readString(size32_t len, char * text, const char * path, size32_t _lenDefault, const char * _default) { base->readString(len, text, path, _lenDefault, _default); }
    virtual void        readStringX(size32_t & len, char * & text, const char * path, size32_t _lenDefault, const char * _default) { base->readStringX(len, text, path, _lenDefault, _default); }
    virtual void        readUnicodeX(size32_t & len, UChar * & text, const char * path, size32_t _lenDefault, const UChar * _default) { base->readUnicodeX(len, text, path, _lenDefault, _default); }
    virtual bool        readIsSetAll(const char * path, bool _default) { return base->readIsSetAll(path, _default); }
    virtual void        readUtf8X(size32_t & len, char * & text, const char * path, size32_t _lenDefault, const char * _default) { base->readUtf8X(len, text, path, _lenDefault, _default); }

    void setBase(IColumnProvider * _base) { base = _base; }

    virtual void        getDataRaw(size32_t len, void * text, const char * path) { base->getDataRaw(len, text, path); }
    virtual void        getDataRawX(size32_t & len, void * & text, const char * path) { base->getDataRawX(len, text, path); }
    virtual void        readDataRaw(size32_t len, void * text, const char * path, size32_t _lenDefault, const void * _default) { base->readDataRaw(len, text, path, _lenDefault, _default); }
    virtual void        readDataRawX(size32_t & len, void * & text, const char * path, size32_t _lenDefault, const void * _default) { base->readDataRawX(len, text, path, _lenDefault, _default); }

protected:
    IColumnProvider * base;

private:
    unsigned callLatencyMs;

};

//=================================================================================================
//Same as ColumnProvider, except returns data as hex64Binary instead of 16 bit hex
class ColumnProviderData64 : public ColumnProvider
{
public:
    ColumnProviderData64(unsigned _callLatencyMs) : ColumnProvider(_callLatencyMs) {}
    virtual void getData(size32_t len, void * text, const char * path)      { ColumnProvider::getDataRaw(len, text, path);  }
    virtual void getDataX(size32_t & len, void * & text, const char * path) { ColumnProvider::getDataRawX(len, text, path);     }
    virtual void readData(size32_t len, void * text, const char * path, size32_t _lenDefault, const void * _default) { ColumnProvider::readDataRaw(len, text, path, _lenDefault, _default); }
    virtual void readDataX(size32_t len, void * text, const char * path, size32_t _lenDefault, const void * _default) { ColumnProvider::readDataRawX(len, text, path, _lenDefault, _default); }
};

//=================================================================================================

IColumnProvider * CreateColumnProvider(unsigned _callLatencyMs, bool _encoding)
{
    if (!_encoding)
        return new ColumnProvider(_callLatencyMs);
    else
        return new ColumnProviderData64(_callLatencyMs);
}

//=================================================================================================


enum WSCType{STsoap, SThttp} ;  //web service call type

//Web Services Call Asynchronous For
interface IWSCAsyncFor: public IInterface
{
    virtual void processException(const Url &url, const void *row, IException *e) = 0;
    virtual void processException(const Url &url, ConstPointerArray &inputRows, IException *e) = 0;
    virtual void checkTimeLimitExceeded() = 0;

    virtual void createHttpRequest(Url &url, StringBuffer &request) = 0;
    virtual int readHttpResponse(StringBuffer &response, ISocket *socket) = 0;
    virtual void processResponse(Url &url, StringBuffer &response, ColumnProvider * meta) = 0;

    virtual StringBuffer getResponsePath() = 0;
    virtual ConstPointerArray & getInputRows() = 0;
    virtual IWSCHelper * getMaster() = 0;
    virtual IEngineRowAllocator * getOutputAllocator() = 0;
    
    virtual void For(unsigned num,unsigned maxatonce,bool abortFollowingException=false,bool shuffled=false) = 0;
    virtual void Do(unsigned idx=0)=0;
};

class CWSCHelper;
IWSCAsyncFor * createWSCAsyncFor(CWSCHelper * _master, CommonXmlWriter &_xmlWriter, ConstPointerArray &_inputRows, XmlReaderOptions _options);

//=================================================================================================

class CMatchCB : public CInterface, implements IXMLSelect
{
    IWSCAsyncFor &parent;
    const Url &url;
    StringAttr tail;
    ColumnProvider * meta;
public:
    IMPLEMENT_IINTERFACE;

    CMatchCB(IWSCAsyncFor &_parent, const Url &_url, const char *_tail, ColumnProvider * _meta) : parent(_parent), url(_url), tail(_tail), meta(_meta)
    {
    }

    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
    {
        Owned<IException> e;
        if (tail.length())
        {
            StringBuffer path(parent.getResponsePath());
            unsigned idx = (unsigned)entry.getInt(path.append("/@sequence").str());
            Owned<IColumnProviderIterator> excIter = entry.getChildIterator("Exception");
            IColumnProvider *excptEntry = excIter->first();
            if (excptEntry)
            {
                int code = (int)excptEntry->getInt("Code");
                size32_t len;
                char *message;
                excptEntry->getStringX(len, message, "Message");
                StringBuffer mstr(len, message);
                rtlFree(message);
                DBGLOG("Roxie exception: %s", mstr.str());
                e.setown(new ReceivedRoxieException(code, mstr.str()));
                parent.processException(url, parent.getInputRows().item(idx), e.getLink());
            }
        }
        if (parent.getMaster()->getRowTransformer() && !e)
        {
            IEngineRowAllocator * outputAllocator = parent.getOutputAllocator();
            Owned<IColumnProviderIterator> iter = entry.getChildIterator(tail);
            IColumnProvider *rowEntry = iter->first();
            while (rowEntry)
            {
                RtlDynamicRowBuilder rowBuilder(outputAllocator);
                size32_t sizeGot;
                if(meta)
                {
                    meta->setBase(rowEntry);
                    sizeGot = parent.getMaster()->transformRow(rowBuilder, meta);
                }
                else
                {
                    sizeGot = parent.getMaster()->transformRow(rowBuilder, rowEntry);
                }
                if (sizeGot > 0)
                    parent.getMaster()->putRow(rowBuilder.finalizeRowClear(sizeGot));
                rowEntry = iter->next();
            }
        }
    }

};

//=================================================================================================

bool isValidHttpValue(char const * in)
{
    for (const byte * ptr = (byte*)in; *ptr; ++ptr)
        if (((*ptr <= '\x1F') && (*ptr != '\x09')) || (*ptr == '\x7F'))
            return false;
    return true;
}

//=================================================================================================
//Web Service Call helper thread
class CWSCHelperThread : public Thread
{
private:
    CWSCHelper * master;
    virtual void outputXmlRows(CommonXmlWriter &xmlWriter, ConstPointerArray &inputRows, const char *itemtag=NULL, bool encode_off=false, char const * itemns = NULL);
    virtual void createESPQuery(CommonXmlWriter &xmlWriter, ConstPointerArray &inputRows);
    virtual void createSOAPliteralOrEncodedQuery(CommonXmlWriter &xmlWriter, ConstPointerArray &inputRows);
    virtual void createXmlSoapQuery(CommonXmlWriter &xmlWriter, ConstPointerArray &inputRows);
    virtual void processQuery(ConstPointerArray &inputRows);
    
    //Thread
    virtual int run();
public:
    CWSCHelperThread(CWSCHelper * _master) : Thread("CWSCHelperThread")
    {
        master = _master;
    }

    ~CWSCHelperThread()
    {
    }
};

//=================================================================================================

class CWSCHelper : public CInterface, implements IWSCHelper
{
private:
    QueueOf<const void, false> outputQ;                     // all access is protected by outputCrit
    CriticalSection outputCrit, errorCrit, toXmlCrit, transformCrit, onfailCrit, timeoutCrit;
    unsigned done;
    InterruptableSemaphore resultAvailable;
    bool complete;
    Linked<ClientCertificate> clientCert;

    static CriticalSection secureContextCrit;
    static Owned<ISecureSocketContext> secureContext;

    CTimeMon timeLimitMon;
    bool timeLimitExceeded;
    IRoxieAbortMonitor * roxieAbortMonitor;

protected:
    CIArrayOf<CWSCHelperThread> threads;
    WSCType wscType;

public:
    CWSCHelper(IWSCRowProvider *_rowProvider, IEngineRowAllocator * _outputAllocator, const char *_authToken, WSCMode _wscMode, ClientCertificate *_clientCert, const IContextLogger &_logctx, IRoxieAbortMonitor *_roxieAbortMonitor, WSCType _wscType)
        : logctx(_logctx), outputAllocator(_outputAllocator), clientCert(_clientCert), roxieAbortMonitor(_roxieAbortMonitor)
    {
        wscMode = _wscMode;
        wscType = _wscType;
        done = 0;
        complete = aborted = timeLimitExceeded = false;

        rowProvider = _rowProvider;
        helper = rowProvider->queryActionHelper();
        callHelper = rowProvider->queryCallHelper();
        flags = helper->getFlags();

        authToken.append(_authToken);

        if (helper->numRetries() < 0)
            maxRetries = 3; // default to 3 retries
        else
            maxRetries = helper->numRetries();

        logXML = (flags & SOAPFlog) != 0;

        timeout = helper->getTimeout();
        if (timeout == (unsigned)-1)
            timeout = 300; // 300 second default
        else if (timeout == 0)
            timeout = WAIT_FOREVER;

        timeLimit = helper->getTimeLimit();
        if (timeLimit == 0  ||  timeLimit == (unsigned)-1)
            timeLimit = WAIT_FOREVER;   //default
        else
            timeLimitMon.reset(timeLimit*1000);

        if (wscType == STsoap)
        {
            soapaction.set(helper->querySoapAction());
            if(soapaction.get() && !isValidHttpValue(soapaction.get()))
                throw MakeStringException(-1, "SOAPAction value contained illegal characters: %s", soapaction.get());

            httpHeaderName.set(helper->queryHttpHeaderName());
            if(httpHeaderName.get() && !isValidHttpValue(httpHeaderName.get()))
                throw MakeStringException(-1, "HTTPHEADER name contained illegal characters: %s", httpHeaderName.get());

            httpHeaderValue.set(helper->queryHttpHeaderValue());
            if(httpHeaderValue.get() && !isValidHttpValue(httpHeaderValue.get()))
                throw MakeStringException(-1, "HTTPHEADER value contained illegal characters: %s", httpHeaderValue.get());

            StringAttr proxyAddress;
            proxyAddress.set(helper->queryProxyAddress());
            if (!proxyAddress.isEmpty())
            {
                UrlListParser proxyUrlListParser(proxyAddress);
                if (0 == proxyUrlListParser.getUrls(proxyUrlArray))
                    throw MakeStringException(0, "SOAPCALL PROXYADDRESS specified no URLs");
            }

            if ((flags & SOAPFliteral) && (flags & SOAPFencoding))
                throw MakeStringException(0, "SOAPCALL 'LITERAL' and 'ENCODING' options are mutually exclusive");

            header.set(helper->queryHeader());
            footer.set(helper->queryFooter());
            if(flags & SOAPFnamespace)
            {
                char const * ns = helper->queryNamespaceName();
                if(ns && *ns)
                    xmlnamespace.set(ns);
            }
        }

        if (wscType == SThttp)
        {
            //Check for unsupported flags
            if ((flags & SOAPFliteral) || (flags & SOAPFencoding))
                throw MakeStringException(0, "HTTPCALL 'LITERAL' and 'ENCODINGD' options not supported");
        }

        if (callHelper)
        {
            char const * ipath = callHelper->queryInputIteratorPath();
            if(ipath && (*ipath == '/'))
                ++ipath;
            inputpath.set(ipath);
        }
        
        service.set(helper->queryService());
        service.trim();

        if (wscType == SThttp)
        {
            service.toUpperCase();  //GET/PUT/POST
            if (strcmp(service.str(), "GET"))
                throw MakeStringException(0, "HTTPCALL Only 'GET' service supported");
            acceptType.set(helper->queryAcceptType());// text/html, text/xml, etc
            acceptType.trim();
            acceptType.toLowerCase();
        }

        if(callHelper)
        {
            rowTransformer = callHelper->queryInputTransformer();
        }
        else
        {
            rowTransformer = NULL;
        }

        UrlListParser urlListParser(helper->queryHosts());
        if ((numUrls = urlListParser.getUrls(urlArray)) > 0)
        {
            if (wscMode == SCrow)
            {
                numRowThreads = 1;

                numUrlThreads = helper->numParallelThreads();
                if (numUrlThreads == 0)
                    numUrlThreads = 1;
                else if (numUrlThreads > MAXWSCTHREADS)
                    numUrlThreads = MAXWSCTHREADS;

                numRecordsPerBatch = 1;
            }
            else
            {
                unsigned totThreads = helper->numParallelThreads();
                if (totThreads < 1)
                    totThreads = 2; // default to 2 threads
                else if (totThreads > MAXWSCTHREADS)
                    totThreads = MAXWSCTHREADS;

                numUrlThreads = (numUrls < totThreads)? numUrls: totThreads;

                numRowThreads = totThreads / numUrlThreads;
                if (numRowThreads < 1)
                    numRowThreads = 1;
                else if (numRowThreads > MAXWSCTHREADS)
                    numRowThreads = MAXWSCTHREADS;

                numRecordsPerBatch = helper->numRecordsPerBatch();
                if (numRecordsPerBatch < 1)
                    numRecordsPerBatch = 1;
            }
        }
        else
            throw MakeStringException(0, "%sCALL specified no URLs",wscType == STsoap ? "SOAP" : "HTTP");


        for (unsigned i=0; i<numRowThreads; i++)
            threads.append(*new CWSCHelperThread(this));
    }

    IMPLEMENT_IINTERFACE;

    ~CWSCHelper()
    {
        complete = true;
        waitUntilDone();
        threads.kill();
        while (outputQ.ordinality())
            outputAllocator->releaseRow(outputQ.dequeue());
    }

    void waitUntilDone()
    {
        ForEachItemIn(i,threads)
            threads.item(i).join();
    }

    void start()
    {
        ForEachItemIn(i,threads)
            threads.item(i).start();
    }

    void abort()
    {
        aborted = true;
        complete = true;
        resultAvailable.signal();
    }

    size32_t __deprecated__getRow(void *buffer)
    {
        const void * row = getRow();
        if (row)
        {
            size32_t sizeGot = outputAllocator->queryOutputMeta()->getRecordSize(row);
            memcpy(buffer, row, sizeGot);
            outputAllocator->releaseRow(row);
            return sizeGot;
        }
        return 0;
    }

    const void * getRow()
    {
        if (complete) return NULL;
        loop
        {
            resultAvailable.wait();
            if (aborted)
                break;
            {
                CriticalBlock block(outputCrit);
                if (outputQ.ordinality())
                {
                    return outputQ.dequeue();
                }
                else if ((done == numRowThreads))
                {
                    complete = true;
                    if (error.get())
                        throw error.getLink();
                    break;
                }
                // should never get here
            }
        }
        return NULL;
    }

    bool rowAvailable()
    {
        CriticalBlock block(outputCrit);
        return (outputQ.ordinality() > 0);
    }

    bool queryDone()
    {
        CriticalBlock block(outputCrit);
        return (done == numRowThreads);
    }

    IException * getError()
    {
        CriticalBlock block(errorCrit);
        return error.getLink();
    }

    inline IEngineRowAllocator * queryOutputAllocator() const { return outputAllocator; }

    ISecureSocket *createSecureSocket(ISocket *sock)
    {
        {
            CriticalBlock b(secureContextCrit);
            if (!secureContext)
            {
                if (clientCert != NULL)
                    secureContext.setown(createSecureSocketContextEx(clientCert->certificate, clientCert->privateKey, clientCert->passphrase, ClientSocket));
                else
                    secureContext.setown(createSecureSocketContext(ClientSocket));
            }
        }
        return secureContext->createSecureSocket(sock);
    }

    bool isTimeLimitExceeded()
    {
        if (timeLimit != WAIT_FOREVER)
        {
            CriticalBlock block(timeoutCrit);
            if (timeLimitExceeded || timeLimitMon.timedout())
            {
                timeLimitExceeded = true;
                return true;
            }
        }
        return false;
    }

    inline IXmlToRowTransformer * getRowTransformer() { return rowTransformer; }

protected:
    friend class CWSCHelperThread;
    friend class CWSCAsyncFor;

    void putRow(const void * row)
    {
        CriticalBlock block(outputCrit);
        outputQ.enqueue(row);
        resultAvailable.signal();
    }

    void setDone()
    {
        CriticalBlock block(outputCrit);
        done++;
        if (done == numRowThreads)
            resultAvailable.signal();
    }

    void setErrorOwn(IException * e)
    {
        CriticalBlock block(errorCrit);
        if (error)
            ::Release(e);
        else
            error.setown(e);
    }

    void toXML(const byte * self, IXmlWriter & out) { CriticalBlock block(toXmlCrit); helper->toXML(self, out); }
    size32_t transformRow(ARowBuilder & rowBuilder, IColumnProvider * row) 
    { 
        CriticalBlock block(transformCrit); 
        NullDiskCallback callback;
        return rowTransformer->transform(rowBuilder, row, &callback); 
    }
    unsigned onFailTransform(ARowBuilder & rowBuilder, const void * left, IException * e) { CriticalBlock block(onfailCrit); return callHelper->onFailTransform(rowBuilder, left, e); }

    StringBuffer authToken;
    WSCMode wscMode;
    IWSCRowProvider * rowProvider;
    IHThorWebServiceCallActionArg * helper;
    IHThorWebServiceCallArg *   callHelper;
    Linked<IEngineRowAllocator> outputAllocator;
    Owned<IException> error;
    UrlArray urlArray;
    UrlArray proxyUrlArray;
    unsigned numRecordsPerBatch;
    unsigned numUrls;
    unsigned numRowThreads;
    unsigned numUrlThreads;
    unsigned maxRetries;
    unsigned timeout; //seconds
    unsigned timeLimit; //seconds
    bool logXML, aborted;
    const IContextLogger &logctx;
    unsigned flags;
    StringAttr soapaction;
    StringAttr httpHeaderName;
    StringAttr httpHeaderValue;
    StringAttr inputpath;
    StringBuffer service;
    StringBuffer acceptType;//for httpcall, text/plain, text/html, text/xml, etc
    StringAttr header;
    StringAttr footer;
    StringAttr xmlnamespace;
    IXmlToRowTransformer * rowTransformer;
};

CriticalSection CWSCHelper::secureContextCrit;
Owned<ISecureSocketContext> CWSCHelper::secureContext; // created on first use

//=================================================================================================

void CWSCHelperThread::outputXmlRows(CommonXmlWriter &xmlWriter, ConstPointerArray &inputRows, const char *itemtag, bool encode_off, char const * itemns)
{
    ForEachItemIn(idx, inputRows)
    {
        if (itemtag)                //TAG
        {
            xmlWriter.outputQuoted("<");
            xmlWriter.outputQuoted(itemtag);
            if(itemns)
            {
                xmlWriter.outputQuoted(" xmlns=\"");
                xmlWriter.outputQuoted(itemns);
                xmlWriter.outputQuoted("\"");
            }
            xmlWriter.outputQuoted(">");
        }

        if (master->header.get())   //OPTIONAL HEADER (specified by "HEADING" option)
            xmlWriter.outputQuoted(master->header.get());

                                    //XML ROW CONTENT
        master->toXML((const byte *)inputRows.item(idx), xmlWriter);

        if (master->footer.get())   //OPTION FOOTER
            xmlWriter.outputQuoted(master->footer.get());

        if (encode_off)             //ENCODING
            xmlWriter.outputQuoted("<encode_>0</encode_>");

        if (itemtag)                //CLOSE TAG
        {
            xmlWriter.outputQuoted("</");
            xmlWriter.outputQuoted(itemtag);
            xmlWriter.outputQuoted(">");
        }
    }
}

void CWSCHelperThread::createESPQuery(CommonXmlWriter &xmlWriter, ConstPointerArray &inputRows)
{
    StringBuffer method_tag;
    method_tag.append(master->service).append("Request");
    StringAttr method_ns;

    if (inputRows.ordinality() > 1)
    {
        xmlWriter.outputQuoted("<");
        xmlWriter.outputQuoted(method_tag.str());
        xmlWriter.outputQuoted("Array");
        if (master->xmlnamespace.get())
        {
            xmlWriter.outputQuoted(" xmlns=\"");
            xmlWriter.outputQuoted(master->xmlnamespace.get());
            xmlWriter.outputQuoted("\"");
        }
        xmlWriter.outputQuoted(">");
    }
    else
    {
        if(master->xmlnamespace.get())
            method_ns.set(master->xmlnamespace.get());
    }

    outputXmlRows(xmlWriter, inputRows, method_tag.str(), (inputRows.ordinality() == 1), method_ns.get());

    if (inputRows.ordinality() > 1)
    {
        xmlWriter.outputQuoted("<encode_>0</encode_>");
        xmlWriter.outputQuoted("</");
        xmlWriter.outputQuoted(method_tag.str());
        xmlWriter.outputQuoted("Array>");
    }
}

//Create servce xml request body, with binding usage of either Literal or Encoded
//Note that Encoded usage requires type encoding for data fields
void CWSCHelperThread::createSOAPliteralOrEncodedQuery(CommonXmlWriter &xmlWriter, ConstPointerArray &inputRows)
{
    xmlWriter.outputQuoted("<");
    xmlWriter.outputQuoted(master->service);

    if (master->flags & SOAPFencoding)
        xmlWriter.outputQuoted(" soapenv:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"");

    if (master->xmlnamespace.get())
    {
        xmlWriter.outputQuoted(" xmlns=\"");
        xmlWriter.outputQuoted(master->xmlnamespace.get());
        xmlWriter.outputQuoted("\"");
    }

    xmlWriter.outputQuoted(">");

    outputXmlRows(xmlWriter, inputRows);

    xmlWriter.outputQuoted("</");
    xmlWriter.outputQuoted(master->service);
    xmlWriter.outputQuoted(">");
}

//Create SOAP body of http request
void CWSCHelperThread::createXmlSoapQuery(CommonXmlWriter &xmlWriter, ConstPointerArray &inputRows)
{
    xmlWriter.outputQuoted("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xmlWriter.outputQuoted("<soap:Envelope");

    xmlWriter.outputQuoted(" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"");
    if (master->flags & SOAPFencoding)
    {   //SOAP RPC/encoded.  'Encoded' usage includes type encoding 
        xmlWriter.outputQuoted(" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"");
        xmlWriter.outputQuoted(" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
    }
    xmlWriter.outputQuoted(">");

    xmlWriter.outputQuoted("<soap:Body>");

    if (master->flags & SOAPFliteral  ||  master->flags & SOAPFencoding)
        createSOAPliteralOrEncodedQuery(xmlWriter, inputRows);
    else
        createESPQuery(xmlWriter, inputRows);

    xmlWriter.outputQuoted("</soap:Body></soap:Envelope>");
}

void CWSCHelperThread::processQuery(ConstPointerArray &inputRows)
{
    unsigned xmlWriteFlags = 0;
    unsigned xmlReadFlags = xr_ignoreNameSpaces; 
    if (master->flags & SOAPFtrim)
        xmlWriteFlags |= XWFtrim;
    if ((master->flags & SOAPFpreserveSpace) == 0)
        xmlReadFlags |= xr_ignoreWhiteSpace;
        XMLWriterType xmlType = !(master->flags & SOAPFencoding) ? WTStandard : WTEncodingData64; 
    CommonXmlWriter *xmlWriter = CreateCommonXmlWriter(xmlWriteFlags, 0, NULL, xmlType);
    if (master->wscType == STsoap)
        createXmlSoapQuery(*xmlWriter, inputRows);

    Owned<IWSCAsyncFor> casyncfor = createWSCAsyncFor(master, *xmlWriter, inputRows, (XmlReaderOptions) xmlReadFlags);
    casyncfor->For(master->numUrls, master->numUrlThreads,false,true); // shuffle URLS for poormans load balance
    delete xmlWriter;
}

int CWSCHelperThread::run()
{
    ConstPointerArray inputRows;

    if (master->wscMode == SCrow)
    {
        inputRows.append(NULL);
        try
        {
            processQuery(inputRows);
        }
        catch (IException *e)
        {
            master->setErrorOwn(e);
        }
        inputRows.pop();
    }
    else
    {
        // following a bit odd but preserving previous semantics (except fixing abort leak)
        loop
        {
            try
            {
                while (!master->complete && !master->error.get())
                {
                    if (master->aborted) {
                        while (inputRows.ordinality() > 0)
                            master->rowProvider->releaseRow(inputRows.pop());
                        return 0;
                    }
                    const void *r = master->rowProvider->getNextRow();
                    if (!r)
                        break;
                    inputRows.append(r);
                    if (inputRows.ordinality() >= master->numRecordsPerBatch)
                        break;
                }
                if (inputRows.ordinality() == 0)
                    break;
                processQuery(inputRows);
            }
            catch (IException *e)
            {
                master->setErrorOwn(e); // going to exit next time round
            }

            while (inputRows.ordinality() > 0)
                master->rowProvider->releaseRow(inputRows.pop());
        }
    }

    master->setDone();

    return 0;
}

//=================================================================================================

IWSCHelper * createSoapCallHelper(IWSCRowProvider *r, IEngineRowAllocator * outputAllocator, const char *authToken, WSCMode wscMode, ClientCertificate *clientCert, const IContextLogger &logctx, IRoxieAbortMonitor * roxieAbortMonitor)
{
    return new CWSCHelper(r, outputAllocator, authToken, wscMode, clientCert, logctx, roxieAbortMonitor, STsoap);
}

IWSCHelper * createHttpCallHelper(IWSCRowProvider *r, IEngineRowAllocator * outputAllocator, const char *authToken, WSCMode wscMode, ClientCertificate *clientCert, const IContextLogger &logctx, IRoxieAbortMonitor * roxieAbortMonitor)
{
    return new CWSCHelper(r, outputAllocator, authToken, wscMode, clientCert, logctx, roxieAbortMonitor, SThttp);
}

//=================================================================================================

class CWSCAsyncFor : implements IWSCAsyncFor, public CInterface, public CAsyncFor
{
    class CSocketDataProvider : public CInterface
    {
        const char * buffer;
        size32_t currLen;
        size32_t maxLen;
        size32_t curPosn;
        ISocket * socket;
        unsigned timeout;
    public:
        CSocketDataProvider(const char * _buffer, size32_t _curPosn, size32_t _currLen, size32_t _maxLen, ISocket * _sock, unsigned _timeout ) 
            : buffer(_buffer), currLen(_currLen), maxLen(_maxLen), curPosn(_curPosn), socket(_sock), timeout(_timeout)
        {
        }
        size32_t getBytes(char * buf, size32_t len)
        {
            if ((maxLen - curPosn) < len)
                throw MakeStringException(-1, "Request %d too large for remaining buffer fragment (%d)",len,maxLen - curPosn);
            size32_t count;
            if ( len <= (currLen-curPosn))
            {   //its already in the buffer
                memcpy(buf, (buffer + curPosn), len);
                curPosn += len;
                count = len;
            }
            else if (curPosn >= currLen)
            {   //nothing in buffer, read from socket
                size32_t bytesRead=0;
                count = 0;
                do
                {
                    socket->read(buf + count, 0, len - count, bytesRead, timeout);
                    count += bytesRead;
                } while (count != len);
                currLen = curPosn = 0;
            }
            else
            {   //only some is in buffer, read rest from socket
                size32_t bytesRead=0;
                size32_t avail = currLen - curPosn;
                memcpy(buf, (buffer + curPosn), avail);
                count = avail;
                do
                {
                    size32_t read;
                    socket->read(buf+avail+bytesRead, 0, len-avail-bytesRead, read, timeout);
                    bytesRead += read;
                } while (len != (bytesRead + avail));
                count += bytesRead;
                currLen = curPosn = 0;
            }
            return count;
        }
    };

private:
    CWSCHelper * master;
    ConstPointerArray &inputRows;
    CommonXmlWriter &xmlWriter;
    IEngineRowAllocator * outputAllocator;
    CriticalSection processExceptionCrit;
    StringBuffer responsePath;
    Owned<CSocketDataProvider> dataProvider;
    XmlReaderOptions options;

    inline void checkRoxieAbortMonitor(IRoxieAbortMonitor * roxieAbortMonitor)
    {
        if (roxieAbortMonitor)
        {
            try 
            {
                roxieAbortMonitor->checkForAbort();//throws
            }
            catch (IException *e)
            {
                StringBuffer s;
                throw MakeStringException(ROXIE_ABORT_EVENT, e->errorMessage(s).str());
            }
        }
    }

    void createHttpRequest(Url &url, StringBuffer &request)
    {
        // Create the HTTP POST request
        if (master->wscType == STsoap)
            request.clear().append("POST ").append(url.path).append(" HTTP/1.1\r\n");
        else
            request.clear().append(master->service).append(" ").append(url.path).append(" HTTP/1.1\r\n");

        if (url.userPasswordPair.length() > 0)
        {
            StringBuffer authToken;
            JBASE64_Encode(url.userPasswordPair.str(), url.userPasswordPair.length(), authToken);
            request.append("Authorization: Basic ").append(authToken).append("\r\n");
        }
        else if (master->authToken.length() > 0)
        {
            request.append("Authorization: Basic ").append(master->authToken).append("\r\n");
        }
        if (master->wscType == STsoap)
        {
            if (master->soapaction.get())
                request.append("SOAPAction: ").append(master->soapaction.get()).append("\r\n");

            if (master->httpHeaderName.get() && master->httpHeaderValue.get())
            {
                StringBuffer hdr = master->httpHeaderName.get();
                hdr.append(": ").append(master->httpHeaderValue);
                if (soapTraceLevel > 6 || master->logXML)
                    master->logctx.CTXLOG("SOAPCALL: Adding HTTP Header(%s)", hdr.str());
                request.append(hdr.append("\r\n"));
            }
            request.append("Content-Type: text/xml\r\n");
        }
        else if(master->wscType == SThttp)
            request.append("Accept: ").append(master->acceptType).append("\r\n");
        else
            assertex(false);

        request.append("Host: ").append(url.host).append(":").append(url.port).append("\r\n");//http 1.1
        if (master->wscType == STsoap)
        {
            request.append(CONTENT_LENGTH).append(xmlWriter.length()).append("\r\n\r\n");
            request.append(xmlWriter.str());//add SOAP xml content
        }
        else
            request.append("\r\n");//httpcall

        if (soapTraceLevel > 6 || master->logXML)
            master->logctx.CTXLOG("%sCALL: request(%s)", master->wscType == STsoap ? "SOAP" : "HTTP", request.str());
    }

    int readHttpResponse(StringBuffer &response, ISocket *socket)
    {
        // Read the POST reply
        // not doesn't *assume* is valid HTTP post format but if it is takes advantage of
        response.clear();
        unsigned bytesRead;
        MemoryAttr buf;
        char *buffer=(char *)buf.allocate(WSCBUFFERSIZE+1);
        int rval = 200;

        // first read header
        size32_t payloadofs = 0;
        size32_t payloadsize = 0;
        StringBuffer dbgheader;
        bool chunked;
        size32_t read = 0;
        do {
            checkTimeLimitExceeded();
            checkRoxieAbortMonitor(master->roxieAbortMonitor);
            socket->read(buffer+read, 0, WSCBUFFERSIZE-read, bytesRead, master->timeout);
            checkTimeLimitExceeded();
            checkRoxieAbortMonitor(master->roxieAbortMonitor);

            read += bytesRead;
            buffer[read] = 0;
            if (strncmp(buffer, "HTTP", 4) == 0) {
                const char *s = strstr(buffer,"\r\n\r\n");
                if (s) {
                    payloadofs = (size32_t)(s-buffer+4);
                    if (soapTraceLevel > 6 || master->logXML)
                        dbgheader.append(payloadofs,buffer);  // needed for tracing below
                    s = strstr(buffer, " ");
                    if (s)
                        rval = atoi(s+1);
                    if (!strstr(buffer,"Transfer-Encoding: chunked"))
                    {
                        chunked = false;
                        s = strstr(buffer,CONTENT_LENGTH);
                        if (s) {
                            s += strlen(CONTENT_LENGTH);
                            if ((size32_t)(s-buffer) < payloadofs)
                                payloadsize = atoi(s);
                        }
                    }
                    else
                    {
                        chunked = true;
                        size32_t chunkSize = 0;
                        size32_t dataLen = 0;
                        char ch;
/*
                        //from http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.4.4
                        "19.4.6 Introduction of Transfer-Encoding"

                        length := 0
                        read chunk-size, chunk-extension (if any) and CRLF
                        while (chunk-size > 0) 
                        {
                            read chunk-data and CRLF
                            append chunk-data to entity-body
                            length := length + chunk-size
                            read chunk-size and CRLF
                        }
*/
                        dataProvider.setown(new CSocketDataProvider(buffer, payloadofs, read, WSCBUFFERSIZE, socket, master->timeout));
                        dataProvider->getBytes(&ch, 1);
                        while (isalpha(ch) || isdigit(ch))
                        {   //get chunk-size
                            if (isdigit(ch))
                                chunkSize = (chunkSize*16) + (ch - '0');
                            else
                                chunkSize = (chunkSize*16) + 10 + (toupper(ch) - 'A');
                            dataProvider->getBytes(&ch, 1);
                        }
                        while (chunkSize && ch != '\n')//consume chunk-extension and CRLF
                            dataProvider->getBytes(&ch, 1);
                        while (chunkSize)
                        {
                            //read chunk data directly into response
                            size32_t count = dataProvider->getBytes(response.reserve(dataLen + chunkSize), chunkSize);
                            assertex(count == chunkSize);
                            dataLen += count;
                            response.setLength(dataLen);

                            dataProvider->getBytes(&ch, 1);//consume CRLF at end of chunk
                            while (ch != '\n')
                                dataProvider->getBytes(&ch, 1);

                            chunkSize = 0;
                            dataProvider->getBytes(&ch, 1);
                            while (isalpha(ch) || isdigit(ch))
                            {   //get next chunk size
                                if (isdigit(ch))
                                    chunkSize = (chunkSize*16) + (ch - '0');
                                else
                                    chunkSize = (chunkSize*16) + 10 + (toupper(ch) - 'A');
                                dataProvider->getBytes(&ch, 1);
                            }
                            while(chunkSize && ch != '\n')//consume chunk-extension and CRLF
                                dataProvider->getBytes(&ch, 1);
                        }
                    }
                    break;
                }
            }
            if (bytesRead == 0)         // socket closed
                break;
        } while (bytesRead&&(read<WSCBUFFERSIZE));
        if (!chunked)
        {
            if (payloadsize)
                response.ensureCapacity(payloadsize);
            char *payload = buffer;
            if (payloadofs) {
                read -= payloadofs;
                payload += payloadofs;
                if (payloadsize&&(read>payloadsize))
                    read = payloadsize;
            }
            if (read)
                response.append(read,payload);
            if (payloadsize) {  // read directly into response
                while (read<payloadsize) {
                    checkTimeLimitExceeded();
                    checkRoxieAbortMonitor(master->roxieAbortMonitor);
                    socket->read(response.reserve(payloadsize-read), 0, payloadsize-read, bytesRead, master->timeout);
                    checkTimeLimitExceeded();
                    checkRoxieAbortMonitor(master->roxieAbortMonitor);

                    read += bytesRead;
                    response.setLength(read);
                    if (bytesRead==0) {
                        master->logctx.CTXLOG("%sCALL: Warning %sHTTP response terminated prematurely",master->wscType == STsoap ? "SOAP" : "HTTP",chunked?"CHUNKED ":NULL);
                        break; // oops  looks likesocket closed early
                    }
                }
            }
            else {
                loop {
                    checkTimeLimitExceeded();
                    checkRoxieAbortMonitor(master->roxieAbortMonitor);
                    socket->read(buffer, 0, WSCBUFFERSIZE, bytesRead, master->timeout);
                    checkTimeLimitExceeded();
                    checkRoxieAbortMonitor(master->roxieAbortMonitor);

                    if (bytesRead==0)
                        break;              // rely on socket closing to terminate message
                    response.append(bytesRead,buffer);
                }
            }
        }
        if (soapTraceLevel > 6 || master->logXML)
            master->logctx.CTXLOG("%sCALL: LEN=%d %sresponse(%s%s)", master->wscType == STsoap ? "SOAP" : "HTTP",response.length(),chunked?"CHUNKED ":"", dbgheader.str(), response.str());
        else if (soapTraceLevel > 8)
            master->logctx.CTXLOG("%sCALL: LEN=%d %sresponse(%s)", master->wscType == STsoap ? "SOAP" : "HTTP",response.length(),chunked?"CHUNKED ":"", response.str()); // not sure this is that useful but...
        return rval;
    }

    void processEspResponse(Url &url, StringBuffer &response, ColumnProvider * meta)
    {
        StringBuffer path(responsePath);
        path.append("/Results/Result/");
        const char *tail;
        if (master->rowTransformer && master->inputpath.get())
        {
            StringBuffer ipath;
            ipath.append("/Envelope/Body/").append(master->inputpath.get());
            if((ipath.length() >= path.length()) && (0 == memcmp(ipath.str(), path.str(), path.length())))
            {
                tail = ipath.str() + path.length();
            }
            else
            {
                path.clear().append(ipath);
                tail = NULL;
            }
        }
        else
            tail = "Dataset/Row";

        CMatchCB matchCB(*this, url, tail, meta);
        Owned<IXMLParse> xmlParser = createXMLParse((const void *)response.str(), (unsigned)response.length(), path.str(), matchCB, options, false);
        while (xmlParser->next());
    }

    void processLiteralResponse(Url &url, StringBuffer &response, ColumnProvider * meta)
    {
        StringBuffer path("/Envelope/Body/");
        if(master->rowTransformer && master->inputpath.get())
            path.append(master->inputpath.get());
        CMatchCB matchCB(*this, url, NULL, meta);
        Owned<IXMLParse> xmlParser = createXMLParse((const void *)response.str(), (unsigned)response.length(), path.str(), matchCB, options, false);
        while (xmlParser->next());
    }

    void processHttpResponse(Url &url, StringBuffer &response, ColumnProvider * meta)
    {
        StringBuffer path;
        if(master->rowTransformer && master->inputpath.get())
            path.append(master->inputpath.get());
        CMatchCB matchCB(*this, url, NULL, meta);
        Owned<IXMLParse> xmlParser = createXMLParse((const void *)response.str(), (unsigned)response.length(), path.str(), matchCB, options, false);
        while (xmlParser->next());
    }

    void processResponse(Url &url, StringBuffer &response, ColumnProvider * meta)
    {
        if (master->wscType == SThttp)
            processHttpResponse(url, response, meta);
        else if (master->flags & SOAPFliteral)
            processLiteralResponse(url, response, meta);
        else if (master->flags & SOAPFencoding)
            processLiteralResponse(url, response, meta);
        else
            processEspResponse(url, response, meta);
    }

    void processException(const Url &url, const void *row, IException *e)
    {
        CriticalBlock block(processExceptionCrit);

        Owned<IException> ne = url.getUrlException(e);
        e->Release();

        if ((master->flags & SOAPFonfail) && master->callHelper)
        {
            try
            {
                RtlDynamicRowBuilder rowBuilder(outputAllocator);
                size32_t sizeGot = master->onFailTransform(rowBuilder, row, ne);
                if (sizeGot > 0)
                    master->putRow(rowBuilder.finalizeRowClear(sizeGot));
            }
            catch (IException *te)
            {
                master->setErrorOwn(te);
            }
        }
        else
            master->setErrorOwn(ne.getClear());
    }

    void processException(const Url &url, ConstPointerArray &inputRows, IException *e)
    {
        Owned<IException> ne = url.getUrlException(e);
        e->Release();

        if ((master->flags & SOAPFonfail) && master->callHelper)
        {
            ForEachItemIn(idx, inputRows)
            {
                try
                {
                    RtlDynamicRowBuilder rowBuilder(outputAllocator);
                    size32_t sizeGot = master->onFailTransform(rowBuilder, inputRows.item(idx), ne);
                    if (sizeGot > 0)
                        master->putRow(rowBuilder.finalizeRowClear(sizeGot));
                }
                catch (IException *te)
                {
                    master->setErrorOwn(te);
                    break;
                }
            }
        }
        else
            master->setErrorOwn(ne.getClear());
    }

    inline void checkTimeLimitExceeded()
    {
        if (master->isTimeLimitExceeded())
            throw MakeStringException(TIMELIMIT_EXCEEDED, "%sCALL TIMELIMIT(%u) exceeded", master->wscType == STsoap ? "SOAP" : "HTTP", master->timeLimit);
    }

public:
    CWSCAsyncFor(CWSCHelper * _master, CommonXmlWriter &_xmlWriter, ConstPointerArray &_inputRows, XmlReaderOptions _options): xmlWriter(_xmlWriter), inputRows(_inputRows), options(_options)
    {
        master = _master;
        outputAllocator = master->queryOutputAllocator();
        responsePath.append("/Envelope/Body/");
        if (inputRows.ordinality() > 1)
        {
            // can we receive a roxie exceptions for the whole RequestArray?
            // if so, we need to handle them here

            responsePath.append(master->service).append("ResponseArray/");
        }
        responsePath.append(master->service).append("Response");
    }

    ~CWSCAsyncFor()
    {
    }

    IMPLEMENT_IINTERFACE;

    void For(unsigned num,unsigned maxatonce,bool abortFollowingException, bool shuffled)
    {
        CAsyncFor::For(num, maxatonce, abortFollowingException, shuffled);
    }

    void Do(unsigned idx)
    {
        StringBuffer request;
        StringBuffer response;
        unsigned attempts = 0;
        unsigned retryInterval = 0;

        Url &url = master->urlArray.item(idx);
        createHttpRequest(url, request);
        unsigned startidx = idx;
        while (!master->aborted)
        {
            Owned<ISocket> socket;
            unsigned startTime, endTime;
            startTime = msTick();
            loop 
            {
                try
                {
                    checkTimeLimitExceeded();
                    Url &connUrl = master->proxyUrlArray.empty() ? url : master->proxyUrlArray.item(0);
                    socket.setown(blacklist->connect(connUrl.port, connUrl.host, master->logctx, (unsigned)master->maxRetries, master->timeout, master->roxieAbortMonitor));
                    if (stricmp(url.method, "https") == 0)
                    {
                        Owned<ISecureSocket> ssock = master->createSecureSocket(socket.getClear());
                        if (ssock) 
                        {
                            checkTimeLimitExceeded();
                            int status = ssock->secure_connect();
                            if (status < 0)
                            {
                                StringBuffer err;
                                err.append("Failure to establish secure connection to ");
                                connUrl.getUrlString(err);
                                err.append(": returned ").append(status);
                                throw MakeStringException(0, err.str());
                            }
                            socket.setown(ssock.getLink());
                        }
                    }
                    break;
                }
                catch (IException *e)
                {
                    if (master->timeLimitExceeded)
                    {
                        master->logctx.CTXLOG("%sCALL exiting: time limit (%u) exceeded",master->wscType == STsoap ? "SOAP" : "HTTP", master->timeLimit);
                        processException(url, inputRows, e);
                        return;
                    }

                    if (e->errorCode() == ROXIE_ABORT_EVENT)
                    {
                        StringBuffer s;
                        master->logctx.CTXLOG("%sCALL exiting: Roxie Abort : %s",master->wscType == STsoap ? "SOAP" : "HTTP",e->errorMessage(s).str());
                        throw e;
                    }

                    do 
                    {
                        idx++;  // try next socket not blacklisted
                        if (idx==master->urlArray.ordinality())
                            idx = 0;
                        if (idx==startidx)
                        {
                            StringBuffer s;
                            master->logctx.CTXLOG("Exception %s", e->errorMessage(s).str());
                            processException(url, inputRows, e);
                            return;
                        }
                    } while (blacklist->blacklisted(url.port, url.host));
                }
            }
            try
            {
                checkTimeLimitExceeded();
                checkRoxieAbortMonitor(master->roxieAbortMonitor);
                socket->write(request.str(), request.length());
                if (soapTraceLevel > 4)
                    master->logctx.CTXLOG("%sCALL: sent request (%s) to %s:%d", master->wscType == STsoap ? "SOAP" : "HTTP",master->service.str(), url.host.str(), url.port);
                checkTimeLimitExceeded();
                checkRoxieAbortMonitor(master->roxieAbortMonitor);

                int rval = readHttpResponse(response, socket);

                if (soapTraceLevel > 4)
                    master->logctx.CTXLOG("%sCALL: received response (%s) from %s:%d", master->wscType == STsoap ? "SOAP" : "HTTP",master->service.str(), url.host.str(), url.port);

                if (rval != 200)
                {
                    if (rval == 503)
                        throw new ReceivedRoxieException(1001, "Server Too Busy");

                    StringBuffer text;
                    text.appendf("HTTP error (%d) in processQuery",rval);
                    rtlAddExceptionTag(text, "soapresponse", response.str());
                    throw MakeStringExceptionDirect(-1, text.str());
                }
                if (response.length() == 0)
                {
                    throw MakeStringException(-1, "Zero length response in processQuery");
                }
                endTime = msTick();
                checkTimeLimitExceeded();
                ColumnProvider * meta = (ColumnProvider*)CreateColumnProvider(endTime-startTime, master->flags&SOAPFencoding?true:false);
                processResponse(url, response, meta);
                delete meta;
                break;
            }
            catch (IReceivedRoxieException *e)
            {
                // server busy ... Sleep and retry
                if (e->errorCode() == 1001)
                {
                    if (retryInterval)
                    {
                        int sleepTime = retryInterval + getRandom() % retryInterval;
                        master->logctx.CTXLOG("Server busy (1001), sleeping for %d milliseconds before retry", sleepTime);
                        Sleep(sleepTime);
                        retryInterval = (retryInterval*2 >= 10000)? 10000: retryInterval*2;
                    }
                    else
                    {
                        master->logctx.CTXLOG("Server busy (1001), retrying");
                        retryInterval = 10;
                    }
                    e->Release();
                }
                else
                {
                    // other roxie exception ...
                    master->logctx.CTXLOG("Exiting: received Roxie exception");
                    if (e->errorRow())
                        processException(url, e->errorRow(), e);
                    else
                        processException(url, inputRows, e);
                    break;
                }
            }
            catch (IException *e)
            {
                if (master->timeLimitExceeded)
                {
                    processException(url, inputRows, e);
                    master->logctx.CTXLOG("%sCALL exiting: time limit (%u) exceeded", master->wscType == STsoap ? "SOAP" : "HTTP", master->timeLimit);
                    break;
                }

                if (e->errorCode() == ROXIE_ABORT_EVENT)
                {
                    StringBuffer s;
                    master->logctx.CTXLOG("%sCALL exiting: Roxie Abort : %s",master->wscType == STsoap ? "SOAP" : "HTTP",e->errorMessage(s).str());
                    throw e;
                }

                // other IException ... retry up to maxRetries
                StringBuffer s;
                master->logctx.CTXLOG("Exception %s - retrying? (%d<%d)", e->errorMessage(s).str(), attempts, master->maxRetries);

                if (attempts >= master->maxRetries)
                {
                    // error affects all inputRows
                    master->logctx.CTXLOG("Exiting: maxRetries exceeded");
                    processException(url, inputRows, e);
                    break;
                }
                master->logctx.CTXLOG("Retrying: maxRetries not exceeded");
                attempts++;
                e->Release();
            }
            catch (std::exception & es)
            {
                if(dynamic_cast<std::bad_alloc *>(&es))
                    throw MakeStringException(-1, "std::exception: out of memory (std::bad_alloc) in CWSCAsyncFor processQuery");
                throw MakeStringException(-1, "std::exception: standard library exception (%s) in CWSCAsyncFor processQuery",es.what());
            }
            catch (...)
            {
                throw MakeStringException(-1, "Unknown exception in processQuery");
            }
        }
    }
    inline virtual StringBuffer getResponsePath() { return responsePath; }
    inline virtual ConstPointerArray & getInputRows() { return inputRows; }
    inline virtual CWSCHelper * getMaster() { return master; }
    inline virtual IEngineRowAllocator * getOutputAllocator() { return outputAllocator; }
};

IWSCAsyncFor * createWSCAsyncFor(CWSCHelper * _master, CommonXmlWriter &_xmlWriter, ConstPointerArray &_inputRows, XmlReaderOptions _options)
{
    return new CWSCAsyncFor(_master, _xmlWriter, _inputRows, _options);
}
