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

#include "jliball.hpp"
#include "jqueue.tpp"
#include "jisem.hpp"

#include "rtlformat.hpp"

#include "thorxmlread.hpp"
#include "thorxmlwrite.hpp"
#include "thorcommon.ipp"
#include "thorsoapcall.hpp"

#include "securesocket.hpp"
#include "eclrtl.hpp"
#include "roxiemem.hpp"
#include "zcrypt.hpp"

using roxiemem::OwnedRoxieString;

#ifndef _WIN32
#include <stdexcept>
#endif

#include <new>

#define CONTENT_LENGTH "Content-Length: "
#define CONTENT_ENCODING "Content-Encoding"
#define ACCEPT_ENCODING "Accept-Encoding"

unsigned soapTraceLevel = 1;

#define WSCBUFFERSIZE 0x10000
#define MAXWSCTHREADS 50    //Max Web Service Call Threads

interface DECL_EXCEPTION IReceivedRoxieException : extends IException
{
public:
    virtual const void *errorRow() = 0;
};

#define EXCEPTION_PREFIX "ReceivedRoxieException:"

class DECL_EXCEPTION ReceivedRoxieException: public IReceivedRoxieException, public CInterface
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

    StringAttr method;
    StringAttr host;
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
            return MakeStringException(e->errorCode(), "%s", text.str());
        else
            return MakeStringExceptionDirect(e->errorCode(), text.str());
    }

    Url() : port(0)
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

        if ((p = strstr(urltext, "://")) != NULL)
        {
            *p = 0;
            p += 3; // skip past the colon-slash-slash
            method.set(urltext);
            urltext = p;
        }
        else
            throw MakeStringException(-1, "Malformed URL");

        if ((p = strchr(urltext, '@')) != NULL)
        {
            // extract username & password
            *p = 0;
            p++;

            userPassword_decode(urltext, userPasswordPair);
            urltext = p;
        }

        if ((p = strchr(urltext, ':')) != NULL)
        {
            // extract the port
            *p = 0;
            p++;

            port = atoi(p);

            host.set(urltext);

            if ((p = strchr(p, '/')) != NULL)
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

            if ((p = strchr(urltext, '/')) != NULL)
            {
                *p = 0;
                p++;

                host.set(urltext);
                path.append("/").append(p);
            }
            else
            {
                host.set(urltext);
                path.append("/");
            }
        }
        // While the code below would make some sense, there is code that relies on being able to use invalid IP addresses and catching the
        // errors that result via ONFAIL.
#if 0
        IpAddress ipaddr(host);
        if ( ipaddr.isNull())
            throw MakeStringException(-1, "Invalid IP address %s", host.str());
#endif
    }
};

typedef IArrayOf<Url> UrlArray;

//=================================================================================================

//MORE: This whole class should really be replaced with a single function, avoiding at least one string clone.
class UrlListParser
{
public:
    UrlListParser(const char * text)
    {
        fullText = text ? strdup(text) : NULL;
    }

    ~UrlListParser()
    {
        free(fullText);
    }

    unsigned getUrls(UrlArray &array)
    {
        if (fullText)
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
        }
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
                e->errorMessage(s);
                e->Release();
                throw MakeStringException(ROXIE_ABORT_EVENT, "%s", s.str());
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
                     unsigned timeoutMS,
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
            Owned<ISocketConnectWait> scw = nonBlockingConnect(ep, timeoutMS == WAIT_FOREVER ? 60000 : timeoutMS*(retries+1));
            for (;;)
            {
                sock.setown(scw->wait(1000));//throws if connect fails or timeoutMS
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
                     unsigned timeoutMS,
                     IRoxieAbortMonitor * roxieAbortMonitor )
    {
        SocketEndpoint ep(host, port);
        return connect(ep, logctx, retries, timeoutMS, roxieAbortMonitor);
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

            virtual void init(void *param) override
            {
                ep.set(*(SocketEndpoint *) param);
            }
            virtual void threadmain() override
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
            virtual bool stop() override
            {
                stopped.signal();
                return true;
            }
            virtual bool canReuse() const override { return true; }
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

class ColumnProvider : public IColumnProvider, public CInterface
{
public:
    ColumnProvider(unsigned _callLatencyMs) : callLatencyMs(_callLatencyMs), base(NULL) {}
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
    virtual void checkTimeLimitExceeded(unsigned * _remainingMS) = 0;

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
IWSCAsyncFor * createWSCAsyncFor(CWSCHelper * _master, CommonXmlWriter &_xmlWriter, ConstPointerArray &_inputRows, PTreeReaderOptions _options);

//=================================================================================================

class CMatchCB : implements IXMLSelect, public CInterface
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
                IERRLOG("Roxie exception: %s", mstr.str());
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

class CWSCHelper : implements IWSCHelper, public CInterface
{
private:
    SimpleInterThreadQueueOf<const void, true> outputQ;
    SpinLock outputQLock;
    CriticalSection toXmlCrit, transformCrit, onfailCrit, timeoutCrit;
    unsigned done;
    Linked<ClientCertificate> clientCert;

    static CriticalSection secureContextCrit;
    static Owned<ISecureSocketContext> secureContext;

    CTimeMon timeLimitMon;
    bool complete, timeLimitExceeded;
    IRoxieAbortMonitor * roxieAbortMonitor;

protected:
    CIArrayOf<CWSCHelperThread> threads;
    WSCType wscType;

public:
    IMPLEMENT_IINTERFACE;

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
        OwnedRoxieString s;

        authToken.append(_authToken);

        if (helper->numRetries() < 0)
            maxRetries = 3; // default to 3 retries
        else
            maxRetries = helper->numRetries();

        //Allow all of these options to be specified separately.  Possibly useful, and the code is cleaner.
        logMin = (flags & SOAPFlogmin) != 0;
        logXML = (flags & SOAPFlog) != 0;
        logUserMsg = (flags & SOAPFlogusermsg) != 0;

        double dval = helper->getTimeout(); // In seconds, but may include fractions of a second...
        if (dval < 0.0) //not provided, or out of range
            timeoutMS = 300*1000; // 300 second default
        else if (dval == 0)
            timeoutMS = WAIT_FOREVER;
        else
            timeoutMS = (unsigned)(dval * 1000);

        dval = helper->getTimeLimit();
        if (dval <= 0.0)
            timeLimitMS = WAIT_FOREVER;
        else
            timeLimitMS = (unsigned)(dval * 1000);

        if (flags & SOAPFhttpheaders)
            httpHeaders.set(s.setown(helper->getHttpHeaders()));

        StringAttr proxyAddress;
        proxyAddress.set(s.setown(helper->getProxyAddress()));
        if (!proxyAddress.isEmpty())
        {
            UrlListParser proxyUrlListParser(proxyAddress);
            if (0 == proxyUrlListParser.getUrls(proxyUrlArray))
                throw MakeStringException(0, "SOAPCALL PROXYADDRESS specified no URLs");
        }

        if (wscType == STsoap)
        {
            soapaction.set(s.setown(helper->getSoapAction()));
            if(soapaction.get() && !isValidHttpValue(soapaction.get()))
                throw MakeStringException(-1, "SOAPAction value contained illegal characters: %s", soapaction.get());

            httpHeaderName.set(s.setown(helper->getHttpHeaderName()));
            if(httpHeaderName.get() && !isValidHttpValue(httpHeaderName.get()))
                throw MakeStringException(-1, "HTTPHEADER name contained illegal characters: %s", httpHeaderName.get());

            httpHeaderValue.set(s.setown(helper->getHttpHeaderValue()));
            if(httpHeaderValue.get() && !isValidHttpValue(httpHeaderValue.get()))
                throw MakeStringException(-1, "HTTPHEADER value contained illegal characters: %s", httpHeaderValue.get());

            if ((flags & SOAPFliteral) && (flags & SOAPFencoding))
                throw MakeStringException(0, "SOAPCALL 'LITERAL' and 'ENCODING' options are mutually exclusive");

            header.set(s.setown(helper->getHeader()));
            footer.set(s.setown(helper->getFooter()));
            if(flags & SOAPFnamespace)
            {
                OwnedRoxieString ns = helper->getNamespaceName();
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
            OwnedRoxieString iteratorPath(callHelper->getInputIteratorPath());
            char const * ipath = iteratorPath;
            if(ipath && (*ipath == '/'))
                ++ipath;
            inputpath.set(ipath);
        }
        
        service.set(s.setown(helper->getService()));
        service.trim();

        if (wscType == SThttp)
        {
            service.toUpperCase();  //GET/PUT/POST
            if (strcmp(service.str(), "GET"))
                throw MakeStringException(0, "HTTPCALL Only 'GET' http method currently supported");
            OwnedRoxieString acceptTypeSupplied(helper->getAcceptType()); // text/html, text/xml, etc
            acceptType.set(acceptTypeSupplied);
            acceptType.trim();
            acceptType.toLowerCase();
        }

        if (callHelper)
            rowTransformer = callHelper->queryInputTransformer();
        else
            rowTransformer = NULL;

        OwnedRoxieString hosts(helper->getHosts());
        UrlListParser urlListParser(hosts);
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
    ~CWSCHelper()
    {
        complete = true;
        waitUntilDone();
        threads.kill();
    }
    void waitUntilDone()
    {
        ForEachItemIn(i,threads)
            threads.item(i).join();
        for (;;)
        {
            const void *row = outputQ.dequeueNow();
            if (!row)
                break;
            outputAllocator->releaseRow(row);
        }
        outputQ.reset();
    }
    void start()
    {
        if (timeLimitMS != WAIT_FOREVER)
            timeLimitMon.reset(timeLimitMS);

        done = 0;
        complete = aborted = timeLimitExceeded = false;

        ForEachItemIn(i,threads)
            threads.item(i).start();
    }
    void abort()
    {
        aborted = true;
        complete = true;
        outputQ.stop();
    }
    const void * getRow()
    {
        if (complete)
            return NULL;
        for (;;)
        {
            const void *row = outputQ.dequeue();
            if (aborted)
                break;
            if (row)
                return row;
            // should only be here if setDone() triggered
            complete = true;
            Owned<IException> e = getError();
            if (e)
                throw e.getClear();
            break;
        }
        return NULL;
    }
    IException * getError()
    {
        SpinBlock sb(outputQLock);
        return error.getLink();
    }
    inline IEngineRowAllocator * queryOutputAllocator() const { return outputAllocator; }
#ifdef _USE_OPENSSL
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
#endif
    bool isTimeLimitExceeded(unsigned *_remainingMS)
    {
        if (timeLimitMS != WAIT_FOREVER)
        {
            CriticalBlock block(timeoutCrit);
            if (timeLimitExceeded || timeLimitMon.timedout(_remainingMS))
            {
                timeLimitExceeded = true;
                return true;
            }
        }
        else
            *_remainingMS = (unsigned)-1;
        return false;
    }

    void addUserLogMsg(const byte * row)
    {
        if (logUserMsg)
        {
            size32_t lenText;
            rtlDataAttr text;
            helper->getLogText(lenText, text.refstr(), row);
            logctx.CTXLOG("%s: %.*s", wscCallTypeText(), lenText, text.getstr());
        }
    }
    inline IXmlToRowTransformer * getRowTransformer() { return rowTransformer; }
    inline const char * wscCallTypeText() const { return wscType == STsoap ? "SOAPCALL" : "HTTPCALL"; }

protected:
    friend class CWSCHelperThread;
    friend class CWSCAsyncFor;

    void putRow(const void * row)
    {
        assertex(row);
        outputQ.enqueue(row);
    }
    void setDone()
    {
        bool doStop;
        {
            SpinBlock sb(outputQLock);
            done++;
            doStop = (done == numRowThreads);
        }
        if (doStop)
        {
            // Note - Don't stop the queue - that effectively discards what's already on there,
            // which is not what we want.
            // Instead, push a NULL to indicate the end of the output.
            outputQ.enqueue(NULL);
        }
    }
    void setErrorOwn(IException * e)
    {
        SpinBlock sb(outputQLock);
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
    unsigned timeoutMS;
    unsigned timeLimitMS;
    bool logXML;
    bool logMin;
    bool logUserMsg;
    bool aborted;
    const IContextLogger &logctx;
    unsigned flags;
    StringAttr soapaction;
    StringAttr httpHeaderName;
    StringAttr httpHeaderValue;
    StringAttr httpHeaders;
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

        master->addUserLogMsg((const byte *)inputRows.item(idx));
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
    unsigned xmlReadFlags = ptr_ignoreNameSpaces;
    if (master->flags & SOAPFtrim)
        xmlWriteFlags |= XWFtrim;
    if ((master->flags & SOAPFpreserveSpace) == 0)
        xmlReadFlags |= ptr_ignoreWhiteSpace;
        XMLWriterType xmlType = !(master->flags & SOAPFencoding) ? WTStandard : WTEncodingData64; 
    CommonXmlWriter *xmlWriter = CreateCommonXmlWriter(xmlWriteFlags, 0, NULL, xmlType);
    if (master->wscType == STsoap)
        createXmlSoapQuery(*xmlWriter, inputRows);

    Owned<IWSCAsyncFor> casyncfor = createWSCAsyncFor(master, *xmlWriter, inputRows, (PTreeReaderOptions) xmlReadFlags);
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
        for (;;)
        {
            try
            {
                while (!master->complete && !master->error.get())
                {
                    if (master->aborted) {
                        while (inputRows.ordinality() > 0)
                            master->rowProvider->releaseRow(inputRows.popGet());
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
                master->rowProvider->releaseRow(inputRows.popGet());
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
bool httpHeaderBlockContainsHeader(const char *httpheaders, const char *header)
{
    if (!httpheaders || !*httpheaders)
        return false;
    VStringBuffer match("\n%s:", header);
    const char *matchStart = match.str()+1;
    if (!strncmp(httpheaders, matchStart, strlen(matchStart)))
        return true;
    if (strstr(httpheaders, match))
        return true;
    return false;
}

bool getHTTPHeader(const char *httpheaders, const char *header, StringBuffer& value)
{
    if (!httpheaders || !*httpheaders || !header || !*header)
        return false;

    const char* pHeader = strstr(httpheaders, header);
    if (!pHeader)
        return false;

    pHeader += strlen(header);
    if (*pHeader != ':')
        return getHTTPHeader(pHeader, header, value);

    pHeader++;
    const char* ppHeader = strchr(pHeader, '\n');
    if (!ppHeader)
        value.append(pHeader);
    else
        value.append(pHeader, 0, ppHeader - pHeader);
    value.trim();
    return true;
}

class CWSCAsyncFor : implements IWSCAsyncFor, public CInterface, public CAsyncFor
{
    class CSocketDataProvider : public CInterface
    {
        const char * buffer;
        size32_t currLen;
        size32_t curPosn;
        ISocket * socket;
        unsigned timeoutMS;
    public:
        CSocketDataProvider(const char * _buffer, size32_t _curPosn, size32_t _currLen, ISocket * _sock, unsigned _timeout )
            : buffer(_buffer), currLen(_currLen), curPosn(_curPosn), socket(_sock), timeoutMS(_timeout)
        {
        }
        size32_t getBytes(char * buf, size32_t len)
        {
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
                    socket->readtms(buf + count, 0, len - count, bytesRead, timeoutMS);
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
                    socket->readtms(buf+avail+bytesRead, 0, len-avail-bytesRead, read, timeoutMS);
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
    PTreeReaderOptions options;
    unsigned remainingMS;

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
                throw MakeStringException(ROXIE_ABORT_EVENT, "%s", e->errorMessage(s).str());
            }
        }
    }

    bool checkContentEncodingSupported(const char* encoding)
    {
        if (strieq(encoding, "gzip"))
            return true;
        if (strieq(encoding, "deflate"))
            return true;
        if (strieq(encoding, "x-deflate"))
            return true;
        return false;
    }

    bool checkContentDecoding(const StringBuffer& headers, StringBuffer& content, StringBuffer& contentEncoding)
    {
        if ((headers.length() == 0) || (content.length() == 0))
            return false;

        getHTTPHeader(headers.str(), CONTENT_ENCODING, contentEncoding);
        if (contentEncoding.isEmpty())
            return false;

        if (!checkContentEncodingSupported(contentEncoding.str()))
            throw MakeStringException(-1, "Content-Encoding:%s not supported", contentEncoding.str());
        return true;
    }

    void decodeContent(const char* contentEncodingType, StringBuffer& content)
    {
#ifdef _USE_ZLIB
        unsigned contentLength = content.length();
        StringBuffer contentDecoded;

        httpInflate((const byte*)content.str(), contentLength, contentDecoded, strieq(contentEncodingType, "gzip"));
        PROGLOG("Content decoded from %d bytes to %d bytes", contentLength, contentDecoded.length());
        content = contentDecoded;

        if (soapTraceLevel > 6 || master->logXML)
            master->logctx.CTXLOG("Content decoded. Original %s %d", CONTENT_LENGTH, contentLength);
#else
            throw MakeStringException(-1, "_USE_ZLIB is required for Content-Encoding:%s", contentEncodingType);
#endif
    }

    bool checkContentEncoding(const char* httpheaders, StringBuffer& contentEncodingType)
    {
        if (xmlWriter.length() == 0)
            return false;

        getHTTPHeader(httpheaders, CONTENT_ENCODING, contentEncodingType);
        if (contentEncodingType.isEmpty())
            return false;

        if (!checkContentEncodingSupported(contentEncodingType.str()))
            throw MakeStringException(-1, "Content-Encoding:%s not supported", contentEncodingType.str());
        return true;
    }

    ZlibCompressionType getEncodeFormat(const char *name)
    {
        if (strieq(name, "gzip"))
            return ZlibCompressionType::GZIP;
        if (strieq(name, "x-deflate"))
            return ZlibCompressionType::ZLIB_DEFLATE;
        if (strieq(name, "deflate"))
            return ZlibCompressionType::DEFLATE;
        return ZlibCompressionType::GZIP; //already checked above, shouldn't be here
    }

    void encodeContent(const char* contentEncodingType, MemoryBuffer& mb)
    {
#ifdef _USE_ZLIB
        zlib_deflate(mb, xmlWriter.str(), xmlWriter.length(), GZ_BEST_SPEED, getEncodeFormat(contentEncodingType));
        PROGLOG("Content encoded from %d bytes to %d bytes", xmlWriter.length(), mb.length());
#else
        throw MakeStringException(-1, "_USE_ZLIB is required for Content-Encoding:%s", contentEncodingType);
#endif
    }

    void logRequest(bool contentEncoded, StringBuffer& request)
    {
        if (soapTraceLevel > 6 || master->logXML)
        {
            if (!contentEncoded)
                master->logctx.CTXLOG("%s: request(%s)", master->wscCallTypeText(), request.str());
            else
                master->logctx.CTXLOG("%s: request(%s), content encoded.", master->wscCallTypeText(), request.str());
        }
    }

    void createHttpRequest(Url &url, StringBuffer &request)
    {
        // Create the HTTP POST request
        if (master->wscType == STsoap)
            request.clear().append("POST ").append(url.path).append(" HTTP/1.1\r\n");
        else
            request.clear().append(master->service).append(" ").append(url.path).append(" HTTP/1.1\r\n");

        const char *httpheaders = master->httpHeaders.get();
        if (httpheaders && *httpheaders)
        {
            if (soapTraceLevel > 6 || master->logXML)
                master->logctx.CTXLOG("%s: Adding HTTP Headers(%s)",  master->wscCallTypeText(), httpheaders);
            request.append(httpheaders);
        }

        if (!httpHeaderBlockContainsHeader(httpheaders, "Authorization"))
        {
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
        }

#ifdef _USE_ZLIB
        if (!httpHeaderBlockContainsHeader(httpheaders, ACCEPT_ENCODING))
            request.appendf("%s: gzip, deflate\r\n", ACCEPT_ENCODING);
#endif
        if (!isEmptyString(master->logctx.queryGlobalId()))
        {
            request.append(master->logctx.queryGlobalIdHttpHeader()).append(": ").append(master->logctx.queryGlobalId()).append("\r\n");
            if (!isEmptyString(master->logctx.queryLocalId()))
                request.append(master->logctx.queryCallerIdHttpHeader()).append(": ").append(master->logctx.queryLocalId()).append("\r\n");  //our localId is reciever's callerId
        }

        if (master->wscType == STsoap)
        {
            if (master->soapaction.get())
                request.append("SOAPAction: ").append(master->soapaction.get()).append("\r\n");
            if (master->httpHeaders.isEmpty() && master->httpHeaderName.get() && master->httpHeaderValue.get())
            {
                //backward compatibility
                StringBuffer hdr = master->httpHeaderName.get();
                hdr.append(": ").append(master->httpHeaderValue);
                if (soapTraceLevel > 6 || master->logXML)
                    master->logctx.CTXLOG("SOAPCALL: Adding HTTP Header(%s)", hdr.str());
                request.append(hdr.append("\r\n"));
            }
            if (!httpHeaderBlockContainsHeader(httpheaders, "Content-Type"))
                request.append("Content-Type: text/xml\r\n");
        }
        else if(master->wscType == SThttp)
            request.append("Accept: ").append(master->acceptType).append("\r\n");
        else
            assertex(false);

        if (master->wscType == STsoap)
        {
            request.append("Host: ").append(url.host).append(":").append(url.port).append("\r\n");//http 1.1

            StringBuffer contentEncodingType;
            if (!checkContentEncoding(httpheaders, contentEncodingType))
            {
                request.append(CONTENT_LENGTH).append(xmlWriter.length()).append("\r\n\r\n");
                request.append(xmlWriter.str());//add SOAP xml content
                logRequest(false, request);
            }
            else
            {
                logRequest(true, request);
                MemoryBuffer encodedContentBuf;
                encodeContent(contentEncodingType.str(), encodedContentBuf);
                request.append(CONTENT_LENGTH).append(encodedContentBuf.length()).append("\r\n\r\n");
                request.append(encodedContentBuf.length(), encodedContentBuf.toByteArray());
            }
        }
        else
        {
            request.append("Host: ").append(url.host);//http 1.1
            if (url.port != 80) //default port?
                request.append(":").append(url.port);
            request.append("\r\n");//http 1.1
            request.append("\r\n");//httpcall
            logRequest(false, request);
        }

        if (master->logMin)
            master->logctx.CTXLOG("%s: request(%s:%u)", master->wscCallTypeText(), url.host.str(), url.port);
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
        StringBuffer dbgheader, contentEncoding;
        bool chunked = false;
        size32_t read = 0;
        do {
            checkTimeLimitExceeded(&remainingMS);
            checkRoxieAbortMonitor(master->roxieAbortMonitor);
            socket->readtms(buffer+read, 0, WSCBUFFERSIZE-read, bytesRead, MIN(master->timeoutMS,remainingMS));
            checkTimeLimitExceeded(&remainingMS);
            checkRoxieAbortMonitor(master->roxieAbortMonitor);

            read += bytesRead;
            buffer[read] = 0;
            if (strncmp(buffer, "HTTP", 4) == 0) {
                const char *s = strstr(buffer,"\r\n\r\n");
                if (s) {
                    payloadofs = (size32_t)(s-buffer+4);
                    dbgheader.append(payloadofs,buffer);
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
                        checkTimeLimitExceeded(&remainingMS);
                        dataProvider.setown(new CSocketDataProvider(buffer, payloadofs, read, socket, MIN(master->timeoutMS,remainingMS)));
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
                            if (chunkSize > WSCBUFFERSIZE)
                                DBGLOG("SOAPCALL chunk size %d", chunkSize);

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
                    checkTimeLimitExceeded(&remainingMS);
                    checkRoxieAbortMonitor(master->roxieAbortMonitor);
                    socket->readtms(response.reserve(payloadsize-read), 0, payloadsize-read, bytesRead, MIN(master->timeoutMS,remainingMS));
                    checkTimeLimitExceeded(&remainingMS);
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
                for (;;) {
                    checkTimeLimitExceeded(&remainingMS);
                    checkRoxieAbortMonitor(master->roxieAbortMonitor);
                    socket->readtms(buffer, 0, WSCBUFFERSIZE, bytesRead, MIN(master->timeoutMS,remainingMS));
                    checkTimeLimitExceeded(&remainingMS);
                    checkRoxieAbortMonitor(master->roxieAbortMonitor);

                    if (bytesRead==0)
                        break;              // rely on socket closing to terminate message
                    response.append(bytesRead,buffer);
                }
            }
        }
        if (checkContentDecoding(dbgheader, response, contentEncoding))
            decodeContent(contentEncoding.str(), response);
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
        Owned<IXMLParse> xmlParser = createXMLParse((const void *)response.str(), (unsigned)response.length(), path.str(), matchCB, options, (master->flags&SOAPFusescontents)!=0);
        while (xmlParser->next());
    }

    void processLiteralResponse(Url &url, StringBuffer &response, ColumnProvider * meta)
    {
        StringBuffer path("/Envelope/Body/");
        if(master->rowTransformer && master->inputpath.get())
            path.append(master->inputpath.get());
        CMatchCB matchCB(*this, url, NULL, meta);
        Owned<IXMLParse> xmlParser = createXMLParse((const void *)response.str(), (unsigned)response.length(), path.str(), matchCB, options, (master->flags&SOAPFusescontents)!=0);
        while (xmlParser->next());
    }

    void processHttpResponse(Url &url, StringBuffer &response, ColumnProvider * meta)
    {
        StringBuffer path;
        if(master->rowTransformer && master->inputpath.get())
            path.append(master->inputpath.get());
        CMatchCB matchCB(*this, url, NULL, meta);
        Owned<IXMLParse> xmlParser;
        if (strieq(master->acceptType.str(), "application/json"))
            xmlParser.setown(createJSONParse((const void *)response.str(), (unsigned)response.length(), path.str(), matchCB, options, (master->flags&SOAPFusescontents)!=0, true));
        else
            xmlParser.setown(createXMLParse((const void *)response.str(), (unsigned)response.length(), path.str(), matchCB, options, (master->flags&SOAPFusescontents)!=0));
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

    inline void checkTimeLimitExceeded(unsigned * remainingMS)
    {
        if (master->isTimeLimitExceeded(remainingMS))
            throw MakeStringException(TIMELIMIT_EXCEEDED, "%sCALL TIMELIMIT(%ums) exceeded", master->wscType == STsoap ? "SOAP" : "HTTP", master->timeLimitMS);
    }

public:
    CWSCAsyncFor(CWSCHelper * _master, CommonXmlWriter &_xmlWriter, ConstPointerArray &_inputRows, PTreeReaderOptions _options): xmlWriter(_xmlWriter), inputRows(_inputRows), options(_options)
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
        unsigned numRetries = 0;
        unsigned retryInterval = 0;

        Url &url = master->urlArray.item(idx);
        createHttpRequest(url, request);
        unsigned startidx = idx;
        while (!master->aborted)
        {
            Owned<ISocket> socket;
            cycle_t startCycles, endCycles;
            startCycles = get_cycles_now();
            for (;;)
            {
                try
                {
                    checkTimeLimitExceeded(&remainingMS);
                    Url &connUrl = master->proxyUrlArray.empty() ? url : master->proxyUrlArray.item(0);
                    socket.setown(blacklist->connect(connUrl.port, connUrl.host, master->logctx, (unsigned)master->maxRetries, master->timeoutMS, master->roxieAbortMonitor));
                    if (stricmp(url.method, "https") == 0)
                    {
#ifdef _USE_OPENSSL
                        Owned<ISecureSocket> ssock = master->createSecureSocket(socket.getClear());
                        if (ssock) 
                        {
                            checkTimeLimitExceeded(&remainingMS);
                            int status = ssock->secure_connect();
                            if (status < 0)
                            {
                                StringBuffer err;
                                err.append("Failure to establish secure connection to ");
                                connUrl.getUrlString(err);
                                err.append(": returned ").append(status);
                                throw MakeStringExceptionDirect(0, err.str());
                            }
                            socket.setown(ssock.getLink());
                        }
#else
                        StringBuffer err;
                        err.append("Failure to establish secure connection to ");
                        connUrl.getUrlString(err);
                        err.append(": OpenSSL disabled in build");
                        throw MakeStringExceptionDirect(0, err.str());
#endif
                    }
                    break;
                }
                catch (IException *e)
                {
                    if (master->timeLimitExceeded)
                    {
                        master->logctx.CTXLOG("%sCALL exiting: time limit (%ums) exceeded",master->wscType == STsoap ? "SOAP" : "HTTP", master->timeLimitMS);
                        processException(url, inputRows, e);
                        return;
                    }

                    if (e->errorCode() == ROXIE_ABORT_EVENT)
                    {
                        StringBuffer s;
                        master->logctx.CTXLOG("%sCALL exiting: Roxie Abort : %s",master->wscType == STsoap ? "SOAP" : "HTTP",e->errorMessage(s).str());
                        throw;
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
                checkTimeLimitExceeded(&remainingMS);
                checkRoxieAbortMonitor(master->roxieAbortMonitor);
                socket->write(request.str(), request.length());
                if (soapTraceLevel > 4)
                    master->logctx.CTXLOG("%sCALL: sent request (%s) to %s:%d", master->wscType == STsoap ? "SOAP" : "HTTP",master->service.str(), url.host.str(), url.port);
                checkTimeLimitExceeded(&remainingMS);
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
                endCycles = get_cycles_now();
                __int64 elapsedNs = cycle_to_nanosec(endCycles-startCycles);
                master->logctx.noteStatistic(StTimeSoapcall, elapsedNs);
                checkTimeLimitExceeded(&remainingMS);
                ColumnProvider * meta = (ColumnProvider*)CreateColumnProvider((unsigned)nanoToMilli(elapsedNs), master->flags&SOAPFencoding?true:false);
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
                    master->logctx.CTXLOG("%sCALL exiting: time limit (%ums) exceeded", master->wscType == STsoap ? "SOAP" : "HTTP", master->timeLimitMS);
                    break;
                }

                if (e->errorCode() == ROXIE_ABORT_EVENT)
                {
                    StringBuffer s;
                    master->logctx.CTXLOG("%sCALL exiting: Roxie Abort : %s",master->wscType == STsoap ? "SOAP" : "HTTP",e->errorMessage(s).str());
                    throw;
                }

                // other IException ... retry up to maxRetries
                StringBuffer s;
                master->logctx.CTXLOG("Exception %s", e->errorMessage(s).str());

                if (numRetries >= master->maxRetries)
                {
                    // error affects all inputRows
                    master->logctx.CTXLOG("Exiting: maxRetries %d exceeded", master->maxRetries);
                    processException(url, inputRows, e);
                    break;
                }
                numRetries++;
                master->logctx.CTXLOG("Retrying: attempt %d of %d", numRetries, master->maxRetries);
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

IWSCAsyncFor * createWSCAsyncFor(CWSCHelper * _master, CommonXmlWriter &_xmlWriter, ConstPointerArray &_inputRows, PTreeReaderOptions _options)
{
    return new CWSCAsyncFor(_master, _xmlWriter, _inputRows, _options);
}
