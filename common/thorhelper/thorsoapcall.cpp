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
#include "jsecrets.hpp"

#include "rtlformat.hpp"

#include "thorxmlread.hpp"
#include "thorxmlwrite.hpp"
#include "thorcommon.ipp"
#include "thorsoapcall.hpp"

#include "securesocket.hpp"
#include "eclrtl.hpp"
#include "roxiemem.hpp"
#include "zcrypt.hpp"
#include "persistent.hpp"

#include <memory>

using roxiemem::OwnedRoxieString;

#ifndef _WIN32
#include <stdexcept>
#endif

#include <new>

#define CONTENT_LENGTH "Content-Length: "
#define CONTENT_ENCODING "Content-Encoding"
#define CONTENT_TYPE "Content-Type"
#define ACCEPT_ENCODING "Accept-Encoding"
#define CONNECTION "Connection"

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

    StringBuffer &getDynamicUrlSecretName(StringBuffer &secretName) const
    {
        return generateDynamicUrlSecretName(secretName, method, userPasswordPair, host, port, path);
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

    void cleanPath(const char *src, StringBuffer &output)
    {
        const char *srcPtr = src;
        char *dst = output.reserveTruncate(strlen(src));
        char *dstPtr = dst;
        while (*srcPtr)
        {
            *dstPtr++ = *srcPtr;
            if ('/' == *srcPtr++)
            {
                while ('/' == *srcPtr)
                    ++srcPtr;
            }
        }
        *dstPtr = '\0';
        output.setLength(dstPtr-dst);
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
                cleanPath(p, path);
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
                host.set(urltext);
                *p = '/';
                cleanPath(p, path);
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

//if Url was globally accessible we could define this in jtrace instead
//http span standards documented here: https://opentelemetry.io/docs/specs/semconv/http/http-spans/
void setSpanURLAttributes(ISpan * clientSpan, const Url & url)
{
    if (clientSpan == nullptr)
        return;

    clientSpan->setSpanAttribute("http.request.method", "POST"); //apparently hardcoded to post
                                                                 //Even though a "service" and 
                                                                 //a "method" are tracked and sometimes
                                                                 //the target service name is used
                                                                 //and there's code and comments suggesting only
                                                                 //GET is supported...
    clientSpan->setSpanAttribute("network.peer.address", url.host.get());
    clientSpan->setSpanAttribute("network.peer.port", url.port);
    clientSpan->setSpanAttribute("network.protocol.name", url.method.get());
}

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

    unsigned getUrls(UrlArray &array, const char *basic_credentials=nullptr)
    {
        if (fullText)
        {
            char *copyFullText = strdup(fullText);

            char *saveptr;
            char *url = strtok_r(copyFullText, "|", &saveptr);
            while (url != NULL)
            {
                Owned<Url> item = new Url(url);
                if (basic_credentials)
                    item->userPasswordPair.set(basic_credentials);
                array.append(*item.getClear());
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

#define ROXIE_ABORT_EVENT  1407
#define TIMELIMIT_EXCEEDED 1408

class BlackLister : public CInterface, implements IThreadFactory
{
    SocketEndpointArray list;  // MORE - should match the options too? Should use a hash table?
    Owned<IThreadPool> pool;
    CriticalSection crit;

    struct DeblacklisterParams
    {
        SocketEndpoint ep;
        unsigned delay = DEBLACKLIST_RETRY_DELAY;
        unsigned retries = DEBLACKLIST_RETRIES;
        unsigned timeout = DEBLACKLIST_CONNECT_TIMEOUT;
    };

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
                logctx.CTXLOG("endpoint %s is blacklisted", ep.getEndpointHostText(s).str());
            }
            return true;
        }
        return false;

    }
    void blacklist(DeblacklisterParams &p, const IContextLogger &logctx)
    {
        unsigned match;
        {
            CriticalBlock b(crit);
            match = list.find(p.ep);
            if (match == NotFound)
            {
                list.append(p.ep);
                pool->start(&p);
            }
        }
        if (match == NotFound)
        {
            if (soapTraceLevel > 0)
            {
                StringBuffer s;
                logctx.CTXLOG("Blacklisting endpoint %s", p.ep.getEndpointHostText(s).str());
            }
        }
        else
        {
            if (soapTraceLevel > 3)
            {
                StringBuffer s;
                logctx.CTXLOG("Endpoint %s is already blacklisted", p.ep.getEndpointHostText(s).str());
            }

        }

    }
    void deblacklist(SocketEndpoint &ep)
    {
        unsigned match;
        {
            CriticalBlock b(crit);
            match = list.find(ep);
            if (match != NotFound)
                list.remove(match);
        }
        if ((match != NotFound) && (soapTraceLevel > 0))
        {
            StringBuffer s;
            DBGLOG("De-blacklisting endpoint %s", ep.getEndpointHostText(s).str());
        }
    }

public:
    IMPLEMENT_IINTERFACE;

    BlackLister()
    {
        pool.setown(createThreadPool("SocketBlacklistPool", this, false, nullptr, 0, 0));
    }

    ISocket* connect(SocketEndpoint &ep,
                     const IContextLogger &logctx,
                     unsigned retries,
                     unsigned timeoutMS,
                     IRoxieAbortMonitor * roxieAbortMonitor,
                     const IWSCRowProvider *blOptions)
    {
        bool useBlacklister = blOptions->useBlacklister();
        if (useBlacklister && lookup(ep, logctx))
        {
            StringBuffer s;
            ep.getEndpointHostText(s);
            throw MakeStringException(JSOCKERR_connection_failed, "%s %s", blOptions->getBLerror(), s.str());
        }
        Owned<IException> exc;

        unsigned numAttemptsRemaining = retries+1;
        if (!numAttemptsRemaining)
            numAttemptsRemaining++;

        unsigned connectTimeMS = timeoutMS;
        if (timeoutMS == WAIT_FOREVER)
            connectTimeMS = 60000 / numAttemptsRemaining;

        while(numAttemptsRemaining-- > 0)
        {
            try
            {
                checkRoxieAbortMonitor(roxieAbortMonitor);
                Owned<ISocket> sock;
                Owned<ISocketConnectWait> scw = nonBlockingConnect(ep, connectTimeMS);
                for (;;)
                {
                    sock.setown(scw->wait(1000)); //throws if connect fails or timeoutMS
                    checkRoxieAbortMonitor(roxieAbortMonitor);
                    if (sock)
                        return sock.getLink();
                }
            }
            catch (IException *e)
            {
                if (e->errorCode() == ROXIE_ABORT_EVENT)
                    throw;
                // MCK - do we checkTimeLimitExceeded(&remainingMS) and possibly error out if timelimit exceeded ?
                if (numAttemptsRemaining > 0)
                {
                    e->Release();
                }
                else
                {
                    EXCLOG(e, nullptr);
                    exc.setownIfNull(e);
                    break;
                }
            }
        }

        if (useBlacklister)
        {
            DeblacklisterParams params;
            params.ep = ep;
            params.delay = blOptions->getBLDelay();
            params.retries = blOptions->getBLRetries();
            params.timeout = blOptions->getBLConnectTimeout();
            blacklist(params, logctx);
        }
        if (exc->errorCode()==JSOCKERR_connection_failed) {
            StringBuffer s;
            ep.getEndpointHostText(s);
            throw MakeStringException(JSOCKERR_connection_failed, "connection failed %s", s.str());
        }
        throw exc.getClear();
        return NULL;
    }

    bool blacklisted (unsigned short port, char const* host)
    {
        SocketEndpoint ep(host, port);
        CriticalBlock b(crit);
        return (list.find(ep)!=NotFound);
    }

    ISocket* connect(unsigned short port,
                     char const* host,
                     const IContextLogger &logctx,
                     unsigned retries,
                     unsigned timeoutMS,
                     IRoxieAbortMonitor * roxieAbortMonitor,
                     const IWSCRowProvider *blOptions)
    {
        SocketEndpoint ep(host, port);
        return connect(ep, logctx, retries, timeoutMS, roxieAbortMonitor, blOptions);
    }

    virtual IPooledThread *createNew()
    {
        class SocketDeblacklister : public CInterface, implements IPooledThread
        {
            BlackLister &parent;
            Semaphore stopped;
            DeblacklisterParams p;
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
                p = *(DeblacklisterParams *) param;
            }
            virtual void threadmain() override
            {
                for (unsigned i = 0; i < p.retries; i++)
                {
                    try
                    {
                        Owned<ISocket> s = ISocket::connect_timeout(p.ep, p.timeout);
                        s->close();
                        break;
                    }
                    catch (IJSOCK_Exception *E)
                    {
                        E->Release();
                        if (stopped.wait(p.delay))
                            return;
                    }
                }
                parent.deblacklist(p.ep);
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

static bool defaultUsePersistConnections = false;
static IPersistentHandler* persistentHandler = nullptr;
static CriticalSection globalFeatureCrit;
static std::atomic<bool> globalFeaturesInitDone{false};
static std::atomic<bool> mapUrlsToSecrets{false};
static std::atomic<bool> warnIfUrlNotMappedToSecret{false};
static std::atomic<bool> requireUrlsMappedToSecrets{false};

void initGlobalFeatures()
{
    CriticalBlock block(globalFeatureCrit);
    if (!globalFeaturesInitDone)
    {
        int maxPersistentRequests = 100;
        defaultUsePersistConnections = false;
        if (!isContainerized())
        {
            defaultUsePersistConnections = queryEnvironmentConf().getPropBool("useHttpCallPersistentRequests", defaultUsePersistConnections);
            maxPersistentRequests = queryEnvironmentConf().getPropInt("maxHttpCallPersistentRequests", maxPersistentRequests); //global (backward compatible)
        }

        Owned<IPropertyTree> conf = getComponentConfig();
        defaultUsePersistConnections = conf->getPropBool("@useHttpCallPersistentRequests", defaultUsePersistConnections);
        maxPersistentRequests = conf->getPropInt("@maxHttpCallPersistentRequests", maxPersistentRequests); //component config wins
        mapUrlsToSecrets = conf->getPropBool("@mapHttpCallUrlsToSecrets", false);
        warnIfUrlNotMappedToSecret = conf->getPropBool("@warnIfUrlNotMappedToSecret", mapUrlsToSecrets);
        requireUrlsMappedToSecrets = conf->getPropBool("@requireUrlsMappedToSecrets", false);

        if (maxPersistentRequests != 0)
            persistentHandler = createPersistentHandler(nullptr, DEFAULT_MAX_PERSISTENT_IDLE_TIME, maxPersistentRequests, PersistentLogLevel::PLogMin, true);
        else
            defaultUsePersistConnections = false;

        globalFeaturesInitDone = true;
    }
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    blacklist = new BlackLister;

    return true;
}

MODULE_EXIT()
{
    blacklist->stop();
    delete blacklist;

    if (persistentHandler)
    {
        persistentHandler->stop(true);
        ::Release(persistentHandler);
        persistentHandler = nullptr;
    }
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
    virtual __uint64    getUInt(const char * path)
    {
        __uint64 ret = base->getUInt(path);
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
    virtual __uint64    readUInt(const char * path, __uint64 _default)
    {
        if(path && *path=='_')
        {
            if (stricmp(path, "_call_latency_ms")==0)
                return callLatencyMs;
            else if (stricmp(path, "_call_latency")==0)
                return (callLatencyMs + 500) / 1000;
        }
        return base->readUInt(path, _default);
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

static const char * getWsCallTypeName(WSCType wscType) { return wscType == STsoap ? "SOAPCALL" : "HTTPCALL"; }

//Web Services Call Asynchronous For
interface IWSCAsyncFor: public IInterface
{
    virtual void processException(const Url &url, const void *row, IException *e) = 0;
    virtual void processException(const Url &url, ConstPointerArray &inputRows, IException *e) = 0;
    virtual void checkTimeLimitExceeded(unsigned * _remainingMS) = 0;

    virtual void createHttpRequest(StringBuffer &request, const Url &url, const IProperties * traceHeaders) = 0;
    virtual int readHttpResponse(StringBuffer &response, ISocket *socket, bool &keepAlive, StringBuffer &contentType) = 0;
    virtual void processResponse(Url &url, StringBuffer &response, ColumnProvider * meta, const char *contentType) = 0;

    virtual const char *getResponsePath() = 0;
    virtual ConstPointerArray & getInputRows() = 0;
    virtual IWSCHelper * getMaster() = 0;
    virtual IEngineRowAllocator * getOutputAllocator() = 0;
    
    virtual void For(unsigned num,unsigned maxatonce,bool abortFollowingException=false,bool shuffled=false) = 0;
    virtual void Do(unsigned idx=0)=0;
};

class CWSCHelper;
IWSCAsyncFor * createWSCAsyncFor(CWSCHelper * _master, IXmlWriterExt &_xmlWriter, ConstPointerArray &_inputRows, PTreeReaderOptions _options);

//=================================================================================================

#define CBExceptionPathExc     0x0001
#define CBExceptionPathExcsExc 0x0002
#define CBExceptionPathExcs    0x0004

class CMatchCB : implements IXMLSelect, public CInterface
{
    IWSCAsyncFor &parent;
    const Url &url;
    StringAttr tail;
    ColumnProvider * meta;
    StringAttr excPath;
    unsigned excFlags = 0;
public:
    IMPLEMENT_IINTERFACE;

    CMatchCB(IWSCAsyncFor &_parent, const Url &_url, const char *_tail, ColumnProvider * _meta, const char *_excPath, unsigned _excFlags) : parent(_parent), url(_url), tail(_tail), meta(_meta), excFlags(_excFlags), excPath(_excPath)
    {
    }

    bool checkGetExceptionEntry(bool check, Owned<IColumnProviderIterator> &excIter, IColumnProvider &parent, IColumnProvider *&excEntry, const char *path)
    {
        if (!check)
            return false;
        excIter.setown(parent.getChildIterator(path));
        excEntry = excIter->first();
        return excEntry != nullptr;
    }

    IColumnProvider *getExceptionEntry(Owned<IColumnProviderIterator> &excIter, IColumnProvider &parent)
    {
        IColumnProvider *excEntry = nullptr;
        if (excPath.length())
        {
            checkGetExceptionEntry(true, excIter, parent, excEntry, excPath.str()); //set by user so don't try others
            return excEntry;
        }
        if (checkGetExceptionEntry((excFlags & CBExceptionPathExc)!=0, excIter, parent, excEntry, "Exception"))
            return excEntry;
        if (checkGetExceptionEntry((excFlags & CBExceptionPathExcsExc)!=0, excIter, parent, excEntry, "Exceptions/Exception")) //ESP xml array
            return excEntry;
        checkGetExceptionEntry((excFlags & CBExceptionPathExcs)!=0, excIter, parent, excEntry, "Exceptions"); //json array
        return excEntry;
    }

    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
    {
        Owned<IException> e;
        if (tail.length()||excPath.length())
        {
            StringBuffer path(parent.getResponsePath());
            unsigned idx = (unsigned)entry.getInt(path.append("/@sequence").str());
            Owned<IColumnProviderIterator> excIter;
            IColumnProvider *excptEntry = getExceptionEntry(excIter, entry);
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
    virtual void outputRows(IXmlWriterExt &xmlWriter, ConstPointerArray &inputRows, const char *itemtag=NULL, bool encode_off=false, char const * itemns = NULL);
    virtual void createESPQuery(IXmlWriterExt &xmlWriter, ConstPointerArray &inputRows);
    virtual void createSOAPliteralOrEncodedQuery(IXmlWriterExt &xmlWriter, ConstPointerArray &inputRows);
    virtual void createXmlSoapQuery(IXmlWriterExt &xmlWriter, ConstPointerArray &inputRows);
    virtual void createHttpPostQuery(IXmlWriterExt &xmlWriter, ConstPointerArray &inputRows, bool appendRequestToName, bool appendEncodeFlag);
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

bool loadConnectSecret(const char *vaultId, const char *secretName, UrlArray &urlArray, StringBuffer &issuer, StringBuffer &proxyAddress, bool & persistEnabled, unsigned & persistMaxRequests, bool required, WSCType wscType)
{
    Owned<const IPropertyTree> secret;
    if (!isEmptyString(secretName))
        secret.setown(getSecret("ecl", secretName, vaultId, nullptr));
    if (!secret)
    {
        if (required)
            throw MakeStringException(0, "%s %s SECRET not found", getWsCallTypeName(wscType), secretName);
        return false;
    }

    StringBuffer url;
    getSecretKeyValue(url, secret, "url");
    if (url.isEmpty())
        throw MakeStringException(0, "%s %s HTTP SECRET must contain url", getWsCallTypeName(wscType), secretName);
    UrlListParser urlListParser(url);
    StringBuffer usernamePasswordPair;
    getSecretKeyValue(usernamePasswordPair, secret, "username");
    if (usernamePasswordPair.length())
    {
        if (strchr(usernamePasswordPair, ':'))
            throw MakeStringException(0, "%s HTTP-CONNECT SECRET username contains illegal colon", getWsCallTypeName(wscType));
        StringBuffer password;
        getSecretKeyValue(password, secret, "password");
        if (password.length())
            usernamePasswordPair.append(':').append(password);
    }
    urlListParser.getUrls(urlArray, usernamePasswordPair);
    getSecretKeyValue(proxyAddress.clear(), secret, "proxy");
    getSecretKeyValue(issuer, secret, "issuer");

    //Options defined in the secret override the defaults and the values specified in ECL
    StringBuffer persist;
    if (getSecretKeyValue(persist, secret, "persist"))
        persistEnabled = strToBool(persist.str());

    if (getSecretKeyValue(persist.clear(), secret, "persistMaxRequests"))
        persistMaxRequests = atoi(persist);

    return true;
}


//=================================================================================================

class CWSCHelper : implements IWSCHelper, public CInterface
{
private:
    SimpleInterThreadQueueOf<const void, true> outputQ;
    SpinLock outputQLock;
    CriticalSection toXmlCrit, transformCrit, onfailCrit;
    unsigned done;
    Owned<IPropertyTree> xpathHints;
    Linked<ClientCertificate> clientCert;

    static CriticalSection secureContextCrit;
    static Owned<ISecureSocketContext> tlsSecureContext;
    static Owned<ISecureSocketContext> localMtlsSecureContext;
    static Owned<ISecureSocketContext> remoteMtlsSecureContext;

    Owned<ISecureSocketContext> customSecureContext;

    CTimeMon timeLimitMon;
    bool complete;
    std::atomic_bool timeLimitExceeded{false};
    bool customClientCert = false;
    bool persistEnabled = false;
    unsigned persistMaxRequests = 0;
    StringAttr clientCertIssuer;
    IRoxieAbortMonitor * roxieAbortMonitor;
    StringBuffer issuer; //TBD sync up with other PR, it will benefit from this being able to come from the secret

protected:
    IArrayOf<CWSCHelperThread> threads;
    WSCType wscType;

public:
    Owned<ISpan> activitySpanScope;
    IMPLEMENT_IINTERFACE;

    CWSCHelper(IWSCRowProvider *_rowProvider, IEngineRowAllocator * _outputAllocator, const char *_authToken, WSCMode _wscMode, ClientCertificate *_clientCert,
               const IContextLogger &_logctx, IRoxieAbortMonitor *_roxieAbortMonitor, WSCType _wscType)
        : logctx(_logctx), outputAllocator(_outputAllocator), clientCert(_clientCert), roxieAbortMonitor(_roxieAbortMonitor)
    {
        activitySpanScope.setown(logctx.queryActiveSpan()->createInternalSpan(_wscType == STsoap ? "SoapCall Activity": "HTTPCall Activity"));
        activitySpanScope->setSpanAttribute("activity_id", _rowProvider->queryActivityId());
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
        logUserTailMsg = (flags & SOAPFlogusertail) != 0;

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

        persistEnabled = defaultUsePersistConnections;
        persistMaxRequests = 0; // 0 implies do not override the default pool size
        if (flags & SOAPFpersist)
        {
            if (flags & SOAPFpersistMax)
            {
                unsigned maxRequests = helper->getPersistMaxRequests();
                if (maxRequests != 0)
                {
                    persistEnabled = true;
                    persistMaxRequests = maxRequests;
                }
                else
                    persistEnabled = false;
            }
            else
                persistEnabled = true;
        }

        if (flags & SOAPFhttpheaders)
            httpHeaders.set(s.setown(helper->getHttpHeaders()));
        if (flags & SOAPFxpathhints)
        {
            s.setown(helper->getXpathHintsXml());
            xpathHints.setown(createPTreeFromXMLString(s.get()));
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

            rowHeader.set(s.setown(helper->getHeader()));
            rowFooter.set(s.setown(helper->getFooter()));
            if (flags & SOAPFmarkupinfo)
            {
                rootHeader.set(s.setown(helper->getRequestHeader()));
                rootFooter.set(s.setown(helper->getRequestFooter()));
            }
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
            if (strcmp(service.str(), "GET") != 0)
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

        StringBuffer proxyAddress;
        proxyAddress.set(s.setown(helper->getProxyAddress()));

        OwnedRoxieString hostsString(helper->getHosts());
        const char *hosts = hostsString.get();

        if (isEmptyString(hosts))
            throw MakeStringException(0, "%s specified no URLs", getWsCallTypeName(wscType));
        if (0==strncmp(hosts, "mtls:", 5))
        {
            clientCertIssuer.set("local");
            hosts += 5;
        }
        else if (0==strncmp(hosts, "remote-mtls:", 12))
        {
            clientCertIssuer.set("remote");
            hosts += 12;
        }
        if (0==strncmp(hosts, "secret:", 7))
        {
            const char *finger = hosts+7;
            if (isEmptyString(finger))
                throw MakeStringException(0, "%s HTTP-CONNECT SECRET specified with no name", getWsCallTypeName(wscType));
            if (!proxyAddress.isEmpty())
                throw MakeStringException(0, "%s PROXYADDRESS can't be used with HTTP-CONNECT secrets", getWsCallTypeName(wscType));
            StringAttr vaultId;
            const char *thumb = strchr(finger, ':');
            if (thumb)
            {
                vaultId.set(finger, thumb-finger);
                finger = thumb + 1;
            }
            StringBuffer secretName("http-connect-");
            secretName.append(finger);
            loadConnectSecret(vaultId, secretName, urlArray, issuer, proxyAddress, persistEnabled, persistMaxRequests, true, wscType);
        }
        else
        {
            UrlListParser urlListParser(hosts);
            urlListParser.getUrls(urlArray);
            if (mapUrlsToSecrets && urlArray.length())
            {
                StringBuffer secretName;
                UrlArray tempArray;
                //TBD: If this is a list of URLs do we A. not check for a mapped secret, B. check the first one, C. Use long secret name including entire list
                Url &url = urlArray.item(0);
                url.getDynamicUrlSecretName(secretName);
                if (secretName.length())
                {
                    if (loadConnectSecret(nullptr, secretName, tempArray, issuer, proxyAddress, persistEnabled, persistMaxRequests, requireUrlsMappedToSecrets, wscType))
                    {
                        logctx.CTXLOG("Mapped %s URL!", wscCallTypeText());
                        if (tempArray.length())
                            urlArray.swapWith(tempArray);
                    }
                    else if (warnIfUrlNotMappedToSecret)
                    {
                        //should we warn even if the url doesn't have credentials embedded?  If HTTPHEADER is being used to pass credentials, we still prefer connect secrets be used instead.
                        logctx.CTXLOG("Security Warning: %s not using a connection secret (auto secret = %s)", wscCallTypeText(), secretName.str());
                    }
                }
            }
        }

        numUrls = urlArray.ordinality();
        if (numUrls == 0)
            throw MakeStringException(0, "%s specified no URLs", getWsCallTypeName(wscType));

        if (!persistentHandler)
            persistEnabled = false;

        if (!proxyAddress.isEmpty())
        {
            UrlListParser proxyUrlListParser(proxyAddress);
            if (0 == proxyUrlListParser.getUrls(proxyUrlArray))
                throw MakeStringException(0, "%s proxy address specified no URLs", getWsCallTypeName(wscType));
        }

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
            threads.item(i).start(true); // inherit context because the threads always have a shorter lifetime than the caller.
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
    ISecureSocketContext *ensureSecureContext(Owned<ISecureSocketContext> &ownedSC)
    {
        if (!ownedSC)
        {
            if (clientCert != NULL)
            {
                Owned<IPropertyTree> config = createSecureSocketConfig(clientCert->certificate, clientCert->privateKey, clientCert->passphrase, true);
                ownedSC.setown(createSecureSocketContextEx2(config, ClientSocket));
            }
            else if (clientCertIssuer.length())
                ownedSC.setown(createSecureSocketContextSecret(clientCertIssuer.str(), ClientSocket));
            else
                ownedSC.setown(createSecureSocketContext(ClientSocket));
        }
        return ownedSC.get();
    }
    ISecureSocketContext *ensureStaticSecureContext()
    {
        CriticalBlock b(secureContextCrit);
        if (clientCertIssuer.length())
        {
            if (strieq(clientCertIssuer.str(), "local"))
                return ensureSecureContext(localMtlsSecureContext);
            if (strieq(clientCertIssuer.str(), "remote"))
                return ensureSecureContext(remoteMtlsSecureContext);
        }
        return ensureSecureContext(tlsSecureContext);
    }
    ISecureSocket *createSecureSocket(ISocket *sock, const char *fqdn = nullptr)
    {
        ISecureSocketContext *sc = (customClientCert) ? ensureSecureContext(customSecureContext) : ensureStaticSecureContext();
        return sc->createSecureSocket(sock, SSLogNormal, fqdn);
    }
#endif
    bool isTimeLimitExceeded(unsigned *_remainingMS)
    {
        if (timeLimitMS != WAIT_FOREVER)
        {
            //No need to protect with a critical section because it does not matter if more than one thread calls timedout
            if (timeLimitExceeded)
            {
                if (_remainingMS)
                    *_remainingMS = 0;
                return true;
            }

            if (timeLimitMon.timedout(_remainingMS))
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
    void addUserLogTailMsg(const byte * row, unsigned timeTaken)
    {
        if (logUserTailMsg)
        {
            size32_t lenText;
            rtlDataAttr text;
            helper->getLogTailText(lenText, text.refstr(), row);
            logctx.CTXLOG("%s [time=%u]: %.*s", wscCallTypeText(), timeTaken, lenText, text.getstr());
        }
    }
    inline IXmlToRowTransformer * getRowTransformer() { return rowTransformer; }
    inline const char * wscCallTypeText() const { return getWsCallTypeName(wscType); }
    inline bool usePersistConnections() const { return persistEnabled; }
    inline unsigned getPersistMaxRequests() const { return persistMaxRequests; }

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
    void toXML(const byte * self, IXmlWriterExt & out) { CriticalBlock block(toXmlCrit); helper->toXML(self, out); }
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
    bool logUserTailMsg = false;
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
    StringAttr rowHeader;
    StringAttr rowFooter;
    StringAttr rootHeader;
    StringAttr rootFooter;
    StringAttr xmlnamespace;
    IXmlToRowTransformer * rowTransformer;
};

CriticalSection CWSCHelper::secureContextCrit;
Owned<ISecureSocketContext> CWSCHelper::tlsSecureContext; // created on first use
Owned<ISecureSocketContext> CWSCHelper::localMtlsSecureContext; // created on first use
Owned<ISecureSocketContext> CWSCHelper::remoteMtlsSecureContext; // created on first use


//=================================================================================================

void CWSCHelperThread::outputRows(IXmlWriterExt &xmlWriter, ConstPointerArray &inputRows, const char *itemtag, bool encode_off, char const * itemns)
{
    ForEachItemIn(idx, inputRows)
    {
        if (idx!=0)
            xmlWriter.checkDelimiter();

        if (itemtag && *itemtag)                //TAG
        {
            xmlWriter.outputBeginNested(itemtag, true);
            if(itemns)
                xmlWriter.outputXmlns("xmlns", itemns);
        }

        if (master->rowHeader.get())   //OPTIONAL HEADER (specified by "HEADING" option)
            xmlWriter.outputInline(master->rowHeader.get());

                                    //XML ROW CONTENT
        master->toXML((const byte *)inputRows.item(idx), xmlWriter);

        if (master->rowFooter.get())   //OPTION FOOTER
            xmlWriter.outputInline(master->rowFooter.get());

        if (encode_off)             //ENCODING
            xmlWriter.outputInt(0, 1, "encode_");

        if (itemtag && *itemtag)                //TAG
            xmlWriter.outputEndNested(itemtag);

        master->addUserLogMsg((const byte *)inputRows.item(idx));
    }
}

void CWSCHelperThread::createHttpPostQuery(IXmlWriterExt &xmlWriter, ConstPointerArray &inputRows, bool appendRequestToName, bool appendEncodeFlag)
{
    StringBuffer method_tag;
    method_tag.append(master->service);
    if (method_tag.length() && appendRequestToName)
        method_tag.append("Request");

    StringBuffer array_tag;
    StringAttr method_ns;

    if (master->rootHeader.get())   //OPTIONAL ROOT REQUEST HEADER
        xmlWriter.outputInline(master->rootHeader.get());

    if (inputRows.ordinality() > 1)
    {
        if (!(master->flags & SOAPFnoroot))
        {
            if (method_tag.length())
            {
                array_tag.append(method_tag).append("Array");
                xmlWriter.outputBeginNested(array_tag, true);
                if (master->xmlnamespace.get())
                    xmlWriter.outputXmlns("xmlns", master->xmlnamespace);
            }
        }
        xmlWriter.outputBeginArray(method_tag);
    }
    else
    {
        if(master->xmlnamespace.get())
            method_ns.set(master->xmlnamespace.get());
    }

    outputRows(xmlWriter, inputRows, method_tag.str(), appendEncodeFlag ? (inputRows.ordinality() == 1) : false, method_ns.get());

    if (inputRows.ordinality() > 1)
    {
        xmlWriter.outputEndArray(method_tag);
        if (appendEncodeFlag)
            xmlWriter.outputInt(0, 1, "encode_");
        if (!(master->flags & SOAPFnoroot))
        {
            if (method_tag.length())
                xmlWriter.outputEndNested(array_tag);
        }
    }

    if (master->rootFooter.get())   //OPTIONAL ROOT REQUEST FOOTER
        xmlWriter.outputInline(master->rootFooter.get());
}

void CWSCHelperThread::createESPQuery(IXmlWriterExt &xmlWriter, ConstPointerArray &inputRows)
{
    createHttpPostQuery(xmlWriter, inputRows, true, true);
}

//Create servce xml request body, with binding usage of either Literal or Encoded
//Note that Encoded usage requires type encoding for data fields
void CWSCHelperThread::createSOAPliteralOrEncodedQuery(IXmlWriterExt &xmlWriter, ConstPointerArray &inputRows)
{
    xmlWriter.outputBeginNested(master->service, true);

    if (master->flags & SOAPFencoding)
        xmlWriter.outputCString("http://schemas.xmlsoap.org/soap/encoding/", "@soapenv:encodingStyle");

    if (master->xmlnamespace.get())
        xmlWriter.outputXmlns("xmlns", master->xmlnamespace.get());

    outputRows(xmlWriter, inputRows);

    xmlWriter.outputEndNested(master->service);
}

//Create SOAP body of http request
void CWSCHelperThread::createXmlSoapQuery(IXmlWriterExt &xmlWriter, ConstPointerArray &inputRows)
{
    xmlWriter.outputQuoted("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xmlWriter.outputBeginNested("soap:Envelope", true);

    xmlWriter.outputXmlns("soap", "http://schemas.xmlsoap.org/soap/envelope/");
    if (master->flags & SOAPFencoding)
    {   //SOAP RPC/encoded.  'Encoded' usage includes type encoding 
        xmlWriter.outputXmlns("xsd", "http://www.w3.org/2001/XMLSchema");
        xmlWriter.outputXmlns("xsi", "http://www.w3.org/2001/XMLSchema-instance");
    }

    xmlWriter.outputBeginNested("soap:Body", true);

    if (master->flags & SOAPFliteral  ||  master->flags & SOAPFencoding)
        createSOAPliteralOrEncodedQuery(xmlWriter, inputRows);
    else
        createESPQuery(xmlWriter, inputRows);

    xmlWriter.outputEndNested("soap:Body");
    xmlWriter.outputEndNested("soap:Envelope");
}

class CWSUserLogCompletor
{
public:
    CWSUserLogCompletor(CWSCHelper &wshelper, ConstPointerArray &rows) : helper(wshelper), inputRows(rows) {}
    ~CWSUserLogCompletor()
    {
        //user log entries were output for the whole batch in the createXXXQuery routine above
        //need to match each with a query complete log entry
        unsigned timeTaken = msTick()-start;
        ForEachItemIn(idx, inputRows)
            helper.addUserLogTailMsg((const byte *)inputRows.item(idx), timeTaken);
    }
private:
    unsigned start = msTick();
    ConstPointerArray &inputRows;
    CWSCHelper &helper;
};

void CWSCHelperThread::processQuery(ConstPointerArray &inputRows)
{
    std::unique_ptr<CWSUserLogCompletor> log;
    if (master->logUserTailMsg)
        log.reset(new CWSUserLogCompletor(*master, inputRows));

    unsigned xmlWriteFlags = 0;
    unsigned xmlReadFlags = ptr_ignoreNameSpaces;
    if (master->flags & SOAPFtrim)
        xmlWriteFlags |= XWFtrim;
    if ((master->flags & SOAPFpreserveSpace) == 0)
        xmlReadFlags |= ptr_ignoreWhiteSpace;

    bool useMarkup = (master->flags & SOAPFmarkupinfo);

    XMLWriterType xmlType = WTStandard;
    if (useMarkup && (master->flags & SOAPFjson))
        xmlType = (master->flags & SOAPFnoroot) ? WTJSONRootless : WTJSONObject;
    else if (master->flags & SOAPFformEncoded)
    {
        xmlType = WTFormUrlEncoded;
        if (master->xpathHints)
        {
            const char *notation = master->xpathHints->queryProp("formnotation");
            if (!isEmptyString(notation))
            {
                //default is "dot" which is of the form a[1].b=22
                if (strieq(notation, "bracket")) //which is of the form a[1][b]=22
                    xmlWriteFlags |= XWFbracketformenc;
                else if (strieq(notation, "esp")) //which is of the form a.1.b=22
                    xmlWriteFlags |= XWFhpccformenc;
            }
        }
    }
    else if (master->flags & SOAPFencoding)
        xmlType = WTEncodingData64;

    Owned<IXmlWriterExt> xmlWriter = createIXmlWriterExt(xmlWriteFlags, 0, nullptr, xmlType);
    if (useMarkup || xmlType == WTFormUrlEncoded)
        createHttpPostQuery(*xmlWriter, inputRows, false, false);
    else if (master->wscType == STsoap )
        createXmlSoapQuery(*xmlWriter, inputRows);
    xmlWriter->finalize();

    Owned<IWSCAsyncFor> casyncfor = createWSCAsyncFor(master, *xmlWriter, inputRows, (PTreeReaderOptions) xmlReadFlags);
    casyncfor->For(master->numUrls, master->numUrlThreads,false,true); // shuffle URLS for poormans load balance
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
    if (!globalFeaturesInitDone)
        initGlobalFeatures();
    return new CWSCHelper(r, outputAllocator, authToken, wscMode, clientCert, logctx, roxieAbortMonitor, STsoap);
}

IWSCHelper * createHttpCallHelper(IWSCRowProvider *r, IEngineRowAllocator * outputAllocator, const char *authToken, WSCMode wscMode, ClientCertificate *clientCert, const IContextLogger &logctx, IRoxieAbortMonitor * roxieAbortMonitor)
{
    if (!globalFeaturesInitDone)
        initGlobalFeatures();
    return new CWSCHelper(r, outputAllocator, authToken, wscMode, clientCert, logctx, roxieAbortMonitor, SThttp);
}

//=================================================================================================

const char * queryAlternativeHeader(const char * header)
{
    if (strieq(header, kGlobalIdHttpHeaderName))
        return kLegacyGlobalIdHttpHeaderName;
    if (strieq(header, kLegacyGlobalIdHttpHeaderName))
        return kGlobalIdHttpHeaderName;
    if (strieq(header, kCallerIdHttpHeaderName))
        return kLegacyCallerIdHttpHeaderName;
    if (strieq(header, kLegacyCallerIdHttpHeaderName))
        return kCallerIdHttpHeaderName;
    return nullptr;
}

bool httpHeaderBlockContainsHeader(const char *httpheaders, const char *header)
{
    if (!httpheaders || !*httpheaders)
        return false;
    VStringBuffer match("\n%s:", header);
    const char *matchStart = match.str()+1;
    if (!strnicmp(httpheaders, matchStart, strlen(matchStart)))
        return true;
    if (stristr(httpheaders, match))
        return true;
    return false;
}

bool getHTTPHeader(const char *httpheaders, const char *header, StringBuffer& value)
{
    if (!httpheaders || !*httpheaders || !header || !*header)
        return false;

    const char* pHeader = stristr(httpheaders, header);
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
                socket->readtms(buf, len, len, count, timeoutMS);
                currLen = curPosn = 0;
            }
            else
            {   //only some is in buffer, read rest from socket
                size32_t avail = currLen - curPosn;
                memcpy(buf, (buffer + curPosn), avail);
                count = avail;
                size32_t bytesRead;
                socket->readtms(buf+avail, len-avail, len-avail, bytesRead, timeoutMS);
                count += bytesRead;
                currLen = curPosn = 0;
            }
            return count;
        }
    };

private:
    CWSCHelper * master;
    ConstPointerArray &inputRows;
    IXmlWriterExt &xmlWriter;
    IEngineRowAllocator * outputAllocator;
    CriticalSection processExceptionCrit;
    StringBuffer responsePath;
    Owned<CSocketDataProvider> dataProvider;
    PTreeReaderOptions options;
    unsigned remainingMS;
    CCycleTimer mTimer;

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
            master->logctx.CTXLOG("Content decoded. Original " CONTENT_LENGTH " %d", contentLength);
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
                master->logctx.mCTXLOG("%s: request(%s)", master->wscCallTypeText(), request.str());
            else
                master->logctx.mCTXLOG("%s: request(%s), content encoded.", master->wscCallTypeText(), request.str());
        }
    }

    void createHttpRequest(StringBuffer &request, const Url &url, const IProperties * traceHeaders)
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
                master->logctx.mCTXLOG("%s: Adding HTTP Headers(%s)", master->wscCallTypeText(), httpheaders);
            request.append(httpheaders);
        }

        if (!httpHeaderBlockContainsHeader(httpheaders, "Authorization"))
        {
            if (url.userPasswordPair.length() > 0)
            {
                StringBuffer authToken;
                JBASE64_Encode(url.userPasswordPair.str(), url.userPasswordPair.length(), authToken, false);
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
        if (traceHeaders)
        {
            Owned<IPropertyIterator> iter = traceHeaders->getIterator();
            ForEach(*iter)
            {
                const char * key = iter->getPropKey();
                bool hasHeader = httpHeaderBlockContainsHeader(httpheaders, key);
                if (!hasHeader)
                {
                    //If this header is http-global-id, check that global-id hasn't been explicitly added already
                    const char * altHeader = queryAlternativeHeader(key);
                    bool hasAltHeader = altHeader && httpHeaderBlockContainsHeader(httpheaders, altHeader);
                    if (!hasAltHeader)
                    {
                        const char * value = iter->queryPropValue();
                        if (!isEmptyString(value))
                            request.append(key).append(": ").append(value).append("\r\n");
                    }
                }
            }
        }

        if (master->wscType == STsoap)
        {
            if (master->soapaction.get())
                request.append("SOAPAction: ").append(master->soapaction.get()).append("\r\n");
            if (master->httpHeaders.isEmpty() && master->httpHeaderName.get() && master->httpHeaderValue.get())
            {
                //backward compatibility
                StringBuffer hdr(master->httpHeaderName.get());
                hdr.append(": ").append(master->httpHeaderValue);
                if (soapTraceLevel > 6 || master->logXML)
                    master->logctx.mCTXLOG("SOAPCALL: Adding HTTP Header(%s)", hdr.str());
                request.append(hdr.append("\r\n"));
            }
            if (!httpHeaderBlockContainsHeader(httpheaders, "Content-Type"))
            {
                if ((master->flags & SOAPFmarkupinfo) && (master->flags & SOAPFjson))
                    request.append("Content-Type: application/json\r\n");
                else if (master->flags & SOAPFformEncoded)
                    request.append("Content-Type: application/x-www-form-urlencoded\r\n");
                else
                    request.append("Content-Type: text/xml\r\n");
            }
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

    int readHttpResponse(StringBuffer &response, ISocket *socket, bool &keepAlive, StringBuffer &contentType)
    {
        // Read the POST reply
        // not doesn't *assume* is valid HTTP post format but if it is takes advantage of
        response.clear();
        unsigned bytesRead;
        MemoryAttr buf;
        char *buffer=(char *)buf.allocate(WSCBUFFERSIZE+1);
        int rval = 200;
        keepAlive = false;

        // first read header
        size32_t payloadofs = 0;
        size32_t payloadsize = 0;
        StringBuffer dbgheader, contentEncoding;
        bool chunked = false;
        size32_t read = 0;
        do {
            checkTimeLimitExceeded(&remainingMS);
            checkRoxieAbortMonitor(master->roxieAbortMonitor);
            readtmsAllowClose(socket, buffer+read, 1, WSCBUFFERSIZE-read, bytesRead, MIN(master->timeoutMS, remainingMS));
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
                    if (!stristr(buffer,"Transfer-Encoding: chunked"))
                    {
                        chunked = false;
                        s = stristr(buffer,CONTENT_LENGTH);
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
                        while (ch != '\n')//consume chunk-extension and CRLF
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
                            while(ch != '\n')//consume chunk-extension and CRLF
                                dataProvider->getBytes(&ch, 1);
                        }
                        //to support persistent connections we need to consume all trailing bytes for each message
                        dataProvider->getBytes(&ch, 1);
                        //the standard allows trailing HTTP headers to be included at the of the chunked message, the following loop will consume those
                        while ( ch != '\r' ) //if there is content before the CRLF it is a trailing HTTP header
                        {
                            while (ch != '\n')//consume the CRLF of the trailing header
                                dataProvider->getBytes(&ch, 1);
                            dataProvider->getBytes(&ch, 1);
                        }
                        while (ch != '\n') //consume final CRLF
                            dataProvider->getBytes(&ch, 1);
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
                    readtmsAllowClose(socket, response.reserve(payloadsize-read), 1, payloadsize-read, bytesRead, MIN(master->timeoutMS, remainingMS));
                    checkTimeLimitExceeded(&remainingMS);
                    checkRoxieAbortMonitor(master->roxieAbortMonitor);

                    read += bytesRead;
                    response.setLength(read);
                    if (bytesRead==0) {
                        master->logctx.CTXLOG("%s: Warning %sHTTP response terminated prematurely", getWsCallTypeName(master->wscType),chunked?"CHUNKED ":"");
                        break; // oops  looks likesocket closed early
                    }
                }
            }
            else {
                for (;;) {
                    checkTimeLimitExceeded(&remainingMS);
                    checkRoxieAbortMonitor(master->roxieAbortMonitor);
                    readtmsAllowClose(socket, buffer, 1, WSCBUFFERSIZE, bytesRead, MIN(master->timeoutMS, remainingMS));
                    checkTimeLimitExceeded(&remainingMS);
                    checkRoxieAbortMonitor(master->roxieAbortMonitor);

                    if (bytesRead==0)
                        break;              // rely on socket closing to terminate message
                    response.append(bytesRead,buffer);
                }
            }
        }
        if (rval == 200 && response.length() > 0)
        {
            keepAlive = checkKeepAlive(dbgheader);
            getHTTPHeader(dbgheader, CONTENT_TYPE, contentType);
        }
        if (checkContentDecoding(dbgheader, response, contentEncoding))
            decodeContent(contentEncoding.str(), response);
        if (soapTraceLevel > 6 || master->logXML)
            master->logctx.mCTXLOG("%s: LEN=%d %sresponse(%s%s)", getWsCallTypeName(master->wscType),response.length(),chunked?"CHUNKED ":"", dbgheader.str(), response.str());
        else if (soapTraceLevel > 8)
            master->logctx.mCTXLOG("%s: LEN=%d %sresponse(%s)", getWsCallTypeName(master->wscType),response.length(),chunked?"CHUNKED ":"", response.str()); // not sure this is that useful but...
        return rval;
    }

    inline const char *queryXpathHint(const char *name)
    {
        if (!master->xpathHints)
            return nullptr;
        return master->xpathHints->queryProp(name);
    }

    void processEspResponse(Url &url, StringBuffer &response, ColumnProvider * meta)
    {
        StringBuffer path(responsePath);
        path.append("/Results/Result/");
        const char *tail = nullptr;
        const char *excPath = nullptr;
        if (master->rowTransformer && master->inputpath.get())
        {
            StringBuffer ipath;
            ipath.append("/Envelope/Body/").append(master->inputpath.get());
            tail = queryXpathHint("rowpath");
            if(!tail && (ipath.length() >= path.length()) && (0 == memcmp(ipath.str(), path.str(), path.length())))
                tail = ipath.str() + path.length();
            else
                path.clear().append(ipath);
            excPath = queryXpathHint("excpath");
        }
        else
            tail = "Dataset/Row";

        CMatchCB matchCB(*this, url, tail, meta, excPath, CBExceptionPathExc);
        Owned<IXMLParse> xmlParser = createXMLParse((const void *)response.str(), (unsigned)response.length(), path.str(), matchCB, options, (master->flags&SOAPFusescontents)!=0);
        while (xmlParser->next());
    }
    void processLiteralResponse(Url &url, StringBuffer &response, ColumnProvider * meta)
    {
        StringBuffer path("/Envelope/Body/");
        const char *tail = nullptr;
        const char *excPath = nullptr;
        if(master->rowTransformer && master->inputpath.get())
        {
            path.append(master->inputpath.get());
            tail = queryXpathHint("rowpath");
            excPath = queryXpathHint("excpath");
        }
        CMatchCB matchCB(*this, url, tail, meta, excPath, CBExceptionPathExc);
        Owned<IXMLParse> xmlParser = createXMLParse((const void *)response.str(), (unsigned)response.length(), path.str(), matchCB, options, (master->flags&SOAPFusescontents)!=0);
        while (xmlParser->next());
    }

    void processHttpResponse(Url &url, StringBuffer &response, ColumnProvider * meta, const char *contentType)
    {
        const char *path = nullptr;
        const char *tail = nullptr;
        const char *excPath = nullptr;
        if(master->rowTransformer && master->inputpath.get())
        {
            path = master->inputpath.get();
            tail = queryXpathHint("rowpath");
            excPath = queryXpathHint("excpath");
        }
        CMatchCB matchCB(*this, url, tail, meta, excPath, CBExceptionPathExc | CBExceptionPathExcs | CBExceptionPathExcsExc);
        Owned<IXMLParse> xmlParser;
        if (strieq(master->acceptType.str(), "application/json") || (master->flags & SOAPFjson))
            xmlParser.setown(createJSONParse((const void *)response.str(), (unsigned)response.length(), path, matchCB, options, (master->flags&SOAPFusescontents)!=0, true));
        else if (master->flags & SOAPFformEncoded)
        {
            //currently only use content-type for form url encoded formats,.. to avoid any backward compatability issues for existing calls
            //  users can control what the server returns by setting the "Accepts" header using the HTTPHEADER option on HTTPCALL and SOAPCALL
            if (strnicmp(contentType, "application/json", 16)==0)
                xmlParser.setown(createJSONParse((const void *)response.str(), (unsigned)response.length(), path, matchCB, options, (master->flags&SOAPFusescontents)!=0, true));
            else
                xmlParser.setown(createXMLParse((const void *)response.str(), (unsigned)response.length(), path, matchCB, options, (master->flags&SOAPFusescontents)!=0));
        }
        else
            xmlParser.setown(createXMLParse((const void *)response.str(), (unsigned)response.length(), path, matchCB, options, (master->flags&SOAPFusescontents)!=0));
        while (xmlParser->next());
    }

    void processResponse(Url &url, StringBuffer &response, ColumnProvider * meta, const char *contentType)
    {
        if (master->wscType == SThttp || master->flags & (SOAPFmarkupinfo | SOAPFformEncoded))
            processHttpResponse(url, response, meta, contentType);
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
            throw MakeStringException(TIMELIMIT_EXCEEDED, "%s TIMELIMIT(%ums) exceeded", getWsCallTypeName(master->wscType), master->timeLimitMS);
    }

    inline bool checkKeepAlive(StringBuffer& headers)
    {
        size32_t verOffset = httpVerOffset();
        if (headers.length() <= verOffset)
            return false;
        StringBuffer httpVer;
        const char* ptr = headers.str() + verOffset;
        while (*ptr && !isspace(*ptr))
            httpVer.append(*ptr++);
        StringBuffer conHeader;
        getHTTPHeader(ptr, CONNECTION, conHeader);
        return isHttpPersistable(httpVer.str(), conHeader.str());
    }

public:
    CWSCAsyncFor(CWSCHelper * _master, IXmlWriterExt &_xmlWriter, ConstPointerArray &_inputRows, PTreeReaderOptions _options): xmlWriter(_xmlWriter), inputRows(_inputRows), options(_options)
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
        remainingMS = 0;
    }

    ~CWSCAsyncFor()
    {
        master->logctx.noteStatistic(StTimeSoapcall, mTimer.elapsedNs());
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
        unsigned startidx = idx;
        while (!master->aborted)
        {
            bool keepAlive = false;
            bool isReused = false;
            PersistentProtocol proto = PersistentProtocol::ProtoTCP;
            SocketEndpoint ep;
            Owned<ISocket> socket;
            CCycleTimer timer;
            for (;;)
            {
                try
                {
                    checkTimeLimitExceeded(&remainingMS);
                    Url &connUrl = master->proxyUrlArray.empty() ? url : master->proxyUrlArray.item(0);
                    ep.set(connUrl.host.get(), connUrl.port);
                    if (ep.isNull())
                        throw MakeStringException(-1, "Failed to resolve host '%s'", nullText(connUrl.host.get()));

                    checkTimeLimitExceeded(&remainingMS);  // after ep.set which might make a potentially long getaddrinfo lookup ...
                    if (strieq(url.method, "https"))
                        proto = PersistentProtocol::ProtoTLS;

                    bool shouldClose = false;
                    Owned<ISocket> psock = master->usePersistConnections() ? persistentHandler->getAvailable(&ep, &shouldClose, proto) : nullptr;
                    if (psock)
                    {
                        isReused = true;
                        keepAlive = !shouldClose;
                        socket.setown(psock.getClear());
                    }
                    else
                    {
                        isReused = false;
                        keepAlive = true;
                        socket.setown(blacklist->connect(ep, master->logctx, (unsigned)master->maxRetries, master->timeoutMS, master->roxieAbortMonitor, master->rowProvider));
                        if (proto == PersistentProtocol::ProtoTLS)
                        {
#ifdef _USE_OPENSSL
                            Owned<ISecureSocket> ssock = master->createSecureSocket(socket.getClear(), connUrl.host);
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
                                    throw makeStringException(0, err.str());
                                }
                                socket.setown(ssock.getClear());
                            }
#else
                            StringBuffer err;
                            err.append("Failure to establish secure connection to ");
                            connUrl.getUrlString(err);
                            err.append(": OpenSSL disabled in build");
                            throw makeStringException(0, err.str());
#endif

                        }
                    }
                    break;
                }
                catch (IException *e)
                {
                    if (master->timeLimitExceeded)
                    {
                        master->activitySpanScope->recordError(SpanError("Time Limit Exceeded", e->errorCode(), true, true));
                        master->logctx.CTXLOG("%s exiting: time limit (%ums) exceeded", getWsCallTypeName(master->wscType), master->timeLimitMS);
                        processException(url, inputRows, e);
                        return;
                    }

                    if (e->errorCode() == ROXIE_ABORT_EVENT)
                    {
                        master->activitySpanScope->recordError(SpanError("Aborted", e->errorCode(), true, true));
                        StringBuffer s;
                        master->logctx.CTXLOG("%s exiting: Roxie Abort : %s", getWsCallTypeName(master->wscType),e->errorMessage(s).str());
                        throw;
                    }

                    do 
                    {
                        idx++;  // try next socket not blacklisted
                        if (idx==master->urlArray.ordinality())
                            idx = 0;
                        if (idx==startidx)
                        {
                            master->activitySpanScope->recordException(e, true, true);
                            StringBuffer s;
                            master->logctx.CTXLOG("Exception %s", e->errorMessage(s).str());
                            processException(url, inputRows, e);
                            return;
                        }
                    } while (blacklist->blacklisted(url.port, url.host));

                    master->activitySpanScope->recordException(e, false, false); //Record the exception, but don't set failure
                }
            }
            try
            {
                checkTimeLimitExceeded(&remainingMS);
                checkRoxieAbortMonitor(master->roxieAbortMonitor);
                OwnedSpanScope socketOperationSpan = master->activitySpanScope->createClientSpan("Socket Write");
                setSpanURLAttributes(socketOperationSpan, url);

                Owned<IProperties> traceHeaders = ::getClientHeaders(socketOperationSpan);
                createHttpRequest(request, url, traceHeaders);

                socket->write(request.str(), request.length());

                if (soapTraceLevel > 4)
                    master->logctx.CTXLOG("%s: sent request (%s) to %s:%d", getWsCallTypeName(master->wscType),master->service.str(), url.host.str(), url.port);
                checkTimeLimitExceeded(&remainingMS);
                checkRoxieAbortMonitor(master->roxieAbortMonitor);

                bool keepAlive2;
                StringBuffer contentType;
                int rval = readHttpResponse(response, socket, keepAlive2, contentType);
                socketOperationSpan->setSpanAttribute("http.response.status_code", (int64_t)rval);
                keepAlive = keepAlive && keepAlive2;

                if (soapTraceLevel > 4)
                    master->logctx.CTXLOG("%s: received response (%s) from %s:%d", getWsCallTypeName(master->wscType),master->service.str(), url.host.str(), url.port);

                if (rval != 200)
                {
                    socketOperationSpan->setSpanStatusSuccess(false);
                    if (rval == 503)
                    {
                        socketOperationSpan->recordError(SpanError("Server Too Busy", 1001, true, true));
                        throw new ReceivedRoxieException(1001, "Server Too Busy");
                    }

                    StringBuffer text;
                    text.appendf("HTTP error (%d) in processQuery",rval);
                    rtlAddExceptionTag(text, "soapresponse", response.str());
                    socketOperationSpan->recordError(SpanError(text.str(), -1, true, true));
                    throw MakeStringExceptionDirect(-1, text.str());
                }
                if (response.length() == 0)
                {
                    socketOperationSpan->recordError(SpanError("Zero length response in processQuery", -1, true, true));
                    throw MakeStringException(-1, "Zero length response in processQuery");
                }
                checkTimeLimitExceeded(&remainingMS);
                ColumnProvider * meta = (ColumnProvider*)CreateColumnProvider((unsigned)nanoToMilli(timer.elapsedNs()), master->flags&SOAPFencoding?true:false);
                processResponse(url, response, meta, contentType);
                delete meta;

                if (master->usePersistConnections())
                {
                    if (isReused)
                        persistentHandler->doneUsing(socket, keepAlive, master->getPersistMaxRequests());
                    else if (keepAlive)
                        persistentHandler->add(socket, &ep, proto);
                }

                socketOperationSpan->setSpanStatusSuccess(true);
                break;
            }
            catch (IReceivedRoxieException *e)
            {
                if (master->usePersistConnections() && isReused)
                    persistentHandler->doneUsing(socket, false);
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
                    master->activitySpanScope->recordException(e, true, true);
                    if (e->errorRow())
                        processException(url, e->errorRow(), e);
                    else
                        processException(url, inputRows, e);
                    break;
                }
            }
            catch (IException *e)
            {
                if (master->usePersistConnections() && isReused)
                    persistentHandler->doneUsing(socket, false);
                if (master->timeLimitExceeded)
                {
                    VStringBuffer msg("%s exiting: time limit (%ums) exceeded", getWsCallTypeName(master->wscType), master->timeLimitMS);
                    master->logctx.CTXLOG("%s", msg.str());
                    master->activitySpanScope->recordError(SpanError(msg.str(), e->errorCode(), true, true));
                    processException(url, inputRows, e);
                    break;
                }

                if (e->errorCode() == ROXIE_ABORT_EVENT)
                {
                    StringBuffer s;
                    master->logctx.CTXLOG("%s exiting: Roxie Abort : %s", getWsCallTypeName(master->wscType),e->errorMessage(s).str());
                    master->activitySpanScope->recordError(SpanError("Aborted", e->errorCode(), true, true));
                    throw;
                }

                // other IException ... retry up to maxRetries
                StringBuffer s;
                master->logctx.CTXLOG("Exception %s", e->errorMessage(s).str());

                if (numRetries >= master->maxRetries)
                {
                    // error affects all inputRows
                    VStringBuffer msg("Exiting: maxRetries %d exceeded", master->maxRetries);
                    master->logctx.CTXLOG("%s", msg.str());
                    master->activitySpanScope->recordError(SpanError(msg.str(), e->errorCode(), true, true));
                    processException(url, inputRows, e);
                    break;
                }
                numRetries++;
                master->logctx.CTXLOG("Retrying: attempt %d of %d", numRetries, master->maxRetries);
                master->activitySpanScope->recordException(e, false, false);
                e->Release();
            }
            catch (std::exception & es)
            {
                if (master->usePersistConnections() && isReused)
                    persistentHandler->doneUsing(socket, false);
                if(dynamic_cast<std::bad_alloc *>(&es))
                {
                    master->activitySpanScope->recordError("std::exception: out of memory (std::bad_alloc) in CWSCAsyncFor processQuery");
                    throw MakeStringException(-1, "std::exception: out of memory (std::bad_alloc) in CWSCAsyncFor processQuery");
                }

                master->activitySpanScope->recordError(es.what());
                throw MakeStringException(-1, "std::exception: standard library exception (%s) in CWSCAsyncFor processQuery",es.what());
            }
            catch (...)
            {
                if (master->usePersistConnections() && isReused)
                    persistentHandler->doneUsing(socket, false);

                master->activitySpanScope->recordError(SpanError("Unknown exception in processQuery", -1, true, true));
                throw MakeStringException(-1, "Unknown exception in processQuery");
            }
        }
    }
    inline virtual const char *getResponsePath() { return responsePath; }
    inline virtual ConstPointerArray & getInputRows() { return inputRows; }
    inline virtual CWSCHelper * getMaster() { return master; }
    inline virtual IEngineRowAllocator * getOutputAllocator() { return outputAllocator; }
};

IWSCAsyncFor * createWSCAsyncFor(CWSCHelper * _master, IXmlWriterExt &_xmlWriter, ConstPointerArray &_inputRows, PTreeReaderOptions _options)
{
    return new CWSCAsyncFor(_master, _xmlWriter, _inputRows, _options);
}
