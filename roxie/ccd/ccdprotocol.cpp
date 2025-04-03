/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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
#include "jlib.hpp"
#include "jthread.hpp"

#include "rtlcommon.hpp"

#include "roxie.hpp"
#include "ccd.hpp"
#include "roxiehelper.hpp"
#include "ccdprotocol.hpp"
#include "securesocket.hpp"

//================================================================================================================================

IHpccProtocolListener *createProtocolListener(const char *protocol, IHpccProtocolMsgSink *sink, unsigned port, unsigned listenQueue, const ISyncedPropertyTree *tlsConfig);

class CHpccProtocolPlugin : implements IHpccProtocolPlugin, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CHpccProtocolPlugin(IHpccProtocolPluginContext &ctx)
    {
        targetNames.appendListUniq(ctx.ctxQueryProp("@querySets"), ",");
        targetAliases.setown(createProperties());
        StringArray tempList;
        tempList.appendListUniq(ctx.ctxQueryProp("@targetAliases"), ",");
        ForEachItemIn(i, tempList)
        {
            const char *alias = tempList.item(i);
            const char *eq = strchr(alias, '=');
            if (eq)
            {
                StringAttr name(alias, eq-alias);
                if (!targetNames.contains(name))
                    targetAliases->setProp(name.str(), ++eq);
            }
        }

        maxBlockSize = ctx.ctxGetPropInt("@maxBlockSize", 10000000);
        defaultXmlReadFlags = ctx.ctxGetPropBool("@defaultStripLeadingWhitespace", true) ? ptr_ignoreWhiteSpace : ptr_none;
        trapTooManyActiveQueries = ctx.ctxGetPropBool("@trapTooManyActiveQueries", true);
        numRequestArrayThreads = ctx.ctxGetPropInt("@requestArrayThreads", 5);
        maxHttpConnectionRequests = ctx.ctxGetPropInt("@maxHttpConnectionRequests", 0);
        maxHttpKeepAliveWait = ctx.ctxGetPropInt("@maxHttpKeepAliveWait", 5000); // In milliseconds
    }
    IHpccProtocolListener *createListener(const char *protocol, IHpccProtocolMsgSink *sink, unsigned port, unsigned listenQueue, const char *config, const ISyncedPropertyTree *tlsConfig)
    {
        return createProtocolListener(protocol, sink, port, listenQueue, tlsConfig);
    }
public:
    StringArray targetNames;
    Owned<IProperties> targetAliases;
    PTreeReaderOptions defaultXmlReadFlags;
    unsigned maxBlockSize;
    unsigned numRequestArrayThreads;
    unsigned maxHttpConnectionRequests = 0;
    unsigned maxHttpKeepAliveWait = 5000;
    bool trapTooManyActiveQueries;
};

Owned<CHpccProtocolPlugin> global;

class ProtocolListener : public Thread, implements IHpccProtocolListener, implements IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE_USING(Thread);
    ProtocolListener(IHpccProtocolMsgSink *_sink) : Thread("RoxieListener")
    {
        running = false;
        sink.setown(dynamic_cast<IHpccNativeProtocolMsgSink*>(_sink));
    }
    virtual IHpccProtocolMsgSink *queryMsgSink()
    {
        return sink;
    }
    static void updateAffinity()
    {
#ifdef CPU_ZERO
        if (sched_getaffinity(0, sizeof(cpu_set_t), &cpuMask))
        {
            if (traceLevel)
                DBGLOG("Unable to get CPU affinity - thread affinity settings will be ignored");
            cpuCores = 0;
            lastCore = 0;
            CPU_ZERO(&cpuMask);
        }
        else
        {
#if __GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 6)
            cpuCores = CPU_COUNT(&cpuMask);
#else
            cpuCores = 0;
            unsigned setSize = CPU_SETSIZE;
            while (setSize--)
            {
                if (CPU_ISSET(setSize, &cpuMask))
                    ++cpuCores;
            }
#endif /* GLIBC */
            if (traceLevel)
                traceAffinitySettings(&cpuMask);
        }
#endif
    }

    virtual void start() override
    {
        // Note we allow a few additional threads than requested - these are the threads that return "Too many active queries" responses
        pool.setown(createThreadPool("RoxieSocketWorkerPool", this, false, nullptr, sink->getPoolSize()+5, INFINITE));
        assertex(!running);
        Thread::start(false);
        started.wait();
    }

    virtual ISocket *createSecureSocket (ISocket *base) const = 0;

    virtual bool stop()
    {
        if (running)
        {
            running = false;
            join();
            Release();
        }
        return pool->joinAll(false);
    }

    void setThreadAffinity(int numCores)
    {
#ifdef CPU_ZERO
        // Note - strictly speaking not threadsafe but any race conditions are (a) unlikely and (b) harmless
        if (cpuCores)
        {
            if (numCores > 0 && numCores < (int) cpuCores)
            {
                cpu_set_t threadMask;
                CPU_ZERO(&threadMask);
                unsigned cores = 0;
                unsigned offset = lastCore;
                unsigned core;
                for (core = 0; core < CPU_SETSIZE; core++)
                {
                    unsigned useCore = (core + offset) % CPU_SETSIZE;
                    if (CPU_ISSET(useCore, &cpuMask))
                    {
                        CPU_SET(useCore, &threadMask);
                        cores++;
                        if ((int) cores == numCores)
                        {
                            lastCore = useCore+1;
                            break;
                        }
                    }
                }
                if (doTrace(traceAffinity))
                    traceAffinitySettings(&threadMask);
                pthread_setaffinity_np(GetCurrentThreadId(), sizeof(cpu_set_t), &threadMask);
            }
            else
            {
                if (doTrace(traceAffinity))
                    traceAffinitySettings(&cpuMask);
                pthread_setaffinity_np(GetCurrentThreadId(), sizeof(cpu_set_t), &cpuMask);
            }
        }
#endif
    }

protected:
    std::atomic<bool> running;
    Semaphore started;
    Owned<IThreadPool> pool;

    Linked<IHpccNativeProtocolMsgSink> sink;

#ifdef CPU_ZERO
    static cpu_set_t cpuMask;
    static unsigned cpuCores;
    static unsigned lastCore;

private:
    static void traceAffinitySettings(cpu_set_t *mask)
    {
        StringBuffer trace;
        for (unsigned core = 0; core < CPU_SETSIZE; core++)
        {
            if (CPU_ISSET(core, mask))
                trace.appendf(",%d", core);
        }
        if (trace.length())
            DBGLOG("Process affinity is set to use core(s) %s", trace.str()+1);
    }
#endif

};

#ifdef CPU_ZERO
cpu_set_t ProtocolListener::cpuMask;
unsigned ProtocolListener::cpuCores;
unsigned ProtocolListener::lastCore;
#endif

class ProtocolSocketListener : public ProtocolListener
{
    unsigned port;
    unsigned listenQueue;
    Owned<ISocket> socket;
    SocketEndpoint ep;
    StringAttr protocol;
    Owned<ISecureSocketContext> secureContext;
    bool isSSL = false;

public:
    ProtocolSocketListener(IHpccProtocolMsgSink *_sink, unsigned _port, unsigned _listenQueue, const char *_protocol, const ISyncedPropertyTree *_tlsConfig)
      : ProtocolListener(_sink)
    {
        port = _port;
        listenQueue = _listenQueue;
        ep.set(port, queryHostIP());
        protocol.set(_protocol);
        isSSL = streq(protocol.str(), "ssl");

#ifdef _USE_OPENSSL
        if (isSSL)
        {
            secureContext.setown(createSecureSocketContextSynced(_tlsConfig, ServerSocket));
        }
#endif
    }

    IHpccProtocolMsgSink *queryMsgSink()
    {
        return sink.get();
    }

    virtual bool stop()
    {
        if (socket)
            socket->cancel_accept();
        return ProtocolListener::stop();
    }

    virtual void disconnectQueue()
    {
        // This is for dali queues only
    }

    virtual void stopListening()
    {
        // Not threadsafe, but we only call this when generating a core file... what's the worst that can happen?
        try
        {
            DBGLOG("Closing listening socket %d", port);
            socket.clear();
            DBGLOG("Closed listening socket %d", port);
        }
        catch(...)
        {
        }
    }

    virtual void runOnce(const char *query);

    void cleanupSocket(ISocket *sock) const
    {
        shutdownAndCloseNoThrow(sock);
    }

    virtual ISocket *createSecureSocket (ISocket *base) const override
    {
        if (!isSSL)
            return base;
#ifdef _USE_OPENSSL
        Owned<ISecureSocket> ssock;
        try
        {
            ssock.setown(secureContext->createSecureSocket(base));
            int loglevel = SSLogMin;
            if (doTrace(traceSockets))
                loglevel = SSLogMax;
            int status = ssock->secure_accept(loglevel);
            if (status < 0)
            {
                // secure_accept may also DBGLOG() errors ...
                cleanupSocket(ssock);
                return nullptr;
            }
        }
        catch (IException *E)
        {
            StringBuffer s;
            E->errorMessage(s);
            OWARNLOG("%s", s.str());
            E->Release();
            cleanupSocket(ssock);
            ssock.clear();
            return nullptr;
        }
        catch (...)
        {
            OWARNLOG("ProtocolSocketListener failure to establish secure connection");
            cleanupSocket(ssock);
            ssock.clear();
            return nullptr;
        }
        return ssock.getClear();
#else
        OWARNLOG("ProtocolSocketListener failure to establish secure connection: OpenSSL disabled in build");
        return nullptr;
#endif

    }
    virtual int run()
    {
        DBGLOG("ProtocolSocketListener (%d threads) listening to socket on port %d", sink->getPoolSize(), port);
        socket.setown(ISocket::create(port, listenQueue));
        running = true;
        started.signal();
        while (running)
        {
            Owned<ISocket> client = socket->accept(true);
            if (client)
            {
                client->set_linger(-1);
                pool->start(client.getClear());
            }
        }
        DBGLOG("ProtocolSocketListener closed query socket");
        return 0;
    }

    virtual IPooledThread *createNew();

    virtual const SocketEndpoint &queryEndpoint() const
    {
        return ep;
    }

    virtual unsigned queryPort() const
    {
        return port;
    }
};

class ProtocolQueryWorker : public CInterface, implements IPooledThread
{
public:
    IMPLEMENT_IINTERFACE;

    ProtocolQueryWorker(ProtocolListener *_listener) : listener(_listener)
    {
        startNs = nsTick();
        time(&startTime);
    }

    //  interface IPooledThread
    virtual void init(void *) override
    {
        startNs = nsTick();
        time(&startTime);
    }

    virtual bool canReuse() const override
    {
        return true;
    }

    virtual bool stop() override
    {
        if (traceLevel)
            DBGLOG("RoxieQueryWorker thread stop requested with query active - ignoring");
        return true;
    }

protected:
    ProtocolListener *listener;
    stat_type startNs;
    time_t startTime;

};

enum class AdaptiveRoot {NamedArray, RootArray, ExtendArray, FirstRow};

class AdaptiveRESTJsonWriter : public CommonJsonWriter
{
    AdaptiveRoot model;
    unsigned depth = 0;
public:
    AdaptiveRESTJsonWriter(AdaptiveRoot _model, unsigned _flags, unsigned _initialIndent, IXmlStreamFlusher *_flusher) :
        CommonJsonWriter(_flags, _initialIndent, _flusher), model(_model)
    {
    }

    virtual void outputBeginArray(const char *fieldname)
    {
        prepareBeginArray(fieldname);
        if (model == AdaptiveRoot::NamedArray || arrays.length()>1)
            appendJSONName(out, fieldname).append('[');
        else if (model == AdaptiveRoot::RootArray)
            out.append('[');
    }
    void outputEndArray(const char *fieldname)
    {
        arrays.pop();
        checkFormat(false, true, -1);
        if (arrays.length() || (model != AdaptiveRoot::FirstRow && model != AdaptiveRoot::ExtendArray))
            out.append(']');
        const char * sep = (fieldname) ? strchr(fieldname, '/') : NULL;
        while (sep)
        {
            out.append('}');
            sep = strchr(sep+1, '/');
        }
    }
    void outputBeginNested(const char *fieldname, bool nestChildren)
    {
        CommonJsonWriter::outputBeginNested(fieldname, nestChildren);
        if (model == AdaptiveRoot::FirstRow)
            depth++;
    }
    void outputEndNested(const char *fieldname)
    {
        CommonJsonWriter::outputEndNested(fieldname);
        if (model == AdaptiveRoot::FirstRow)
        {
            depth--;
            if (fieldname && streq(fieldname, "Row") && depth==0)
            {
                flush(true);
                flusher = nullptr;
            }
        }
    }
};

class AdaptiveRESTXmlWriter : public CommonXmlWriter
{
    StringAttr tag;
    AdaptiveRoot model = AdaptiveRoot::NamedArray;
    unsigned depth = 0;
public:
    AdaptiveRESTXmlWriter(AdaptiveRoot _model, const char *tagname, unsigned _flags, unsigned _initialIndent, IXmlStreamFlusher *_flusher) :
        CommonXmlWriter(_flags, _initialIndent, _flusher), tag(tagname), model(_model)
    {
    }
    void outputBeginNested(const char *fieldname, bool nestChildren)
    {
        if (model == AdaptiveRoot::FirstRow)
        {
            if (!depth && tag.length())
                fieldname = tag.str();
            depth++;
        }
        CommonXmlWriter::outputBeginNested(fieldname, nestChildren);
    }
    void outputEndNested(const char *fieldname)
    {
        if (model == AdaptiveRoot::FirstRow)
        {
            depth--;
            if (!depth)
            {
                CommonXmlWriter::outputEndNested(tag.length() ? tag.str() : fieldname);
                flush(true);
                flusher = nullptr;
                return;
            }
        }
        CommonXmlWriter::outputEndNested(fieldname);
    }
};

IXmlWriterExt * createAdaptiveRESTWriterExt(AdaptiveRoot model, const char *tagname, unsigned _flags, unsigned _initialIndent, IXmlStreamFlusher *_flusher, XMLWriterType xmlType)
{
    if (xmlType==WTJSONRootless)
        return new AdaptiveRESTJsonWriter(model, _flags, _initialIndent, _flusher);
    return new AdaptiveRESTXmlWriter(model, tagname, _flags, _initialIndent, _flusher);
}

//================================================================================================================

class CHpccNativeResultsWriter : implements IHpccNativeProtocolResultsWriter, public CInterface
{
protected:
    SafeSocket *client;
    CriticalSection resultsCrit;
    IPointerArrayOf<FlushingStringBuffer> resultMap;

    StringAttr queryName;
    StringAttr tagName;
    StringAttr resultFilter;

    const IContextLogger &logctx;
    Owned<FlushingStringBuffer> probe;
    TextMarkupFormat mlFmt;
    PTreeReaderOptions xmlReadFlags;
    bool isBlocked;
    bool isRaw;
    bool isHTTP;
    bool adaptiveRoot = false;
    bool onlyUseFirstRow = false;

public:
    IMPLEMENT_IINTERFACE;
    CHpccNativeResultsWriter(const char *queryname, SafeSocket *_client, bool _isBlocked, TextMarkupFormat _mlFmt, bool _isRaw, bool _isHTTP, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags) :
        client(_client), queryName(queryname), logctx(_logctx), mlFmt(_mlFmt), xmlReadFlags(_xmlReadFlags), isBlocked(_isBlocked), isRaw(_isRaw), isHTTP(_isHTTP)
    {
    }
    ~CHpccNativeResultsWriter()
    {
    }
    inline void setAdaptiveRoot(){adaptiveRoot = true; client->setAdaptiveRoot(true);}
    inline void setTagName(const char *tag){tagName.set(tag);}
    inline void setOnlyUseFirstRow(){onlyUseFirstRow = true;}
    inline void setResultFilter(const char *_resultFilter){resultFilter.set(_resultFilter);}
    virtual FlushingStringBuffer *queryResult(unsigned sequence, bool extend=false)
    {
        CriticalBlock procedure(resultsCrit);
        while (!resultMap.isItem(sequence))
            resultMap.append(NULL);
        FlushingStringBuffer *result = resultMap.item(sequence);
        if (!result)
        {
            if (mlFmt==MarkupFmt_JSON)
                result = new FlushingJsonBuffer(client, isBlocked, isHTTP, logctx, extend);
            else
                result = new FlushingStringBuffer(client, isBlocked, mlFmt, isRaw, isHTTP, logctx);
            result->isSoap = isHTTP;
            result->queryName.set(queryName);
            resultMap.replace(result, sequence);
        }
        return result;
    }
    virtual FlushingStringBuffer *createFlushingBuffer()
    {
        return new FlushingStringBuffer(client, isBlocked, mlFmt, isRaw, isHTTP, logctx);
    }
    bool checkAdaptiveResult(const char *name)
    {
        if (!adaptiveRoot)
            return false;
        if (!resultFilter || !*resultFilter)
            return true;
        return (streq(resultFilter, name));
    }
    virtual IXmlWriter *addDataset(const char *name, unsigned sequence, const char *elementName, bool &appendRawData, unsigned writeFlags, bool _extend, const IProperties *xmlns)
    {
        FlushingStringBuffer *response = queryResult(sequence, _extend);
        if (response)
        {
            appendRawData = response->isRaw;
            bool adaptive = checkAdaptiveResult(name);
            if (adaptive)
            {
                elementName = nullptr;
                if (response->mlFmt!=MarkupFmt_JSON && !onlyUseFirstRow && tagName.length())
                    elementName = tagName.str();
            }
            response->startDataset(elementName, name, sequence, _extend, xmlns, adaptive);
            if (response->mlFmt==MarkupFmt_XML || response->mlFmt==MarkupFmt_JSON)
            {
                if (response->mlFmt==MarkupFmt_JSON)
                    writeFlags |= XWFnoindent;
                AdaptiveRoot rootType = AdaptiveRoot::ExtendArray;
                if (adaptive)
                {
                    if (onlyUseFirstRow)
                        rootType = AdaptiveRoot::FirstRow;
                    else
                        rootType = AdaptiveRoot::RootArray;
                }

                Owned<IXmlWriter> xmlwriter = createAdaptiveRESTWriterExt(rootType, tagName, writeFlags, 1, response, (response->mlFmt==MarkupFmt_JSON) ? WTJSONRootless : WTStandard);
                xmlwriter->outputBeginArray("Row");
                return xmlwriter.getClear();
            }
        }
        return NULL;
    }
    virtual void finalizeXmlRow(unsigned sequence)
    {
        if (mlFmt==MarkupFmt_XML || mlFmt==MarkupFmt_JSON)
        {
            FlushingStringBuffer *r = queryResult(sequence);
            if (r)
            {
                r->incrementRowCount();
                r->flush(false);
            }
        }
    }
    inline void startScalar(FlushingStringBuffer *r, const char *name, unsigned sequence)
    {
        if (checkAdaptiveResult(name))
        {
            r->startScalar(name, sequence, true, tagName.length() ? tagName.str() : name);
            return;
        }
        r->startScalar(name, sequence);
    }
    virtual void appendRaw(unsigned sequence, unsigned len, const char *data)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
            r->append(len, data);
    }
    virtual void appendRawRow(unsigned sequence, unsigned len, const char *data)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            r->append(len, data);
            r->incrementRowCount();
            r->flush(false);
        }
    }
    virtual void appendSimpleRow(unsigned sequence, const char *str)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
            r->append(str);
    }

    virtual void setResultBool(const char *name, unsigned sequence, bool value)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            startScalar(r, name, sequence);
            if (isRaw)
                r->append(sizeof(value), (char *)&value);
            else
                r->append(value ? "true" : "false");
        }
    }
    virtual void setResultData(const char *name, unsigned sequence, int len, const void * data)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            startScalar(r, name, sequence);
            r->encodeData(data, len);
        }
    }
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            startScalar(r, name, sequence);
            if (isRaw)
                r->append(len, (const char *) data);
            else
                UNIMPLEMENTED;
        }
    }
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            startScalar(r, name, sequence);
            if (isRaw)
                r->append(len, (char *)data);
            else if (mlFmt==MarkupFmt_XML)
            {
                assertex(transformer);
                CommonXmlWriter writer(xmlReadFlags|XWFnoindent, 0, r);
                transformer->toXML(isAll, len, (byte *)data, writer);
            }
            else if (mlFmt==MarkupFmt_JSON)
            {
                assertex(transformer);
                CommonJsonWriter writer(xmlReadFlags|XWFnoindent, 0, r);
                transformer->toXML(isAll, len, (byte *)data, writer);
            }
            else
            {
                assertex(transformer);
                r->append('[');
                if (isAll)
                    r->appendf("*]");
                else
                {
                    SimpleOutputWriter x;
                    transformer->toXML(isAll, len, (const byte *) data, x);
                    r->appendf("%s]", x.str());
                }
            }
        }
    }

    virtual void setResultDecimal(const char *name, unsigned sequence, int len, int precision, bool isSigned, const void *val)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            startScalar(r, name, sequence);
            if (isRaw)
                r->append(len, (char *)val);
            else
            {
                StringBuffer s;
                if (isSigned)
                    outputXmlDecimal(val, len, precision, NULL, s);
                else
                    outputXmlUDecimal(val, len, precision, NULL, s);
                r->append(s);
            }
        }
    }
    virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            if (isRaw)
            {
                startScalar(r, name, sequence);
                r->append(sizeof(value), (char *)&value);
            }
            else
                r->setScalarInt(name, sequence, value, size);
        }
    }

    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            if (isRaw)
            {
                startScalar(r, name, sequence);
                r->append(sizeof(value), (char *)&value);
            }
            else
                r->setScalarUInt(name, sequence, value, size);
        }
    }

    virtual void setResultReal(const char *name, unsigned sequence, double value)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            startScalar(r, name, sequence);
            r->append(value);
        }
    }
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            startScalar(r, name, sequence);
            if (r->isRaw)
            {
                r->append(len, str);
            }
            else
            {
                r->encodeString(str, len);
            }
        }
    }
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            startScalar(r, name, sequence);
            if (r->isRaw)
            {
                r->append(len*2, (const char *) str);
            }
            else
            {
                rtlDataAttr buff;
                unsigned bufflen = 0;
                rtlUnicodeToCodepageX(bufflen, buff.refstr(), len, str, "utf-8");
                r->encodeString(buff.getstr(), bufflen, true); // output as UTF-8
            }
        }
    }
    virtual void setResultVarString(const char * name, unsigned sequence, const char * value)
    {
        setResultString(name, sequence, strlen(value), value);
    }
    virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value)
    {
        setResultUnicode(name, sequence, rtlUnicodeStrlen(value), value);
    }
    virtual void flush()
    {
        ForEachItemIn(seq, resultMap)
        {
            FlushingStringBuffer *result = resultMap.item(seq);
            if (result)
                result->flush(true);
        }
    }
    virtual void finalize(unsigned seqNo, const char *delim, const char *filter, bool *needDelimiter)
    {
        ForEachItemIn(seq, resultMap)
        {
            FlushingStringBuffer *result = resultMap.item(seq);
            if (result && (!filter || !*filter || streq(filter, result->queryResultName())))
            {
                result->flush(true);
                for(;;)
                {
                    size32_t length;
                    void *payload = result->getPayload(length);
                    if (!length)
                        break;
                    if (needDelimiter && *needDelimiter)
                    {
                        StringAttr s(delim); //write() will take ownership of buffer
                        size32_t len = s.length();
                        client->write((void *)s.detach(), len, true);
                        *needDelimiter=false;
                    }
                    client->write(payload, length, true);
                }
                if (delim && needDelimiter)
                    *needDelimiter=true;
            }
        }
    }
};

class CHpccXmlResultsWriter : public CHpccNativeResultsWriter
{
public:
    CHpccXmlResultsWriter(const char *queryname, SafeSocket *_client, bool _isHTTP, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags) :
        CHpccNativeResultsWriter(queryname, _client, false, MarkupFmt_XML, false, _isHTTP, _logctx, _xmlReadFlags)
    {
    }

    virtual void addContent(TextMarkupFormat fmt, const char *content, const char *name)
    {
        StringBuffer xml;
        if (!content || !*content)
            return;
        if (fmt==MarkupFmt_JSON)
        {
            Owned<IPropertyTree> convertPT = createPTreeFromXMLString(content, ipt_fast);
            if (name && *name)
                appendXMLOpenTag(xml, name);
            toXML(convertPT, xml, 0, 0);
            if (name && *name)
                appendXMLCloseTag(xml, name);
        }
    }

    virtual void finalize(unsigned seqNo)
    {
        if (!isHTTP)
        {
            flush();
            return;
        }
        CriticalBlock b(resultsCrit);
        CriticalBlock b1(client->queryCrit());

        StringBuffer responseHead, responseTail;
        responseHead.append("<Results><Result>");
        unsigned len = responseHead.length();
        client->write(responseHead.detach(), len, true);

        ForEachItemIn(seq, resultMap)
        {
            FlushingStringBuffer *result = resultMap.item(seq);
            if (result)
            {
                result->flush(true);
                for(;;)
                {
                    size32_t length;
                    void *payload = result->getPayload(length);
                    if (!length)
                        break;
                    client->write(payload, length, true);
                }
            }
        }

        responseTail.append("</Result></Results>");
        len = responseTail.length();
        client->write(responseTail.detach(), len, true);
    }
};

class CHpccJsonResultsWriter : public CHpccNativeResultsWriter
{
public:
    CHpccJsonResultsWriter(const char *queryname, SafeSocket *_client, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags) :
        CHpccNativeResultsWriter(queryname, _client, false, MarkupFmt_JSON, false, true, _logctx, _xmlReadFlags)
    {
    }

    virtual FlushingStringBuffer *createFlushingBuffer()
    {
        return new FlushingJsonBuffer(client, isBlocked, isHTTP, logctx);
    }

    virtual void finalize(unsigned seqNo)
    {
        CriticalBlock b(resultsCrit);
        CriticalBlock b1(client->queryCrit());

        StringBuffer responseHead, responseTail;
        appendJSONName(responseHead, "Results").append(" {");
        unsigned len = responseHead.length();
        client->write(responseHead.detach(), len, true);

        bool needDelimiter = false;
        ForEachItemIn(seq, resultMap)
        {
            FlushingStringBuffer *result = resultMap.item(seq);
            if (result)
            {
               result->flush(true);
               for(;;)
               {
                   size32_t length;
                   void *payload = result->getPayload(length);
                   if (!length)
                       break;
                   if (needDelimiter)
                   {
                       StringAttr s(","); //write() will take ownership of buffer
                       size32_t len = s.length();
                       client->write((void *)s.detach(), len, true);
                       needDelimiter=false;
                   }
                   client->write(payload, length, true);
               }
               needDelimiter=true;
            }
        }

        responseTail.append("}");
        len = responseTail.length();
        client->write(responseTail.detach(), len, true);
    }

};

class CHpccNativeProtocolResponse : implements IHpccNativeProtocolResponse, public CInterface
{
protected:
    SafeSocket *client;
    StringAttr queryName;
    StringArray resultFilter;
    StringBuffer rootTag;
    const IContextLogger &logctx;
    TextMarkupFormat mlFmt;
    PTreeReaderOptions xmlReadFlags;
    Owned<CHpccNativeResultsWriter> results; //hpcc results section
    IPointerArrayOf<FlushingStringBuffer> contentsMap; //other sections
    CriticalSection contentsCrit;
    unsigned protocolFlags;
    bool isHTTP;

public:
    IMPLEMENT_IINTERFACE;
    CHpccNativeProtocolResponse(const char *queryname, SafeSocket *_client, TextMarkupFormat _mlFmt, unsigned flags, bool _isHTTP, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags, const char *_resultFilterString, const char *_rootTag) :
        client(_client), queryName(queryname), rootTag(_rootTag), logctx(_logctx), mlFmt(_mlFmt), xmlReadFlags(_xmlReadFlags), protocolFlags(flags), isHTTP(_isHTTP)
    {
        resultFilter.appendList(_resultFilterString, ".");
        if (!rootTag.length() && resultFilter.length())
            rootTag.set(resultFilter.item(0)).replace(' ', '_');
    }
    ~CHpccNativeProtocolResponse()
    {
    }
    virtual unsigned getFlags()
    {
        return protocolFlags;
    }
    inline bool getIsRaw()
    {
        return (protocolFlags & HPCC_PROTOCOL_NATIVE_RAW);
    }
    inline bool getIsBlocked()
    {
        return (protocolFlags & HPCC_PROTOCOL_BLOCKED);
    }
    inline bool getTrim()
    {
        return (protocolFlags & HPCC_PROTOCOL_TRIM);
    }
    virtual FlushingStringBuffer *queryAppendContentBuffer()
    {
        CriticalBlock procedure(contentsCrit);
        FlushingStringBuffer *content;
        if (mlFmt==MarkupFmt_JSON)
            content = new FlushingJsonBuffer(client, getIsBlocked(), isHTTP, logctx);
        else
            content = new FlushingStringBuffer(client, getIsBlocked(), mlFmt, getIsRaw(), isHTTP, logctx);
        content->isSoap = isHTTP;
        content->queryName.set(queryName);
        if (!isHTTP)
            content->startBlock();
        contentsMap.append(content);
        return content;
    }

    virtual IHpccProtocolResultsWriter *queryHpccResultsSection()
    {
        if (!results)
        {
            results.setown(new CHpccNativeResultsWriter(queryName, client, getIsBlocked(), mlFmt, getIsRaw(), isHTTP, logctx, xmlReadFlags));
            if (rootTag.length())
                results->setTagName(rootTag);
            if (resultFilter.length())
            {
                results->setAdaptiveRoot();
                results->setResultFilter(resultFilter.item(0));
            }
            if (resultFilter.isItem(1) && strieq("row", resultFilter.item(1)))
                results->setOnlyUseFirstRow();
        }
        return results;
    }

    virtual void appendContent(TextMarkupFormat mlFmt, const char *content, const char *name=NULL)
    {
        throwUnexpected();
    }
    virtual IXmlWriter *writeAppendContent(const char *name = NULL)
    {
        throwUnexpected();
    }
    virtual void finalize(unsigned seqNo)
    {
        flush();
        if (!isHTTP)
        {
            unsigned replyLen = 0;
            client->write(&replyLen, sizeof(replyLen));
        }
    }
    virtual bool checkConnection()
    {
        if (client)
            return client->checkConnection();
        else
            return true;
    }
    virtual void sendHeartBeat()
    {
        if (client)
            client->sendHeartBeat(logctx);
    }
    virtual SafeSocket *querySafeSocket()
    {
        return client;
    }
    virtual void flush()
    {
        if (results)
            results->flush();
        ForEachItemIn(i, contentsMap)
            contentsMap.item(i)->flush(true);
    }
};

class CHpccJsonResponse : public CHpccNativeProtocolResponse
{
private:
    bool needDelimiter = false;
public:
    CHpccJsonResponse(const char *queryname, SafeSocket *_client, unsigned flags, bool _isHttp, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags, const char *_resultFilter, const char *_rootTag) :
        CHpccNativeProtocolResponse(queryname, _client, MarkupFmt_JSON, flags, _isHttp, _logctx, _xmlReadFlags, _resultFilter, _rootTag)
    {
    }

    virtual IHpccProtocolResultsWriter *getHpccResultsSection()
    {
        if (!results)
            results.setown(new CHpccJsonResultsWriter(queryName, client, logctx, xmlReadFlags));
        return results;
    }


    virtual void appendContent(TextMarkupFormat mlFmt, const char *content, const char *name=NULL)
    {
        if (mlFmt!=MarkupFmt_XML && mlFmt!=MarkupFmt_JSON)
            return;
        StringBuffer json;
        if (mlFmt==MarkupFmt_XML)
        {
            Owned<IPropertyTree> convertPT = createPTreeFromXMLString(StringBuffer("<Control>").append(content).append("</Control>"), ipt_fast);
            toJSON(convertPT, json, 0, 0);
            content = json.str();
        }
        FlushingStringBuffer *contentBuffer = queryAppendContentBuffer();
        StringBuffer tag;
        if (name && *name)
            appendJSONName(tag, name);
        contentBuffer->append(tag);
        contentBuffer->append(content);
    }
    virtual IXmlWriter *writeAppendContent(const char *name = NULL)
    {
        FlushingStringBuffer *content = queryAppendContentBuffer();
        if (name && *name)
        {
            StringBuffer tag;
            appendJSONName(tag, name);
            content->append(tag);
        }
        Owned<IXmlWriter> xmlwriter = createIXmlWriterExt(XWFnoindent, 1, content, WTJSONRootless);
        return xmlwriter.getClear();
    }
    void outputContent()
    {
        ForEachItemIn(seq, contentsMap)
        {
            FlushingStringBuffer *content = contentsMap.item(seq);
            if (content)
            {
               content->flush(true);
               for(;;)
               {
                   size32_t length;
                   void *payload = content->getPayload(length);
                   if (!length)
                       break;
                   if (needDelimiter)
                   {
                       StringAttr s(","); //write() will take ownership of buffer
                       size32_t len = s.length();
                       client->write((void *)s.detach(), len, true);
                       needDelimiter=false;
                   }
                   client->write(payload, length, true);
               }
               needDelimiter=true;
            }
        }
    }
    virtual void finalize(unsigned seqNo)
    {
        if (!isHTTP)
        {
            CHpccNativeProtocolResponse::finalize(seqNo);
            return;
        }

        CriticalBlock b(contentsCrit);
        CriticalBlock b1(client->queryCrit());

        StringBuffer responseHead, responseTail;
        if (!resultFilter.ordinality() && !(protocolFlags & HPCC_PROTOCOL_CONTROL))
        {
            StringBuffer name(queryName.get());
            if (isHTTP)
                name.append("Response");
            appendJSONName(responseHead, name.str()).append(" {");
            appendJSONValue(responseHead, "sequence", seqNo);
            appendJSONName(responseHead, "Results").append(" {");

            unsigned len = responseHead.length();
            client->write(responseHead.detach(), len, true);
        }
        if (results)
            results->finalize(seqNo, ",", resultFilter.ordinality() ? resultFilter.item(0) : NULL, &needDelimiter);
        if (!resultFilter.ordinality())
            outputContent();
        if (!resultFilter.ordinality() && !(protocolFlags & HPCC_PROTOCOL_CONTROL))
        {
            responseTail.append("}}");
            unsigned len = responseTail.length();
            client->write(responseTail.detach(), len, true);
        }
    }
};

class CHpccXmlResponse : public CHpccNativeProtocolResponse
{
public:
    CHpccXmlResponse(const char *queryname, SafeSocket *_client, unsigned flags, bool _isHTTP, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags, const char *_resultFilter, const char *_rootTag) :
        CHpccNativeProtocolResponse(queryname, _client, MarkupFmt_XML, flags, _isHTTP, _logctx, _xmlReadFlags, _resultFilter, _rootTag)
    {
    }

    virtual IHpccProtocolResultsWriter *getHpccResultsSection()
    {
        if (!results)
            results.setown(new CHpccXmlResultsWriter(queryName, client, isHTTP, logctx, xmlReadFlags));
        return results;
    }

    virtual void appendContent(TextMarkupFormat mlFmt, const char *content, const char *name=NULL)
    {
        if (mlFmt!=MarkupFmt_XML && mlFmt!=MarkupFmt_JSON)
            return;
        StringBuffer xml;
        if (mlFmt==MarkupFmt_JSON)
        {
            Owned<IPropertyTree> convertPT = createPTreeFromJSONString(content, ipt_fast);
            toXML(convertPT, xml, 0, 0);
            content = xml.str();
        }
        FlushingStringBuffer *contentBuffer = queryAppendContentBuffer();
        if (name && *name)
        {
            StringBuffer tag;
            appendXMLOpenTag(tag, name);
            contentBuffer->append(tag.append('\n'));
            appendXMLCloseTag(tag.clear(), name);
            contentBuffer->setTail(tag.append('\n'));
        }
        contentBuffer->append(content);
    }
    virtual IXmlWriter *writeAppendContent(const char *name = NULL)
    {
        FlushingStringBuffer *content = queryAppendContentBuffer();
        StringBuffer tag;
        if (name && *name)
        {
            appendXMLOpenTag(tag, name);
            content->append(tag);
            appendXMLCloseTag(tag.clear(), name);
            content->setTail(tag);
        }
        Owned<IXmlWriter> xmlwriter = createIXmlWriterExt(0, 1, content, WTStandard);
        return xmlwriter.getClear();
    }
    void outputContent()
    {
        ForEachItemIn(seq, contentsMap)
        {
            FlushingStringBuffer *content = contentsMap.item(seq);
            if (content)
            {
               content->flush(true);
               if (!this->isHTTP)
                   continue;
               for(;;)
               {
                   size32_t length;
                   void *payload = content->getPayload(length);
                   if (!length)
                       break;
                   client->write(payload, length, true);
               }
            }
        }
    }
    virtual void finalize(unsigned seqNo)
    {
        if (!isHTTP)
        {
            CHpccNativeProtocolResponse::finalize(seqNo);
            return;
        }
        CriticalBlock b(contentsCrit);
        CriticalBlock b1(client->queryCrit());

        StringBuffer responseHead, responseTail;
        if (!resultFilter.ordinality() && !(protocolFlags & HPCC_PROTOCOL_CONTROL))
        {
            responseHead.append("<").append(queryName);
            responseHead.append("Response").append(" xmlns=\"urn:hpccsystems:ecl:").appendLower(queryName.length(), queryName.str()).append('\"');
            responseHead.append(" sequence=\"").append(seqNo).append("\"><Results><Result>");
            unsigned len = responseHead.length();
            client->write(responseHead.detach(), len, true);
        }

        if (results)
            results->finalize(seqNo, NULL, resultFilter.ordinality() ? resultFilter.item(0) : NULL, nullptr);
        if (!resultFilter.ordinality())
            outputContent();

        if (!resultFilter.ordinality() && !(protocolFlags & HPCC_PROTOCOL_CONTROL))
        {
            responseTail.append("</Result></Results></").append(queryName);
            if (isHTTP)
                responseTail.append("Response");
            responseTail.append('>');
            unsigned len = responseTail.length();
            client->write(responseTail.detach(), len, true);
        }
    }
};

IHpccProtocolResponse *createProtocolResponse(const char *queryname, SafeSocket *client, HttpHelper &httpHelper, const IContextLogger &logctx, unsigned protocolFlags, PTreeReaderOptions xmlReadFlags)
{
    StringAttr filter, tag;
    httpHelper.getResultFilterAndTag(filter, tag);
    if ((protocolFlags & HPCC_PROTOCOL_NATIVE_RAW) || (protocolFlags & HPCC_PROTOCOL_NATIVE_ASCII))
        return new CHpccNativeProtocolResponse(queryname, client, MarkupFmt_Unknown, protocolFlags, false, logctx, xmlReadFlags, filter, tag);
    else if (httpHelper.queryResponseMlFormat()==MarkupFmt_JSON)
        return new CHpccJsonResponse(queryname, client, protocolFlags, httpHelper.isHttp(), logctx, xmlReadFlags, filter, tag);
    return new CHpccXmlResponse(queryname, client, protocolFlags, httpHelper.isHttp(), logctx, xmlReadFlags, filter, tag);

}

class CHttpRequestAsyncFor : public CInterface, public CAsyncFor
{
private:
    const char *queryName, *queryText, *querySetName;
    const IContextLogger &logctx;
    IArrayOf<IPropertyTree> &requestArray;
    Linked<IHpccProtocolMsgSink> sink;
    Linked<IHpccProtocolMsgContext> msgctx;
    SafeSocket &client;
    HttpHelper &httpHelper;
    PTreeReaderOptions xmlReadFlags;
    unsigned &memused;
    unsigned &agentReplyLen;
    unsigned &agentDuplicates;
    unsigned &agentResends;
    CriticalSection crit;
    unsigned flags;

public:
    CHttpRequestAsyncFor(const char *_queryName, IHpccProtocolMsgSink *_sink, IHpccProtocolMsgContext *_msgctx, IArrayOf<IPropertyTree> &_requestArray,
            SafeSocket &_client, HttpHelper &_httpHelper, unsigned _flags, unsigned &_memused, unsigned &_agentReplyLen, unsigned &_agentDuplicates, unsigned &_agentResends,
            const char *_queryText, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags, const char *_querySetName)
    : querySetName(_querySetName), logctx(_logctx), requestArray(_requestArray), sink(_sink), msgctx(_msgctx), client(_client), httpHelper(_httpHelper), xmlReadFlags(_xmlReadFlags)
      , memused(_memused), agentReplyLen(_agentReplyLen), agentDuplicates(_agentDuplicates), agentResends(_agentResends), flags(_flags)
    {
        queryName = _queryName;
        queryText = _queryText;
    }

    void onException(IException *E)
    {
        //if (!logctx.isBlind())
        //    logctx.CTXLOG("FAILED: %s", queryText);
        StringBuffer error("EXCEPTION: ");
        E->errorMessage(error);
        IERRLOG("%s", error.str());
        client.checkSendHttpException(httpHelper, E, queryName);
        E->Release();
    }

    void Do(unsigned idx)
    {
        try
        {
            IPropertyTree &request = requestArray.item(idx);
            Owned<IHpccProtocolResponse> protocol = createProtocolResponse(request.queryName(), &client, httpHelper, logctx, flags, xmlReadFlags);
            // MORE - agentReply etc should really be atomic
            StringAttr statsWuid;
            sink->onQueryMsg(msgctx, &request, protocol, flags, xmlReadFlags, querySetName, idx, memused, agentReplyLen, agentDuplicates, agentResends, statsWuid);
        }
        catch (IException * E)
        {
            onException(E);
        }
        catch (...)
        {
            onException(MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception"));
        }
    }
};

enum class WhiteSpaceHandling
{
    Default,
    Strip,
    Preserve
};

class QueryNameExtractor : implements IPTreeNotifyEvent, public CInterface
{
public:
    TextMarkupFormat mlFmt;
    StringAttr prefix;
    StringAttr name;
    unsigned headerDepth;
    bool isSoap;
    bool isRequestArray;
    bool isRequest = false;
    WhiteSpaceHandling whitespace=WhiteSpaceHandling::Default;
    bool more;

public:
    IMPLEMENT_IINTERFACE;

    QueryNameExtractor(TextMarkupFormat _mlFmt) : mlFmt(_mlFmt), headerDepth(0), isSoap(false), isRequestArray(false), more(true)
    {
    }
    void extractName(HttpHelper &httpHelper, const char *msg, const IContextLogger &logctx, const char *peer, unsigned port)
    {
        const char *urlName = httpHelper.queryQueryName(); //"Adaptive REST" query name and attrs can come from URL
        if (httpHelper.isHttpGet() || httpHelper.isMappedToInputParameter()) //these types can't have roxie attrs in the content body
        {
            name.set(urlName); //if blank will return error as expected
            return;
        }

        Owned<IPullPTreeReader> parser;
        if (mlFmt==MarkupFmt_JSON)
            parser.setown(createPullJSONStringReader(msg, *this));
        else if (mlFmt==MarkupFmt_XML)
            parser.setown(createPullXMLStringReader(msg, *this));
        if (!parser)
            return;
        while (more && parser->next());
        if (urlName && *urlName)
        {
            name.set(urlName);
            return;
        }
        if (name.isEmpty())
        {
            const char *fmt = mlFmt==MarkupFmt_XML ? "XML" : "JSON";
            IException *E = MakeStringException(-1, "ERROR: Invalid %s queryName not found - received from %s:%d - %s", fmt, peer, port, msg);
            logctx.logOperatorException(E, __FILE__, __LINE__, "Invalid query %s", fmt);
            throw E;
        }
        String nameStr(name.get());
        if (nameStr.endsWith("RequestArray"))
        {
            isRequestArray = true;
            name.set(nameStr.str(), nameStr.length() - strlen("RequestArray"));
        }
        else if (nameStr.endsWith("Request"))
        {
            isRequest = true;
            name.set(nameStr.str(), nameStr.length() - strlen("Request"));
        }
    }
    virtual void beginNode(const char *tag, bool arrayitem, offset_t startOffset) override
    {
        if (streq(tag, "__object__"))
            return;
        const char *local = strchr(tag, ':');
        if (local)
            local++;
        else
            local = tag;
        if (mlFmt==MarkupFmt_XML)
        {
            if (!isSoap && streq(local, "Envelope"))
            {
                isSoap=true;
                return;
            }
            if (isSoap && streq(local, "Header"))
            {
                headerDepth++;
                return;
            }
            if (isSoap && !headerDepth && streq(local, "Body"))
                return;
        }
        if (!headerDepth)
        {
            name.set(local);
            if (tag!=local)
                prefix.set(tag, local-tag-1);
        }
    }
    virtual void newAttribute(const char *attr, const char *value)
    {
        if (!name.isEmpty() && strieq(attr, "@stripWhitespaceFromStoredDataset"))
        {
            whitespace = strToBool(value) ? WhiteSpaceHandling::Strip : WhiteSpaceHandling::Preserve;
            more = false;
        }
    }
    virtual void beginNodeContent(const char *tag)
    {
        if (!name.isEmpty())
            more = false;
    }
    virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
    {
        if (!name.isEmpty())
            more = false;
        else if (headerDepth) //will never be true if !isSoap
        {
            const char *local = strchr(tag, ':');
            if (local)
                local++;
            else
                local = tag;
            if (streq(local, "Header"))
                headerDepth--;
        }
    }

};

static Owned<IActiveQueryLimiterFactory> queryLimiterFactory;

class RoxieSocketWorker : public ProtocolQueryWorker
{
    SocketEndpoint ep;
    Owned<ISocket> rawClient;
    Owned<SafeSocket> client;
    Owned<IHpccNativeProtocolMsgSink> sink;

public:
    RoxieSocketWorker(ProtocolSocketListener *_pool, SocketEndpoint &_ep)
        : ProtocolQueryWorker(_pool), ep(_ep)
    {
        sink.set(dynamic_cast<IHpccNativeProtocolMsgSink*>(_pool->queryMsgSink()));
    }

    //  interface IPooledThread
    virtual void init(void *_r) override
    {
        rawClient.setown((ISocket *) _r);
        ProtocolQueryWorker::init(_r);
    }

    virtual void threadmain() override
    {
        ISocket *secure = listener->createSecureSocket(rawClient.getLink());
        if (secure)
        {
            client.setown(new CSafeSocket(secure));
            rawClient.clear();
            doMain("");
        }
        else
        {
            rawClient->shutdownNoThrow();
            rawClient.clear();
        }
    }

    virtual void runOnce(const char *query)
    {
        doMain(query);
    }

private:
    static void sendHttpServerTooBusy(SafeSocket &client, const IContextLogger &logctx)
    {
        StringBuffer message;

        message.append("HTTP/1.0 503 Server Too Busy\r\n\r\n");
        message.append("Server too busy, please try again later");

        StringBuffer err("Too many active queries");  // write out Too many active queries - make searching for this error consistent
        if (!global->trapTooManyActiveQueries)
        {
            err.appendf("  %s", message.str());
            logctx.CTXLOG("%s", err.str());
        }
        else
        {
            IException *E = MakeStringException(ROXIE_TOO_MANY_QUERIES, "%s", err.str());
            logctx.logOperatorException(E, __FILE__, __LINE__, "%s", message.str());
            E->Release();
        }

        try
        {
            client.setHttpMode(false); //For historical reasons HTTP mode really means SOAP/JSON, we want raw HTTP here.  Should be made more clear
            client.write(message.str(), message.length());
        }
        catch (IException *E)
        {
            logctx.logOperatorException(E, __FILE__, __LINE__, "Exception caught in sendHttpServerTooBusy");
            E->Release();
        }
        catch (...)
        {
            logctx.logOperatorException(NULL, __FILE__, __LINE__, "sendHttpServerTooBusy write failed (Unknown exception)");
        }
    }

    void skipProtocolRoot(Owned<IPropertyTree> &queryPT, HttpHelper &httpHelper, const char *queryName)
    {
        if (queryPT)
        {
            const char *tagName = queryPT->queryName();
            if (httpHelper.isHttp())
            {
                if (httpHelper.queryRequestMlFormat()==MarkupFmt_JSON)
                {
                    if (strieq(tagName, "__array__"))
                        throw MakeStringException(ROXIE_DATA_ERROR, "JSON request array not implemented");
                    if (strieq(tagName, "__object__"))
                    {
                        queryPT.setown(queryPT->getPropTree("*[1]"));
                        if (!queryPT)
                            throw MakeStringException(ROXIE_DATA_ERROR, "Malformed JSON request (missing Body)");
                    }
                }
                else
                {
                    if (strieq(tagName, "envelope"))
                        queryPT.setown(queryPT->getPropTree("Body/*"));
                    else if (!strnicmp(httpHelper.queryContentType(), "application/soap", strlen("application/soap")))
                        throw MakeStringException(ROXIE_DATA_ERROR, "Malformed SOAP request");
                    if (!queryPT)
                        throw MakeStringException(ROXIE_DATA_ERROR, "Malformed SOAP request (missing Body)");
                    queryPT->removeProp("@xmlns:m");
                    queryPT->renameProp("/", queryName);  // reset the name of the tree
                }
            }
        }
        else
            throw MakeStringException(ROXIE_DATA_ERROR, "Malformed request");
    }

    void sanitizeQuery(Owned<IPropertyTree> &queryPT, StringAttr &queryName, StringBuffer &saniText, HttpHelper &httpHelper, const char *&uid, bool &isBlind, bool &isDebug, IProperties * inlineTraceHeaders)
    {
        if (queryPT)
        {
            // convert to XML with attribute values in single quotes - makes replaying queries easier
            uid = queryPT->queryProp("@uid");
            if (!uid)
                uid = queryPT->queryProp("_TransactionId");
            isBlind = queryPT->getPropBool("@blind", false) || queryPT->getPropBool("_blind", false);
            isDebug = queryPT->getPropBool("@debug");
            if (queryPT->hasProp("_trace") && inlineTraceHeaders)
            {
                if (queryPT->hasProp("_trace/traceparent"))
                    inlineTraceHeaders->setProp("traceparent", queryPT->queryProp("_trace/traceparent"));
                if (queryPT->hasProp("_trace/Global-Id"))
                    inlineTraceHeaders->setProp("Global-Id", queryPT->queryProp("_trace/Global-Id"));
                if (queryPT->hasProp("_trace/Caller-Id"))
                    inlineTraceHeaders->setProp("Caller-Id", queryPT->queryProp("_trace/Caller-Id"));
            }

            toXML(queryPT, saniText, 0, isBlind ? (XML_SingleQuoteAttributeValues | XML_Sanitize) : XML_SingleQuoteAttributeValues);
        }
    }
    void createQueryPTree(Owned<IPropertyTree> &queryPT, HttpHelper &httpHelper, const char *text, byte flags, byte options, const char *queryName)
    {
        StringBuffer logxml;
        if (httpHelper.queryRequestMlFormat()==MarkupFmt_URL)
        {
            queryPT.setown(httpHelper.createPTreeFromParameters(flags));
            toXML(queryPT, logxml);
            DBGLOG("%s", logxml.str());
            return;
        }
        if (httpHelper.queryRequestMlFormat()==MarkupFmt_JSON)
            queryPT.setown(createPTreeFromJSONString(text, flags, (PTreeReaderOptions) options));
        else
            queryPT.setown(createPTreeFromXMLString(text, flags, (PTreeReaderOptions) options));
        queryPT.setown(httpHelper.checkAddWrapperForAdaptiveInput(queryPT.getClear(), flags));
        skipProtocolRoot(queryPT, httpHelper, queryName);
        if (queryPT->hasProp("_stripWhitespaceFromStoredDataset"))
        {
            bool stripTag = queryPT->getPropBool("_stripWhitespaceFromStoredDataset");
            bool stripFlag = (options & ptr_ignoreWhiteSpace) != 0;
            if (stripTag != stripFlag)
            {
                if (stripTag)
                    options |= ptr_ignoreWhiteSpace;
                else
                    options &= ~ptr_ignoreWhiteSpace;
                //The tag _stripWhitespaceFromStoredDataset can appear anywhere at the same level as query inputs
                //it can't be checked until after parsing the full request, so if it changes the parse flags
                //we have to parse the request again now
                createQueryPTree(queryPT, httpHelper, text, flags, options, queryName);
            }
        }
    }
    const char *queryRequestIdHeader(HttpHelper &httpHelper, const char *header, StringAttr &headerused)
    {
        const char *id = httpHelper.queryRequestHeader(header);
        if (id && *id)
            headerused.set(header);
        return id;
    }

    const char *queryRequestGlobalIdHeader(HttpHelper &httpHelper, IContextLogger &logctx)
    {
        const char *id = httpHelper.queryRequestHeader(kGlobalIdHttpHeaderName);
        if (!id || !*id)
            id = httpHelper.queryRequestHeader(kLegacyGlobalIdHttpHeaderName);        // Backward compatibility - passed on as global-id
        return id;
    }

    const char *queryRequestCallerIdHeader(HttpHelper &httpHelper, IContextLogger &logctxheaderused)
    {
        const char *id = httpHelper.queryRequestHeader(kCallerIdHttpHeaderName);
        if (!id || !*id)
            id = httpHelper.queryRequestHeader(kLegacyCallerIdHttpHeaderName);        // Backward compatibility - passed on as caller-id
        return id;
    }

    void doMain(const char *runQuery)
    {
        StringBuffer rawText(runQuery);
        unsigned memused = 0;
        IpAddress peer;
        bool continuationNeeded = false;
        bool resetQstart = false;
        bool isStatus = false;
        unsigned remainingHttpConnectionRequests = global->maxHttpConnectionRequests ? global->maxHttpConnectionRequests : 1;
        unsigned readWait = WAIT_FOREVER;

        Owned<IHpccProtocolMsgContext> msgctx = sink->createMsgContext(startTime);

readAnother:
        unsigned agentsReplyLen = 0;
        unsigned agentsDuplicates = 0;
        unsigned agentsResends = 0;
        StringArray allTargets;
        sink->getTargetNames(allTargets);
        HttpHelper httpHelper(&allTargets, global->targetAliases.get());
        try
        {
            if (client)
            {
                client->querySocket()->getPeerAddress(peer);
                if (!client->readBlocktms(rawText.clear(), readWait, &httpHelper, continuationNeeded, isStatus, global->maxBlockSize))
                {
                    if (doTrace(traceSockets, TraceFlags::Max))
                    {
                        StringBuffer b;
                        DBGLOG("No data reading query from socket");
                    }
                    client.clear();
                    return;
                }
            }
            if (resetQstart)
            {
                resetQstart = false;
                startNs = nsTick();
                time(&startTime);
                msgctx.setown(sink->createMsgContext(startTime));
            }
        }
        catch (IException * E)
        {
            bool expectedError = false;
            if (resetQstart) //persistent connection - initial request has already been processed
            {
                switch (E->errorCode())
                {
                    //closing of persistent socket is not an error
                    case JSOCKERR_not_opened:
                    case JSOCKERR_broken_pipe:
                    case JSOCKERR_timeout_expired:
                    case JSOCKERR_graceful_close:
                        expectedError = true;
                    default:
                        break;
                }
            }
            if (doTrace(traceSockets) && !expectedError)
            {
                StringBuffer b;
                IERRLOG("Error reading query from socket: %s", E->errorMessage(b).str());
            }
            E->Release();
            client.clear();
            return;
        }

        PerfTracer perf;
        IRoxieContextLogger &logctx = static_cast<IRoxieContextLogger&>(*msgctx->queryLogContext());
        bool isHTTP = httpHelper.isHttp();
        if (isHTTP)
        {
            if (httpHelper.allowKeepAlive())
                client->setHttpKeepAlive(remainingHttpConnectionRequests > 1);
            else
                remainingHttpConnectionRequests = 1;
        }

        TextMarkupFormat mlResponseFmt = MarkupFmt_Unknown;
        TextMarkupFormat mlRequestFmt = MarkupFmt_Unknown;
        if (!isStatus)
        {
            if (!isHTTP)
                mlResponseFmt = mlRequestFmt = MarkupFmt_XML;
            else
            {
                mlResponseFmt = httpHelper.queryResponseMlFormat();
                mlRequestFmt = httpHelper.queryRequestMlFormat();
            }
        }

        bool failed = false;
        bool isRequest = false;
        bool isRequestArray = false;
        bool isBlind = false;
        bool isDebug = false;
        unsigned protocolFlags = isHTTP ? 0 : HPCC_PROTOCOL_NATIVE;
        unsigned requestArraySize = 0; //for logging, considering all the ways requests can be counted this name seems least confusing

        Owned<IPropertyTree> queryPT;
        StringBuffer sanitizedText;
        StringBuffer peerStr;
        peer.getHostText(peerStr);
        const char *uid = "-";

        StringAttr queryName;
        StringAttr queryPrefix;
        StringAttr statsWuid;
        WhiteSpaceHandling whitespace = WhiteSpaceHandling::Default;
        try
        {
            if (httpHelper.isHttpGet() || httpHelper.isFormPost())
            {
                queryName.set(httpHelper.queryQueryName());
                if (httpHelper.isControlUrl())
                    queryPrefix.set("control");
            }
            else if (mlRequestFmt==MarkupFmt_XML || mlRequestFmt==MarkupFmt_JSON)
            {
                QueryNameExtractor extractor(mlRequestFmt);
                extractor.extractName(httpHelper, rawText.str(), logctx, peerStr, ep.port);
                queryName.set(extractor.name);
                queryPrefix.set(extractor.prefix);
                whitespace = extractor.whitespace;
                isRequest = extractor.isRequest;
                isRequestArray = extractor.isRequestArray;
                if (httpHelper.isHttp())
                    httpHelper.setUseEnvelope(extractor.isSoap);
            }

            if (!queryName)
            {
                // or should we set("") ?  Is an empty queryName ever valid ?
                if (doTrace(traceSockets, TraceFlags::Max))
                {
                    DBGLOG("missing/invalid query name from socket");
                }
                client.clear();
                return;
            }

            if (streq(queryPrefix.str(), "control"))
            {
                if (httpHelper.isHttp())
                    client->setHttpMode(queryName, false, httpHelper);

                bool aclupdate = strieq(queryName.str(), "aclupdate"); //ugly
                byte iptFlags = aclupdate ? ipt_caseInsensitive|ipt_fast : ipt_fast;

                createQueryPTree(queryPT, httpHelper, rawText, iptFlags, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_ignoreNameSpaces), queryName);

                //IPropertyTree *root = queryPT;
                if (!strchr(queryName.str(), ':'))
                {
                    VStringBuffer fullname("control:%s", queryName.str()); //just easier to keep for debugging and internal checking
                    queryPT->renameProp("/", fullname);
                }
                Owned<IHpccProtocolResponse> protocol = createProtocolResponse(queryPT->queryName(), client, httpHelper, logctx, protocolFlags | HPCC_PROTOCOL_CONTROL, global->defaultXmlReadFlags);
                sink->onControlMsg(msgctx, queryPT, protocol);
                protocol->finalize(0);
                if (streq(queryName.str(), "lock") || streq(queryName.str(), "childlock")) //don't reset startNs, lock time should be included
                    goto readAnother;
            }
            else if (isStatus)
            {
                client->write("OK", 2);
            }
            else
            {
                StringBuffer querySetName;
                if (isHTTP)
                {
                    client->setHttpMode(queryName, isRequestArray, httpHelper);
                    querySetName.set(httpHelper.queryTarget());
                    if (querySetName.length())
                    {
                        const char *target = global->targetAliases->queryProp(querySetName.str());
                        if (target)
                            querySetName.set(target);
                    }
                }

                if (!streq(queryPrefix.str(), "debug"))
                    msgctx->initQuery(querySetName, queryName); //needed here to allow checking hash options

                if (whitespace == WhiteSpaceHandling::Default) //value in the request wins
                    whitespace = msgctx->getStripWhitespace() ? WhiteSpaceHandling::Strip : WhiteSpaceHandling::Preserve; //might be changed by hash option, returns default otherwise

                unsigned readFlags = (unsigned) global->defaultXmlReadFlags | ptr_ignoreNameSpaces;
                readFlags &= ~ptr_ignoreWhiteSpace;
                readFlags |= (whitespace == WhiteSpaceHandling::Strip ? ptr_ignoreWhiteSpace : ptr_none);
                try
                {
                    createQueryPTree(queryPT, httpHelper, rawText.str(), ipt_caseInsensitive|ipt_fast, (PTreeReaderOptions)readFlags, queryName);
                }
                catch (IException *E)
                {
                    logctx.logOperatorException(E, __FILE__, __LINE__, "Invalid XML received from %s:%d - %s", peerStr.str(), listener->queryPort(), rawText.str());
                    logctx.CTXLOG("ERROR: Invalid XML received from %s:%d - %s", peerStr.str(), listener->queryPort(), rawText.str());
                    throw;
                }

                uid = NULL;
                Owned<IProperties> inlineTraceHeaders = createProperties(true);
                sanitizeQuery(queryPT, queryName, sanitizedText, httpHelper, uid, isBlind, isDebug, inlineTraceHeaders);

                msgctx->startSpan(uid, querySetName, queryName, isHTTP ? httpHelper.queryRequestHeaders() : inlineTraceHeaders);

                if (!uid)
                    uid = "-";

                sink->checkAccess(peer, queryName, sanitizedText, isBlind);

                if (isDebug)
                    msgctx->verifyAllowDebug();
                isBlind = msgctx->checkSetBlind(isBlind);

                if (msgctx->logFullQueries())
                {
                    StringBuffer soapStr;
                    (isRequest) ? soapStr.append("SoapRequest") : (isRequestArray) ? soapStr.append("SoapRequest") : soapStr.clear();
                    logctx.CTXLOG("%s %s:%d %s %s %s", isBlind ? "BLIND:" : "QUERY:", peerStr.str(), listener->queryPort(), uid, soapStr.str(), sanitizedText.str());
                }
                if (strieq(queryPrefix.str(), "debug"))
                {
                    FlushingStringBuffer response(client, false, MarkupFmt_XML, false, isHTTP, logctx);
                    response.startDataset("Debug", NULL, (unsigned) -1);
                    CommonXmlWriter out(0, 1);
                    sink->onDebugMsg(msgctx, uid, queryPT, out);
                    response.append(out.str());
                }
                else
                {
                    Owned<IActiveQueryLimiter> l;
                    if (queryLimiterFactory)
                        l.setown(queryLimiterFactory->create(listener));
                    if (l && !l->isAccepted())
                    {
                        if (isHTTP)
                        {
                            sendHttpServerTooBusy(*client, logctx);
                            logctx.CTXLOG("FAILED: %s", sanitizedText.str());
                            logctx.CTXLOG("EXCEPTION: Too many active queries");
                            remainingHttpConnectionRequests = 1;
                        }
                        else
                        {
                            IException *e = MakeStringException(ROXIE_TOO_MANY_QUERIES, "Too many active queries");
                            if (msgctx->trapTooManyActiveQueries())
                                logctx.logOperatorException(e, __FILE__, __LINE__, NULL);
                            throw e;
                        }
                    }
                    else
                    {
                        int bindCores = queryPT->getPropInt("@bindCores", msgctx->getBindCores());
                        if (bindCores > 0)
                            listener->setThreadAffinity(bindCores);
                        IArrayOf<IPropertyTree> requestArray;
                        if (isHTTP)
                        {
                            if (isRequestArray)
                            {
                                StringBuffer reqIterString;
                                reqIterString.append(queryName).append("Request");

                                Owned<IPropertyTreeIterator> reqIter = queryPT->getElements(reqIterString.str());
                                ForEach(*reqIter)
                                {
                                    IPropertyTree *fixedreq = createPTree(queryName, ipt_caseInsensitive|ipt_fast);
                                    Owned<IPropertyTreeIterator> iter = reqIter->query().getElements("*");
                                    ForEach(*iter)
                                    {
                                        fixedreq->addPropTree(iter->query().queryName(), LINK(&iter->query()));
                                    }
                                    Owned<IAttributeIterator> aiter = queryPT->getAttributes();
                                    ForEach(*aiter)
                                    {
                                        fixedreq->setProp(aiter->queryName(), aiter->queryValue());
                                    }
                                    Owned<IAttributeIterator> aiter2 = reqIter->query().getAttributes();
                                    ForEach(*aiter2)
                                    {
                                        fixedreq->setProp(aiter2->queryName(), aiter2->queryValue());
                                    }
                                    requestArray.append(*fixedreq);
                                    requestArraySize++;
                                }
                            }
                            else
                            {
                                IPropertyTree *fixedreq = createPTree(queryName, ipt_caseInsensitive|ipt_fast);
                                Owned<IPropertyTreeIterator> iter = queryPT->getElements("*");
                                ForEach(*iter)
                                {
                                    fixedreq->addPropTree(iter->query().queryName(), LINK(&iter->query()));
                                }
                                Owned<IAttributeIterator> aiter = queryPT->getAttributes();
                                ForEach(*aiter)
                                {
                                    fixedreq->setProp(aiter->queryName(), aiter->queryValue());
                                }
                                requestArray.append(*fixedreq);
                                requestArraySize = 1;

                                msgctx->setIntercept(queryPT->getPropBool("@log", false));
                                msgctx->setTraceLevel(queryPT->getPropInt("@traceLevel", logctx.queryTraceLevel()));
                            }
                            if (httpHelper.getTrim())
                                protocolFlags |= HPCC_PROTOCOL_TRIM;
                        }
                        else
                        {
                            const char *format = queryPT->queryProp("@format");
                            if (format)
                            {
                                if (stricmp(format, "raw") == 0)
                                {
                                    protocolFlags |= HPCC_PROTOCOL_NATIVE_RAW;
                                    if (client) //not stand alone roxie exe
                                        protocolFlags |= HPCC_PROTOCOL_BLOCKED;
                                    mlResponseFmt = MarkupFmt_Unknown;
                                }
                                else if (stricmp(format, "bxml") == 0)
                                {
                                    protocolFlags |= HPCC_PROTOCOL_BLOCKED;
                                    mlResponseFmt = MarkupFmt_XML;
                                }
                                else if (stricmp(format, "ascii") == 0)
                                {
                                    protocolFlags |= HPCC_PROTOCOL_NATIVE_ASCII;
                                    mlResponseFmt = MarkupFmt_Unknown;
                                }
                                else if (stricmp(format, "xml") != 0) // xml is the default
                                    throw MakeStringException(ROXIE_INVALID_INPUT, "Unsupported format specified: %s", format);
                            }
                            if (queryPT->getPropBool("@trim", false))
                                protocolFlags |= HPCC_PROTOCOL_TRIM;
                            msgctx->setIntercept(queryPT->getPropBool("@log", false));
                            msgctx->setTraceLevel(queryPT->getPropInt("@traceLevel", logctx.queryTraceLevel()));
                            if (queryPT->getPropBool("@perf", false))
                                perf.start();
                        }

                        msgctx->noteQueryActive();

                        if (isHTTP)
                        {
                            CHttpRequestAsyncFor af(queryName, sink, msgctx, requestArray, *client, httpHelper, protocolFlags, memused, agentsReplyLen, agentsDuplicates, agentsResends, sanitizedText, logctx, (PTreeReaderOptions)readFlags, querySetName);
                            af.For(requestArray.length(), global->numRequestArrayThreads);
                        }
                        else
                        {
                            Owned<IHpccProtocolResponse> protocol = createProtocolResponse(queryPT->queryName(), client, httpHelper, logctx, protocolFlags, (PTreeReaderOptions)readFlags);
                            sink->onQueryMsg(msgctx, queryPT, protocol, protocolFlags, (PTreeReaderOptions)readFlags, querySetName, 0, memused, agentsReplyLen, agentsDuplicates, agentsResends, statsWuid);
                        }
                    }
                }
            }
        }
        catch (IException * E)
        {
            failed = true;
            logctx.CTXLOG("FAILED: %s", sanitizedText.str());
            StringBuffer error;
            E->errorMessage(error);
            logctx.CTXLOG("EXCEPTION: %s", error.str());
            unsigned code = E->errorCode();
            if (QUERYINTERFACE(E, ISEH_Exception))
                code = ROXIE_INTERNAL_ERROR;
            else if (QUERYINTERFACE(E, IOutOfMemException))
                code = ROXIE_MEMORY_ERROR;
            if (client)
            {
                if (isHTTP)
                    client->checkSendHttpException(httpHelper, E, queryName);
                else
                    client->sendException("Roxie", code, error.str(), (protocolFlags & HPCC_PROTOCOL_NATIVE_RAW), logctx);
            }
            else
            {
                fprintf(stderr, "EXCEPTION: %s\n", error.str());
            }
            E->Release();
        }
#ifndef _DEBUG
        catch(...)
        {
            failed = true;
            logctx.CTXLOG("FAILED: %s", sanitizedText.str());
            logctx.CTXLOG("EXCEPTION: Unknown exception");
            {
                if (isHTTP)
                {
                    Owned<IException> E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception");
                    client->checkSendHttpException(httpHelper, E, queryName);
                }
                else
                    client->sendException("Roxie", ROXIE_INTERNAL_ERROR, "Unknown exception", (protocolFlags & HPCC_PROTOCOL_BLOCKED), logctx);
            }
        }
#endif
        if (isHTTP)
        {
            try
            {
                client->flush();
            }
            catch (IException * E)
            {
                StringBuffer error("RoxieSocketWorker failed to write to socket ");
                E->errorMessage(error);
                logctx.CTXLOG("%s", error.str());
                E->Release();

            }
            catch(...)
            {
                logctx.CTXLOG("RoxieSocketWorker failed to write to socket (Unknown exception)");
            }
        }
        unsigned bytesOut = client? client->bytesOut() : 0;
        stat_type elapsedNs = nsTick() - startNs;
        unsigned elapsedMs = nanoToMilli(elapsedNs);
        if (client)
        {
            logctx.noteStatistic(StTimeSocketReadIO, client->getStatistic(StTimeSocketReadIO));
            logctx.noteStatistic(StTimeSocketWriteIO, client->getStatistic(StTimeSocketWriteIO));
            logctx.noteStatistic(StSizeSocketRead, client->getStatistic(StSizeSocketRead));
            logctx.noteStatistic(StSizeSocketWrite, client->getStatistic(StSizeSocketWrite));
            logctx.noteStatistic(StNumSocketReads, client->getStatistic(StNumSocketReads));
            logctx.noteStatistic(StNumSocketWrites, client->getStatistic(StNumSocketWrites));
        }

        sink->noteQuery(msgctx.get(), peerStr, failed, bytesOut, elapsedNs,  memused, agentsReplyLen, agentsDuplicates, agentsResends, continuationNeeded, requestArraySize);
        if (continuationNeeded)
        {
            rawText.clear();
            resetQstart = true;
            goto readAnother;
        }
        else
        {
            try
            {
                if (client && !isHTTP && !isStatus)
                {
                    if (queryPT)
                    {
                        if (queryPT->getPropBool("@perf", false))
                        {
                            perf.stop();
                            StringBuffer &perfInfo = perf.queryResult();
                            FlushingStringBuffer response(client, (protocolFlags & HPCC_PROTOCOL_BLOCKED), mlResponseFmt, (protocolFlags & HPCC_PROTOCOL_NATIVE_RAW), false, logctx);
                            response.startDataset("PerfTrace", NULL, (unsigned) -1);
                            response.flushXML(perfInfo, true);
                        }
                        if (msgctx->getIntercept())
                        {
                            FlushingStringBuffer response(client, (protocolFlags & HPCC_PROTOCOL_BLOCKED), mlResponseFmt, (protocolFlags & HPCC_PROTOCOL_NATIVE_RAW), false, logctx);
                            response.startDataset("Tracing", NULL, (unsigned) -1);
                            msgctx->outputLogXML(response);
                        }
                        if (statsWuid.length())
                        {
                            FlushingStringBuffer response(client, (protocolFlags & HPCC_PROTOCOL_BLOCKED), mlResponseFmt, (protocolFlags & HPCC_PROTOCOL_NATIVE_RAW), false, logctx);
                            response.startDataset("Statistics", NULL, (unsigned) -1);
                            VStringBuffer xml(" <wuid>%s</wuid>\n", statsWuid.str());
                            response.flushXML(xml, true);
                        }
                        if (queryPT->getPropBool("@summaryStats", alwaysSendSummaryStats))
                        {
                            FlushingStringBuffer response(client, (protocolFlags & HPCC_PROTOCOL_BLOCKED), mlResponseFmt, (protocolFlags & HPCC_PROTOCOL_NATIVE_RAW), false, logctx);
                            response.startDataset("SummaryStats", NULL, (unsigned) -1);
                            VStringBuffer s(" COMPLETE: %s %s complete in %u msecs memory=%u Mb agentsreply=%u duplicatePackets=%u resentPackets=%u resultsize=%u continue=%d", queryName.get(), uid, elapsedMs, memused, agentsReplyLen, agentsDuplicates, agentsResends, bytesOut, continuationNeeded);
                            logctx.getStats(s).newline();
                            response.flushXML(s, true);
                        }
                    }
                    unsigned replyLen = 0;
                    client->write(&replyLen, sizeof(replyLen));
                }
                if (isHTTP && --remainingHttpConnectionRequests > 0)
                {
                    readWait = global->maxHttpKeepAliveWait;
                    resetQstart = true;
                    goto readAnother;
                }

                client.clear();
            }
            catch (IException * E)
            {
                StringBuffer error("RoxieSocketWorker failed to close socket ");
                E->errorMessage(error);
                logctx.CTXLOG("%s", error.str()); // MORE - audience?
                E->Release();

            }
            catch(...)
            {
                logctx.CTXLOG("RoxieSocketWorker failed to close socket (Unknown exception)"); // MORE - audience?
            }
        }
    }
};


IPooledThread *ProtocolSocketListener::createNew()
{
    return new RoxieSocketWorker(this, ep);
}

void ProtocolSocketListener::runOnce(const char *query)
{
    Owned<RoxieSocketWorker> p = new RoxieSocketWorker(this, ep);
    p->runOnce(query);
}

IHpccProtocolListener *createProtocolListener(const char *protocol, IHpccProtocolMsgSink *sink, unsigned port, unsigned listenQueue, const ISyncedPropertyTree *tlsConfig)
{
    if (traceLevel)
    {
        const char *certIssuer = "none";
        if (tlsConfig)
        {
            Owned<const IPropertyTree> tlsInfo = tlsConfig->getTree();
            if (tlsInfo && tlsInfo->hasProp("@issuer"))
                certIssuer = tlsInfo->queryProp("@issuer");
        }
        DBGLOG("Creating Roxie socket listener, protocol %s, issuer=%s, pool size %d, listen queue %d%s", protocol, certIssuer, sink->getPoolSize(), listenQueue, sink->getIsSuspended() ? " SUSPENDED":"");
    }
    return new ProtocolSocketListener(sink, port, listenQueue, protocol, tlsConfig);
}

extern IHpccProtocolPlugin *loadHpccProtocolPlugin(IHpccProtocolPluginContext *ctx, IActiveQueryLimiterFactory *_limiterFactory)
{
    if (!queryLimiterFactory)
        queryLimiterFactory.set(_limiterFactory);
    if (global)
        return global.getLink();
    if (!ctx)
        return NULL;
    global.setown(new CHpccProtocolPlugin(*ctx));
    return global.getLink();
}

extern void unloadHpccProtocolPlugin()
{
    queryLimiterFactory.clear();
    global.clear();
}

//================================================================================================================================
