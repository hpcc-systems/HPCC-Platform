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

#include "roxie.hpp"
#include "roxiehelper.hpp"
#include "ccdprotocol.hpp"

//================================================================================================================================

IHpccProtocolListener *createProtocolListener(const char *protocol, IHpccProtocolMsgSink *sink, unsigned port, unsigned listenQueue);

class CHpccProtocolPlugin : public CInterface, implements IHpccProtocolPlugin
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
    }
    IHpccProtocolListener *createListener(const char *protocol, IHpccProtocolMsgSink *sink, unsigned port, unsigned listenQueue, const char *config)
    {
        return createProtocolListener(protocol, sink, port, listenQueue);
    }
public:
    StringArray targetNames;
    Owned<IProperties> targetAliases;
    PTreeReaderOptions defaultXmlReadFlags;
    unsigned maxBlockSize;
    unsigned numRequestArrayThreads;
    bool trapTooManyActiveQueries;
};

Owned<CHpccProtocolPlugin> global;

class ProtocolListener : public Thread, implements IHpccProtocolListener, implements IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE;
    ProtocolListener(IHpccProtocolMsgSink *_sink) : Thread("RoxieListener")
    {
        running = false;
        sink.set(dynamic_cast<IHpccNativeProtocolMsgSink*>(_sink));
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
                traceAffinity(&cpuMask);
        }
#endif
    }

    virtual void start()
    {
        // Note we allow a few additional threads than requested - these are the threads that return "Too many active queries" responses
        pool.setown(createThreadPool("RoxieSocketWorkerPool", this, NULL, sink->getPoolSize()+5, INFINITE));
        assertex(!running);
        Thread::start();
        started.wait();
    }

    virtual bool stop(unsigned timeout)
    {
        if (running)
        {
            running = false;
            join();
            Release();
        }
        return pool->joinAll(false, timeout);
    }

    virtual bool suspend(bool suspendIt)
    {
        return sink->suspend(suspendIt);
    }

    void setThreadAffinity(int numCores)
    {
#ifdef CPU_ZERO
        // Note - strictly speaking not threadsafe but any race conditions are (a) unlikely and (b) harmless
        if (cpuCores)
        {
            if (numCores > 0 && numCores < cpuCores)
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
                        if (cores == numCores)
                        {
                            lastCore = useCore+1;
                            break;
                        }
                    }
                }
                if (traceLevel > 3)
                    traceAffinity(&threadMask);
                pthread_setaffinity_np(GetCurrentThreadId(), sizeof(cpu_set_t), &threadMask);
            }
            else
            {
                if (traceLevel > 3)
                    traceAffinity(&cpuMask);
                pthread_setaffinity_np(GetCurrentThreadId(), sizeof(cpu_set_t), &cpuMask);
            }
        }
#endif
    }

protected:
    bool running;
    Semaphore started;
    Owned<IThreadPool> pool;

    Linked<IHpccNativeProtocolMsgSink> sink;

#ifdef CPU_ZERO
    static cpu_set_t cpuMask;
    static unsigned cpuCores;
    static unsigned lastCore;

private:
    static void traceAffinity(cpu_set_t *mask)
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

public:
    ProtocolSocketListener(IHpccProtocolMsgSink *_sink, unsigned _port, unsigned _listenQueue)
      : ProtocolListener(_sink)
    {
        port = _port;
        listenQueue = _listenQueue;
        ep.set(port, queryHostIP());
    }

    IHpccProtocolMsgSink *queryMsgSink()
    {
        return sink.get();
    }

    virtual bool stop(unsigned timeout)
    {
        if (socket)
            socket->cancel_accept();
        return ProtocolListener::stop(timeout);
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

    virtual int run()
    {
        DBGLOG("ProtocolSocketListener (%d threads) listening to socket on port %d", sink->getPoolSize(), port);
        socket.setown(ISocket::create(port, listenQueue));
        running = true;
        started.signal();
        while (running)
        {
            ISocket *client = socket->accept(true);
            if (client)
            {
                client->set_linger(-1);
                pool->start(client);
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
        qstart = msTick();
        time(&startTime);
    }

    //  interface IPooledThread
    virtual void init(void *)
    {
        qstart = msTick();
        time(&startTime);
    }

    virtual bool canReuse()
    {
        return true;
    }

    virtual bool stop()
    {
        ERRLOG("RoxieQueryWorker stopped with queries active");
        return true;
    }

protected:
    ProtocolListener *listener;
    unsigned qstart;
    time_t startTime;

};

//================================================================================================================

class CHpccNativeResultsWriter : public CInterface, implements IHpccNativeProtocolResultsWriter
{
protected:
    SafeSocket *client;
    CriticalSection resultsCrit;
    IPointerArrayOf<FlushingStringBuffer> resultMap;

    StringAttr queryName;
    const IContextLogger &logctx;
    Owned<FlushingStringBuffer> probe;
    TextMarkupFormat mlFmt;
    PTreeReaderOptions xmlReadFlags;
    bool isBlocked;
    bool isRaw;
    bool isHTTP;
    bool trim;
    bool failed;

public:
    IMPLEMENT_IINTERFACE;
    CHpccNativeResultsWriter(const char *queryname, SafeSocket *_client, bool _isBlocked, TextMarkupFormat _mlFmt, bool _isRaw, bool _isHTTP, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags) :
        client(_client), queryName(queryname), logctx(_logctx), mlFmt(_mlFmt), xmlReadFlags(_xmlReadFlags), isBlocked(_isBlocked), isRaw(_isRaw), isHTTP(_isHTTP)
    {
    }
    ~CHpccNativeResultsWriter()
    {
    }
    virtual FlushingStringBuffer *queryResult(unsigned sequence)
    {
        CriticalBlock procedure(resultsCrit);
        while (!resultMap.isItem(sequence))
            resultMap.append(NULL);
        FlushingStringBuffer *result = resultMap.item(sequence);
        if (!result)
        {
            if (mlFmt==MarkupFmt_JSON)
                result = new FlushingJsonBuffer(client, isBlocked, isHTTP, logctx);
            else
                result = new FlushingStringBuffer(client, isBlocked, mlFmt, isRaw, isHTTP, logctx);
            result->isSoap = isHTTP;
            result->trim = trim;
            result->queryName.set(queryName);
            resultMap.replace(result, sequence);
        }
        return result;
    }
    virtual FlushingStringBuffer *createFlushingBuffer()
    {
        return new FlushingStringBuffer(client, isBlocked, mlFmt, isRaw, isHTTP, logctx);
    }
    virtual IXmlWriter *addDataset(const char *name, unsigned sequence, const char *elementName, bool &appendRawData, unsigned writeFlags, bool _extend, const IProperties *xmlns)
    {
        FlushingStringBuffer *response = queryResult(sequence);
        if (response)
        {
            appendRawData = response->isRaw;
            response->startDataset(elementName, name, sequence, _extend, xmlns);
            if (response->mlFmt==MarkupFmt_XML || response->mlFmt==MarkupFmt_JSON)
            {
                if (response->mlFmt==MarkupFmt_JSON)
                    writeFlags |= XWFnoindent;
                Owned<IXmlWriter> xmlwriter = createIXmlWriterExt(writeFlags, 1, response, (response->mlFmt==MarkupFmt_JSON) ? WTJSON : WTStandard);
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
            r->startScalar(name, sequence);
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
            r->startScalar(name, sequence);
            r->encodeData(data, len);
        }
    }
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            r->startScalar(name, sequence);
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
            r->startScalar(name, sequence);
            if (isRaw)
                r->append(len, (char *)data);
            else if (mlFmt==MarkupFmt_XML)
            {
                assertex(transformer);
                CommonXmlWriter writer(xmlReadFlags|XWFnoindent, 0);
                transformer->toXML(isAll, len, (byte *)data, writer);
                r->append(writer.str());
            }
            else if (mlFmt==MarkupFmt_JSON)
            {
                assertex(transformer);
                CommonJsonWriter writer(xmlReadFlags|XWFnoindent, 0);
                transformer->toXML(isAll, len, (byte *)data, writer);
                r->append(writer.str());
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
            r->startScalar(name, sequence);
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
                r->startScalar(name, sequence);
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
                r->startScalar(name, sequence);
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
            r->startScalar(name, sequence);
            r->append(value);
        }
    }
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str)
    {
        FlushingStringBuffer *r = queryResult(sequence);
        if (r)
        {
            r->startScalar(name, sequence);
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
            r->startScalar(name, sequence);
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
    virtual void finalize(unsigned seqNo)
    {
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
    }
    virtual void appendProbeGraph(const char *xml)
    {
        if (!xml)
        {
            if (probe)
                probe.clear();
            return;
        }
        if (!probe)
        {
            probe.setown(new FlushingStringBuffer(client, isBlocked, MarkupFmt_XML, false, isHTTP, logctx));
            probe->startDataset("_Probe", NULL, (unsigned) -1);  // initialize it
        }

        probe->append("\n");
        probe->append(xml);
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
            Owned<IPropertyTree> convertPT = createPTreeFromXMLString(content);
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

class CHpccNativeProtocolResponse : public CInterface, implements IHpccNativeProtocolResponse
{
protected:
    SafeSocket *client;
    StringAttr queryName;
    const IContextLogger &logctx;
    TextMarkupFormat mlFmt;
    PTreeReaderOptions xmlReadFlags;
    Owned<CHpccNativeResultsWriter> results; //hpcc results section
    IPointerArrayOf<FlushingStringBuffer> contentsMap; //other sections
    CriticalSection contentsCrit;
    unsigned protocolFlags;
    bool isHTTP;
    bool failed;

public:
    IMPLEMENT_IINTERFACE;
    CHpccNativeProtocolResponse(const char *queryname, SafeSocket *_client, TextMarkupFormat _mlFmt, unsigned flags, bool _isHTTP, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags) :
        client(_client), queryName(queryname), logctx(_logctx), mlFmt(_mlFmt), xmlReadFlags(_xmlReadFlags), protocolFlags(flags), isHTTP(_isHTTP)
    {
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
        content->trim = getTrim();
        content->queryName.set(queryName);
        if (!isHTTP)
            content->startBlock();
        contentsMap.append(content);
        return content;
    }

    virtual IHpccProtocolResultsWriter *queryHpccResultsSection()
    {
        if (!results)
            results.setown(new CHpccNativeResultsWriter(queryName, client, getIsBlocked(), mlFmt, getIsRaw(), isHTTP, logctx, xmlReadFlags));
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
        return client->checkConnection();
    }
    virtual void sendHeartBeat()
    {
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
    virtual void appendProbeGraph(const char *xml)
    {
        if (results)
            results->appendProbeGraph(xml);
    }
};

class CHpccJsonResponse : public CHpccNativeProtocolResponse
{
public:
    CHpccJsonResponse(const char *queryname, SafeSocket *_client, unsigned flags, bool _isHttp, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags) :
        CHpccNativeProtocolResponse(queryname, _client, MarkupFmt_JSON, flags, _isHttp, _logctx, _xmlReadFlags)
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
            Owned<IPropertyTree> convertPT = createPTreeFromXMLString(content);
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
        Owned<IXmlWriter> xmlwriter = createIXmlWriterExt(XWFnoindent, 1, content, WTJSON);
        return xmlwriter.getClear();
    }
    virtual void outputContent()
    {
        CriticalBlock b1(client->queryCrit());

        bool needDelimiter = false;
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

        StringBuffer responseHead, responseTail;
        StringBuffer name(queryName.get());
        if (isHTTP)
            name.append("Response");
        appendJSONName(responseHead, name.str()).append(" {");
        appendJSONValue(responseHead, "sequence", seqNo);
        if (contentsMap.length() || results)
            delimitJSON(responseHead);
        unsigned len = responseHead.length();
        client->write(responseHead.detach(), len, true);

        outputContent();
        if (results)
            results->finalize(seqNo);

        responseTail.append("}");
        len = responseTail.length();
        client->write(responseTail.detach(), len, true);
    }
};

class CHpccXmlResponse : public CHpccNativeProtocolResponse
{
public:
    CHpccXmlResponse(const char *queryname, SafeSocket *_client, unsigned flags, bool _isHTTP, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags) :
        CHpccNativeProtocolResponse(queryname, _client, MarkupFmt_XML, flags, _isHTTP, _logctx, _xmlReadFlags)
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
            Owned<IPropertyTree> convertPT = createPTreeFromJSONString(content);
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
    virtual void outputContent()
    {
        CriticalBlock b1(client->queryCrit());

        bool needDelimiter = false;
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

        StringBuffer responseHead, responseTail;
        responseHead.append("<").append(queryName);
        responseHead.append("Response").append(" xmlns=\"urn:hpccsystems:ecl:").appendLower(queryName.length(), queryName.str()).append('\"');
        responseHead.append(" sequence=\"").append(seqNo).append("\">");
        unsigned len = responseHead.length();
        client->write(responseHead.detach(), len, true);

        outputContent();
        if (results)
            results->finalize(seqNo);

        responseTail.append("</").append(queryName);
        if (isHTTP)
            responseTail.append("Response");
        responseTail.append('>');
        len = responseTail.length();
        client->write(responseTail.detach(), len, true);
    }
};

IHpccProtocolResponse *createProtocolResponse(const char *queryname, SafeSocket *client, HttpHelper &httpHelper, const IContextLogger &logctx, unsigned protocolFlags, PTreeReaderOptions xmlReadFlags)
{
    if (protocolFlags & HPCC_PROTOCOL_NATIVE_RAW || protocolFlags & HPCC_PROTOCOL_NATIVE_ASCII)
        return new CHpccNativeProtocolResponse(queryname, client, MarkupFmt_Unknown, protocolFlags, false, logctx, xmlReadFlags);
    else if (httpHelper.queryContentFormat()==MarkupFmt_JSON)
        return new CHpccJsonResponse(queryname, client, protocolFlags, httpHelper.isHttp(), logctx, xmlReadFlags);
    return new CHpccXmlResponse(queryname, client, protocolFlags, httpHelper.isHttp(), logctx, xmlReadFlags);

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
    unsigned &slaveReplyLen;
    CriticalSection crit;
    unsigned flags;

public:
    CHttpRequestAsyncFor(const char *_queryName, IHpccProtocolMsgSink *_sink, IHpccProtocolMsgContext *_msgctx, IArrayOf<IPropertyTree> &_requestArray,
            SafeSocket &_client, HttpHelper &_httpHelper, unsigned _flags, unsigned &_memused, unsigned &_slaveReplyLen, const char *_queryText, const IContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags, const char *_querySetName)
    : sink(_sink), msgctx(_msgctx), requestArray(_requestArray), client(_client), httpHelper(_httpHelper), memused(_memused),
      slaveReplyLen(_slaveReplyLen), logctx(_logctx), xmlReadFlags(_xmlReadFlags), querySetName(_querySetName), flags(_flags)
    {
        queryName = _queryName;
        queryText = _queryText;
    }

    IMPLEMENT_IINTERFACE;

    void onException(IException *E)
    {
        //if (!logctx.isBlind())
        //    logctx.CTXLOG("FAILED: %s", queryText);
        StringBuffer error("EXCEPTION: ");
        E->errorMessage(error);
        DBGLOG("%s", error.str());
        client.checkSendHttpException(httpHelper, E, queryName);
        E->Release();
    }

    void Do(unsigned idx)
    {
        try
        {
            IPropertyTree &request = requestArray.item(idx);
            Owned<IHpccProtocolResponse> protocol = createProtocolResponse(request.queryName(), &client, httpHelper, logctx, flags, xmlReadFlags);
            sink->onQueryMsg(msgctx, &request, protocol, flags, xmlReadFlags, querySetName, idx, memused, slaveReplyLen);
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

//ADF - Haven't changed it yet, but this should eliminate the need to parse the query twice below
//I can load the query and lookup the parse flags before doing a full parse
//if it turns out I need more info I may delete this.
class QueryNameExtractor : public CInterface, implements IPTreeNotifyEvent
{
public:
    TextMarkupFormat mlFmt;
    StringAttr prefix;
    StringAttr name;
    unsigned headerDepth;
    bool isSoap;
    bool isRequestArray;
    bool stripWhitespace;
    bool more;

public:
    IMPLEMENT_IINTERFACE;

    QueryNameExtractor(TextMarkupFormat _mlFmt, bool _stripWhitespace) : mlFmt(_mlFmt), headerDepth(0), isSoap(false), isRequestArray(false), stripWhitespace(_stripWhitespace), more(true)
    {
    }
    void extractName(const char *msg, const IContextLogger &logctx, const char *peer, unsigned port)
    {
        Owned<IPullPTreeReader> parser;
        if (mlFmt==MarkupFmt_JSON)
            parser.setown(createPullJSONStringReader(msg, *this));
        else if (mlFmt==MarkupFmt_XML)
            parser.setown(createPullXMLStringReader(msg, *this));
        if (!parser)
            return;
        while (more && parser->next());
        if (name.isEmpty())
        {
            const char *fmt = mlFmt==MarkupFmt_XML ? "XML" : "JSON";
            IException *E = MakeStringException(-1, "ERROR: Invalid %s received from %s:%d - %s queryName not found", fmt, peer, port, msg);
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
            name.set(nameStr.str(), nameStr.length() - strlen("Request"));
        }
    }
    virtual void beginNode(const char *tag, offset_t startOffset)
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
        if (!name.isEmpty() && streq(attr, "@_stripWhitespaceFromStoredDataset"))
        {
            stripWhitespace = strToBool(value);
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
        else if (headerDepth && streq(tag, "Header"))
            headerDepth--;
    }

};

static Owned<IActiveQueryLimiterFactory> queryLimiterFactory;

class RoxieSocketWorker : public ProtocolQueryWorker
{
    SocketEndpoint ep;
    Owned<SafeSocket> client;
    Owned<IHpccNativeProtocolMsgSink> sink;

public:
    IMPLEMENT_IINTERFACE;

    RoxieSocketWorker(ProtocolSocketListener *_pool, SocketEndpoint &_ep)
        : ProtocolQueryWorker(_pool), ep(_ep)
    {
        sink.set(dynamic_cast<IHpccNativeProtocolMsgSink*>(_pool->queryMsgSink()));
    }

    //  interface IPooledThread
    virtual void init(void *_r)
    {
        client.setown(new CSafeSocket((ISocket *) _r));
        ProtocolQueryWorker::init(_r);
    }

    virtual void main()
    {
        doMain("");
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

    void skipProtocolRoot(Owned<IPropertyTree> &queryPT, HttpHelper &httpHelper, StringAttr &queryName, bool &isRequest, bool &isRequestArray)
    {
        if (queryPT)
        {
            queryName.set(queryPT->queryName());
            isRequest = false;
            isRequestArray = false;
            if (httpHelper.isHttp())
            {
                if (httpHelper.queryContentFormat()==MarkupFmt_JSON)
                {
                    if (strieq(queryName, "__object__"))
                    {
                        queryPT.setown(queryPT->getPropTree("*[1]"));
                        queryName.set(queryPT->queryName());
                        isRequest = true;
                        if (!queryPT)
                            throw MakeStringException(ROXIE_DATA_ERROR, "Malformed JSON request (missing Body)");
                    }
                    else if (strieq(queryName, "__array__"))
                        throw MakeStringException(ROXIE_DATA_ERROR, "JSON request array not implemented");
                    else
                        throw MakeStringException(ROXIE_DATA_ERROR, "Malformed JSON request");
                }
                else
                {
                    if (strieq(queryName, "envelope"))
                        queryPT.setown(queryPT->getPropTree("Body/*"));
                    else if (!strnicmp(httpHelper.queryContentType(), "application/soap", strlen("application/soap")))
                        throw MakeStringException(ROXIE_DATA_ERROR, "Malformed SOAP request");
                    else
                        httpHelper.setUseEnvelope(false);
                    if (!queryPT)
                        throw MakeStringException(ROXIE_DATA_ERROR, "Malformed SOAP request (missing Body)");
                    String reqName(queryPT->queryName());
                    queryPT->removeProp("@xmlns:m");

                    // following code is moved from main() - should be no performance hit
                    String requestString("Request");
                    String requestArrayString("RequestArray");

                    if (reqName.endsWith(requestArrayString))
                    {
                        isRequestArray = true;
                        queryName.set(reqName.str(), reqName.length() - requestArrayString.length());
                    }
                    else if (reqName.endsWith(requestString))
                    {
                        isRequest = true;
                        queryName.set(reqName.str(), reqName.length() - requestString.length());
                    }
                    else
                        queryName.set(reqName.str());

                    queryPT->renameProp("/", queryName.get());  // reset the name of the tree
                }
            }
        }
    }

    void sanitizeQuery(Owned<IPropertyTree> &queryPT, StringAttr &queryName, StringBuffer &saniText, HttpHelper &httpHelper, const char *&uid, bool &isRequest, bool &isRequestArray, bool &isBlind, bool &isDebug)
    {
        if (queryPT)
        {
            skipProtocolRoot(queryPT, httpHelper, queryName, isRequest, isRequestArray);

            // convert to XML with attribute values in single quotes - makes replaying queries easier
            uid = queryPT->queryProp("@uid");
            if (!uid)
                uid = queryPT->queryProp("_TransactionId");
            isBlind = queryPT->getPropBool("@blind", false) || queryPT->getPropBool("_blind", false);
            isDebug = queryPT->getPropBool("@debug") || queryPT->getPropBool("_Probe", false);
            toXML(queryPT, saniText, 0, isBlind ? (XML_SingleQuoteAttributeValues | XML_Sanitize) : XML_SingleQuoteAttributeValues);
        }
        else
            throw MakeStringException(ROXIE_DATA_ERROR, "Malformed request");
    }
    void parseQueryPTFromString(Owned<IPropertyTree> &queryPT, HttpHelper &httpHelper, const char *text, PTreeReaderOptions options)
    {
        if (strieq(httpHelper.queryContentType(), "application/json"))
            queryPT.setown(createPTreeFromJSONString(text, ipt_caseInsensitive, options));
        else
            queryPT.setown(createPTreeFromXMLString(text, ipt_caseInsensitive, options));
    }

    void doMain(const char *runQuery)
    {
        StringBuffer rawText(runQuery);
        unsigned memused = 0;
        IpAddress peer;
        bool continuationNeeded = false;
        bool isStatus = false;

        Owned<IHpccProtocolMsgContext> msgctx = sink->createMsgContext(startTime);
        IContextLogger &logctx = *msgctx->queryLogContext();

readAnother:
        unsigned slavesReplyLen = 0;
        StringArray allTargets;
        sink->getTargetNames(allTargets);
        HttpHelper httpHelper(&allTargets);
        try
        {
            if (client)
            {
                client->querySocket()->getPeerAddress(peer);
                if (!client->readBlock(rawText.clear(), WAIT_FOREVER, &httpHelper, continuationNeeded, isStatus, global->maxBlockSize))
                {
                    if (traceLevel > 8)
                    {
                        StringBuffer b;
                        DBGLOG("No data reading query from socket");
                    }
                    client.clear();
                    return;
                }
            }
            if (continuationNeeded)
            {
                qstart = msTick();
                time(&startTime);
            }
        }
        catch (IException * E)
        {
            if (traceLevel > 0)
            {
                StringBuffer b;
                DBGLOG("Error reading query from socket: %s", E->errorMessage(b).str());
            }
            E->Release();
            client.clear();
            return;
        }

        bool isHTTP = httpHelper.isHttp();
        TextMarkupFormat mlFmt = isHTTP ? httpHelper.queryContentFormat() : MarkupFmt_XML;

        bool failed = false;
        bool isRequest = false;
        bool isRequestArray = false;
        bool isBlind = false;
        bool isDebug = false;
        unsigned protocolFlags = isHTTP ? 0 : HPCC_PROTOCOL_NATIVE;

        Owned<IPropertyTree> queryPT;
        StringBuffer sanitizedText;
        StringBuffer peerStr;
        peer.getIpText(peerStr);
        const char *uid = "-";

        StringAttr queryName;
        StringAttr queryPrefix;
        bool stripWhitespace = msgctx->getStripWhitespace();
        if (mlFmt==MarkupFmt_XML || mlFmt==MarkupFmt_JSON)
        {
            QueryNameExtractor extractor(mlFmt, stripWhitespace);
            extractor.extractName(rawText.str(), logctx, peerStr, ep.port);
            queryName.set(extractor.name);
            queryPrefix.set(extractor.prefix);
            stripWhitespace = extractor.stripWhitespace;
        }
        try
        {
            if (streq(queryPrefix.str(), "control"))
            {
                if (httpHelper.isHttp())
                    client->setHttpMode(queryName, false, httpHelper);

                bool aclupdate = strieq(queryName, "aclupdate"); //ugly
                byte iptFlags = aclupdate ? ipt_caseInsensitive : 0;

                if (mlFmt==MarkupFmt_JSON)
                    queryPT.setown(createPTreeFromJSONString(rawText.str(), iptFlags, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_ignoreNameSpaces)));
                else
                    queryPT.setown(createPTreeFromXMLString(rawText.str(), iptFlags, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_ignoreNameSpaces)));

                IPropertyTree *root = queryPT;
                skipProtocolRoot(queryPT, httpHelper, queryName, isRequest, isRequestArray);
                if (!strchr(queryName, ':'))
                {
                    VStringBuffer fullname("control:%s", queryName.str()); //just easier to keep for debugging and internal checking
                    queryPT->renameProp("/", fullname);
                }
                Owned<IHpccProtocolResponse> protocol = createProtocolResponse(queryPT->queryName(), client, httpHelper, logctx, protocolFlags, global->defaultXmlReadFlags);
                sink->onControlMsg(msgctx, queryPT, protocol);
                protocol->finalize(0);
                if (streq(queryName, "lock") || streq(queryName, "childlock"))
                    goto readAnother;
            }
            else if (isStatus)
            {
                client->write("OK", 2);
            }
            else
            {
                unsigned readFlags = (unsigned) global->defaultXmlReadFlags | ptr_ignoreNameSpaces;
                readFlags &= ~ptr_ignoreWhiteSpace;
                readFlags |= (stripWhitespace ? ptr_ignoreWhiteSpace : ptr_none);
                try
                {
                    parseQueryPTFromString(queryPT, httpHelper, rawText.str(), (PTreeReaderOptions)readFlags);
                }
                catch (IException *E)
                {
                    logctx.logOperatorException(E, __FILE__, __LINE__, "Invalid XML received from %s:%d - %s", peerStr.str(), listener->queryPort(), rawText.str());
                    logctx.CTXLOG("ERROR: Invalid XML received from %s:%d - %s", peerStr.str(), listener->queryPort(), rawText.str());
                    throw;
                }

                uid = NULL;
                sanitizeQuery(queryPT, queryName, sanitizedText, httpHelper, uid, isRequest, isRequestArray, isBlind, isDebug);
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
                    if (msgctx->initQuery(querySetName, queryName))
                    {
                        int bindCores = queryPT->getPropInt("@bindCores", msgctx->getBindCores());
                        if (bindCores > 0)
                            listener->setThreadAffinity(bindCores);
                        IArrayOf<IPropertyTree> requestArray;
                        if (isHTTP)
                        {
                            mlFmt = httpHelper.queryContentFormat();
                            if (isRequestArray)
                            {
                                StringBuffer reqIterString;
                                reqIterString.append(queryName).append("Request");

                                Owned<IPropertyTreeIterator> reqIter = queryPT->getElements(reqIterString.str());
                                ForEach(*reqIter)
                                {
                                    IPropertyTree *fixedreq = createPTree(queryName, ipt_caseInsensitive);
                                    Owned<IPropertyTreeIterator> iter = reqIter->query().getElements("*");
                                    ForEach(*iter)
                                    {
                                        fixedreq->addPropTree(iter->query().queryName(), LINK(&iter->query()));
                                    }
                                    requestArray.append(*fixedreq);
                                }
                            }
                            else
                            {
                                IPropertyTree *fixedreq = createPTree(queryName, ipt_caseInsensitive);
                                Owned<IPropertyTreeIterator> iter = queryPT->getElements("*");
                                ForEach(*iter)
                                {
                                    fixedreq->addPropTree(iter->query().queryName(), LINK(&iter->query()));
                                }
                                requestArray.append(*fixedreq);
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
                                    mlFmt = MarkupFmt_Unknown;
                                }
                                else if (stricmp(format, "bxml") == 0)
                                {
                                    protocolFlags |= HPCC_PROTOCOL_BLOCKED;
                                }
                                else if (stricmp(format, "ascii") == 0)
                                {
                                    protocolFlags |= HPCC_PROTOCOL_NATIVE_ASCII;
                                    mlFmt = MarkupFmt_Unknown;
                                }
                                else if (stricmp(format, "xml") != 0) // xml is the default
                                    throw MakeStringException(ROXIE_INVALID_INPUT, "Unsupported format specified: %s", format);
                            }
                            if (queryPT->getPropBool("@trim", false))
                                protocolFlags |= HPCC_PROTOCOL_TRIM;
                            msgctx->setIntercept(queryPT->getPropBool("@log", false));
                            msgctx->setTraceLevel(queryPT->getPropInt("@traceLevel", logctx.queryTraceLevel()));
                        }

                        msgctx->noteQueryActive();

                        if (isHTTP)
                        {
                            CHttpRequestAsyncFor af(queryName, sink, msgctx, requestArray, *client, httpHelper, protocolFlags, memused, slavesReplyLen, sanitizedText, logctx, (PTreeReaderOptions)readFlags, querySetName);
                            af.For(requestArray.length(), global->numRequestArrayThreads);
                        }
                        else
                        {
                            Owned<IHpccProtocolResponse> protocol = createProtocolResponse(queryPT->queryName(), client, httpHelper, logctx, protocolFlags, (PTreeReaderOptions)readFlags);
                            sink->onQueryMsg(msgctx, queryPT, protocol, protocolFlags, (PTreeReaderOptions)readFlags, querySetName, 0, memused, slavesReplyLen);
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
        unsigned elapsed = msTick() - qstart;
        if (continuationNeeded)
        {
            rawText.clear();
            goto readAnother;
        }
        else
        {
            try
            {
                if (client && !isHTTP && !isStatus)
                {
                    if (msgctx->getIntercept())
                    {
                        FlushingStringBuffer response(client, (protocolFlags & HPCC_PROTOCOL_BLOCKED), mlFmt, (protocolFlags & HPCC_PROTOCOL_NATIVE_RAW), false, logctx);
                        response.startDataset("Tracing", NULL, (unsigned) -1);
                        msgctx->outputLogXML(response);
                    }
                    unsigned replyLen = 0;
                    client->write(&replyLen, sizeof(replyLen));
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

IHpccProtocolListener *createProtocolListener(const char *protocol, IHpccProtocolMsgSink *sink, unsigned port, unsigned listenQueue)
{
    if (traceLevel)
        DBGLOG("Creating Roxie socket listener, protocol %s, pool size %d, listen queue %d%s", protocol, sink->getPoolSize(), listenQueue, sink->getIsSuspended() ? " SUSPENDED":"");
    return new ProtocolSocketListener(sink, port, listenQueue);
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


//================================================================================================================================
