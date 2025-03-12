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

#include <platform.h>
#include <stdio.h>
#include "jmisc.hpp"
#include "jlib.hpp"
#include "jsocket.hpp"
#include "jstream.ipp"
#include "portlist.h"
#include "jdebug.hpp"
#include "jthread.hpp"
#include "jfile.hpp"
#include "securesocket.hpp"

#include "rmtclient.hpp"

bool abortEarly = false;
bool forceHTTP = false;
bool useSSL = false;
bool abortAfterFirst = false;
bool echoResults = false;
bool saveResults = true;
bool showTiming = false;
bool showStatus = true;
bool sendToSocket = false;
bool parallelBlocked = false;
bool justResults = false;
bool multiThread = false;
bool manyResults = false;
bool sendFileAfterQuery = false;
bool doLock = false;
bool roxieLogMode = false;
bool rawOnly = false;
bool rawSend = false;
bool remoteStreamQuery = false;
bool remoteStreamForceResend = false;
bool remoteStreamSendCursor = false;
bool autoXML = true;
bool generateClientSpans = false;
StringBuffer tracingConfig;
int verboseDbgLevel = 0;

StringBuffer sendFileName;
StringAttr queryNameOverride;

unsigned delay = 0;
unsigned runningQueries;
unsigned multiThreadMax;
unsigned maxLineSize = 10000000;

Owned<ISocket> persistSocket;
bool persistConnections = false;
Owned<ISecureSocketContext> persistSecureContext;
Owned<ISecureSocket> persistSSock;

int repeats = 0;
StringBuffer queryPrefix;

Semaphore okToSend;
Semaphore done;
Semaphore finishedReading;
FILE * trace;
CriticalSection traceCrit;

unsigned queryDelayMS = 0;
unsigned queryAbsDelayMS = 0;  // ex: -u0 -qd 1000 for 1 q/s ...
unsigned totalQueryCnt = 0;
double totalQueryMS = 0.0;

Owned<ISpan> serverSpan;
//---------------------------------------------------------------------------

void SplitIpPort(StringAttr & ip, unsigned & port, const char * address)
{
    const char * colon = strchr(address, ':');
    if (colon)
    {
        ip.set(address,colon-address);
        port = atoi(colon+1);
    }
    else
        ip.set(address);
}

void showMessage(const char * text)
{
    if (!justResults)
    {
        if (echoResults)
            fwrite(text, strlen(text), 1, stdout);
        if (saveResults && trace != NULL)
            fwrite(text, strlen(text), 1, trace);
    }
}

void sendFile(const char * filename, ISocket * socket) 
{
    FILE *in = fopen(filename, "rb");
    unsigned size = 0;
    void * buff = NULL;

    if (in)
    {
        fseek(in, 0, SEEK_END);
        size = ftell(in);
        fseek(in, 0, SEEK_SET);
        buff = malloc(size);
        size_t numRead = fread(buff, 1, size, in);
        fclose(in);
        if (numRead != size)
		{
            printf("read from file %s failed (%u/%u)\n", filename, (unsigned)numRead, size);
            size = 0;
		}
    }
	else
		printf("read from file %s failed\n", filename);

    unsigned dllLen = size;
    _WINREV(dllLen);
    socket->write(&dllLen, sizeof(dllLen));
    socket->write(buff, size);
    free(buff);
}

#define CHUNK_SIZE 152*2000

void sendFileChunk(const char * filename, offset_t offset, ISocket * socket) 
{
    FILE *in = fopen(filename, "rb");
    unsigned size = 0;
    void * buff = NULL;

    if (in)
    {
        fseek(in, 0, SEEK_END);
        offset_t endOffset = ftell(in);
        fseek(in, offset, SEEK_SET);
        if (endOffset < offset)
            size = 0;
        else
            size = (unsigned)(endOffset - offset);
        if (size > CHUNK_SIZE)
            size = CHUNK_SIZE;
        buff = malloc(size);
        size_t numRead = fread(buff, 1, size, in);
        fclose(in);
        if (numRead != size)
		{
            printf("read from file %s failed (%u/%u)\n", filename, (unsigned)numRead, size);
            size = 0;
		}
    }
	else
		printf("read from file %s failed\n", filename);


    if (size > 0)
    {
        MemoryBuffer sendBuffer;
        unsigned rev = size + strlen(filename) + 10;
        rev |= 0x80000000;
        _WINREV(rev);
        sendBuffer.append(rev);
        sendBuffer.append('R');
        rev = 0; // should put the sequence number here
        _WINREV(rev);
        sendBuffer.append(rev);
        rev = 0; // should put the # of recs in msg here
        _WINREV(rev);
        sendBuffer.append(rev);
        sendBuffer.append(strlen(filename)+1, filename);
        sendBuffer.append(size, buff);

        socket->write(sendBuffer.toByteArray(), sendBuffer.length());
    }
    else
    {
        unsigned zeroLen = 0;
        socket->write(&zeroLen, sizeof(zeroLen));
    }

    free(buff);
}

int readResults(ISocket * socket, bool readBlocked, bool useHTTP, StringBuffer &result, const char *query, size32_t queryLen)
{
    if (readBlocked)
        socket->set_block_mode(BF_SYNC_TRANSFER_PULL,0,60*1000);

    StringBuffer remoteReadCursor;
    unsigned len;
    bool is_status;
    bool isBlockedResult;
    for (;;)
    {
        if (delay)
            MilliSleep(delay);
        is_status = false;
        isBlockedResult = false;
        try
        {
            if (useHTTP)
                len = 0x10000;
            else if (readBlocked)
                len = socket->receive_block_size();
            else
            {
                socket->read(&len, sizeof(len));
                _WINREV(len);                    
            }
        }
        catch(IException * e)
        {
            if (manyResults)
                showMessage("End of result multiple set\n");
            else
                pexception("failed to read len data", e);
            e->Release();
            return 1;
        }

        if (len == 0)
        {
            if (manyResults)
            {
                showMessage("----End of result set----\n");
                continue;
            }
            break;
        }

        bool pluginRequest = false;
        bool dataBlockRequest = false;
        bool remoteReadRequest = false;
        if (len & 0x80000000)
        {
            unsigned char flag;
            socket->read(&flag, sizeof(flag));
            switch (flag)
            {
            case '-':
                if (echoResults)
                    fputs("Error:", stdout);
                if (saveResults && trace != NULL)
                    fputs("Error:", trace);
                break;
            case 'D':
                showMessage("request for datablock\n");
                dataBlockRequest = true;
                break;
            case 'P':
                showMessage("request for plugin\n");
                pluginRequest = true;
                break;
            case 'S':
                 if (showStatus)
                 showMessage("Status:");
                 is_status=true;
                 break;
            case 'T':
                 showMessage("Timing:\n");
                 break;
            case 'X':
                showMessage("---Compound query finished---\n");
                return 1;
            case 'R':
                isBlockedResult = true;
                break;
            case 'J':
                remoteReadRequest = true;
                break;
            }
            len &= 0x7FFFFFFF;
            len--;      // flag already read
        }

        MemoryBuffer mb;
        mb.setEndian(__BIG_ENDIAN);
        char *mem = (char *)mb.reserveTruncate(len+1);
        char * t = mem;
        size32_t sendlen = len;
        t[len]=0;
        try
        {
            if (useHTTP)
            {
                try
                {
                    socket->read(t, 1, len, sendlen);
                }
                catch (IException *E)
                {
                    if (E->errorCode()!= JSOCKERR_graceful_close)
                        throw;
                    E->Release();
                    break;
                }
                if (!sendlen)
                    break;
            }
            else if (readBlocked)
                socket->receive_block(t, len); 
            else
                socket->read(t, len);
        }
        catch(IException * e)
        {
            pexception("failed to read data", e);
            e->Release();
            return 1;
        }
        if (pluginRequest)
        {
            //Not very robust!  A poor man's implementation for testing...
            StringBuffer dllname, libname;
            const char * dot = strchr(t, '.');
            dllname.append("\\edata\\bin\\debug\\").append(t);
            libname.append("\\edata\\bin\\debug\\").append(dot-t,t).append(".lib");

            sendFile(dllname.str(), socket);
            sendFile(libname.str(), socket);
        }
        else if (dataBlockRequest)
        {
            //Not very robust!  A poor man's implementation for testing...
            offset_t offset;
            mb.read(offset);
            sendFileChunk((const char *)mb.readDirect(offset), offset, socket);
        }
        else if (remoteReadRequest)
        {
            auto cmd = queryRemoteStreamCmd();
            size32_t remoteStreamCmdSz = sizeof(cmd);
            size32_t jsonQueryLen = queryLen - remoteStreamCmdSz;
            const char *jsonQuery = query + remoteStreamCmdSz;
            Owned<IPropertyTree> requestTree = createPTreeFromJSONString(jsonQueryLen, jsonQuery);
            Owned<IPropertyTree> responseTree; // used if response is xml or json
            const char *outputFmtStr = requestTree->queryProp("format");
            const char *response = nullptr;
            if (!outputFmtStr || strieq("xml", outputFmtStr))
            {
                response = (const char *)mb.readDirect(len);
                responseTree.setown(createPTreeFromXMLString(len, response));
                assertex(responseTree);
            }
            else if (strieq("json", outputFmtStr))
            {
                response = (const char *)mb.readDirect(len);
                // JCSMORE - json string coming back from IXmlWriterExt is always rootless the moment, so workaround it by supplying ptr_noRoot to reader
                // writer should be fixed.
                Owned<IPropertyTree> tree = createPTreeFromJSONString(len, response, ipt_none, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_noRoot));
                responseTree.setown(tree->getPropTree("Response"));
                assertex(responseTree);
            }
            else if (!strieq("binary", outputFmtStr))
                throw MakeStringException(0, "Unknown output format: %s", outputFmtStr);
            unsigned cursorHandle;
            if (responseTree)
                cursorHandle = responseTree->getPropInt("handle");
            else
                mb.read(cursorHandle);
            bool retrySend = false;
            if (cursorHandle)
            {
                PROGLOG("Got handle back: %u; len=%u", cursorHandle, len);
                StringBuffer xml;
                if (responseTree)
                {
                    if (echoResults && response)
                    {
                        fputs(response, stdout);
                        fflush(stdout);
                    }
                    if (!responseTree->getProp("cursorBin", remoteReadCursor))
                        break;
                }
                else
                {
                    size32_t dataLen;
                    mb.read(dataLen);
                    if (!dataLen)
                        break;
                    [[maybe_unused]] const void *rowData = mb.readDirect(dataLen);
                    // JCSMORE - output binary row data?

                    // cursor
                    size32_t cursorLen;
                    mb.read(cursorLen);
                    if (!cursorLen)
                        break;
                    const void *cursor = mb.readDirect(cursorLen);
                    JBASE64_Encode(cursor, cursorLen, remoteReadCursor, true);
                }

                if (remoteStreamForceResend)
                    cursorHandle = NotFound; // fake that it's a handle dafilesrv doesn't know about

                Owned<IPropertyTree> requestTree = createPTree();
                requestTree->setPropInt("handle", cursorHandle);

                // Only the handle is needed for continuation, but this tests the behaviour of some clients which may send cursor per request (e.g. to refresh)
                if (remoteStreamSendCursor)
                    requestTree->setProp("cursorBin", remoteReadCursor);

                requestTree->setProp("format", outputFmtStr);
                StringBuffer requestStr;
                requestStr.append(queryRemoteStreamCmd());
                toJSON(requestTree, requestStr);

                if (verboseDbgLevel > 0)
                {
                    fputs("\nNext request:", stdout);
                    fputs(requestStr.str()+remoteStreamCmdSz, stdout);
                    fputs("\n", stdout);
                    fflush(stdout);
                }

                sendlen = requestStr.length();
                _WINREV(sendlen);

                try
                {
                    if (!rawSend && !useHTTP)
                        socket->write(&sendlen, sizeof(sendlen));
                    socket->write(requestStr.str(), requestStr.length());
                }
                catch (IJSOCK_Exception *e)
                {
                    retrySend = true;
                    EXCLOG(e, nullptr);
                    e->Release();
                }
            }
            else // dafilesrv didn't know who I was, resent query + serialized cursor
                retrySend = true;
            if (retrySend)
            {
                PROGLOG("Retry send for handle: %u", cursorHandle);
                requestTree->setProp("cursorBin", remoteReadCursor);
                StringBuffer requestStr;
                requestStr.append(queryRemoteStreamCmd());
                toJSON(requestTree, requestStr);

                PROGLOG("requestStr = %s", requestStr.str()+remoteStreamCmdSz);
                sendlen = requestStr.length();
                _WINREV(sendlen);
                if (!rawSend && !useHTTP)
                    socket->write(&sendlen, sizeof(sendlen));
                socket->write(requestStr.str(), requestStr.length());
            }
        }
        else
        {
            if (isBlockedResult)
            {
                t += 8;
                t += strlen(t)+1;
                sendlen -= (t - mem);
            }
            if (echoResults && (!is_status || showStatus))
            {
                fwrite(t, sendlen, 1, stdout);
                fflush(stdout);
            }
            if (!is_status)
                result.append(sendlen, t);
        }

        if (abortAfterFirst)
            return 0;
    }
    return 0;
}


class ReceiveThread : public Thread
{
public:
    virtual int run();
};

int ReceiveThread::run()
{
    ISocket * socket = ISocket::create(3456);
    ISocket * client = socket->accept();
    StringBuffer result;
    readResults(client, parallelBlocked, false, result, nullptr, 0);
    client->Release();
    socket->Release();
    finishedReading.signal();
    return 0;
}

//------------------------------------------------------------------------

/**
 *  Return: 0 - success
 *          nonzero - error
 */

int doSendQuery(const char * ip, unsigned port, const char * base)
{
    OwnedActiveSpanScope clientSpan(serverSpan->createClientSpan("testsocket_sendQuery"));
    Owned<ISocket> socket;
    Owned<ISecureSocketContext> secureContext;
    __int64 starttime, endtime;
    StringBuffer ipstr;
    CTimeMon tm;
    if (queryDelayMS)
        tm.reset(queryDelayMS);

    try
    {
        if (strcmp(ip, ".")==0)
            ip = GetCachedHostName();
        else
        {
            const char *dash = strchr(ip, '-');
            if (dash && isdigit(dash[1]) && dash>ip && isdigit(dash[-1]))
            {
                if (persistConnections)
                    UNIMPLEMENTED;
                const char *startrange = dash-1;
                while (isdigit(startrange[-1]))
                    startrange--;
                char *endptr;
                unsigned firstnum = atoi(startrange);
                unsigned lastnum = strtol(dash+1, &endptr, 10);
                if (lastnum > firstnum)
                {
                    static unsigned counter;
                    static CriticalSection counterCrit;
                    CriticalBlock b(counterCrit);
                    ipstr.append(startrange - ip, ip).append((counter++ % (lastnum+1-firstnum)) + firstnum).append(endptr);
                    ip = ipstr.str();
                    printf("Sending to %s\n", ip);
                }
            }
        }
        starttime= get_cycles_now();
        if (persistConnections)
        {
            if (!persistSocket)
            {
                SocketEndpoint ep(ip,port);
                persistSocket.setown(ISocket::connect_timeout(ep, 1000));
                if (useSSL)
                {
#ifdef _USE_OPENSSL
                    if (!persistSecureContext)
                        persistSecureContext.setown(createSecureSocketContext(ClientSocket));
                    persistSSock.setown(persistSecureContext->createSecureSocket(persistSocket.getClear(), SSLogNormal, ip));
                    int res = persistSSock->secure_connect();
                    if (res < 0)
                        throw MakeStringException(-1, "doSendQuery : Failed to establish secure connection");
                    persistSocket.setown(persistSSock.getClear());
#else
                    throw MakeStringException(-1, "OpenSSL disabled in build");
#endif
                }
            }
            socket = persistSocket;
        }
        else
        {
            SocketEndpoint ep(ip,port);
            socket.setown(ISocket::connect_timeout(ep, 100000));
            if (useSSL)
            {
#ifdef _USE_OPENSSL
                secureContext.setown(createSecureSocketContext(ClientSocket));
                Owned<ISecureSocket> ssock = secureContext->createSecureSocket(socket.getClear(), SSLogNormal, ip);
                int res = ssock->secure_connect();
                if (res < 0)
                    throw MakeStringException(-1, "doSendQuery : Failed to establish secure connection");
                socket.setown(ssock.getClear());
#else
                throw MakeStringException(1, "OpenSSL disabled in build");
#endif
            }
        }
    }
    catch(IException * e)
    {
        pexception("failed to connect to server", e);
        return 1;
    }

    StringBuffer fullQuery;
    bool useHTTP = forceHTTP || strstr(base, "<soap:Envelope") != NULL;
    if (useHTTP)
    {
        StringBuffer newQuery;
        Owned<IPTree> p = createPTreeFromXMLString(base, ipt_none, ptr_none);
        const char *queryName = p->queryName();
        if ((stricmp(queryName, "envelope") != 0) && (stricmp(queryName, "envelope") != 0))
        {
            if (queryNameOverride.length())
                queryName = queryNameOverride;
            newQuery.appendf("<Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"><Body><%sRequest>", queryName);
            Owned<IPTreeIterator> elements = p->getElements("./*");
            ForEach(*elements)
            {
                IPTree &elem = elements->query();
                toXML(&elem, newQuery, 0, XML_SingleQuoteAttributeValues);
            }
            newQuery.appendf("</%sRequest></Body></Envelope>", queryName);
            base = newQuery.str();
        }

        StringBuffer tpHeader;
        if (generateClientSpans && clientSpan->isValid())
        {
            Owned<IProperties> clientHeaders = createProperties();
            clientSpan->getClientHeaders(clientHeaders.get());

            if (clientHeaders->hasProp("traceparent"))
                tpHeader.appendf("--header traceparent: %s\r\n", clientHeaders->queryProp("traceparent"));
        }

        // note - don't support queryname override unless original query is xml
        fullQuery.appendf("POST /doc HTTP/1.0\r\n%sContent-Type: application/x-www-form-urlencoded\r\nContent-Length: %d\r\n\r\n", tpHeader.str(), (int) strlen(base)).append(base);
    }
    else
    {
        if (sendToSocket)
        {
            Thread * receive = new ReceiveThread();
            //MORE: The caller should really join this thread before terminating
            receive->start(false);
            receive->Release();
        }

        if (doLock)
        {
            const char *lock = "<control:lock/>";
            unsigned locklen = strlen(lock);
            _WINREV(locklen);
            socket->write(&locklen, sizeof(locklen));
            socket->write(lock, strlen(lock));
            StringBuffer lockResult;
            readResults(socket, false, false, lockResult, nullptr, 0);
        }
        if (queryNameOverride.length())
        {
            try
            {
                Owned<IPTree> p = createPTreeFromXMLString(base, ipt_none, ptr_none);
                p->renameProp("/", queryNameOverride);
                toXML(p, fullQuery.clear());
            }
            catch (IException *E)
            {
                StringBuffer s;
                printf("Error: %s", E->errorMessage(s).str());
                E->Release();
                return 1;
            }
        }
        else
        {
            if (remoteStreamQuery)
                fullQuery.append(queryRemoteStreamCmd());
            fullQuery.append(base);
        }

        if (generateClientSpans && clientSpan->isValid())
        {
            Owned<IProperties> retrievedClientHeaders = createProperties();
            clientSpan->getClientHeaders(retrievedClientHeaders.get());

            if (retrievedClientHeaders->hasProp("traceparent"))
            {
                Owned<IPTree> fullQueryTree = createPTreeFromXMLString(fullQuery.str(), ipt_none, ptr_none);

                IPropertyTree * traceHeaderTree = fullQueryTree->queryPropTree("_trace");
                if (traceHeaderTree) //query included _trace header, just overwrite the traceparent entry
                {
                    traceHeaderTree->setProp("traceparent", retrievedClientHeaders->queryProp("traceparent"));
                }
                else // no _trace header in the original query, ensure _trace branch and set traceparent
                {
                    fullQueryTree->setPropTree("_trace");
                    fullQueryTree->setProp("_trace/traceparent", retrievedClientHeaders->queryProp("traceparent"));
                }

                toXML(fullQueryTree, fullQuery.clear(), 0, XML_SingleQuoteAttributeValues); //rebuild the xml with _trace
            }
        }
    }

    const char * query = fullQuery.str();

    size32_t queryLen=(size32_t)strlen(query);
    size32_t len = queryLen;
    size32_t sendlen = len;
    if (persistConnections)
        sendlen |= 0x80000000;
    _WINREV(sendlen);

    try
    {
        if (!rawSend && !useHTTP)
            socket->write(&sendlen, sizeof(sendlen));

        if (verboseDbgLevel > 0)
        {
            fprintf(stdout, "about to write %u <%s>\n", len, query);
            fflush(stdout);
        }

        socket->write(query, len);

        if (sendFileAfterQuery)
        {
            FILE *in = fopen(sendFileName.str(), "rb");
            if (in)
            {
                char buffer[1024];
                for (;;)
                {
                    len = fread(buffer, 1, sizeof(buffer), in);
                    sendlen = len;
                    _WINREV(sendlen);                    
                    socket->write(&sendlen, sizeof(sendlen));
                    if (!len)
                        break;
                    socket->write(buffer, len);
                }
                fclose(in);
            }
            else
                printf("File %s could not be opened\n", sendFileName.str());
        }
    }
    catch(IException * e)
    {
        pexception("failed to write data", e);
        return 1;
    }

    if (abortEarly)
        return 0;
            
    // back-end does some processing.....

    StringBuffer result;
    int ret = readResults(socket, false, useHTTP, result, query, queryLen);

    if ((ret == 0) && !justResults)
    {
        endtime = get_cycles_now();
        CriticalBlock b(traceCrit);

        if (trace != NULL)
        {
            if (rawOnly == false)
            {
                fprintf(trace, "query: %s\n", query);

                if (saveResults)
                    fprintf(trace, "result: %s\n", result.str());
            }
            else
            {
                fprintf(trace, "%s", result.str());
            }

            double queryTimeMS = (double)(cycle_to_nanosec(endtime - starttime))/1000000;
            totalQueryMS += queryTimeMS;
            totalQueryCnt++;
            if (showTiming && rawOnly == false)
            {
                fprintf(trace, "Time taken = %.3f msecs\n", queryTimeMS);
                fputs("----------------------------------------------------------------------------\n", trace);
            }
        }
    }

    if (!persistConnections)
    {
        socket->close();
    }

    if (queryDelayMS)
    {
        unsigned remaining;
        if (!tm.timedout(&remaining))
            Sleep(remaining);
    }

    return 0;
}


class QueryThread : public Thread
{
public:
    QueryThread(const char * _ip, unsigned _port, const char * _base) : ip(_ip),port(_port),base(_base) {}

    virtual int run()
    {
        doSendQuery(ip, port, base);
        done.signal();
        if (multiThreadMax)
            okToSend.signal();
        return 0;
    }

protected:
    StringAttr      ip;
    unsigned        port;
    StringAttr      base;
};

int sendQuery(const char * ip, unsigned port, const char * base)
{
    if (!multiThread)
        return doSendQuery(ip, port, base);

    if (multiThreadMax)
        okToSend.wait();

    runningQueries++;
    Thread * thread = new QueryThread(ip, port, base);
    //MORE: The caller should really join this thread before terminating
    thread->start(false);
    thread->Release();

    if (multiThread && queryAbsDelayMS && !multiThreadMax)
        Sleep(queryAbsDelayMS);

    return 0;
}

void usage(int exitCode)
{
    printf("testsocket ip<:port> [flags] [query | -f[f] file.sql | -]\n");
    printf("  -         take query from stdin\n");
    printf("  -a        abort before input received\n");
    printf("  -a1       abort after first packet receieved\n");
    printf("  -c        test sending response to a socket\n");
    printf("  -cb       test sending response to a block mode socket\n");
    printf("  -d        force delay after each packet\n");
    printf("  -f        take query from file\n");
    printf("  -ff       take multiple queries from file, one per line\n");
    printf("  -tff      take multiple queries from file, one per line, preceded by the time at which it should be submitted (relative to time on first line)\n");
    printf("  -k        don't save the results to result.txt\n");
    printf("  -m        only save results to result.txt\n");
    printf("  -maxLineSize <n> set maximum query line length\n");
    printf("  -n        multiple results - keep going until socket closes\n");
    printf("  -o        set output filename\n");
    printf("  -or       set output filename for raw output\n");
    printf("  -persist  use persistent connection\n");
    printf("  -pr <text>add a prefix to the query\n");
    printf("  -q        quiet - don't echo query\n");
    printf("  -qname xx Use xx as queryname in place of the xml root element name\n");
    printf("  -qd       delay (ms) to possibly wait between start of query and start of next query\n");
    printf("  -r <n>    repeat the query several times\n");
    printf("  -rl       roxie logfile mode\n");
    printf("  -rs       remote stream request\n");
    printf("  -rsr      force remote stream resend per continuation request\n");
    printf("  -rssc     send cursor per continuation request\n");
    printf("  -s        add stars to indicate transfer packets\n");
    printf("  -ss       suppress XML Status messages to screen (always suppressed from tracefile)\n");
    printf("  -ssl      use ssl\n");
    printf("  -td       add debug timing statistics to trace\n");
    printf("  -tf       add full timing statistics to trace\n");
    printf("  -time     add timing to trace\n");
    printf("  -u<max>   run queries on separate threads\n");
    printf("  -v        debug output\n");
    printf("  -cascade  cascade query (to all roxie nodes)\n");
    printf("  -lock     locked cascade query (to all roxie nodes)\n");
    printf("  -x        raw send\n");
    printf("  -gcs      generate and propagate client spans per request\n");
    printf("  -ctc <YAMLconfig> customized JTrace tracing configuration provided\n");

    exit(exitCode);
}

int main(int argc, char **argv) 
{
    InitModuleObjects();

    StringAttr outputName("result.txt");

    bool fromFile = false;
    bool fromStdIn = false;
    bool fromMultiFile = false;
    bool timedReplay = false;
    if (argc < 2 && !(argc==2 && strstr(argv[1], "::")))
        usage(1);

    int arg = 2;
    bool echoSingle = true;
    while (arg < argc && *argv[arg]=='-')
    {
        if (stricmp(argv[arg], "-time") == 0)
        {
            showTiming = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-a") == 0)
        {
            abortEarly = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-a1") == 0)
        {
            abortAfterFirst = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-c") == 0)
        {
            sendToSocket = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-cb") == 0)
        {
            sendToSocket = true;
            parallelBlocked = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-d") == 0)
        {
            delay  = 300;
            ++arg;
        }
        else if (stricmp(argv[arg], "-http") == 0)
        {
            forceHTTP = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-ssl") == 0)
        {
            useSSL = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-") == 0)
        {
            fromStdIn = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-f") == 0)
        {
            fromFile = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-x") == 0)
        {
            rawSend = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-ff") == 0)
        {
            fromMultiFile = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-tff") == 0)
        {
            fromMultiFile = true;
            timedReplay = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-k") == 0)
        {
            saveResults = false;
            ++arg;
        }
        else if (stricmp(argv[arg], "-m") == 0)
        {
            justResults = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-maxlinesize") == 0)
        {
            ++arg;
            if (arg>=argc)
                usage(1);           
            maxLineSize = atoi(argv[arg]);
            ++arg;
        }
        else if (stricmp(argv[arg], "-n") == 0)
        {
            manyResults = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-o") == 0)
        {
            outputName.set(argv[arg+1]);
            arg+=2;
        }
        else if (stricmp(argv[arg], "-or") == 0)
        {
            rawOnly = true;
            outputName.set(argv[arg+1]);
            arg+=2;
        }
        else if (stricmp(argv[arg], "-persist") == 0)
        {
            persistConnections = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-pr") == 0)
        {
            queryPrefix.append(argv[arg+1]);
            arg+=2;
        }
        else if (stricmp(argv[arg], "-q") == 0)
        {
            echoSingle = false;
            ++arg;
        }
        else if (stricmp(argv[arg], "-qd") == 0)
        {
            ++arg;
            if (arg>=argc)
                usage(1);
            queryDelayMS = atoi(argv[arg]);
            ++arg;
        }
        else if (stricmp(argv[arg], "-qname") == 0)
        {
            queryNameOverride.set(argv[arg+1]);
            arg+=2;
        }
        else if (stricmp(argv[arg], "-r") == 0)
        {
            ++arg;
            if (arg>=argc)
                usage(1);           
            repeats = atoi(argv[arg]);
            ++arg;
        }
        else if (stricmp(argv[arg], "-rl") == 0)
        {
            roxieLogMode = true;
            fromMultiFile = true;
            ++arg;
        }
        else if (stricmp(argv[arg], "-ss") == 0)
        {
            showStatus = false;
            ++arg;
        }
        else if (stricmp(argv[arg], "-v") == 0)
        {
            verboseDbgLevel++;
            ++arg;
        }
        else if (memicmp(argv[arg], "-u", 2) == 0)
        {
            multiThread = true;
            multiThreadMax = atoi(argv[arg]+2);
            if (multiThreadMax)
                okToSend.signal(multiThreadMax);
            ++arg;
        }
        else if (stricmp(argv[arg], "-lock") == 0)
        {
            doLock = true;
            ++arg;
        }
        else if (memicmp(argv[arg], "-z", 2) == 0)
        {
            sendFileAfterQuery = true;
            sendFileName.append(argv[arg+1]);
            OwnedIFile f = createIFile(sendFileName.str());
            if (!f->exists() || f->isFile()==fileBool::foundNo)
            {
                printf("file %s does not exist\n", sendFileName.str());
                exit (EXIT_FAILURE);
            }
            arg+=2;
        }
        else if (strieq(argv[arg], "-rs"))
        {
            remoteStreamQuery = true;
            ++arg;
        }
        else if (strieq(argv[arg], "-rsr"))
        {
            remoteStreamForceResend = true;
            ++arg;
        }
        else if (strieq(argv[arg], "-rssc"))
        {
            remoteStreamSendCursor = true;
            ++arg;
        }
        else if (strieq(argv[arg], "-noxml"))
        {
            autoXML = false;
            ++arg;
        }
        else if (strieq(argv[arg], "-gcs"))
        {
            generateClientSpans = true;
            ++arg;
        }
        else if (strieq(argv[arg], "-ctc")) 
        {
            if (arg + 1 < argc)
                tracingConfig.set(argv[++arg]);
            else
                usage(1);

            arg++;
        }
        else
        {
            printf("Unknown argument %s, ignored\n", argv[arg]);
            ++arg;
        }
    }
    if (persistConnections && multiThread)
    {
        printf("Multi-thread (-u) not available with -persist - ignored\n");
        multiThread = false;
    }

    if (multiThread && queryDelayMS && !multiThreadMax)
    {
        queryAbsDelayMS = queryDelayMS;
        queryDelayMS = 0;
    }

    if (generateClientSpans)
    {
        if (tracingConfig.length() == 0)
        {
            tracingConfig.set(R"!!(global:
        tracing:
            disable: false
        )!!");
        }

        try
        {
            Owned<IPropertyTree> testTree = createPTreeFromYAMLString(tracingConfig.str(), ipt_none, ptr_ignoreWhiteSpace, nullptr);
            if (testTree == nullptr)
            {
                printf("Invalid YAML tracing configuration detected: '%s'\n", tracingConfig.str());
                usage(2);
            }
            Owned<IPropertyTree> traceConfig = testTree->getPropTree("global");

            initTraceManager("testsocket", traceConfig, nullptr);

            serverSpan.setown(queryTraceManager().createServerSpan("testsocket_server", nullptr)); //can we avoid creating a server span?
        }
        catch(...)
        {
            printf("Could not initialize JTrace tracing manager!\n\n");
            usage(2);
        }
    }
    else
    {
        serverSpan.setown(getNullSpan());
    }

    StringAttr ip;
    unsigned socketPort = (useSSL) ? ROXIE_SSL_SERVER_PORT : ROXIE_SERVER_PORT;
    SplitIpPort(ip, socketPort, argv[1]);

    int ret = 0;
    trace = fopen(outputName, "w");

    if (trace == NULL)
    {
        printf("Can't open %s for writing\n", outputName.str());
    }

    __int64 starttime,endtime;
    starttime = get_cycles_now();
    if (arg < argc || fromStdIn)
    {
        echoResults = echoSingle;
        do
        {
            const char * query = argv[arg];
            if (fromMultiFile)
            {
                FILE *in = fopen(query, "rt");
                if (in)
                {
                    CDateTime firstTime;
                    CDateTime startTime;
                    bool firstLine = true;
                    char *buffer = new char[maxLineSize];
                    for (;;)
                    {
                        if (fgets(buffer, maxLineSize, in)==NULL) // buffer overflow possible - do I care?
                            break;
                        if (timedReplay)
                        {
                            if (firstLine)
                            {
                                firstTime.setNow();
                                firstTime.setTimeString(buffer, &query);
                                startTime.setNow();
                            }
                            else
                            {
                                CDateTime queryTime, nowTime;
                                queryTime.setNow();
                                queryTime.setTimeString(buffer, &query);
                                nowTime.setNow();
                                int sleeptime = (int)((queryTime.getSimple()-firstTime.getSimple()) - (nowTime.getSimple()-startTime.getSimple()));
                                if (sleeptime < 0)
                                    DBGLOG("Running behind %d seconds", -sleeptime);
                                else if (sleeptime)
                                {
                                    DBGLOG("Sleeping %d seconds", sleeptime);
                                    Sleep(sleeptime*1000);
                                }
                                StringBuffer targetTime;
                                queryTime.getTimeString(targetTime);
                                DBGLOG("Virtual time is %s", targetTime.str());
                            }
                        }
                        else
                        {
                            query = buffer;
                            while (isspace(*query)) query++;
                            if (roxieLogMode)
                            {
                                char *start = (char *) strchr(query, '<');
                                if (start)
                                {
                                    char *end = (char *) strchr(start, '"');
                                    if (end && end[1]=='\n')
                                    {
                                        query = start;
                                        *end = 0;
                                    }
                                }
                            }
                        }
                        if (query)
                        {
                            ret = sendQuery(ip, socketPort, query);
                            firstLine = false;
                        }
                    }
                    delete [] buffer;
                    fclose(in);
                }
                else
                    printf("File %s could not be opened\n", query);
            }
            else if (fromFile || fromStdIn)
            {
                FILE *in = fromStdIn ? stdin : fopen(query, "rt");
                if (in)
                {
                    StringBuffer fileContents;
                    char buffer[1024];
                    int bytes;
                    for (;;)
                    {
                        bytes = fread(buffer, 1, sizeof(buffer), in);
                        if (!bytes)
                            break;
                        fileContents.append(buffer, 0, bytes);
                    }
                    if (in != stdin)
                        fclose(in);
                    ret = sendQuery(ip, socketPort, fileContents.str());
                }
                else
                    printf("File %s could not be opened\n", query);
            }
            else
            {
                if (*query == '<' || !autoXML)
                    ret = sendQuery(ip, socketPort, query);
                else
                {
                    VStringBuffer xquery("<%s/>", query);
                    ret = sendQuery(ip, socketPort, xquery);
                }

                if (sendToSocket)
                    finishedReading.wait();
            }
        } while (--repeats > 0);
    }
    else
        usage(2);

    while (runningQueries--)
        done.wait();
    if (persistConnections && persistSocket)
    {
        int sendlen=0;
        persistSocket->write(&sendlen, sizeof(sendlen));
        persistSocket->close();
    }

    endtime = get_cycles_now();
    if (!justResults)
    {
        if (rawOnly == false)
        {
            if (trace != NULL)
            {
                fprintf(trace, "Total Time taken = %.3f msecs\n", (double)(cycle_to_nanosec(endtime - starttime))/1000000);
                if (totalQueryCnt)
                {
                    double timePerQueryMS = totalQueryMS / totalQueryCnt;
                    fprintf(trace, "Total Queries: %u Avg t/q = %.3f msecs\n", totalQueryCnt, timePerQueryMS);
                }
                fputs("----------------------------------------------------------------------------\n", trace);
            }
        }
    }
    if (trace != NULL)
    {
        fclose(trace);
    }
    
#ifdef _DEBUG
    releaseAtoms();
#endif

    return ret;
}
