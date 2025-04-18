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

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>

#include "jlib.hpp"
#include "jsmartsock.hpp"
#include "jmisc.hpp"
#include "jqueue.tpp"
#include "roxie.hpp"
#ifdef _USE_OPENSSL
# include "securesocket.hpp"
#endif

static int in_width = 0;
static int out_width = 1;
static unsigned recordsPerQuery = 10000;
static unsigned bytesPerBlock = 0x8000;
static unsigned maxRetries = 0;
static int readTimeout = 300;

#define MAXBUFSIZE 0x8000
#define MAXTHREADS 50


static Mutex readMutex;
static Mutex writeMutex;
static StringBuffer hosts;
static ISmartSocketFactory *smartSocketFactory = nullptr;
static bool Aborting = false;
static StringBuffer fatalError;
static CriticalSection fatalErrorSect;

static bool useSSL = false;

interface IReceivedRoxieException : extends IException
{
};

class DECL_EXCEPTION ReceivedRoxieException: public IReceivedRoxieException, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    ReceivedRoxieException(int code, const char *_msg) : errcode(code), msg(_msg) { };
    int errorCode() const { return (errcode); };
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        return str.append("ReceivedRoxieException: (").append(msg).append(")");
    };
    MessageAudience errorAudience() const { return (MSGAUD_user); };

private:
    int errcode;
    StringAttr msg;
};


class RoxieThread : public Thread
{
private:
    Owned<ISmartSocket> roxieSock;
    StringBuffer query;
    StringBuffer resultName;
    StringBuffer traceParent;

    QueueOf<MemoryBuffer, false> outputQ;

    char *inputBuffer = nullptr;
    unsigned inputLen = 0;
    unsigned curInput = 0;

    unsigned bytesPerQuery = 0;
    unsigned recordsPerBlock = 0;

public:
    unsigned recordsRead = 0;
    unsigned recordsWritten = 0;

    RoxieThread(const char *_query, const char *_resultName, const char * _traceParent) : query(_query), resultName(_resultName), traceParent(_traceParent)
    {
        recordsRead = recordsWritten = 0;
        bytesPerQuery = recordsPerQuery * in_width;
        recordsPerBlock = bytesPerBlock / in_width;
        recordsPerBlock = (recordsPerBlock < 1)? 1: recordsPerBlock;
    }

    void sendQuery()
    {
        MemoryBuffer sendBuffer;
        if (!traceParent.isEmpty())
        {
            try
            {
                Owned<IPTree> queryTree = createPTreeFromXMLString(query, ipt_none, ptr_none);
                ensurePTree(queryTree, "_trace");
                queryTree->setProp("_trace/traceparent", traceParent.str());
                toXML(queryTree, query.clear());
            }
            catch (IException * e)
            {
                StringBuffer s;
                printf("Warning: could not inject trace parent: '%s'", e->errorMessage(s).str());
                e->Release();
            }
        }
        unsigned queryLen = query.length();
        unsigned revQueryLen = queryLen;
        _WINREV(revQueryLen);
        sendBuffer.append(revQueryLen);
        sendBuffer.append(queryLen, query.str());

        PROGLOG("Sending query %s", query.str());
        roxieSock->write(sendBuffer.toByteArray(), sendBuffer.length());
        PROGLOG("Sent query");
    }

    bool readInput()
    {
        synchronized procedure(readMutex);

        int bytesRead = 0;
        inputLen = 0;
        while ((inputLen < bytesPerQuery) && (bytesRead = read(0, inputBuffer+inputLen, bytesPerQuery-inputLen)) > 0)
            inputLen += bytesRead;

        if (bytesRead < 0)
        {
            throw makeOsException(errno, "readInput");
        }

        if (inputLen > 0)
        {
            recordsRead += (inputLen / in_width);
        }

        PROGLOG("Read %d bytes (%d records) from stdin", inputLen, inputLen / in_width);

        if (bytesRead == 0)
            return true;

        return false;
    }

    void writeOutput()
    {
        synchronized procedure(writeMutex);

        while (outputQ.ordinality() > 0)
        {
            MemoryBuffer *mb = outputQ.dequeue();
            size32_t len = mb->length();
            const char *obuf = mb->toByteArray();
            const char *finger = strchr(obuf+9, '\0') + 1;
            len -= (finger-obuf);
            unsigned numRecords = len / out_width;

            while (len > 0)
            {
                PROGLOG("Writing %d bytes to stdout", len);
                int bw = write(1, finger, len);
                if (bw < 0)
                {
                    throw makeOsException(errno, "writeOutput");
                }
                PROGLOG("Wrote %d bytes to stdout (tried to write %d)", bw, len);
                len -= bw;
                finger += bw;
            }

            recordsWritten += numRecords;

            delete mb;
        }
    }

    void sendData(const char *reqbuf)
    {
        static unsigned sequenceNumber = 0;

        const char *inputName = reqbuf+9;
        int inputNameLen = strlen(inputName);

        if (curInput < inputLen)
        {
            unsigned recordsLeft = (inputLen - curInput) / in_width;
            unsigned numRecords = (recordsLeft < recordsPerBlock)? recordsLeft: recordsPerBlock;
            unsigned numBytes = numRecords * in_width;

            MemoryBuffer sendBuffer;
            unsigned rev = numBytes + inputNameLen + 10;
            rev |= 0x80000000;
            _WINREV(rev);
            sendBuffer.append(rev);
            sendBuffer.append('R');
            rev = sequenceNumber++;
            _WINREV(rev);
            sendBuffer.append(rev);
            rev = numRecords;
            _WINREV(rev);
            sendBuffer.append(rev);
            sendBuffer.append(inputNameLen+1, inputName);
            sendBuffer.append(numBytes, inputBuffer+curInput);

            PROGLOG("Writing %d bytes from inputQ to socket", numBytes);
            roxieSock->write(sendBuffer.toByteArray(), sendBuffer.length());
            PROGLOG("Wrote %d bytes from inputQ to socket", numBytes);

            curInput += numBytes;
        }
        else
        {
            unsigned zeroLen = 0;
            PROGLOG("Writing %d bytes from stdin to socket", zeroLen);
            roxieSock->write(&zeroLen, sizeof(zeroLen));
            PROGLOG("Wrote %d bytes from stdin to socket", zeroLen);
        }
    }

    void processQuery()
    {
        while (1)
        {
            bool blockedMsg = false;
            size32_t r = 0;

            PROGLOG("Reading 4 bytes from socket");

            size32_t len;
            roxieSock->read(&len,sizeof(len),sizeof(len),r, readTimeout);

            PROGLOG("Read 4 bytes from socket, got %d", r);
            _WINREV4(len);

            if (len & 0x80000000)
            {
                len ^= 0x80000000;
                blockedMsg = true;
            }

            if (len == 0)
                break;

            r=0;
            MemoryBuffer *x = new MemoryBuffer();
            char *obuf = (char *) x->reserve(len);
            PROGLOG("Reading %d bytes from socket", len);

            try
            {
                roxieSock->read(obuf, len, len, r, readTimeout);
            }
            catch (...)
            {
                delete x;
                throw;
            }

            PROGLOG("Read %d bytes from socket, got %d", len, r);

            if (blockedMsg)
            {
                if (*obuf == 'D')
                {
                    sendData(obuf);
                }
                else if (*obuf == 'R')
                {
                    const char *finger = obuf+9;
                    const char *payload = finger + strlen(finger) + 1;

                    if (strcmp(finger, "Exception") == 0)
                    {
                        StringBuffer body(x->length() - (payload-obuf), payload);
                        delete x;
                        StringBuffer xml;
                        xml.append("<Exception>").append(body).append("</Exception>");
                        Owned<IPropertyTree> ep = createPTreeFromXMLString(xml.str());
                        int code = ep->getPropInt("./Code", 0);
                        SocketEndpoint peerEp;
                        StringBuffer peerStr;
                        OERRLOG("Connected to %s", roxieSock->querySocket()->getPeerEndpoint(peerEp).getEndpointHostText(peerStr).str());
                        OERRLOG("Roxie exception: %s", body.str());
                        throw new ReceivedRoxieException(code, body.str());
                    }
                    else if (resultName.length() == 0 || strcmp(finger, resultName.str()) == 0)
                    {
                        if ((len - (payload-obuf)) % out_width != 0)
                        {
                            throw MakeStringException(-1,"Fatal error: received %u bytes of data, not a multiple of -ow %u",len-(size32_t)(payload-obuf),out_width);
                        }
                        
                        outputQ.enqueue(x);
                        continue;
                    }
                }
            }
            else
            {
                Owned<IPropertyTree> ep = createPTreeFromXMLString(x->length(), x->toByteArray());

                if (strcmp(ep->queryName(), "Exception") == 0)
                {
                    StringBuffer xml(x->length(), x->toByteArray());
                    delete x;
                    SocketEndpoint peerEp;
                    StringBuffer peerStr;
                    OERRLOG("Connected to %s", roxieSock->querySocket()->getPeerEndpoint(peerEp).getEndpointHostText(peerStr).str());
                    OERRLOG("Roxie exception: %s", xml.str());
                    int code = 0;
                    try
                    {
                        Owned<IPropertyTree> ep = createPTreeFromXMLString(xml.str());
                        code = ep->getPropInt("./Code", 0);
                    }
                    catch (IException *E)
                    {
                        E->Release();
                    }
                    throw new ReceivedRoxieException(code, xml.str());
                }
            }

            delete x;
        }
    }

    int run()
    {
        PROGLOG("in thread");
        bool done = false;
        unsigned attempts;
        int retryInterval;
        Owned<IRandomNumberGenerator> random = createRandomNumberGenerator();
        random->seed((unsigned)get_cycles_now());

        inputBuffer = (char *) malloc(recordsPerQuery * in_width);

        try {
            while (!done&&!Aborting)
            {
                done = readInput();
                if (inputLen > 0)
                {
                    attempts = 0;
                    retryInterval = 1;
                    unsigned excAttempts = 0;

                    while (!Aborting)
                    {
                        try
                        {
                            roxieSock.setown(smartSocketFactory->connect_timeout(9000));

                            SocketEndpoint peerEp;
                            StringBuffer peerStr;
                            PROGLOG("Connected to %s", roxieSock->querySocket()->getPeerEndpoint(peerEp).getEndpointHostText(peerStr).str());

                            sendQuery();

                            curInput = 0;
                            processQuery();

                            roxieSock->close();
                            roxieSock.clear();
                            break;
                        }
                        catch (IReceivedRoxieException *e)
                        {
                            if (e->errorCode() == ROXIE_TOO_MANY_QUERIES) // server busy exception
                            {
                                int sleepTime = retryInterval + random->next() % retryInterval;
                                PROGLOG("Thread sleeping for %d seconds", sleepTime);
                                Sleep(sleepTime*1000);
                                retryInterval = (retryInterval*2 >= 30)? 30: retryInterval*2;
                            }
                            else
                            {
                                IWARNLOG("Caught Roxie exception - retrying? (%d<%d)", attempts, maxRetries);

                                if (attempts < maxRetries)
                                {
                                    IWARNLOG("Retrying: maxRetries not exceeded");
                                    attempts++;
                                }
                                else
                                {
                                    EXCLOG(e,"Roxie exception");
                                    CriticalBlock block(fatalErrorSect);
                                    Aborting = true;
                                    if (fatalError.length()==0) 
                                        e->errorMessage(fatalError);
                                    OERRLOG("Exiting: maxRetries exceeded");
                                    break;
                                }
                            }

                            e->Release();
                            while (outputQ.ordinality() > 0)
                                delete outputQ.dequeue();
                        }
                        catch (IException *e)
                        {
                            if (excAttempts++ == 10) {  // no sense in looping forever
                                Aborting = true;
                                StringBuffer s;
                                e->errorMessage(s);
                                e->Release();
                                s.append(" - Failed to connect to ").append(hosts);
                                if (fatalError.length()==0) 
                                    fatalError.append(s.str());
                                OERRLOG("%s",s.str());
                                break;
                            }
                            EXCLOG(e, "Caught exception - retrying");
                            e->Release();
                            while (outputQ.ordinality() > 0)
                                delete outputQ.dequeue();
                            
                        }
                    }
                    if (!Aborting) {
                        writeOutput();

                        PROGLOG("Thread progress: %d records read, %d written", recordsRead, recordsWritten);
                    }
                }
            }
        }
        catch (IException *e)
        {
            EXCLOG(e,"roxiepipe thread");
            if (!Aborting) {
                CriticalBlock block(fatalErrorSect);
                Aborting = true;
                if (fatalError.length()==0)
                    e->errorMessage(fatalError);
            }
            e->Release();
        }

        if (Aborting)
            PROGLOG("roxiepipe thread aborting");
            
        free(inputBuffer);

        return 0;
    }
};

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    ::Release(smartSocketFactory);
}

void usage()
{
    printf("roxiepipe - pipes data to/from roxie\n");
    printf("Options:\n");
    printf("  -vip         Virtual IP mode (do not retry)\n");
    printf("  -iw   N      Input width in bytes (required)\n");
    printf("  -ow   N      Output width in bytes\n");
    printf("  -q    name   Query name (required)\n");
    printf("  -h    hosts  Target hostname or ip (required)\n");
    printf("  -r    name   Result name\n");
    printf("  -i    name   Input from file (default is stdin)\n");
    printf("  -o    name   Output from file (default is stdout)\n");
    printf("  -t    N      Number of threads\n");
    printf("  -b    N      Records per query\n");
    printf("  -mr   N      Max retries\n");
    printf("  -to   N      Read timeout\n");
    printf("  -wu   wuid   Workunit id\n");
    printf("  -l    name   Logfile name\n");
    printf("  -tp   trace  Provide trace context (traceparent)\n");
    printf("               Sample format: 00-beca49ca8f3138a2842e5cf21402bfff-4b960b3e4647da3f-01\n");
    printf("  -ssl         Use SSL connections\n");
}

int main(int argc, char *argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();

    StringBuffer query;
    StringBuffer resultName;
    StringBuffer logFile;
    StringBuffer traceParent;
    unsigned numThreads = 2;
    bool retryMode = true;

    int i;
    StringBuffer cmdLine(argv[0]);
    for (i=1; i<argc; i++)
    {
        cmdLine.append(' ').append(argv[i]);
    }
    if (argc==1)
    {
        usage();
        exit(2);
    }

    for (i=1; i<argc; i++)
    {
        if (stricmp(argv[i], "-help") == 0 || stricmp(argv[i], "--help") == 0)      //no retry
        {
            usage();
            exit(2);
        }
        else if (stricmp(argv[i], "-vip") == 0)      //no retry
        {
            retryMode = false;
        }
        else if (stricmp(argv[i], "-iw") == 0)  //REQUIRED: input width - int
        {
            i++;
            if (i < argc)
                in_width = atoi(argv[i]);
        }
        else if (stricmp(argv[i], "-ow") == 0)  //output width - int
        {
            i++;
            if (i < argc)
                out_width = atoi(argv[i]);
        }
        else if (stricmp(argv[i], "-q") == 0)   //REQUIRED: query - string
        {
            i++;
            if (i < argc)
                query.append(argv[i]);
        }
        else if (stricmp(argv[i], "-h") == 0)   //REQUIRED: hosts - string
        {
            i++;
            if (i < argc)
                hosts.append(argv[i]);
        }
        else if (stricmp(argv[i], "-r") == 0)   //result name - string
        {
            i++;
            if (i < argc)
                resultName.append(argv[i]);
        }
        else if (stricmp(argv[i], "-i") == 0)   //input - string
        {
            i++;
            if (i < argc)
            {
                if (!freopen(argv[i], "rb", stdin))
                {
                    fatalError.appendf("Could not open file %s for input (error %d)\n)", argv[i], errno);
                    break;
                }
            }
        }
        else if (stricmp(argv[i], "-o") == 0)   //output - string
        {
            i++;
            if (i < argc)
            {
                if (!freopen(argv[i], "wb", stdout))
                {
                    fatalError.appendf("Could not open file %s for output (error %d))", argv[i], errno);
                    break;
                }
            }
        }
        else if (stricmp(argv[i], "-n") == 0)   //ignored (was start node)
        {
            i++;
            // ignore start node
        }
        else if (stricmp(argv[i], "-t") == 0)   //num threads - int
        {
            i++;
            if (i < argc)
                numThreads = atoi(argv[i]);
        }
        else if (stricmp(argv[i], "-b") == 0)   //records per query = int
        {
            i++;
            if (i < argc)
                recordsPerQuery = atoi(argv[i]);
        }
        else if (stricmp(argv[i], "-mr") == 0)  //max retries - int
        {
            i++;
            if (i <argc)
                maxRetries = atoi(argv[i]);
        }
        else if (stricmp(argv[i], "-to") == 0)  //read timeout - int
        {
            i++;
            if (i <argc)
                readTimeout = atoi(argv[i]);
        }
        else if (stricmp(argv[i], "-wu") == 0)  //workunit - string
        {
            i++;
            // We don't actually do anything with the workunit, just allow it to be passed so that it appears in tracing
        }
        else if (stricmp(argv[i], "-l") == 0)   //logfile name
        {
            i++;
            if (i <argc)
                logFile.clear().append(argv[i]);
        }
        else if (stricmp(argv[i], "-tp") == 0)   //trace parent provided
        {
            i++;
            if (i < argc)
                traceParent.clear().append(argv[i]);
        }
#if 0
        else if (stricmp(argv[i], "-d") == 0)
        {
            DebugBreak();
        }
#endif
        else if (stricmp(argv[i], "-ssl") == 0)
        {
#ifdef _USE_OPENSSL
            useSSL = true;
#else
            fatalError.append("-ssl argument not supported : OpenSSL disabled in build");
            break;
#endif
        }
        else
        {
            fatalError.appendf("Unknown/unexpected parameter %s", argv[i]);
            break;
        }
    }

    PROGLOG("roxiepipe starting, command line %s", cmdLine.str());

    StringBuffer logDir;
    if (!logFile.length())
    {
        if (!getConfigurationDirectory(NULL,"log","roxiepipe","roxiepipe",logDir))
            logDir.append(".");
    }

    //Build logfile from component properties settings
    Owned<IComponentLogFileCreator> lf;
    lf.setown(createComponentLogFileCreator(logDir.str(), "roxiepipe"));
    if (logFile.length())
        lf->setCompleteFilespec(logFile.str());
    lf->setCreateAliasFile(false);
    try 
    {
        lf->beginLogging();
        PROGLOG("Logging to %s",lf->queryLogFileSpec());
    } catch (IException *e)
    {
        e->errorMessage(fatalError);
        e->Release();
    }
    queryLogMsgManager()->removeMonitor(queryStderrLogMsgHandler()); // only want fprintf(stderr)

    if (!fatalError.length())
    {
        if (in_width == 0 || out_width == 0 || query.length() == 0 || hosts.length() == 0)
        {
            fatalError.append("Missing/invalid parameter (-iw, -q & -h are all required)");
        }
        else 
        {
            
        #ifdef _WIN32
            _setmode(_fileno(stdin), _O_BINARY);
            _setmode(_fileno(stdout), _O_BINARY);
        #endif
            numThreads = (numThreads < 1)? 1: ((numThreads > MAXTHREADS)? MAXTHREADS: numThreads);
            recordsPerQuery = (recordsPerQuery < 1)? 10000: recordsPerQuery;

            RoxieThread *rt[MAXTHREADS];

            try
            {
#ifdef _USE_OPENSSL
                if (useSSL)
                    smartSocketFactory = createSecureSmartSocketFactory(hosts.str(), createSecureSocketConfig(nullptr, nullptr, nullptr, true), retryMode);
                else
#endif
                    smartSocketFactory = createSmartSocketFactory(hosts.str(), retryMode);
            }
            catch (ISmartSocketException *e)
            {
                e->errorMessage(fatalError);
                e->Release();
            }
            
            if (fatalError.length()==0) 
            {
                for (i=0; i<(int)numThreads; i++)
                {
                    rt[i] = new RoxieThread(query.str(), resultName.str(), traceParent.str());
                    PROGLOG("Starting thread %d", i);
                    rt[i]->start(true);
                }

                unsigned totalRead = 0;
                unsigned totalWritten = 0;

                for (i=0; i<(int)numThreads; i++)
                {
                    PROGLOG("Waiting for thread %d to finish", i);
                    rt[i]->join();
                    PROGLOG("Final stats for thread %d: %d records read, %d %swritten", i, rt[i]->recordsRead, rt[i]->recordsWritten, (out_width==1)?"bytes ":"");
                    totalRead += rt[i]->recordsRead;
                    totalWritten += rt[i]->recordsWritten;
                }

                PROGLOG("Final roxiepipe stats: %d records read, %d %swritten", totalRead, totalWritten, (out_width==1)?"bytes ":"");
            }
        }
    }
    if (fatalError.length()) {
        OERRLOG("EXIT: %s",fatalError.str());
        fprintf(stderr, "%s\n", fatalError.str());
        fflush(stderr);
        Sleep(1000);
    }
    return Aborting?1:0;
}
