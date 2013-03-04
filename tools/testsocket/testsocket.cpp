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

bool abortEarly = false;
bool forceHTTP = false;
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

StringBuffer sendFileName;
StringAttr queryNameOverride;

unsigned delay = 0;
unsigned runningQueries;
unsigned multiThreadMax;
unsigned maxLineSize = 10000000;

ISocket *persistSocket;
bool persistConnections;

int repeats = 0;
StringBuffer queryPrefix;

Semaphore okToSend;
Semaphore done;
Semaphore finishedReading;
FILE * trace;
CriticalSection traceCrit;

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
        if (saveResults)
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
        size = ftell(in);
        fseek(in, offset, SEEK_SET);
        if (size < offset)
            size = 0;
        else
            size -= offset;
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

int readResults(ISocket * socket, bool readBlocked, bool useHTTP, StringBuffer &result)
{
    if (readBlocked)
        socket->set_block_mode(BF_SYNC_TRANSFER_PULL,0,60*1000);

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

        bool isSpecial = false;
        bool pluginRequest = false;
        bool dataBlockRequest = false;
        if (len & 0x80000000)
        {
            unsigned char flag;
            isSpecial = true;
            socket->read(&flag, sizeof(flag));
            switch (flag)
            {
            case '-':
                if (echoResults)
                    fputs("Error:", stdout);
                if (saveResults)
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
            }
            len &= 0x7FFFFFFF;
            len--;      // flag already read
        }

        char * mem = (char*) malloc(len+1);
        char * t = mem;
        unsigned sendlen = len;
        t[len]=0;
        try
        {
            if (useHTTP)
            {
                try
                {
                    socket->read(t, 0, len, sendlen);
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
            memcpy(&offset, t, sizeof(offset));
            _WINREV(offset);
            sendFileChunk(t+sizeof(offset), offset, socket);
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

        free(mem);
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
    readResults(client, parallelBlocked, false, result);
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
    ISocket * socket;
    __int64 starttime, endtime;
    StringBuffer ipstr;
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
            if (!persistSocket) {
                SocketEndpoint ep(ip,port);
                persistSocket = ISocket::connect_timeout(ep, 1000);
            }
            socket = persistSocket;
        }
        else {
            SocketEndpoint ep(ip,port);
            socket = ISocket::connect_timeout(ep,1000);
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
        // note - don't support queryname override unless original query is xml
        fullQuery.appendf("POST /doc HTTP/1.0\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: %d\r\n\r\n", (int) strlen(base)).append(base);
    }
    else
    {
        if (sendToSocket)
        {
            Thread * receive = new ReceiveThread();
            receive->start();
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
            readResults(socket, false, false, lockResult);
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
            fullQuery.append(base);
    }

    const char * query = fullQuery.toCharArray();
    int len=strlen(query);
    int sendlen = len;
    if (persistConnections)
        sendlen |= 0x80000000;
    _WINREV(sendlen);                    

    try
    {
        if (!useHTTP)
            socket->write(&sendlen, sizeof(sendlen));
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
    int ret = readResults(socket, false, useHTTP, result);

    if ((ret == 0) && !justResults)
    {
        endtime = get_cycles_now();
        CriticalBlock b(traceCrit);
        fprintf(trace, "query: %s\n", query);
        if (saveResults)
            fprintf(trace, "result: %s\n", result.str());

        if (showTiming)
            fprintf(trace, "Time taken = %.3f msecs\n", (double)(cycle_to_nanosec(endtime - starttime)/1000000));
        fputs("----------------------------------------------------------------------------\n", trace);
    }

    if (!persistConnections)
    {
        socket->close();
        socket->Release();
    }
    return 0;
}


class QueryThread : public Thread
{
public:
    QueryThread(const char * _ip, unsigned _port, const char * _base) : ip(_ip),port(_port),base(_base) {}

    virtual int run() { doSendQuery(ip, port, base); done.signal(); okToSend.signal(); return 0; }

protected:
    StringAttr      ip;
    unsigned            port;
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
    thread->start();
    thread->Release();
    return 0;
}

void usage(int exitCode)
{
    printf("testsocket ip<:port> [flags] [query | -f[f] file.sql]\n");
    printf("  -a        abort before input recieved\n");
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
    printf("  -persist  use persistant connection\n");
    printf("  -pr <text>add a prefix to the query\n");
    printf("  -q        quiet - don't echo query\n");
    printf("  -qname xx Use xx as queryname in place of the xml root element name\n");
    printf("  -r <n>    repeat the query several times\n");
    printf("  -rl       roxie logfile mode\n");
    printf("  -s        add stars to indicate transfer packets\n");
    printf("  -ss       suppress XML Status messages to screen (always suppressed from tracefile)\n");
    printf("  -td       add debug timing statistics to trace\n");
    printf("  -tf       add full timing statistics to trace\n");
    printf("  -time     add timing to trace\n");
    printf("  -u<max>   run queries on separate threads\n");
    printf("  -cascade  cascade query (to all roxie nodes)\n");
    printf("  -lock     locked cascade query (to all roxie nodes)\n");
    
    exit(exitCode);
}

int main(int argc, char **argv) 
{
#if 0
    {
        SocketListCreator c;
        SocketEndpoint ep;
        ep.ip.ip[0] = 192;
        ep.ip.ip[1] = 168;
        ep.ip.ip[2] = 6;
        ep.ip.ip[3] = 186;
        ep.port = 3004;
        c.addSocket(ep);
        ep.ip.ip[3] = 180;
        c.addSocket(ep);

        c.addSocket("192.168.6.187", 3004);
        c.addSocket("192.168.6.188", 3004);
        c.addSocket("192.168.6.189", 3004);
        c.addSocket("192.168.6.190", 3004);
        c.addSocket("192.168.6.191", 3004);
        c.addSocket("192.168.6.192", 3004);
        c.addSocket("192.168.6.194", 3004);
        c.addSocket("192.168.6.195", 3004);
        c.addSocket("192.168.6.196", 3004);
        c.addSocket("192.168.6.197", 3004);
        c.addSocket("192.168.6.198", 3004);
        c.addSocket("192.168.6.199", 3004);
        c.addSocket("192.168.7.115", 3004);
        printf("%s\n",c.getText());
        SocketListParser p(c.getText());

        p.first(0);
        StringAttr ip;
        unsigned port;
        while (p.next(ip, port))
            printf("%s:%d\n",ip.get(),port);
        return 0;
    }
#endif


#ifndef _WIN32
    InitModuleObjects();
#endif
    StringAttr outputName("result.txt");

    bool fromFile = false;
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
        else if (stricmp(argv[arg], "-f") == 0)
        {
            fromFile = true;
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
            if (!f->exists() || !f->isFile())
            {
                printf("file %s does not exist\n", sendFileName.str());
                exit (EXIT_FAILURE);
            }
            arg+=2;
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

    StringAttr ip;
    unsigned socketPort = ROXIE_SERVER_PORT;
    SplitIpPort(ip, socketPort, argv[1]);

    int ret = 0;
    trace = fopen(outputName, "w");
    __int64 starttime,endtime;
    starttime = get_cycles_now();
    if (arg < argc)
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
                                int sleeptime = (queryTime.getSimple()-firstTime.getSimple()) - (nowTime.getSimple()-startTime.getSimple());
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
            else if (fromFile)
            {
                FILE *in = fopen(query, "rt");
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
                    fclose(in);
                    ret = sendQuery(ip, socketPort, fileContents.toCharArray());
                }
                else
                    printf("File %s could not be opened\n", query);
            }
            else
            {
                ret = sendQuery(ip, socketPort, query);

                if (sendToSocket)
                    finishedReading.wait();
            }
        } while (--repeats > 0);
    }

    while (runningQueries--)
        done.wait();
    if (persistConnections && persistSocket)
    {
        int sendlen=0;
        persistSocket->write(&sendlen, sizeof(sendlen));
        persistSocket->close();
        persistSocket->Release();
    }

    endtime = get_cycles_now();
    if (!justResults)
    {
        fprintf(trace, "Total Time taken = %.3f msecs\n", (double)(cycle_to_nanosec(endtime - starttime)/1000000));
        fputs("----------------------------------------------------------------------------\n", trace);
    }
    fclose(trace);
    
#ifdef _DEBUG
    releaseAtoms();
#endif

    return ret;
}
