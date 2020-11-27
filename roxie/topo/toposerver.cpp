/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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
#include <signal.h>
#include <thread>
#include <map>
#include <string>
#include <sstream>
#include "portlist.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"
#include "jmd5.hpp"
#include "jfile.hpp"
#include "jptree.hpp"

/**
 * While billed as a topology server (and used for that by Roxie), this service actually remembers and
 * returns arbitrary strings supplied by its clients, provided that the strings in question have been
 * supplied within the timeout period. In Roxie usage, the strings in question describe each client's
 * IP address and role, so that any client can discover the current system topology.
 *
 * Incoming queries and responses use "testsocket" format (big-endian 4 byte length followed by payload).
 * Payload is a series of strings with \n termination. A string starting with = supplies an md5 sum of
 * the current active state. If incoming query supplies an md5 that matches the current active state, then
 * no state is returned (just the md5).
 *
 */

static void topo_server_usage()
{
    printf("\nTopology Server: Starts the Roxie Topology service.\n");
    printf("\ttoposerver [options below]\n");

    printf("\nOptions:\n");
#ifndef _CONTAINERIZED
    printf("  --daemon|-d <instanceName>: Run daemon as instance\n");
#endif
    printf("  --port=[integer]          : Network port (default %d)\n", TOPO_SERVER_PORT);
    printf("  --traceLevel=[integer]    : Amount of information to dump on logs (default 1)\n");
#ifndef _CONTAINERIZED
    printf("  --stdlog=[boolean]        : Standard log format (based on tracelevel)\n");
    printf("  --logdir=[filename]       : Outputs to logfile, rather than stdout\n");
#endif
    printf("  --help|-h                 : This message\n");
    printf("\n");
}

unsigned traceLevel = 0;
unsigned topoPort = TOPO_SERVER_PORT;
std::map<std::string, unsigned> topology;
StringBuffer cachedResponse;
StringBuffer cachedDigest;
bool responseDirty = true;
unsigned lastTimeoutCheck = 0;
unsigned lastTopologyReport = 0;
const unsigned timeoutCheckInterval = 1000;
const unsigned heartbeatInterval = 5000;
const unsigned timeoutHeartbeatInterval = 10000;
const unsigned topologyReportInterval = 60000;
bool aborted = false;
Semaphore stopping;
StringBuffer topologyFile;

extern "C" void caughtSIGPIPE(int sig)
{
    DBGLOG("Caught sigpipe %d", sig);
}

extern "C" void caughtSIGHUP(int sig)
{
    DBGLOG("Caught sighup %d", sig);
}

extern "C" void caughtSIGALRM(int sig)
{
    DBGLOG("Caught sigalrm %d", sig);
}

extern "C" void caughtSIGTERM(int sig)
{
    DBGLOG("Caught sigterm %d", sig);
    stopping.signal();
    aborted = true;
}

extern "C" void caughtSIGABRT(int sig)
{
    DBGLOG("Caught signal %d", sig);
    stopping.signal();
    aborted = true;
}

void init_signals()
{
    signal(SIGTERM, caughtSIGTERM);
    signal(SIGABRT, caughtSIGABRT);
#ifndef _WIN32
    signal(SIGSTOP, caughtSIGABRT);
    signal(SIGINT, caughtSIGABRT);
    signal(SIGPIPE, caughtSIGPIPE);
    signal(SIGHUP, caughtSIGHUP);
    signal(SIGALRM, caughtSIGALRM);

#endif
}

void updateTopology(const std::string &newInfo)
{
    if (newInfo[0]=='-')
    {
        if (topology.erase(newInfo.substr(1)))
            responseDirty = true;
    }
    else
    {
        unsigned &found = topology[newInfo];
        if (found==0)
            responseDirty = true;
        found = msTick();
    }
}

void timeoutTopology()
{
    unsigned now = msTick();
    if (now - lastTimeoutCheck < timeoutCheckInterval)
        return;
    for (auto it = topology.cbegin(); it != topology.cend(); /* no increment */)
    {
        unsigned lastSeen = it->second;
        if (now-lastSeen > timeoutHeartbeatInterval)
        {
            if (traceLevel)
            {
                DBGLOG("No heartbeat for %u ms for %s", now-lastSeen, it->first.c_str());
            }
            it = topology.erase(it);
            responseDirty = true;
        }
        else
        {
            ++it;
        }
    }
    lastTimeoutCheck = now;
}

void reportTopology()
{
    unsigned now = msTick();
    if (now - lastTopologyReport < topologyReportInterval)
        return;
    DBGLOG("Current state:");
    for (const auto& it : topology)
    {
        DBGLOG(" %s - %ums", it.first.c_str(), now-it.second);
    }
    lastTopologyReport = now;
}

void regenerateResponse()
{
    if (responseDirty)
    {
        cachedResponse.clear();
        cachedDigest.set("=");
        for (const auto& it : topology)
        {
           cachedResponse.append(it.first.c_str()).append('\n');
        }
        md5_string(cachedResponse, cachedDigest);
        cachedDigest.append('\n');
        responseDirty = false;
    }
}

void doServer(ISocket *socket)
{
    std::thread pinger([]()
    {
        // Force a periodic refresh so we see missing heartbeats even when none are coming in, and we can interrupt the socket on closedown
        //
        bool aborting = false;
        SocketEndpoint me(".", topoPort);
        while (!aborting)
        {
            if (stopping.wait(heartbeatInterval))
                aborting = true;
            try
            {
                Owned<ISocket> p = ISocket::connect(me);
                p->write("\0\0\0\0", 4);
                p->close();
            }
            catch (IException *e)
            {
                e->Release();
            }
        }
    });
    while (!aborted)
    {
        try
        {
            Owned<ISocket> client = socket->accept();
            timeoutTopology();
            unsigned packetLen;
            client->read(&packetLen, 4);
            _WINREV(packetLen);
            if (packetLen>0)
            {
                MemoryBuffer mb;
                char *mem = (char *)mb.reserveTruncate(packetLen);
                client->read(mem, packetLen);
                if (traceLevel>=5)
                    DBGLOG("Received request %.*s", packetLen, mem);
                std::istringstream ss(std::string(mem, packetLen));
                std::string line;
                std::string suppliedDigest;
                while (std::getline(ss, line, '\n'))
                {
                    if (line[0]=='=')
                        suppliedDigest.swap(line);
                    else
                        updateTopology(line);
                }
                regenerateResponse();
                bool match = suppliedDigest.append("\n").compare(cachedDigest) == 0;
                unsigned rlen = cachedDigest.length();
                if (!match)
                    rlen += cachedResponse.length();
                _WINREV(rlen);
                client->write(&rlen, 4);
                if (traceLevel>=5)
                    DBGLOG("Sending digest %s", cachedDigest.str());
                client->write(cachedDigest.str(), cachedDigest.length());
                if (!match)
                {
                    if (traceLevel>=5)
                        DBGLOG("Sending response %s", cachedResponse.str());
                    client->write(cachedResponse.str(), cachedResponse.length());
                }
                else if (traceLevel>=5)
                    DBGLOG("Sending empty response as no changes");
            }
            try
            {
                client->write("\x0\x0\x0\x0",4);
            }
            catch(IException * e)
            {
                // Completely ignore an exception here - someone that did not want a reply may have already closed the socket
                e->Release();
            }
            if (traceLevel)
                reportTopology();
        }
        catch(IException * e)
        {
            EXCLOG(e, "Failed to process request");
            e->Release();
        }
    }
    pinger.join();
}

static constexpr const char * defaultYaml = R"!!(
  version: "1.0"
  roxie:
    logdir: ""
    topoport: 9004
    stdlog: true
    traceLevel: 1
)!!";

int main(int argc, const char *argv[])
{
    EnableSEHtoExceptionMapping();
    setTerminateOnSEH();
    init_signals();
    // We need to do the above BEFORE we call InitModuleObjects
    try
    {
        InitModuleObjects();
    }
    catch (IException *E)
    {
        EXCLOG(E);
        E->Release();
        return EXIT_FAILURE;
    }
    Owned<IFile> sentinelFile = createSentinelTarget();
    removeSentinelFile(sentinelFile);
    try
    {
        for (unsigned i=0; i<(unsigned)argc; i++)
        {
            if (stricmp(argv[i], "--help")==0 ||
                stricmp(argv[i], "-h")==0)
            {
                topo_server_usage();
                return EXIT_SUCCESS;
            }
#ifndef _CONTAINERIZED
            else if (streq(argv[i],"--daemon") || streq(argv[i],"-d")) {
                if (daemon(1,0) || write_pidfile(argv[++i])) {
                    perror("Failed to daemonize");
                    return EXIT_FAILURE;
                }
            }
#endif
        }

        // locate settings xml file in runtime dir
        Owned<IPropertyTree> topology = loadConfiguration(defaultYaml, argv, "roxie", "ROXIE", nullptr, nullptr);
        traceLevel = topology->getPropInt("@traceLevel", 1);
        topoPort = topology->getPropInt("@topoport", TOPO_SERVER_PORT);
#ifndef _CONTAINERIZED
        if (topology->getPropBool("@stdlog", traceLevel != 0))
            queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time | MSGFIELD_milliTime | MSGFIELD_thread | MSGFIELD_prefix);
        else
            removeLog();
        const char *logdir = topology->queryProp("@logdir");
        if (logdir && *logdir)
        {
            Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(logdir, "toposerver");
            lf->setMaxDetail(TopDetail);
            lf->beginLogging();
            queryLogMsgManager()->enterQueueingMode();
            queryLogMsgManager()->setQueueDroppingLimit(512, 32);
        }
#else
        setupContainerizedLogMsgHandler();
#endif
        Owned<ISocket> socket = ISocket::create(topoPort);
        if (traceLevel)
            DBGLOG("Topology server starting");

        writeSentinelFile(sentinelFile);
        doServer(socket);
        if (traceLevel)
            DBGLOG("Topology server stopping");
        removeSentinelFile(sentinelFile);
    }
    catch (IException *E)
    {
        EXCLOG(E);
        E->Release();
    }
    catch (...)
    {
        Owned<IException> E = makeStringException(MSGAUD_programmer, 999, "Unexpected exception");
        EXCLOG(E);
        E->Release();
    }
    ExitModuleObjects();
    return 0;
}
