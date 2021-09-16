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

struct TopologyEntry
{
    unsigned lastSeen = 0;
    time_t instance = 0;
};

unsigned traceLevel = 0;
unsigned topoPort = TOPO_SERVER_PORT;
std::map<std::string, TopologyEntry> topology;
StringBuffer cachedResponse;
StringBuffer cachedDigest;
bool responseDirty = true;
unsigned lastTimeoutCheck = 0;
unsigned lastTopologyReport = 0;

unsigned timeoutCheckInterval = 1000;       // How often we check to see what has expired
unsigned heartbeatInterval = 5000;          // How often nodes send heartbeats
unsigned timeoutHeartbeatServer = 60000;    // How long before a server is marked as down
unsigned timeoutHeartbeatAgent = 10000;     // How long before an agent is marked as down
unsigned removeHeartbeatInterval = 120000;  // How long before a node is removed from list
unsigned topologyReportInterval = 60000;    // How often topology is reported to logging (if traceLevel >= 2)
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

void updateTopology(const std::string &newInfo, time_t instance)
{
    TopologyEntry &found = topology[newInfo];
    if (found.lastSeen==0 || found.instance != instance)
        responseDirty = true;
    found.lastSeen = msTick();
    found.instance = instance;
}

void timeoutTopology()
{
    unsigned now = msTick();
    if (now - lastTimeoutCheck < timeoutCheckInterval)
        return;
    for (auto it = topology.begin(); it != topology.end(); /* no increment */)
    {
        bool isServer = it->first.rfind("server", 0)==0;
        unsigned lastSeen = it->second.lastSeen;
        unsigned timeout = isServer ? timeoutHeartbeatServer : timeoutHeartbeatAgent;
        // If a server is missing a heartbeat for a while, we mark it as down. Queued packets for that server will get discarded, and
        // it will be sorted to the end of the priority list for agent requests
        // The timeout is different for server vs agent - for servers, we want to be sure it really is down, and there's no huge cost for waiting,
        // while for agents we want to divert traffic away from it ASAP (so long as there are other destinations available
        if (now-lastSeen > timeout)
        {
            if (traceLevel)
            {
                DBGLOG("No heartbeat for %u ms for %s", now-lastSeen, it->first.c_str());
            }
            responseDirty = true;
            if (now-lastSeen > removeHeartbeatInterval)
            {
                it = topology.erase(it);
                continue;
            }
            it->second.instance = 0;  // By leaving the entry present but with instance=0, we will ensure that all clients get to see that the machine is no longer present
        }
        ++it;
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
        DBGLOG(" %s - %ums", it.first.c_str(), now-it.second.lastSeen);
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
           cachedResponse.append(it.first.c_str()).append('\t').append((__uint64) it.second.instance).append('\n');
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
                // TLS TODO: secure_connect() here if globally configured for mtls ...
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
            // TLS TODO: secure_accept() here if globally configured for mtls ...
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
                    {
                        time_t instance = 0;
                        auto end = line.find('\t', 0);
                        if (end != line.npos)
                        {
                            char *tail = nullptr;
                            std::string instanceStr = line.substr(end+1);
                            instance = strtoul(instanceStr.c_str(), &tail, 10);
                            if (*tail)
                                DBGLOG("Unexpected characters parsing instance value in topology entry %s", line.c_str());
                            line = line.substr(0, end);
                        }
                        if (line[0]=='-')
                        {
                            line = line.substr(1);
                            instance = 0;
                        }
                        if (traceLevel >= 6)
                            DBGLOG("Adding entry %s instance %" I64F "u", line.c_str(), (__uint64) instance);
                        updateTopology(line, instance);
                    }
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
            if (traceLevel>1)
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
  toposerver:
    logdir: ""
    port: 9004
    stdlog: true
    traceLevel: 1
)!!";

int main(int argc, const char *argv[])
{
    EnableSEHtoExceptionMapping();
    setTerminateOnSEH();
    init_signals();

    if (!checkCreateDaemon(argc, argv))
        return EXIT_FAILURE;

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
    try
    {
        Owned<IFile> sentinelFile = createSentinelTarget();
        removeSentinelFile(sentinelFile);

        for (unsigned i=0; i<(unsigned)argc; i++)
        {
            if (stricmp(argv[i], "--help")==0 ||
                stricmp(argv[i], "-h")==0)
            {
                topo_server_usage();
                return EXIT_SUCCESS;
            }
        }

        // locate settings xml file in runtime dir
        Owned<IPropertyTree> topology = loadConfiguration(defaultYaml, argv, "toposerver", "TOPOSERVER", nullptr, nullptr);
        traceLevel = topology->getPropInt("@traceLevel", 1);
        topoPort = topology->getPropInt("@port", TOPO_SERVER_PORT);

        timeoutCheckInterval = topology->getPropInt("@timeoutCheckInterval", timeoutCheckInterval);
        heartbeatInterval = topology->getPropInt("@heartbeatInterval", heartbeatInterval);
        timeoutHeartbeatAgent = topology->getPropInt("@timeoutHeartbeatAgent", timeoutHeartbeatAgent);
        timeoutHeartbeatServer = topology->getPropInt("@timeoutHeartbeatServer", timeoutHeartbeatServer);
        removeHeartbeatInterval = topology->getPropInt("@removeHeartbeatInterval", removeHeartbeatInterval);
        topologyReportInterval = topology->getPropInt("@topologyReportInterval", topologyReportInterval);

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
            DBGLOG("Topology server starting on port %u", topoPort);

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
