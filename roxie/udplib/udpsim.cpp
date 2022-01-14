/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#include "udplib.hpp"
#include "udpsha.hpp"
#include "udptrs.hpp"
#include "udpipmap.hpp"
#include "roxiemem.hpp"
#include "jptree.hpp"
#include "portlist.h"

using roxiemem::DataBuffer;
using roxiemem::IDataBufferManager;

#ifdef SOCKET_SIMULATION

Owned<IDataBufferManager> dbm;

static unsigned numThreads = 20;
static unsigned numReceiveSlots = 100;
static unsigned packetsPerThread = 0;
static unsigned minWork = 0;
static unsigned maxWork = 0;
static unsigned optWorkFrequency = 0;
static bool restartSender = false;
static bool restartReceiver = false;
static bool sendFlowWithData = false;

static constexpr const char * defaultYaml = R"!!(
version: "1.0"
udpsim:
  dropDataPackets: false
  dropDataPacketsPercent: 0
  dropOkToSendPackets: 0
  dropRequestReceivedPackets: 0
  dropRequestToSendPackets: 0
  dropRequestToSendMorePackets: 0
  dropSendStartPackets: 0
  dropSendCompletedPackets: 0
  help: false
  minWork: 0                # minimum amount of work
  maxWork: 0                # maximum work per set of packets, if 0 use minWork
  workFrequency: 5          # Do work once every 5 packets
  numThreads: 20
  numReceiveSlots: 100
  outputconfig: false
  packetsPerThread: 10000
  restartReceiver: false
  restartSender: false
  sanityCheckUdpSettings: true
  sendFlowWithData: false
  udpResendLostPackets: true
  udpFlowAckTimeout: 2
  updDataSendTimeout: 20
  udpRequestTimeout: 20
  udpPermitTimeout: 50
  udpResendDelay: 0
  udpMaxPermitDeadTimeouts: 5
  udpRequestDeadTimeout: 10000
  udpMaxPendingPermits: 10
  udpMaxClientPercent: 200
  udpMinSlotsPerSender: 1
  udpAssumeSequential: false
  udpAllowAsyncPermits: false
  udpTraceLevel: 1
  udpTraceTimeouts: true
  udpTestSocketDelay: 0
  udpTestSocketJitter: false
  udpTestVariableDelay: false
  udpTraceFlow: false
  useQueue: false
  udpAdjustThreadPriorities: false
)!!";

bool isNumeric(const char *str)
{
    while (*str)
    {
        if (!isdigit(*str))
            return false;
        str++;
    }
    return true;
}

bool isBoolean(const char *str)
{
    return streq(str, "true") || streq(str, "false");
}

void usage()
{
    printf("USAGE: udpsim [options]\n");
    printf("Options are:\n");
    Owned<IPropertyTree> defaults = createPTreeFromYAMLString(defaultYaml);
    IPropertyTree * allowed = defaults->queryPropTree("udpsim");
    Owned<IAttributeIterator> aiter = allowed->getAttributes();
    ForEach(*aiter)
    {
        printf("  --%s", aiter->queryName()+1);
        if (isBoolean(aiter->queryValue()))
            printf("[=0|1]\n");
        else
            printf("=nn\n");
    }
    ExitModuleObjects();
    releaseAtoms();
    exit(2);
}

void initOptions(int argc, const char **argv)
{
    Owned<IPropertyTree> defaults = createPTreeFromYAMLString(defaultYaml);
    IPropertyTree * allowed = defaults->queryPropTree("udpsim");
    for (unsigned argNo = 1; argNo < argc; argNo++)
    {
        const char *arg = argv[argNo];
        if (arg[0]=='-' && arg[1]=='-')
        {
            arg += 2;
            StringBuffer attrname("@");
            const char * eq = strchr(arg, '=');
            if (eq)
                attrname.append(eq-arg, arg);
            else
                attrname.append(arg);
            if (!allowed->hasProp(attrname))
            {
                printf("Unrecognized option %s\n\n", attrname.str()+1);
                usage();
            }
            if (!eq && !isBoolean(allowed->queryProp(attrname)))
            {
                printf("Option %s requires a value\n\n", attrname.str()+1);
                usage();
            }
        }
        else
        {
            printf("Unexpected argument %s\n\n", arg);
            usage();
        }
    }

    Owned<IPropertyTree> options = loadConfiguration(defaultYaml, argv, "udpsim", "UDPSIM", nullptr, nullptr);
    if (options->getPropBool("@help", false))
        usage();
#ifdef TEST_DROPPED_PACKETS
    udpDropDataPackets = options->getPropBool("@dropDataPackets", false);
    udpDropDataPacketsPercent = options->getPropInt("@dropDataPacketsPercent", 0);
    udpDropFlowPackets[flowType::ok_to_send] = options->getPropInt("@dropOkToSendPackets", 0);  // drop 1 in N
    udpDropFlowPackets[flowType::request_received] = options->getPropInt("@dropRequestReceivedPackets", 0);  // drop 1 in N
    udpDropFlowPackets[flowType::request_to_send] = options->getPropInt("@dropRequestToSendPackets", 0);  // drop 1 in N
    udpDropFlowPackets[flowType::request_to_send_more] = options->getPropInt("@dropRequestToSendMorePackets", 0);  // drop 1 in N
    udpDropFlowPackets[flowType::send_start] = options->getPropInt("@dropSendStartPackets", 0);  // drop 1 in N
    udpDropFlowPackets[flowType::send_completed] = options->getPropInt("@dropSendCompletedPackets", 0);  // drop 1 in N
#endif
    restartSender = options->getPropBool("@restartSender");
    restartReceiver = options->getPropBool("@restartReceiver");

    minWork = options->getPropInt("@minWork", 0);
    maxWork = options->getPropInt("@maxWork", 0);
    optWorkFrequency = options->getPropInt("@workFrequency", 0);

    numThreads = options->getPropInt("@numThreads", 0);
    udpTraceLevel = options->getPropInt("@udpTraceLevel", 1);
    udpTraceTimeouts = options->getPropBool("@udpTraceTimeouts", true);
    udpResendLostPackets = options->getPropBool("@udpResendLostPackets", true);

    udpPermitTimeout = options->getPropInt("@udpPermitTimeout", udpPermitTimeout);
    udpRequestTimeout = options->getPropInt("@udpRequestTimeout", udpRequestTimeout);
    udpFlowAckTimeout = options->getPropInt("@udpFlowAckTimeout", udpFlowAckTimeout);
    updDataSendTimeout = options->getPropInt("@udpDataSendTimeout", updDataSendTimeout);
    udpResendDelay = options->getPropInt("@udpResendDelay", udpResendDelay);
    udpMaxPermitDeadTimeouts = options->getPropInt("@udpMaxPermitDeadTimeouts", udpMaxPermitDeadTimeouts);
    udpRequestDeadTimeout = options->getPropInt("@udpRequestDeadTimeout", udpRequestDeadTimeout);

    udpAssumeSequential = options->getPropBool("@udpAssumeSequential", udpAssumeSequential);
    udpAllowAsyncPermits = options->getPropBool("@udpAllowAsyncPermits", udpAllowAsyncPermits);
    udpMaxPendingPermits = options->getPropInt("@udpMaxPendingPermits", 1);
    udpMinSlotsPerSender = options->getPropInt("@udpMinSlotsPerSender", udpMinSlotsPerSender);
    udpMaxClientPercent = options->getPropInt("@udpMaxClientPercent", udpMaxClientPercent);

    udpTraceFlow = options->getPropBool("@udpTraceFlow", false);
    udpTestUseUdpSockets = !options->getPropBool("@useQueue");
    udpTestSocketDelay = options->getPropInt("@udpTestSocketDelay", 0);
    udpTestSocketJitter = options->getPropBool("@udpTestSocketJitter");
    udpTestVariableDelay = options->getPropBool("@udpTestVariableDelay");
    if (udpTestSocketJitter && !udpTestSocketDelay)
    {
        printf("udpTestSocketDelay requires udpTestSocketDelay to be set - setting to 1\n");
        udpTestSocketDelay = 1;
    }
    if (udpTestVariableDelay && !udpTestSocketDelay)
    {
        printf("udpTestVariableDelay requires udpTestSocketDelay to be set - setting to 1\n");
        udpTestSocketDelay = 1;
    }
    if (udpTestSocketDelay && udpTestUseUdpSockets)
    {
        printf("udpTestSocketDelay requires queue mode (--useQueue=1) - setting it on\n");
        udpTestUseUdpSockets = false;
    }
    udpAdjustThreadPriorities = options->getPropBool("@udpAdjustThreadPriorities", udpAdjustThreadPriorities);
    packetsPerThread = options->getPropInt("@packetsPerThread");
    numReceiveSlots = options->getPropInt("@numReceiveSlots");

    isUdpTestMode = true;
    roxiemem::setTotalMemoryLimit(false, true, false, 20*1024*1024, 0, NULL, NULL);
    dbm.setown(roxiemem::createDataBufferManager(roxiemem::DATA_ALIGNMENT_SIZE));

    if (options->getPropBool("sanityCheckUdpSettings", true))
    {
        unsigned __int64 networkSpeed = options->getPropInt64("@udpNetworkSpeed", 10 * U64C(0x40000000));
        sanityCheckUdpSettings(numReceiveSlots, numThreads, networkSpeed);
    }
}

// How many times the simulated sender [i] should start
unsigned numStarts(unsigned i)
{
    if (i==1 && restartSender)
        return 2;
    return 1;
}

void simulateTraffic()
{
    const unsigned maxSendQueueSize = 100;
    try
    {
        myNode.setIp(IpAddress("1.2.3.4"));
        Owned<IReceiveManager> rm = createReceiveManager(CCD_SERVER_FLOW_PORT, CCD_DATA_PORT, CCD_CLIENT_FLOW_PORT, numReceiveSlots, false);
        unsigned begin = msTick();
        std::atomic<unsigned> workValue{0};

        asyncFor(numThreads+1, numThreads+1, [&workValue, maxSendQueueSize, &rm](unsigned i)
        {
            if (!i)
            {
                if (restartReceiver)
                {
                    Sleep(100);
                    rm.clear();
                    rm.setown(createReceiveManager(CCD_SERVER_FLOW_PORT, CCD_DATA_PORT, CCD_CLIENT_FLOW_PORT, numReceiveSlots, false));
                }
            }
            else
            {
                unsigned header = 0;
                const unsigned serverFlowPort = sendFlowWithData ? CCD_DATA_PORT : CCD_SERVER_FLOW_PORT;
                unsigned myStarts = numStarts(i);
                for (unsigned startNo = 0; startNo < myStarts; startNo++)
                {
                    IpAddress pretendIP(VStringBuffer("8.8.8.%d", i));
                    // Note - this is assuming we send flow on the data port (that option defaults true in roxie too)
                    Owned<ISendManager> sm = createSendManager(serverFlowPort, CCD_DATA_PORT, CCD_CLIENT_FLOW_PORT, maxSendQueueSize, 3, pretendIP, nullptr, false);
                    Owned<IMessagePacker> mp = sm->createMessagePacker(0, 0, &header, sizeof(header), myNode, 0);
                    unsigned numPackets = packetsPerThread / myStarts;
                    for (unsigned j = 0; j < packetsPerThread; j++)
                    {
                        if (minWork || maxWork)
                        {
                            if ((j % optWorkFrequency) == 0)
                            {
                                unsigned work = minWork;
                                if (maxWork > minWork)
                                {
                                    //Add some variability in the amount of work required for each packet
                                    unsigned extra = hashc((const byte *)&j, sizeof(j), i) % (maxWork - minWork);
                                    work += extra;
                                }

                                unsigned tally = 0;
                                for (unsigned iWork=0; iWork < work; iWork++)
                                {
                                    tally = hashc((const byte *)&iWork, sizeof(iWork), tally);
                                }
                                workValue += tally;
                            }
                        }
                        void *buf = mp->getBuffer(500, false);
                        memset(buf, i, 500);
                        mp->putBuffer(buf, 500, false);
                        mp->flush();
                    }

                    // Wait until all the packets have been sent and acknowledged, for last start only
                    // For prior starts, we are trying to simulate a sender stopping abruptly (e.g. from a restart) so we don't want to close it down cleanly.
                    if (startNo == myStarts-1)
                        while (!sm->allDone())
                            Sleep(50);
                    DBGLOG("UdpSim sender thread %d sent %d packets", i, numPackets);
                }
                DBGLOG("UdpSim sender thread %d completed", i);
            }
        });
        printf("UdpSim test took %ums\n", msTick() - begin);
    }
    catch (IException * e)
    {
        EXCLOG(e);
        e->Release();
    }
}

int main(int argc, const char **argv)
{
    InitModuleObjects();
    strdup("Make sure leak checking is working");
    queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time|MSGFIELD_microTime|MSGFIELD_milliTime|MSGFIELD_thread|MSGFIELD_prefix);
    initOptions(argc, argv);
    simulateTraffic();
    ExitModuleObjects();
    releaseAtoms();
    return 0;
}

#else

int main(int argc, const char **arv)
{
    printf("udpsim requires a build with SOCKET_SIMULATION enabled\n");
    return 2;
}

#endif
