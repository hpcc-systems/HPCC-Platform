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
static unsigned packetsPerThread = 0;
static bool sendFlowWithData = false;

static constexpr const char * defaultYaml = R"!!(
version: "1.0"
udpsim:
  dropDataPackets: false
  dropOkToSendPackets: 0
  dropRequestReceivedPackets: 0
  dropRequestToSendPackets: 0
  dropRequestToSendMorePackets: 0
  dropSendStartPackets: 0
  dropSendCompletedPackets: 0
  help: false
  numThreads: 20
  outputconfig: false
  packetsPerThread: 10000
  sanityCheckUdpSettings: true
  sendFlowWithData: false
  udpResendLostPackets: true
  udpFlowAckTimeout: 2
  updDataSendTimeout: 20
  udpRequestTimeout: 20
  udpPermitTimeout: 50
  udpResendTimeout: 40
  udpMaxPermitDeadTimeouts: 5
  udpRequestDeadTimeout: 10000
  udpMaxPendingPermits: 10
  udpMaxClientPercent: 200
  udpAssumeSequential: false
  udpAllowAsyncPermits: true
  udpTraceLevel: 1
  udpTraceTimeouts: true
  udpTraceFlow: false
  udpAdjustThreadPriorities: true
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
    udpDropFlowPackets[flowType::ok_to_send] = options->getPropInt("@dropOkToSendPackets", 0);  // drop 1 in N
    udpDropFlowPackets[flowType::request_received] = options->getPropInt("@dropRequestReceivedPackets", 0);  // drop 1 in N
    udpDropFlowPackets[flowType::request_to_send] = options->getPropInt("@dropRequestToSendPackets", 0);  // drop 1 in N
    udpDropFlowPackets[flowType::request_to_send_more] = options->getPropInt("@dropRequestToSendMorePackets", 0);  // drop 1 in N
    udpDropFlowPackets[flowType::send_start] = options->getPropInt("@dropSendStartPackets", 0);  // drop 1 in N
    udpDropFlowPackets[flowType::send_completed] = options->getPropInt("@dropSendCompletedPackets", 0);  // drop 1 in N
#endif
    numThreads = options->getPropInt("@numThreads", 0);
    udpTraceLevel = options->getPropInt("@udpTraceLevel", 1);
    udpTraceTimeouts = options->getPropBool("@udpTraceTimeouts", true);
    udpResendLostPackets = options->getPropBool("@udpResendLostPackets", true);

    udpPermitTimeout = options->getPropInt("@udpPermitTimeout", udpPermitTimeout);
    udpRequestTimeout = options->getPropInt("@udpRequestTimeout", udpRequestTimeout);
    udpFlowAckTimeout = options->getPropInt("@udpFlowAckTimeout", udpFlowAckTimeout);
    updDataSendTimeout = options->getPropInt("@udpDataSendTimeout", updDataSendTimeout);
    udpResendTimeout = options->getPropInt("@udpResendTimeout", udpResendTimeout);
    udpMaxPermitDeadTimeouts = options->getPropInt("@udpMaxPermitDeadTimeouts", udpMaxPermitDeadTimeouts);
    udpRequestDeadTimeout = options->getPropInt("@udpRequestDeadTimeout", udpRequestDeadTimeout);

    udpAssumeSequential = options->getPropBool("@udpAssumeSequential", udpAssumeSequential);
    udpAllowAsyncPermits = options->getPropBool("@udpAllowAsyncPermits", udpAllowAsyncPermits);
    udpMaxPendingPermits = options->getPropInt("@udpMaxPendingPermits", 1);
    udpTraceFlow = options->getPropBool("@udpTraceFlow", false);
    udpAdjustThreadPriorities = options->getPropBool("@udpAdjustThreadPriorities", udpAdjustThreadPriorities);
    packetsPerThread = options->getPropInt("@packetsPerThread");

    isUdpTestMode = true;
    roxiemem::setTotalMemoryLimit(false, true, false, 20*1024*1024, 0, NULL, NULL);
    dbm.setown(roxiemem::createDataBufferManager(roxiemem::DATA_ALIGNMENT_SIZE));

    if (options->getPropBool("sanityCheckUdpSettings", true))
    {
        unsigned __int64 networkSpeed = options->getPropInt64("@udpNetworkSpeed", 10 * U64C(0x40000000));
        unsigned numReceiveSlots = 100;
        sanityCheckUdpSettings(numReceiveSlots, numThreads, networkSpeed);
    }
}

void simulateTraffic()
{
    const unsigned numReceiveSlots = 100;
    const unsigned maxSendQueueSize = 100;
    try
    {
        myNode.setIp(IpAddress("1.2.3.4"));
        Owned<IReceiveManager> rm = createReceiveManager(CCD_SERVER_FLOW_PORT, CCD_DATA_PORT, CCD_CLIENT_FLOW_PORT, numReceiveSlots, false);
        unsigned begin = msTick();
        asyncFor(numThreads, numThreads, [maxSendQueueSize](unsigned i)
        {
            unsigned header = 0;
            const unsigned serverFlowPort = sendFlowWithData ? CCD_DATA_PORT : CCD_SERVER_FLOW_PORT;
            IpAddress pretendIP(VStringBuffer("8.8.8.%d", i));
            // Note - this is assuming we send flow on the data port (that option defaults true in roxie too)
            Owned<ISendManager> sm = createSendManager(serverFlowPort, CCD_DATA_PORT, CCD_CLIENT_FLOW_PORT, maxSendQueueSize, 3, pretendIP, nullptr, false);
            Owned<IMessagePacker> mp = sm->createMessagePacker(0, 0, &header, sizeof(header), myNode, 0);
            for (unsigned j = 0; j < packetsPerThread; j++)
            {
                void *buf = mp->getBuffer(500, false);
                memset(buf, i, 500);
                mp->putBuffer(buf, 500, false);
            }
            mp->flush();

            //wait until all the packets have been sent and acknowledged
            while(!sm->allDone())
                Sleep(50);
        });
        printf("UdpSim test took %ums\n", msTick() - begin);
    }
    catch (IException * e)
    {
        StringBuffer msg;
        printf("Exception: %s\n", e->errorMessage(msg).str());
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
