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

///* simple test
#include "udplib.hpp"
#include "roxiemem.hpp"
//#include "udptrr.hpp"
//#include "udptrs.hpp"

#include "jthread.hpp"
#include "jsocket.hpp"
#include "jsem.hpp"
#include "jdebug.hpp"
#include <time.h>

/*=============================================================================================
Findings:
- Detect gaps in incoming sequence
- Implement then test lost packet resending 
- Probably worth special casing self->self comms. (later)

*/

#if 1
void usage()
{
    printf("USAGE: uttest [options] iprange\n");
    printf("Options are:\n");
    printf(
        "--jumboFrames\n"
        "--useAeron\n"
        "--udpLocalWriteSocketSize nn\n"
        "--udpRetryBusySenders nn\n"
        "--maxPacketsPerSender nn\n"
        "--udpQueueSize nn\n"
        "--udpRTSTimeout nn\n"
        "--udpSnifferEnabled 0|1\n"     
        "--udpTraceCategories nn\n"
        "--udpTraceLevel nn\n"
        "--dontSendToSelf\n"
        "--sendSize nnMB\n"
        "--rawSpeedTest\n"
        "--rawBufferSize nn\n"
        );
    ExitModuleObjects();
    releaseAtoms();
    exit(1);
}

const char *multicastIPStr = "239.1.1.1";
IpAddress multicastIP(multicastIPStr);
unsigned udpNumQs = 1;
unsigned numNodes;
unsigned myIndex;
unsigned udpQueueSize = 100;
unsigned maxPacketsPerSender = 0x7fffffff;
bool sending = true;
bool receiving = true;
bool dontSendToSelf = false;
offset_t sendSize = 0;

bool doSortSimulator = false;
bool simpleSequential = true;
float slowNodeSkew = 1.0;
unsigned numSortSlaves = 50;
bool useAeron = false;
bool doRawTest = false;
unsigned rawBufferSize = 1024;

unsigned rowSize = 100; // MORE - take params
bool variableRows = true;
unsigned maxMessageSize=10000;
bool incrementRowSize = variableRows;
unsigned maxRowSize=5000;
unsigned minRowSize=1;
bool readRows = true;

IpAddressArray allNodes;

struct TestHeader
{
    unsigned sequence;
    unsigned nodeIndex;
};

class SendAsFastAsPossible : public Thread
{
    ISocket *flowSocket;
    unsigned size;
    static SpinLock ratelock;
    static unsigned lastReport;
    static unsigned totalSent;

public:
    SendAsFastAsPossible(unsigned port, unsigned sendSize)
    {
        SocketEndpoint ep(port, allNodes.item(0));
        flowSocket = ISocket::udp_connect(ep);
        size = sendSize;
    }

    virtual int run()
    {
        byte *buffer = new byte[65535];
        if (!lastReport)
            lastReport = msTick();
        for (;;)
        {
            unsigned lim = (1024 * 1024) / size;
            for (unsigned i = 0; i < lim; i++)
                flowSocket->write(buffer, size);

            SpinBlock b(ratelock);
            totalSent += lim * size;
            unsigned now = msTick();
            unsigned elapsed = now - lastReport;
            if (elapsed >= 1000)
            {
                unsigned rate = (((__int64) totalSent) * 1000) / elapsed;
                DBGLOG("%.2f Mbytes/sec", ((float) rate) / (1024*1024));
                totalSent = 0;
                lastReport = now;
            }
        }
        throwUnexpected(); // loop never terminates, but some compilers complain about missing return without this line
    }
};

SpinLock SendAsFastAsPossible::ratelock;
unsigned SendAsFastAsPossible::lastReport = 0;
unsigned SendAsFastAsPossible::totalSent = 0;

class Receiver : public Thread
{
    bool running;
    Semaphore started;
    offset_t allReceived;
    CriticalSection arsect;
public:
    Receiver() : Thread("Receiver")
    {
        running = false;
        allReceived = 0;
    }

    virtual void start()
    {
        Thread::start();
        started.wait();
    }

    void stop(offset_t torecv)
    {
        CriticalBlock block(arsect);
        while (allReceived<torecv) {
            PROGLOG("Waiting for Receiver (%" I64F "d bytes remaining)",torecv-allReceived);
            CriticalUnblock unblock(arsect);
            Sleep(1000);
        }
        running = false;
    }

    virtual int run()
    {
        Owned<IReceiveManager> rcvMgr;
        if (useAeron)
        {
            SocketEndpoint myEP(7000, myNode.getIpAddress());
            rcvMgr.setown(createAeronReceiveManager(myEP));
        }
        else
            rcvMgr.setown(createReceiveManager(7000, 7001, 7002, 7003, multicastIP, udpQueueSize, maxPacketsPerSender));
        Owned<roxiemem::IRowManager> rowMgr = roxiemem::createRowManager(0, NULL, queryDummyContextLogger(), NULL, false);
        Owned<IMessageCollator> collator = rcvMgr->createMessageCollator(rowMgr, 1);
        unsigned lastReport = 0;
        offset_t receivedTotal = 0;
        offset_t lastTotal = 0;
        unsigned *received = new unsigned[numNodes];
        unsigned *lastSequence = new unsigned[numNodes];
        for (unsigned i = 0; i < numNodes; i++)
        {
            received[i] = 0;
            lastSequence[i] = 0;
        }
        running = true;
        started.signal();
        unsigned start = msTick();
        unsigned lastReceived = start;
        while (running)
        {
            bool dummy;
            Owned <IMessageResult> result = collator->getNextResult(2000, dummy);
            if (result)
            {
                if (!lastReport)
                {
                    start = msTick(); // get first message free....
                    lastReport = msTick();                  
                }
                // process data here....
                unsigned headerLength;
                const TestHeader *header = (const TestHeader *) result->getMessageHeader(headerLength);
                assertex (headerLength == sizeof(TestHeader) && header->nodeIndex < numNodes);
                if (header->sequence > lastSequence[header->nodeIndex])
                {
                    if (header->sequence != lastSequence[header->nodeIndex]+1)
                    {
                        DBGLOG("Missing messages %u-%u from node %u", lastSequence[header->nodeIndex]+1, header->sequence-1, header->nodeIndex);
                    }
                    lastSequence[header->nodeIndex] = header->sequence;
                }
                else
                {
                    DBGLOG("Out-of-sequence message %u from node %u", header->sequence, header->nodeIndex);
                    if (header->sequence+256 < lastSequence[header->nodeIndex])
                    {
                        DBGLOG("Assuming receiver restart");
                        lastSequence[header->nodeIndex] = header->sequence;
                    }
                }
                if (readRows)
                {
                    Owned<IMessageUnpackCursor> cursor = result->getCursor(rowMgr);
                    for (;;)
                    {
                        if (variableRows)
                        {
                            RecordLengthType *rowlen = (RecordLengthType *) cursor->getNext(sizeof(RecordLengthType));
                            if (rowlen)
                            {
                                const void *data = cursor->getNext(*rowlen);
                                // MORE - check contents
                                received[header->nodeIndex] += *rowlen;
                                receivedTotal += *rowlen;
                                allReceived += *rowlen;
                                ReleaseRoxieRow(rowlen);
                                ReleaseRoxieRow(data);
                            }
                            else
                                break;
                        }
                        else
                            UNIMPLEMENTED;
                    }
                }
            }   
            lastReceived = msTick();
            if (lastReport && (lastReceived - lastReport > 10000))
            {
                lastReport = lastReceived;
                offset_t receivedRecent = receivedTotal - lastTotal;
                DBGLOG("Received %" I64F "u bytes, rate = %.2f MB/s", receivedRecent, ((double)receivedRecent)/10485760.0);
                for (unsigned i = 0; i < numNodes; i++)
                {
                    DBGLOG("  %u bytes from node %u", received[i], i);
                    received[i] = 0;
                }
                DBGLOG("Received %" I64F "u bytes total", receivedTotal);
                lastTotal = receivedTotal;
            }
        }
        {
            CriticalBlock block(arsect);
            double totalRate = (((double)allReceived)/1048576.0)/((lastReceived-start)/1000.0);
            DBGLOG("Node %d All Received %" I64F "d bytes, rate = %.2f MB/s", myIndex, allReceived, totalRate);
        }
        rcvMgr->detachCollator(collator);
        delete [] received;
        delete [] lastSequence;
        return 0;
    }
};

void testNxN()
{
    if (maxPacketsPerSender > udpQueueSize)
        maxPacketsPerSender = udpQueueSize;
    Owned <ISendManager> sendMgr;
    if (useAeron)
        sendMgr.setown(createAeronSendManager(7000, udpNumQs, myNode.getIpAddress()));
    else
        sendMgr.setown(createSendManager(7000, 7001, 7002, 7003, multicastIP, 100, udpNumQs, NULL));
    Receiver receiver;

    IMessagePacker **packers = new IMessagePacker *[numNodes];
    unsigned *sequences = new unsigned[numNodes];
    for (unsigned i = 0; i < numNodes; i++)
    {
        sequences[i] = 1;
        packers[i] = NULL;
    }

    DBGLOG("Ready to start");
    if (receiving)
    {
        receiver.start();
        if (numNodes > 1)
            Sleep(5000);
    }
    offset_t sentTotal = 0;
    offset_t lastTotal = 0;
    if (sending)
    {
        Sleep(5000); // Give receivers a fighting chance
        unsigned dest = 0;
        unsigned start = msTick();
        unsigned last = start;
        if (sendSize)
        {
            unsigned n = dontSendToSelf ? numNodes -1 : numNodes;
            sendSize /= 100*n;
            sendSize *= 100*n;
        }
        for (;;)
        {
            do {
                dest++;
                if (dest == numNodes)
                    dest = 0;
            }
            while (dontSendToSelf&&(dest==myIndex));
            if (!packers[dest])
            {
                TestHeader t = {sequences[dest], myIndex};
                ServerIdentifier destServer;
                destServer.setIp(allNodes.item(dest));
                packers[dest] = sendMgr->createMessagePacker(1, sequences[dest], &t, sizeof(t), destServer, 0);
            }
            void *row = packers[dest]->getBuffer(rowSize, variableRows);
            memset(row, 0xaa, rowSize);
            packers[dest]->putBuffer(row, rowSize, variableRows);
            if (packers[dest]->size() > maxMessageSize)
            {
                unsigned now = msTick();
                if (now-last>10000) {
                    offset_t sentRecent = sentTotal - lastTotal;
                    DBGLOG("Sent %" I64F "d bytes total, rate = %.2f MB/s", sentTotal, (((double)sentTotal)/1048576.0)/((now-start)/1000.0));
                    DBGLOG("Sent %" I64F "d bytes this period, rate = %.2f MB/s", sentRecent, (((double)sentRecent)/1048576.0)/((now-last)/1000.0));
                    last = now;
                    lastTotal = sentTotal;
                }
                packers[dest]->flush();
                packers[dest]->Release();
                packers[dest] = NULL;
                sequences[dest]++;
            }
            sentTotal += rowSize;
            if (incrementRowSize)
            {
                rowSize++;
                if (rowSize==maxRowSize)
                    rowSize = minRowSize;
            }
            if (sendSize && sentTotal>=sendSize)
                break;
        }
        for (unsigned i = 0; i < numNodes; i++)
        {
            if (packers[i])
                packers[i]->flush();
        }
        DBGLOG("Node %d All Sent %" I64F "d bytes total, rate = %.2f MB/s", myIndex, sentTotal, (((double)sentTotal)/1048576.0)/((msTick()-start)/1000.0));
        while (!sendMgr->allDone())
        {
            DBGLOG("Node %d waiting for queued data to be flushed", myIndex);
            Sleep(1000);
        }
        DBGLOG("All data sent");
    }
    else if (receiving)
        Sleep(3000000);
    receiver.stop(sentTotal);
    receiver.join();
    Sleep(10*1000); // possible receivers may request retries so should leave senders alive for a bit
    for (unsigned ii = 0; ii < numNodes; ii++)
    {
        if (packers[ii])
            packers[ii]->Release();
    }
    delete [] packers;
    delete [] sequences;

}

void rawSendTest()
{
    unsigned startPort = 7002;
    for (unsigned senders = 0; senders < 10; senders++)
    {
        DBGLOG("Starting sender %d on port %d", senders+1, startPort);
        SendAsFastAsPossible *newSender = new SendAsFastAsPossible(startPort++, rawBufferSize);
        newSender->start();
        Sleep(10000);
    }
}

class SortMaster 
{
    unsigned __int64 receivingMask;
    unsigned __int64 sendingMask;
    unsigned __int64 *nodeData;
    int numSlaves;
    CriticalSection masterCrit;

    int *nextNode;
    Semaphore *receiveSem;

    int *receivesCompleted;

public:
    SortMaster(int _numSlaves)
    {
        receivingMask = 0;
        sendingMask = 0;
        numSlaves = _numSlaves;
        nodeData = new unsigned __int64[numSlaves];
        for (int i = 0; i < numSlaves; i++)
            nodeData[i] = ((unsigned __int64) 1) << i;
        nextNode = NULL;
        receiveSem = NULL;
        receivesCompleted = NULL;
        if (simpleSequential)
        {
            nextNode = new int[numSlaves];
            receiveSem = new Semaphore[numSlaves];
            for (int i = 0; i < numSlaves; i++)
            {
                nextNode[i] = i+1;
                receiveSem[i].signal();
            }
            nextNode[numSlaves-1] = 0;
        }
        else
        {
            receivesCompleted = new int[numSlaves];
            for (int i = 0; i < numSlaves; i++)
                receivesCompleted[i] = 0;
        }
    }

    ~SortMaster()
    {
        delete [] nodeData;
        delete [] nextNode;
        delete [] receiveSem;
        delete [] receivesCompleted;
    }

    int requestToSend(int sendingNode)
    {
        int receivingNode = 0;
        if (simpleSequential)
        {
            {
                CriticalBlock b(masterCrit);
                receivingNode = nextNode[sendingNode];
                nextNode[sendingNode]++;
                if (nextNode[sendingNode] >= numSlaves)
                    nextNode[sendingNode] = 0;
                sendingMask |= (((unsigned __int64) 1)<<sendingNode);
                receivingMask |= (((unsigned __int64) 1)<<receivingNode);
            }
            receiveSem[receivingNode].wait();
        }
        else
        {
            // Nigel's algorithm - find a node that this slave hasn't yet sent to, which is idle, which is furthest behind, and (if poss) which is not currently sending
            int bestScore = -1;
            while (bestScore == -1)
            {
                CriticalBlock b(masterCrit);
                for (int i = 0; i < numSlaves; i++)
                {
                    if ((nodeData[sendingNode] & (((unsigned __int64) 1) << i)) == 0)
                    {
                        // I still need to send to this node
                        if ((receivingMask & (((unsigned __int64) 1) << i)) == 0)
                        {
                            // and it is idle...
                            int score = 2*receivesCompleted[i];
                            if ((sendingMask & (((unsigned __int64) 1) << i)) != 0)
                                score++;
                            if (score < bestScore || bestScore==-1)
                            {
                                bestScore = score;
                                receivingNode = i;
                            }
                        }
                    }
                }
                if (bestScore == -1)
                {
                    CriticalUnblock b(masterCrit);
                    Sleep(10); // MORE - should wait until something changes then retry 
                }
                else
                {
                    sendingMask |= (((unsigned __int64) 1)<<sendingNode);
                    receivingMask |= (((unsigned __int64) 1)<<receivingNode);
                    nodeData[sendingNode] |= (((unsigned __int64) 1) << receivingNode);
                    break;
                }
            }
        }
        return receivingNode;
    };

    void noteTransferStart(int sendingNode, int receivingNode)
    {
        // Nothing here at the moment - we set the masks in requestToSend to ensure atomicity
    };

    void noteTransferEnd(int sendingNode, int receivingNode)
    {
        CriticalBlock b(masterCrit);
        sendingMask &= ~(((unsigned __int64) 1)<<sendingNode);
        receivingMask &= ~(((unsigned __int64) 1)<<receivingNode);
        if (simpleSequential)
            receiveSem[receivingNode].signal();
        else
            receivesCompleted[receivingNode]++;

    };

    inline int queryNumSlaves() const
    {
        return numSlaves;
    }
};

class SortSlave : public Thread
{
    SortMaster *master;
    int myIdx;
    int slavesDone;

    int dataSize(int targetIndex)
    {
        if (targetIndex==0 && slowNodeSkew)
            return (int)(1000 * slowNodeSkew);
        else
            return 1000;
    }

public:
    SortSlave()
    {
        master = NULL;
        myIdx = -1;
        slavesDone = 0;
    }
    void init(SortMaster *_master, unsigned _myIdx) 
    {
        master = _master;
        myIdx = _myIdx;
        slavesDone = 0;
    }
    void sendTo(unsigned datasize, unsigned slaveIdx)
    {
        assert(slaveIdx != myIdx);
        DBGLOG("Node %d sending %d bytes to node %d", myIdx, datasize, slaveIdx);
        master->noteTransferStart(myIdx, slaveIdx);
        Sleep(datasize);
        master->noteTransferEnd(myIdx, slaveIdx);
    }

    virtual int run()
    {
        while (slavesDone < (master->queryNumSlaves() - 1))
        {
            unsigned nextDest = master->requestToSend(myIdx);
            sendTo(dataSize(nextDest), nextDest);
            slavesDone++;
        }
        return 0;
    }
};

void sortSimulator()
{
    // test out various ideas for determining the order in which n nodes should exchange data....

    SortMaster master(numSortSlaves);
    SortSlave *slaves = new SortSlave[numSortSlaves];
    unsigned start = msTick();
    for (unsigned i = 0; i < numSortSlaves; i++)
    {
        slaves[i].init(&master, i);
        slaves[i].start();
    }
    for (unsigned j = 0; j < numSortSlaves; j++)
    {
        slaves[j].join();
    }
    unsigned elapsed = msTick() - start;
    DBGLOG("Complete in %d.%03d seconds", elapsed / 1000, elapsed % 1000);
    DBGLOG("sequential=%d, skewFactor %f", (int) simpleSequential, slowNodeSkew);
    delete[] slaves;
}

int main(int argc, char * argv[] ) 
{
    InitModuleObjects();
    if (argc < 2)
        usage();

    strdup("Make sure leak checking is working");
    queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time | MSGFIELD_thread | MSGFIELD_prefix);

    {
        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator("UDPTRANSPORT");
        lf->setCreateAliasFile(false);
        lf->setRolling(false);
        lf->setAppend(false);
        lf->setMaxDetail(TopDetail);
        lf->setMsgFields(MSGFIELD_STANDARD);
        lf->beginLogging();
    }

    StringBuffer cmdline;
    int c;
    for (c = 0; c < argc; c++) {
        if (c)
            cmdline.append(' ');
        cmdline.append(argv[c]);
    }
    DBGLOG("%s",cmdline.str());
//  queryLogMsgManager()->enterQueueingMode();
//  queryLogMsgManager()->setQueueDroppingLimit(512, 32);
    udpRequestToSendTimeout = 5000;
    for (c = 1; c < argc; c++)
    {
        const char *ip = argv[c];
        const char *dash = strchr(ip, '-');
        if (dash==ip)
        {
            if (strcmp(ip, "--udpQueueSize")==0)
            {
                c++;
                if (c==argc || !isdigit(*argv[c]))
                    usage();
                udpQueueSize = atoi(argv[c]);
            }
            if (strcmp(ip, "--udpRTSTimeout")==0)
            {
                c++;
                if (c==argc || !isdigit(*argv[c]))
                    usage();
                udpRequestToSendTimeout = atoi(argv[c]);
            }
            else if (strcmp(ip, "--jumboFrames")==0)
            {
                roxiemem::setDataAlignmentSize(0x2000);
            }
            else if (strcmp(ip, "--useAeron")==0)
            {
                useAeron = true;
            }
            else if (strcmp(ip, "--rawSpeedTest")==0)
            {
                doRawTest = true;
            }
            else if (strcmp(ip, "--udpLocalWriteSocketSize")==0)
            {
                c++;
                if (c==argc)
                    usage();
                udpLocalWriteSocketSize = atoi(argv[c]);
            }
            else if (strcmp(ip, "--udpRetryBusySenders")==0)
            {
                c++;
                if (c==argc)
                    usage();
                udpRetryBusySenders = atoi(argv[c]);
            }
            else if (strcmp(ip, "--maxPacketsPerSender")==0)
            {
                c++;
                if (c==argc)
                    usage();
                maxPacketsPerSender = atoi(argv[c]);
            }
            else if (strcmp(ip, "--udpSnifferEnabled")==0)
            {
                c++;
                if (c==argc)
                    usage();
                udpSnifferEnabled = atoi(argv[c]) != 0;
            }
            else if (strcmp(ip, "--udpTraceLevel")==0)
            {
                c++;
                if (c==argc)
                    usage();
                udpTraceLevel = atoi(argv[c]);
            }
            else if (strcmp(ip, "--udpTraceCategories")==0)
            {
                c++;
                if (c==argc)
                    usage();
                udpTraceCategories = atoi(argv[c]);
            }
            else if (strcmp(ip, "--dontSendToSelf")==0)
            {
                dontSendToSelf = true;
            }
            else if (strcmp(ip, "--sortSimulator")==0)
            {
                doSortSimulator = true;
            }
            else if (strcmp(ip, "--sendSize")==0)
            {
                c++;
                if (c==argc)
                    usage();
                sendSize = (offset_t)atoi(argv[c])*(offset_t)0x100000;
            }
            else if (strcmp(ip, "--rawBufferSize")==0)
            {
                c++;
                if (c==argc)
                    usage();
                rawBufferSize = atoi(argv[c]);
            }
            else
                usage();
        }
        else if (dash && isdigit(dash[1]) && dash>ip && isdigit(dash[-1]))
        {
            const char *startrange = dash-1;
            while (isdigit(startrange[-1]))
                startrange--;
            char *endptr;
            unsigned firstnum = atoi(startrange);
            unsigned lastnum = strtol(dash+1, &endptr, 10);
            while (firstnum <= lastnum)
            {
                StringBuffer ipstr;
                ipstr.append(startrange - ip, ip).append(firstnum).append(endptr);
                const IpAddress nodeIP(ipstr);
                allNodes.append(nodeIP);
                nodeIP.getIpText(ipstr.clear());
                printf("Added node %s\n", ipstr.str());
                firstnum++;
            }
        }
        else
        {
            const IpAddress nodeIP(ip);
            allNodes.append(nodeIP);
            printf("Added node %s\n", ip);
        }
    }
    if (doRawTest)
        rawSendTest();
    else if (doSortSimulator)
        sortSimulator();
    else
    {
        numNodes = allNodes.ordinality();
        myNode.setIp(IpAddress("."));
        myIndex = numNodes;
        ForEachItemIn(idx, allNodes)
        {
            if (allNodes.item(idx).ipequals(myNode.getIpAddress()))
            {
                myIndex = idx;
                break;
            }
        }
        if (myIndex >= numNodes)
        {
            printf("ERROR: my ip does not appear to be in range\n");
            usage();
        }
        roxiemem::setTotalMemoryLimit(false, true, false, 1048576000, 0, NULL, NULL);
        testNxN();
        roxiemem::releaseRoxieHeap();
    }
    ExitModuleObjects();
    releaseAtoms();
    return 0;
}

#else
// Ole's old test - look at sometime!
#define MAX_PACKERS 10
#define MAX_PACKETS 20

struct PackerInfo {
    unsigned numPackets;
    unsigned packetsSizes[MAX_PACKETS];
};


char *progName;
bool noendwait = false;
unsigned thisTrace = 1;
unsigned modeType = 0;
unsigned myIndex = 0;
unsigned destA = 0;
unsigned destB = 0;
char *multiCast = "239.1.1.2";
unsigned udpNumQs = 3;

unsigned numPackers = 2;
unsigned numSizes = 4;
unsigned numSends = 10;
unsigned initSize = 100;
unsigned sizeMulti = 2;
unsigned delayPackers = 0;
unsigned getUnpackerTimeout = 10000;
unsigned packerHdrSize = 32;
struct PackerInfo packersInfo[MAX_PACKERS];  // list of packers info, if used. each is alist of sizes (msgs).
unsigned numPackersInfo = 0;

void usage(char *err = NULL) 
{
    if (err) fprintf(stderr, "Usage Error: %s\n", err);
    fprintf(stderr, "Usage: %s [ -send [-destA IP] [-destB IP] ] [-receive]\n", progName);
    fprintf(stderr, "          [-multiCast IP] [-udpTimeout sec] [-udpMaxTimeouts val]\n");
    fprintf(stderr, "          [-udpNumQs val] [-udpQsPriority val] [-packerHdrSize val]\n");
    fprintf(stderr, "          [-numPackers val] [-numSizes val] [-numSends val]\n");
    fprintf(stderr, "          [-initSize val] [-sizeMulti val] [-delayPackers msec]\n");
    fprintf(stderr, "          [-udpTrace val] [-thisTrace val] [-noendwait]\n");

    fprintf(stderr, " [-send]              : Sets the mode to sender mode (i.e roxie slave like) <default dual mode>\n");
    fprintf(stderr, " [-receive]           : Sets the mode to receiver mode (i.e roxie server like) <default dual mode>\n");
    fprintf(stderr, " [-destA IP]          : Sets the sender destination ip address to IP (i.e roxie server IP) <default to local host>\n");
    fprintf(stderr, " [-destB IP]          : Sets the sender second destination ip address to IP <default no sec dest>\n");
    fprintf(stderr, " [-multiCast IP]      : Sets the sniffer multicast ip address to IP <default %s>\n", multiCast);
    fprintf(stderr, " [-udpTimeout msec]   : Sets the sender udpRequestToSendTimeout value  <default %i>\n", udpRequestToSendTimeout);
    fprintf(stderr, " [-udpMaxTimeouts val]: Sets the sender udpMaxRetryTimedoutReqs value <default %i>\n", udpMaxRetryTimedoutReqs);
    fprintf(stderr, " [-udpNumQs val]      : Sets the sender's number of output queues <default %i>\n", udpNumQs);
    fprintf(stderr, " [-udpQsPriority val] : Sets the sender's output queues priority udpQsPriority <default %i>\n", udpOutQsPriority);
    fprintf(stderr, " [-packerHdrSize val] : Sets the packers header size (like RoxieHeader) <default %i>\n", packerHdrSize);
    fprintf(stderr, " [-numPackers val]    : Sets the number of packers/unpackers to create/expect <default %i>\n", numPackers);
    fprintf(stderr, " [-packers val vale .]: Sets a packer specific packet sizes, this option can be repeated as many packers as needed\n");
    fprintf(stderr, " [-numSizes val]      : Sets the number of packet data sizes to try sending/receiving <default %i>\n", numSizes);
    fprintf(stderr, " [-numSends val]]     : Sets the number of msgs per size per packer to send <default %i>\n", numSends);
    fprintf(stderr, " [-initSize val]      : Sets the size of the first msg(s) per packer to send <default %i>\n", initSize);
    fprintf(stderr, " [-sizeMulti val]     : Sets the multiplier value of the size of subsequent msgs per packer <default %i>\n", sizeMulti);
    fprintf(stderr, " [-delayPackers msec] : Sets the delay value between sent packers (simulate roxie server/slave) <default %i>\n", delayPackers);
    fprintf(stderr, " [-getUnpackerTimeout msec] : Sets the timeout value used when calling getNextUnpacker <default %i>\n", getUnpackerTimeout);
    fprintf(stderr, " [-thisTrace val]     : Sets the trace level of this program <default %i>\n", thisTrace);
    fprintf(stderr, " [-udpTrace val]      : Sets the udpTraveLevel value <default %i>\n", udpTraceLevel);

    fprintf(stderr,"\n\nEnter q to terminate program : ");
    fflush(stdout);

    char tmpBuf[10]; scanf("%s", tmpBuf);
    exit(1);
}

#define SND_MODE_BIT 0x01
#define RCV_MODE_BIT 0x02


int main(int argc, char * argv[] ) 
{
    InitModuleObjects();
    progName = argv[0];
    destA = myIndex = addRoxieNode(GetCachedHostName());

    udpRequestToSendTimeout = 5000;
    udpMaxRetryTimedoutReqs = 3;
    udpOutQsPriority = 5;
    udpTraceLevel = 1;

    setTotalMemoryLimit(104857600);

    char errBuff[100];

    for (int i = 1; i < argc; i++)
    {
        if (*argv[i] == '-')
        {
            if(stricmp(argv[i]+1,"send")==0)
                modeType |= SND_MODE_BIT;
            else if(stricmp(argv[i]+1,"receive")==0)
                modeType |= RCV_MODE_BIT;
            else if(stricmp(argv[i]+1,"noendwait")==0)
                noendwait = true;
            else if(stricmp(argv[i]+1,"destA")==0)
            {
                if (i+1 < argc) 
                {
                    destA = addRoxieNode(argv[++i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing IP address after \"%s\"", argv[i]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"destB")==0)
            {
                if (i+1 < argc) 
                {
                    destB = addRoxieNode(argv[++i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing IP address after \"%s\"", argv[i]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"multiCast")==0)
            {
                if (++i < argc) 
                {
                    multiCast = argv[i];
                }
                else 
                {
                    sprintf(errBuff,"Missing IP address after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"udpTimeout")==0)
            {
                if (++i < argc) 
                {
                    udpRequestToSendTimeout = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"udpMaxTimeouts")==0)
            {
                if (++i < argc) 
                {
                    udpMaxRetryTimedoutReqs = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"udpNumQs")==0)
            {
                if (++i < argc) 
                {
                    udpNumQs = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"udpQsPriority")==0)
            {
                if (++i < argc) 
                {
                    udpOutQsPriority = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            
            else if(stricmp(argv[i]+1,"packerHdrSize")==0)
            {
                if (++i < argc) 
                {
                    packerHdrSize = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }   
            else if(stricmp(argv[i]+1,"numPackers")==0)
            {
                if (++i < argc) 
                {
                    numPackers = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"packer")==0)
            {
                if (numPackersInfo >= MAX_PACKERS) 
                {
                    sprintf(errBuff,"Too many packers are listed  - max=%i", MAX_PACKERS);
                    usage(errBuff);
                }

                struct PackerInfo &packerInfo = packersInfo[numPackersInfo];
                packerInfo.numPackets = 0;
                while ((++i < argc) && (*argv[i] != '-'))
                {
                    if (packerInfo.numPackets >= MAX_PACKETS) 
                    {
                        sprintf(errBuff,"Too many packets in packer - max=%i", MAX_PACKETS);
                        usage(errBuff);
                    }
                    packerInfo.packetsSizes[packerInfo.numPackets] = atoi(argv[i]);
                    packerInfo.numPackets++;
                }
                if (packerInfo.numPackets == 0) 
                {
                    sprintf(errBuff,"Missing packer packets info");
                    usage(errBuff);
                }
                --i;
                numPackersInfo++;
            }   
            else if(stricmp(argv[i]+1,"numSizes")==0)
            {
                if (++i < argc) 
                {
                    numSizes = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"numSends")==0)
            {
                if (++i < argc) 
                {
                    numSends = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"initSize")==0)
            {
                if (++i < argc) 
                {
                    initSize = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"sizeMulti")==0)
            {
                if (++i < argc) 
                {
                    sizeMulti = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"delayPackers")==0)
            {
                if (++i < argc) 
                {
                    delayPackers = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"getUnpackerTimeout")==0)
            {
                if (++i < argc) 
                {
                    getUnpackerTimeout = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"thisTrace")==0)
            {
                if (++i < argc) 
                {
                    thisTrace = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else if(stricmp(argv[i]+1,"udpTrace")==0)
            {
                if (++i < argc) 
                {
                    udpTraceLevel = atoi(argv[i]);
                }
                else 
                {
                    sprintf(errBuff,"Missing value after \"%s\"", argv[i-1]);
                    usage(errBuff);
                }
            }
            else
            {
                sprintf(errBuff,"Invalid argument option \"%s\"", argv[i]);
                usage(errBuff);
            }
        }
        else
        {
            sprintf(errBuff,"Argument option \"%s\" missing \"-\" ", argv[i]);
            usage(errBuff);
        }
    }


    // default is daul mode (send and receive)
    if (!modeType) modeType = SND_MODE_BIT | RCV_MODE_BIT;
    
    IReceiveManager *rcvMgr = NULL;
    IRowManager *rowMgr = NULL;
    IMessageCollator *msgCollA = NULL;
    IMessageCollator *msgCollB = NULL;

    ISendManager *sendMgr = NULL;

    if (modeType & RCV_MODE_BIT) 
    {
        rcvMgr = createReceiveManager(7000, 7001, 7002, 7003, multiCast, 100, 0x7fffffff);
        rowMgr = createRowManager(0, NULL, queryDummyContextLogger(), NULL, false);
        msgCollA = rcvMgr->createMessageCollator(rowMgr, 100);
        if (destB)
        {
            msgCollB = rcvMgr->createMessageCollator(rowMgr, 200);
        }
        Sleep(1000);
    }

    if (modeType & SND_MODE_BIT)
    {
        sendMgr = createSendManager(7000, 7001, 7002, 7003, multiCast, 100, udpNumQs, 100, NULL, myIndex);
        Sleep(5000);

        char locBuff[100000];
        for (unsigned packerNum=0; packerNum < numPackers; packerNum++) 
        {
            unsigned totalSize = 0;
            char packAHdr[100];
            char packBHdr[100];
            sprintf(packAHdr,"helloA%i", packerNum);
            if (thisTrace)
                printf("Creating packer - hdrLen=%i header %s\n", packerHdrSize, packAHdr);
            IMessagePacker *msgPackA = sendMgr->createMessagePacker(100, 0, packAHdr, packerHdrSize, destA, 1);
            IMessagePacker *msgPackB = NULL;
            if (destB)
            {
                sprintf(packBHdr,"helloB%i", packerNum);
                if (thisTrace)
                    printf("Creating packer - hdrLen=%i header %s\n", packerHdrSize, packBHdr);
                msgPackB = sendMgr->createMessagePacker(200, 0, packBHdr, packerHdrSize, destB, 0);
            }
            unsigned buffSize = initSize;
            int pkIx = packerNum;
            int nmSizes = numSizes;
            if (numPackersInfo) 
            {
                if (pkIx >= numPackersInfo)  pkIx %= numPackersInfo;
                nmSizes = packersInfo[pkIx].numPackets;
            }
            for (unsigned sizeNum=0; sizeNum < nmSizes; sizeNum++, buffSize *= sizeMulti) 
            {
                unsigned nmSends = numSends;
                if (numPackersInfo)
                {
                    nmSends = 1;
                    buffSize = packersInfo[pkIx].packetsSizes[sizeNum];
                }
                for (unsigned sendNum=0; sendNum < nmSends; sendNum++) 
                {
                    sprintf(locBuff,"size=%i num=%i multi=%i packer=%i hello world", 
                            buffSize, sendNum, sizeNum, packerNum);
                    if (thisTrace > 1)
                        printf("Sending data : %s\n", locBuff);

                    char *transBuff = (char*) msgPackA->getBuffer(buffSize, false);
                    strncpy(transBuff, locBuff, buffSize);
                    msgPackA->putBuffer(transBuff, buffSize, false);
                    
                    if (msgPackB)
                    {
                        transBuff = (char*) msgPackB->getBuffer(buffSize, false);
                        strncpy(transBuff, locBuff, buffSize);
                        msgPackB->putBuffer(transBuff, buffSize, false);
                    }

                    totalSize += buffSize;
                }
            }
            msgPackA->flush(true);
            msgPackA->Release();
            if (thisTrace)
                printf("Packer %s total data size = %i\n", packAHdr, totalSize);

            if (msgPackB) 
            {
                msgPackB->flush(true);
                msgPackB->Release();
                if (thisTrace)
                    printf("Packer %s total data size = \n", packBHdr, totalSize);
            }

            if (delayPackers) Sleep(delayPackers);
        }
        
        while(!sendMgr->allDone()) Sleep(50);
    }

    if (modeType & RCV_MODE_BIT)
    {
        for (unsigned unpackerNum=0; unpackerNum < numPackers; unpackerNum++) 
        {
            bool anyActivity_a;
            bool anyActivity_b;
            IMessageResult *resultA = msgCollA->getNextResult(getUnpackerTimeout, anyActivity_a);
            if (!resultA) 
            {
                printf("timeout waiting on msgCollA->getNextResult(%i,..)\n", getUnpackerTimeout);
            }
            IMessageResult *resultB = NULL;
            if (msgCollB) 
            {
                resultB = msgCollB->getNextResult(getUnpackerTimeout, anyActivity_b);
                if (!resultB) 
                {
                    printf("timeout waiting on msgCollB->getNextResult(%i,..)\n", getUnpackerTimeout);
                }
            }
            unsigned len;
            const void *hdr;
            char locBuff[100000];
            if (resultA)
            {
                hdr = resultA->getMessageHeader(len);
                if (thisTrace)
                    printf("Got unpacker - hdrLen=%i header %s\n", len, hdr);
            }
            if (resultB)
            {
                hdr = resultB->getMessageHeader(len);
                if (thisTrace)
                    printf("Got unpacker - hdrLen=%i header \"%s\"\n", len, hdr);
            }
        
            if (!resultA && resultB) 
            {
                resultA = resultB;
                resultB = NULL;
            }

            if (!resultA) continue;
            Owned<IMessageUnpackCursor> unpackA = resultA->getCursor(rowMgr);
            Owned<IMessageUnpackCursor> unpackB = resultB ? resultB->getCursor(rowMgr) : NULL;

            unsigned totalSize = 0;
            unsigned buffSize = initSize;
            if (unpackerNum) 
            {
                int size;
                if (thisTrace)
                    printf("Calling getNext() for all data available in packer \"%s\"\n", hdr);
                void * p= unpackA->getNext(0x0ffffffff,&size);
                totalSize += size;
            }
            else 
            {
                if (thisTrace)
                    printf("Calling getNext() with diff sizes for packer \"%s\"\n", hdr);
                buffSize = initSize;
                int pkIx = unpackerNum;
                int nmSizes = numSizes;
                if (numPackersInfo) 
                {
                    if (pkIx >= numPackersInfo)  pkIx %= numPackersInfo;
                    nmSizes = packersInfo[pkIx].numPackets;
                }
                for (unsigned sizeNum=0; sizeNum < nmSizes; sizeNum++, buffSize *= sizeMulti) 
                {
                    unsigned nmSends = numSends;
                    if (numPackersInfo)
                    {
                        nmSends = 1;
                        buffSize = packersInfo[pkIx].packetsSizes[sizeNum];
                    }
                    for (unsigned sendNum=0; sendNum < nmSends; sendNum++) 
                    {
                        int size;
                        void *transBuff= unpackA->getNext(buffSize, &size);
                        if (!transBuff) 
                        {
                            if (thisTrace > 1)
                                printf("end of data\n");
                        }
                        else {
                            totalSize += size;
                            memcpy(locBuff, transBuff, size);
                            locBuff[size]=0;
                            if (thisTrace > 1)
                                printf("Received (for size=%i num=%i multi=%i unpacker=%i) data : %s\n", 
                                        buffSize, sendNum, sizeNum, unpackerNum, locBuff);
                        }
                    }
                }
            }
            
            if (thisTrace)
                printf("Unpacker %s total data size = %i\n", hdr, totalSize);           

            buffSize=initSize;
            if (thisTrace > 1)
                printf("Trying to read more than written\n");
            void *transBuff = unpackA->getNext(buffSize);
            if (!transBuff) 
            {
                if (thisTrace > 1)
                    printf("OK: Could not read more than written\n");
            }
            else 
            {
                memcpy(locBuff, transBuff, buffSize);
                locBuff[buffSize]=0;
                printf("WARNING: read more than written: (%s)\n", locBuff);
            }
            printf("\n\n\n");
            
            unpackA->Release();
            if (unpackB) unpackB->Release();
        }

    }


    if (msgCollA) 
    {
        rcvMgr->detachCollator(msgCollA);
        msgCollA->Release();
    }
    if (msgCollB)
    {
        rcvMgr->detachCollator(msgCollB);
        msgCollB->Release();
    }
    if (sendMgr) sendMgr->Release();
    if (rcvMgr) rcvMgr->Release();

    if (!noendwait) 
    {
        printf("\n\nEnter q to terminate program : ");
        scanf("%s", errBuff);
    }
    return 0;
}
#endif


