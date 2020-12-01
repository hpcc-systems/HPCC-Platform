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

#include "jmisc.hpp"
#include "udplib.hpp"
#include "udptopo.hpp"
#include "udpipmap.hpp"
#include "roxie.hpp"
#include "portlist.h"
#include <thread>
#include <string>
#include <sstream>
#include <map>

unsigned initIbytiDelay; // In milliseconds
unsigned minIbytiDelay;  // In milliseconds

unsigned ChannelInfo::getIbytiDelay(unsigned primarySubChannel) const  // NOTE - zero-based
{
    unsigned delay = 0;
    unsigned subChannel = primarySubChannel;
    while (subChannel != mySubChannel)
    {
        delay += currentDelay[subChannel];
        subChannel++;
        if (subChannel == numSubChannels)
            subChannel = 0;
    }
    return delay;
}

void ChannelInfo::noteChannelsSick(unsigned primarySubChannel) const
{
    unsigned subChannel = primarySubChannel;
    while (subChannel != mySubChannel)
    {
        unsigned newDelay = currentDelay[subChannel] / 2;
        if (newDelay < minIbytiDelay)
            newDelay = minIbytiDelay;
        currentDelay[subChannel] = newDelay;
        subChannel++;
        if (subChannel == numSubChannels)
            subChannel = 0;
    }
}

void ChannelInfo::noteChannelHealthy(unsigned subChannel) const
{
    currentDelay[subChannel] = initIbytiDelay;
}

ChannelInfo::ChannelInfo(unsigned _mySubChannel, unsigned _numSubChannels, unsigned _replicationLevel)
: mySubChannel(_mySubChannel), numSubChannels(_numSubChannels), myReplicationLevel(_replicationLevel)
{
    for (unsigned i = 0; i < numSubChannels; i++)
        currentDelay.emplace_back(initIbytiDelay);
}

bool ChannelInfo::otherAgentHasPriority(unsigned priorityHash, unsigned otherAgentSubChannel) const
{
    unsigned primarySubChannel = (priorityHash % numSubChannels);
    // could be coded smarter! Basically mysub - prim < theirsub - prim using modulo arithmetic, I think
    while (primarySubChannel != mySubChannel)
    {
        if (primarySubChannel == otherAgentSubChannel)
            return true;
        primarySubChannel++;
        if (primarySubChannel >= numSubChannels)
            primarySubChannel = 0;
    }
    return false;
}

static unsigned *createNewNodeHealthScore(const ServerIdentifier &)
{
    return new unsigned(initIbytiDelay);
}

static IpMapOf<unsigned> buddyHealth(createNewNodeHealthScore);   // For each buddy IP ever seen, maintains a score of how long I should wait for it to respond when it is the 'first responder'

void noteNodeSick(const ServerIdentifier &node)
{
    // NOTE - IpMapOf is thread safe (we never remove entries). Two threads hitting at the same time may result in the change from one being lost, but that's not a disaster
    unsigned current = buddyHealth[node];
    unsigned newDelay = current / 2;
    if (newDelay < minIbytiDelay)
        newDelay = minIbytiDelay;
    buddyHealth[node] = newDelay;
}

void noteNodeHealthy(const ServerIdentifier &node)
{
    // NOTE - IpMapOf is thread safe (we never remove entries). Two threads hitting at the same time may result in the change from one being lost, but that's not a disaster
    buddyHealth[node] = initIbytiDelay;
}

unsigned getIbytiDelay(const ServerIdentifier &node)
{
    return buddyHealth[node];
}

class CTopologyServer : public CInterfaceOf<ITopologyServer>
{
public:
    CTopologyServer();
    CTopologyServer(const char *topologyInfo);

    virtual const SocketEndpointArray &queryAgents(unsigned channel) const override;
    virtual const SocketEndpointArray &queryServers(unsigned port) const override;
    virtual const ChannelInfo &queryChannelInfo(unsigned channel) const override;
    virtual const std::vector<unsigned> &queryChannels() const override;
    virtual bool implementsChannel(unsigned channel) const override;
    virtual void report(StringBuffer &ret) const override;

private:
    std::map<unsigned, SocketEndpointArray> agents;  // indexed by channel
    std::map<unsigned, SocketEndpointArray> servers; // indexed by port
    static const SocketEndpointArray nullArray;
    std::map<unsigned, ChannelInfo> channelInfo;
    std::map<unsigned, unsigned> mySubChannels;
    std::vector<unsigned> channels;
    std::vector<unsigned> replicationLevels;
#ifdef _DEBUG
    StringAttr rawData;
#endif
};

SocketEndpoint myAgentEP;

CTopologyServer::CTopologyServer()
{
}

CTopologyServer::CTopologyServer(const char *topologyInfo)
#ifdef _DEBUG
    : rawData(topologyInfo)
#endif
{
    std::istringstream ss(topologyInfo);
    std::string line;
    while (std::getline(ss, line, '\n'))
    {
        StringArray fields;
        fields.appendList(line.c_str(), "|", true);
        if (fields.length()==4)
        {
            const char *role = fields.item(0);
            const char *channelStr = fields.item(1);
            const char *epStr = fields.item(2);
            const char *replStr = fields.item(3);
            char *tail = nullptr;
            unsigned channel = strtoul(channelStr, &tail, 10);
            if (*tail)
            {
                DBGLOG("Unexpected characters parsing channel in topology entry %s", line.c_str());
                continue;
            }
            tail = nullptr;
            unsigned repl = strtoul(replStr, &tail, 10);
            if (*tail)
            {
                DBGLOG("Unexpected characters parsing replication level in topology entry %s", line.c_str());
                continue;
            }
            SocketEndpoint ep;
            if (!ep.set(epStr))
            {
                DBGLOG("Unable to process endpoint information in topology entry %s", line.c_str());
                continue;
            }
            if (streq(role, "agent"))
            {
                agents[channel].append(ep);
                if (ep.equals(myAgentEP))
                {
                    mySubChannels[channel] = agents[channel].ordinality()-1;
                    channels.push_back(channel);
                    replicationLevels.push_back(repl);
                }
                agents[0].append(ep);
            }
            else if (streq(role, "server"))
                servers[ep.port].append(ep);
        }
    }
    for (unsigned i = 0; i < channels.size(); i++)
    {
        unsigned channel = channels[i];
        unsigned repl = replicationLevels[i];
        unsigned subChannel = mySubChannels[channel];
        channelInfo.emplace(std::make_pair(channel, ChannelInfo(subChannel, agents[channel].ordinality(), repl)));
    }
}

const SocketEndpointArray &CTopologyServer::queryAgents(unsigned channel) const
{
    auto match = agents.find(channel);
    if (match == agents.end())
        return nullArray;
    return match->second;
}

const SocketEndpointArray &CTopologyServer::queryServers(unsigned port) const
{
    auto match = servers.find(port);
    if (match == servers.end())
        return nullArray;
    return match->second;
}

const ChannelInfo &CTopologyServer::queryChannelInfo(unsigned channel) const
{
    auto match = channelInfo.find(channel);
    if (match == channelInfo.end())
        throw makeStringExceptionV(ROXIE_INTERNAL_ERROR, "queryChannelInfo requesting info for unexpected channel %u", channel);
    return match->second;
}

const std::vector<unsigned> &CTopologyServer::queryChannels() const
{
    return channels;
}

bool CTopologyServer::implementsChannel(unsigned channel) const
{
    if (channel)
    {
        return std::find(channels.begin(), channels.end(), channel) != channels.end();
    }
    else
        return true;   // Kinda-sorta - perhaps not true if separated servers from agents, but even then child queries may access channel 0
}

void CTopologyServer::report(StringBuffer &ret) const
{
#ifdef _DEBUG
    ret.append(rawData);
#else
    UNIMPLEMENTED;
#endif
}

const SocketEndpointArray CTopologyServer::nullArray;

// Class TopologyManager (there is a single instance) handles interaction with topology servers
// to provide a TopologyServer reflecting current known cluster topology

class TopologyManager
{
public:
    TopologyManager() { currentTopology.setown(new CTopologyServer); };
    void setServers(const StringArray &_topoServers);
    void setRoles(const std::vector<RoxieEndpointInfo> &myRoles);
    void closedown();
    const ITopologyServer &getCurrent();

    bool update();
    unsigned numServers() const { return topoServers.length(); }
    void freeze(bool frozen);

private:
    Owned<const ITopologyServer> currentTopology;
    SpinLock lock;
    StringArray topoServers;
    const unsigned topoConnectTimeout = 1000;
    const unsigned maxReasonableResponse = 32*32*1024;  // At ~ 32 bytes per entry, 1024 channels and 32-way redundancy that's a BIG cluster!
    StringBuffer md5;
    StringBuffer topoBuf;
    bool frozen = false;    // used for testing
};

static TopologyManager topologyManager;

bool TopologyManager::update()
{
    if (frozen)
        return false;
    bool updated = false;
    ForEachItemIn(idx, topoServers)
    {
        try
        {
            SocketEndpointArray eps;
            eps.fromName(topoServers.item(idx), TOPO_SERVER_PORT);
            ForEachItemIn(idx, eps)
            {
                const SocketEndpoint &ep = eps.item(idx);
                Owned<ISocket> topo = ISocket::connect_timeout(ep, topoConnectTimeout);
                if (topo)
                {
                    unsigned topoBufLen = md5.length()+topoBuf.length();
                    _WINREV(topoBufLen);
                    topo->write(&topoBufLen, 4);
                    topo->write(md5.str(), md5.length());
                    topo->write(topoBuf.str(), topoBuf.length());
                    unsigned responseLen;
                    topo->read(&responseLen, 4);
                    _WINREV(responseLen);
                    if (!responseLen)
                    {
                        DBGLOG("Unexpected empty response from topology server %s", topoServers.item(idx));
                    }
                    else
                    {
                        if (responseLen > maxReasonableResponse)
                        {
                            DBGLOG("Unexpectedly large response (%u) from topology server %s", responseLen, topoServers.item(idx));
                        }
                        else
                        {
                            MemoryBuffer mb;
                            char *mem = (char *)mb.reserveTruncate(responseLen+1);
                            topo->read(mem, responseLen);
                            mem[responseLen] = '\0';
                            if (responseLen>=md5.length() && mem[0]=='=')
                            {
                                if (md5.length()==0 || memcmp(mem, md5.str(), md5.length())!=0)
                                {
                                    const char *eol = strchr(mem, '\n');
                                    if (eol)
                                    {
                                        eol++;
                                        md5.clear().append(eol-mem, mem);  // Note: includes '\n'
                                        Owned<const ITopologyServer> newServer = new CTopologyServer(eol);
                                        SpinBlock b(lock);
                                        currentTopology.swap(newServer);
                                        updated = true;
                                    }
                                }
                            }
                            else
                            {
                                StringBuffer s;
                                DBGLOG("Unexpected response from topology server %s: %.*s", topoServers.item(idx), responseLen, mem);
                            }
                        }
                    }
                }
            }
        }
        catch (IException *E)
        {
            DBGLOG("While connecting to %s", topoServers.item(idx));
            EXCLOG(E);
            E->Release();
        }
    }
    return updated;
}

void TopologyManager::freeze(bool _frozen)
{
    frozen = _frozen;
}

const ITopologyServer &TopologyManager::getCurrent()
{
    SpinBlock b(lock);
    return *currentTopology.getLink();
}

void TopologyManager::setServers(const StringArray &_topoServers)
{
    ForEachItemIn(idx, _topoServers)
        topoServers.append(_topoServers.item(idx));
}

void TopologyManager::setRoles(const std::vector<RoxieEndpointInfo> &myRoles)
{
    topoBuf.clear();
    for (const auto &role : myRoles)
    {
        switch (role.role)
        {
        case RoxieEndpointInfo::RoxieServer: topoBuf.append("server|"); break;
        case RoxieEndpointInfo::RoxieAgent: topoBuf.append("agent|"); break;
        default: throwUnexpected();
        }
        topoBuf.append(role.channel).append('|');
        role.ep.getUrlStr(topoBuf);
        topoBuf.append('|').append(role.replicationLevel);
        topoBuf.append('\n');
    }
    Owned<const ITopologyServer> newServer = new CTopologyServer(topoBuf);   // We set the initial topology to just the local information we know about
    SpinBlock b(lock);
    currentTopology.swap(newServer);
}

void TopologyManager::closedown()
{
    topoBuf.replaceString("server|", "-server|");
    topoBuf.replaceString("agent|", "-agent|");
    freeze(false);
    update();
}

extern UDPLIB_API const ITopologyServer *getTopology()
{
    return &topologyManager.getCurrent();
}

extern UDPLIB_API void freezeTopology(bool frozen)
{
    topologyManager.freeze(frozen);
}

extern UDPLIB_API unsigned getNumAgents(unsigned channel)
{
    Owned<const ITopologyServer> topology = getTopology();
    return topology->queryAgents(channel).ordinality();
}

#ifndef _CONTAINERIZED
extern UDPLIB_API void createStaticTopology(const std::vector<RoxieEndpointInfo> &allRoles, unsigned traceLevel)
{
    topologyManager.setRoles(allRoles);
}
#endif

static std::thread topoThread;
static Semaphore abortTopo;
const unsigned topoUpdateInterval = 5000;

extern UDPLIB_API void initializeTopology(const StringArray &topoValues, const std::vector<RoxieEndpointInfo> &myRoles)
{
    topologyManager.setServers(topoValues);
    topologyManager.setRoles(myRoles);
}

extern UDPLIB_API void publishTopology(unsigned traceLevel)
{
    if (topologyManager.numServers())
    {
        topoThread = std::thread([traceLevel]()
        {
            topologyManager.update();
            unsigned waitTime = 1000;  // First time around we don't wait as long, so that system comes up faster
            while (!abortTopo.wait(waitTime))
            {
                if (topologyManager.update() && traceLevel)
                {
                    DBGLOG("Topology information updated:");
                    Owned<const ITopologyServer> c = getTopology();
                    const SocketEndpointArray &eps = c->queryAgents(0);
                    ForEachItemIn(idx, eps)
                    {
                        StringBuffer s;
                        DBGLOG("Agent %d: %s", idx, eps.item(idx).getIpText(s).str());
                    }
                }
                waitTime = topoUpdateInterval;
            }
            topologyManager.closedown();
        });
    }
}

extern UDPLIB_API void stopTopoThread()
{
    if (topoThread.joinable())
    {
        abortTopo.signal();
        topoThread.join();
    }
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class BuddyHealthTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(BuddyHealthTest);
        CPPUNIT_TEST(testBuddyHealth);
    CPPUNIT_TEST_SUITE_END();

    void testBuddyHealth()
    {
        initIbytiDelay = 64;
        minIbytiDelay = 16;
        IpAddress a1("123.4.5.1");
        IpAddress a2("123.4.6.2");
        IpAddress a3("123.4.5.3");
        CPPUNIT_ASSERT(getIbytiDelay(a1)==initIbytiDelay);
        noteNodeSick(a1);
        noteNodeSick(a2);
        CPPUNIT_ASSERT(getIbytiDelay(a1)==initIbytiDelay/2);
        CPPUNIT_ASSERT(getIbytiDelay(a2)==initIbytiDelay/2);
        CPPUNIT_ASSERT(getIbytiDelay(a3)==initIbytiDelay);
        noteNodeHealthy(a1);
        CPPUNIT_ASSERT(getIbytiDelay(a1)==initIbytiDelay);
        CPPUNIT_ASSERT(getIbytiDelay(a2)==initIbytiDelay/2);
        CPPUNIT_ASSERT(getIbytiDelay(a3)==initIbytiDelay);
        noteNodeSick(a2);
        noteNodeSick(a2);
        noteNodeSick(a2);
        noteNodeSick(a2);
        noteNodeSick(a2);
        CPPUNIT_ASSERT(getIbytiDelay(a2)==minIbytiDelay);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( BuddyHealthTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( BuddyHealthTest, "BuddyHealthTest" );

#endif
