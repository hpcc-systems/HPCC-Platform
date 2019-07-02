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
#include <thread>
#include <string>
#include <sstream>
#include <map>


class CTopologyServer : public CInterfaceOf<ITopologyServer>
{
public:
    CTopologyServer();
    CTopologyServer(const char *topologyInfo);

    virtual const SocketEndpointArray &querySlaves(unsigned channel) const override;
private:
    std::map<unsigned, SocketEndpointArray> arrays;
    static const SocketEndpointArray nullArray;
};

CTopologyServer::CTopologyServer()
{
}

CTopologyServer::CTopologyServer(const char *topologyInfo)
{
    std::istringstream ss(topologyInfo);
    std::string line;
    while (std::getline(ss, line, '\n'))
    {
        if (line.compare(0, 6, "slave|")==0)
        {
            char *endptr;
            unsigned channel = strtol(line.c_str()+6, &endptr, 10);
            endptr = strchr(endptr, '|');
            assertex(endptr);
            SocketEndpoint ep(endptr+1);
            assertex(!ep.isNull());
            arrays[channel].append(ep);
            arrays[0].append(ep);
        }
    }
}

const SocketEndpointArray &CTopologyServer::querySlaves(unsigned channel) const
{
    auto match = arrays.find(channel);
    if (match == arrays.end())
        return nullArray;
    return match->second;
}

const SocketEndpointArray CTopologyServer::nullArray;

// Class TopologyManager (there is a single instance) handles interaction with topology servers
// to provide a TopologyServer reflecting current known cluster topology

class TopologyManager
{
public:
    TopologyManager() { currentTopology.setown(new CTopologyServer); };
    void setServers(const SocketEndpointArray &_topoServers);
    void setRoles(const std::vector<RoxieEndpointInfo> &myRoles);
    const ITopologyServer &getCurrent();

    void update();
private:
    Owned<const ITopologyServer> currentTopology;
    SpinLock lock;
    SocketEndpointArray topoServers;
    const unsigned topoConnectTimeout = 1000;
    const unsigned maxReasonableResponse = 32*32*1024;  // At ~ 32 bytes per entry, 1024 channels and 32-way redundancy that's a BIG cluster!
    StringBuffer md5;
    StringBuffer topoBuf;
} topology;

void TopologyManager::update()
{
    ForEachItemIn(idx, topoServers)
    {
        try
        {
            Owned<ISocket> topo = ISocket::connect_timeout(topoServers.item(idx), topoConnectTimeout);
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
                    StringBuffer s;
                    DBGLOG("Unexpected empty response from topology server %s", topoServers.item(idx).getUrlStr(s).str());
                }
                else
                {
                    if (responseLen > maxReasonableResponse)
                    {
                        StringBuffer s;
                        DBGLOG("Unexpectedly large response (%u) from topology server %s", responseLen, topoServers.item(idx).getUrlStr(s).str());
                    }
                    else
                    {
                        MemoryBuffer mb;
                        char *mem = (char *)mb.reserveTruncate(responseLen);
                        topo->read(mem, responseLen);
                        if (responseLen>=md5.length() && mem[0]=='=')
                        {
                            if (md5.length()==0 || memcmp(mem, md5.str(), md5.length())!=0)
                            {
                                const char *eol = strchr(mem, '\n');
                                if (eol)
                                {
                                    eol++;
                                    md5.clear().append(eol-mem, mem);  // Note: includes '\n'
                                    auto newServer = new CTopologyServer(eol);
                                    SpinBlock b(lock);
                                    currentTopology.setown(newServer);
                                }
                            }
                        }
                        else
                        {
                            StringBuffer s;
                            DBGLOG("Unexpected response from topology server %s: %.*s", topoServers.item(idx).getUrlStr(s).str(), responseLen, mem);
                        }
                    }
                }
            }
        }
        catch (IException *E)
        {
            EXCLOG(E);
            E->Release();
        }
    }
}

const ITopologyServer &TopologyManager::getCurrent()
{
    SpinBlock b(lock);
    return *currentTopology.getLink();
}

void TopologyManager::setServers(const SocketEndpointArray &_topoServers)
{
    ForEachItemIn(idx, _topoServers)
        topoServers.append(_topoServers.item(idx));
}

void TopologyManager::setRoles(const std::vector<RoxieEndpointInfo> &myRoles)
{
    topoBuf.clear();
    for (auto role : myRoles)
    {
        switch (role.role)
        {
        case RoxieEndpointInfo::RoxieServer: topoBuf.append("server|"); break;
        case RoxieEndpointInfo::RoxieSlave: topoBuf.append("slave|"); break;
        default: throwUnexpected();
        }
        topoBuf.append(role.channel).append('|');
        role.ep.getUrlStr(topoBuf);
        topoBuf.append('\n');
    }
}
extern UDPLIB_API const ITopologyServer *getTopology()
{
    return &topology.getCurrent();
}

std::thread topoThread;
Semaphore abortTopo;
const unsigned topoUpdateInterval = 5000;

extern UDPLIB_API void startTopoThread(const SocketEndpointArray &topoServers, const std::vector<RoxieEndpointInfo> &myRoles, unsigned traceLevel)
{
    topology.setServers(topoServers);
    topology.setRoles(myRoles);
    topoThread = std::thread([traceLevel]()
    {
        while (!abortTopo.wait(topoUpdateInterval))
        {
            topology.update();
            if (traceLevel > 2)
            {
                Owned<const ITopologyServer> c = getTopology();
                const SocketEndpointArray &eps = c->querySlaves(0);
                ForEachItemIn(idx, eps)
                {
                    StringBuffer s;
                    DBGLOG("Slave %d: %s", idx, eps.item(idx).getIpText(s).str());
                }
            }
        }
    });
}

extern UDPLIB_API void stopTopoThread()
{
    if (topoThread.joinable())
    {
        abortTopo.signal();
        topoThread.join();
    }
}

