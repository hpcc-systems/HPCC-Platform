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

#ifndef UDPTOPO_INCL
#define UDPTOPO_INCL
#include "jlib.hpp"
#include "jsocket.hpp"
#include "udplib.hpp"

interface ITopologyServer : public IInterface
{
    virtual const SocketEndpointArray &querySlaves(unsigned channel) const = 0;
};

extern UDPLIB_API const ITopologyServer *getTopology();

struct RoxieEndpointInfo
{
    enum Role { RoxieServer, RoxieSlave } role;
    unsigned channel;
    SocketEndpoint ep;
};

extern UDPLIB_API void startTopoThread(const SocketEndpointArray &topoServers, const std::vector<RoxieEndpointInfo> &myRoles, unsigned traceLevel);

#endif
