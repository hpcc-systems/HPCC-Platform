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


#ifndef JBROADCAST_HPP
#define JBROADCAST_HPP

#include "jiface.hpp"

enum bctag_t
{
    bctag_unknown = -1,
    bctag_general = 0,
    bctag_DYNAMIC = 1,
};

bctag_t jlib_decl allocBcTag();
void jlib_decl freeBcTag();

interface IBroadcast : extends IInterface
{
    virtual bool send(bctag_t, size32_t sz, const void *data) = 0;
    virtual void stop() = 0;
    virtual void stopClients() = 0;
};

interface IBroadcastReceiver : extends IInterface
{
    virtual bool eos() = 0;
    virtual bool read(MemoryBuffer &mb) = 0;
    virtual void stop() = 0;
};


IBroadcast jlib_decl *createGroupBroadcast(SocketEndpointArray &eps, const char *mcastIp, unsigned mcastPort, unsigned broadcasterAckPort);
IBroadcast jlib_decl *createGroupBroadcast(SocketEndpointArray &eps, SocketEndpoint &mcastEp, unsigned broadcasterAckPort);
IBroadcastReceiver jlib_decl *createGroupBroadcastReceiver(bctag_t tag);

void jlib_decl startMCastRecvServer(const char *broadcastRoot, unsigned groupMember, SocketEndpoint &mcastEp, unsigned broadcasterAckPort);
void jlib_decl startMCastRecvServer(const char *broadcastRoot, unsigned rank, const char *mcastIp, unsigned mcastPort, unsigned broadcasterAckPort);
void jlib_decl stopMCastRecvServer();

enum bcopt_t
{
    bcopt_pollDelay,
    bcopt_useUniCast,
    bcopt_tracingLevel,
    bcopt_unicastLimit,
    bcopt_unicastPcent,
};

void jlib_decl setBroadcastOpt(bcopt_t opt, unsigned value);

#endif
