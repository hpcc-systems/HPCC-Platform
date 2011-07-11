/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */


#ifndef JBROADCAST_HPP
#define JBROADCAST_HPP


#include "jexpdef.hpp"
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
