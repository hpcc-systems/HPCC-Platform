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


#ifndef _thdemon_hpp
#define _thdemon_hpp

/* This file defines various constructs shared between the server and client */


#define THOR_DEBUG_MONITOR_ENABLED

#define MAX_THOR_DEBUG_MONITOR_PAYLOAD_SIZE     8192    

enum demonpacket_t 
{
    DP_sysinfo,         // sent to a client when it connect
    DP_graphinfo,       // sent to all clients when a new graph is started, also sent to client when it connects
    DP_heartbeat,       // sent to all clients
    DP_graphprogress,   // sent to all clients from time to time
};


struct DeMonPacket
{
    size32_t size;
    demonpacket_t type;
    byte payload[MAX_THOR_DEBUG_MONITOR_PAYLOAD_SIZE];

    size32_t minSize() { return sizeof(size) + sizeof(type); }
    size32_t maxSize() { return minSize() + MAX_THOR_DEBUG_MONITOR_PAYLOAD_SIZE; }
};


struct ThorSysInfo                  // sent as the payload in a sysinfo packet
{
    int thorMajorVersion;
    int thorMinorVersion;
    byte performanceDataLevel;      // 0 - 2
};


#endif
