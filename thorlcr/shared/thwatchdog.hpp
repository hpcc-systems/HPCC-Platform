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

#ifndef THWATCHDOG_HPP
#define THWATCHDOG_HPP

#include "jsocket.hpp"
#include "thor.hpp"

#define HEARTBEAT_INTERVAL      15          // seconds
#define UDP_DATA_MAX            1024 * 8    // 8k
#define THORBEAT_INTERVAL       10*1000     // 10 sec!
#define THORBEAT_RETRY_INTERVAL 4*60*1000   // 4 minutes


struct HeartBeatPacketHeader
{
    size32_t packetSize;   // used as validity check must be first
    SocketEndpoint sender;
    unsigned tick;         // sequence check
    size32_t progressSize; // size of progress data (following performamce data)
};

#endif

