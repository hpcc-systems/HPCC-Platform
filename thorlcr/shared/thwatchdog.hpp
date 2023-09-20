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
public:
    size32_t packetSize = 0;   // used as validity check must be first
    size32_t progressSize = 0; // size of progress data (following performance data)
    unsigned tick = 0;         // sequence check
    SocketEndpoint sender;

public:
    void serialize(MemoryBuffer & out) const
    {
        out.append(packetSize).append(tick).append(progressSize);
        sender.serialize(out);
    }
    void deserialize(MemoryBuffer & in)
    {
        in.read(packetSize).read(tick).read(progressSize);
        sender.deserialize(in);
    }
};

#endif

