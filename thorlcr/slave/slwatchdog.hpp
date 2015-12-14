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

#ifndef SLWATCHDOG_HPP
#define SLWATCHDOG_HPP

#include "jiface.hpp"
#include "thwatchdog.hpp"

class CGraphBase;
interface ISlaveWatchdog : extends IInterface
{
    virtual void startGraph(CGraphBase &graph) = 0;
    virtual void stopGraph(CGraphBase &graph, MemoryBuffer *mb=NULL) = 0;
    virtual void stop() = 0;
    virtual void debugRequest(CMessageBuffer &msg, const char *request) const = 0;
};

ISlaveWatchdog *createProgressHandler(bool udp=false);

#endif

