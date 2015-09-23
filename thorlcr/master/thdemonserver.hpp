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

#ifndef _thdemonserver_hpp
#define _thdemonserver_hpp

#include "jthread.hpp"
#include "thwatchdog.hpp"


interface IWUGraphProgress;
class CGraphBase;
interface IDeMonServer : extends IInterface
{
    virtual void takeHeartBeat(MemoryBuffer &progressMbb) = 0;
    virtual void startGraph(CGraphBase *graph) = 0;
    virtual void endGraph(CGraphBase *graph, bool success) = 0;
    virtual void endGraphs() = 0;
};


IDeMonServer *createDeMonServer();


#endif
