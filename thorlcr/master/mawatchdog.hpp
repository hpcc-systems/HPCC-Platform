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

#ifndef MAWATCHDOG_HPP
#define MAWATCHDOG_HPP

#include "jlib.hpp"
#include "jsocket.hpp"
#include "jthread.hpp"
#include "jmutex.hpp"

class CMachineStatus;
struct HeartBeatPacketHeader;

class CMasterWatchdogBase : public CSimpleInterface, implements IThreaded
{
    PointerArray state;
    SocketEndpoint master;
    Mutex mutex;
    int retrycount;
    CThreaded threaded;
protected:
    bool stopped;
    unsigned watchdogMachineTimeout;
public:
    CMasterWatchdogBase();
    ~CMasterWatchdogBase();
    void addSlave(const SocketEndpoint &slave);
    void removeSlave(const SocketEndpoint &slave);
    CMachineStatus *findSlave(const SocketEndpoint &ep);
    void checkMachineStatus();
    unsigned readPacket(HeartBeatPacketHeader &hb, MemoryBuffer &mb);
    void start();
    void stop();
    virtual void threadmain() override;

    virtual unsigned readData(MemoryBuffer &mb) = 0;
    virtual void stopReading() = 0;
};

CMasterWatchdogBase *createMasterWatchdog(bool udp=false, bool startNow=false);

#endif

