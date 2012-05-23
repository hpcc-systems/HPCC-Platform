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
    void main();

    virtual unsigned readData(MemoryBuffer &mb) = 0;
    virtual void stopReading() = 0;
};

CMasterWatchdogBase *createMasterWatchdog(bool udp=false);

#endif

