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
struct HeartBeatPacket;

class CMasterWatchdog : public CSimpleInterface, implements IThreaded
{
    CThreaded threaded;
public:
    CMasterWatchdog();
    ~CMasterWatchdog();
    void addSlave(const SocketEndpoint &slave);
    void removeSlave(const SocketEndpoint &slave);
    CMachineStatus *findSlave(const SocketEndpoint &ep);
    void checkMachineStatus();
    void stop();
    void main();
private:
    PointerArray state;
    SocketEndpoint master;
    ISocket *sock;
    Mutex mutex;
    bool stopped;
    int retrycount;
};

#endif

