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

#ifndef _thdemonserver_hpp
#define _thdemonserver_hpp

#include "jthread.hpp"
#include "thwatchdog.hpp"


interface IWUGraphProgress;
class CGraphBase;
interface IDeMonServer : extends IInterface
{
    virtual void takeHeartBeat(const SocketEndpoint &sender, MemoryBuffer &progressMbb) = 0;
    virtual void startGraph(CGraphBase *graph) = 0;
    virtual void reportGraph(IWUGraphProgress *progress, CGraphBase *graph, bool finished) = 0;
    virtual void endGraph(CGraphBase *graph, bool success) = 0;
    virtual void endGraphs() = 0;
};


IDeMonServer *createDeMonServer();


#endif
