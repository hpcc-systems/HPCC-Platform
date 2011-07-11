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

#ifndef HRPCUTIL_HPP
#define HRPCUTIL_HPP

#include "hrpcsock.hpp"

void SplitIpPort(StringAttr & ip, unsigned & port, const char * address);
IHRPCtransport * MakeTcpTransportFromUrl(const char *target, unsigned defaultPort);
void TcpWhoAmI(StringAttr & out);

void ListenUntilDead(HRPCserver & server, const char * errorMessage);
void ListenUntilDead(HRPCserver & server, IHRPCtransport * transport, const char * errorMessage);
IHRPCtransport * TryMakeServerTransport(unsigned port, const char * errorMessage);
void MultipleConnect(unsigned n,HRPCmodule *modules,int timeout,bool fast);
void MultipleConnect(unsigned n,HRPCmodule **modules,int timeout,bool fast);

#endif
