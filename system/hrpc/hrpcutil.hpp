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
