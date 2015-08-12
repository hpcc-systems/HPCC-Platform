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

#ifndef DAFTCFG_HPP
#define DAFTCFG_HPP

#include "jsocket.hpp"

#define DAFT_VERSION            7

//All of these are fairly redundant since dead connections raise exceptions
#define FTTIME_PROGRESS         24 * 60 * 60 * 1000 // Maximum expected time between progress packets.
#define FTTIME_SENDPROGRESS     1000 * 1000         // Maximum expected time for slave to receive acknowledge of packet.
#define FTTIME_PARTITION        24 * 60 * 60 * 1000 // How long to wait for connection from slave
#define FTTIME_DIRECTORY        24 * 60 * 60 * 1000 // How long to wait for connection from slave
#define FTTIME_SIZES            24 * 60 * 60 * 1000 // How long to wait for connection from slave

#define RUN_SLAVES_ON_THREADS                       // Disable for debugging.

const char * queryFtSlaveExecutable(const IpAddress &ip, StringBuffer &ret);

#endif
