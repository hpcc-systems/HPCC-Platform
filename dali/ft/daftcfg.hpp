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
