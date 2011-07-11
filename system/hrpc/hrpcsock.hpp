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

#ifndef HRPCSOCK_HPP
#define HRPCSOCK_HPP

#include "hrpc.hpp"

class SocketEndpoint;

IHRPCtransport *MakeTcpTransport(   const char *target, // NULL for server, "" if name not yet known
                                    int port,
                                    int listenqsize=10  // listen queue size for server 
                                    ) ;

// Alternatives
IHRPCtransport *MakeClientTcpTransport( SocketEndpoint &endpoint ) ;
IHRPCtransport *MakeServerTcpTransport( int port,int listenqsize=10 );  
IHRPCtransport *MakeServerTcpTransport( SocketEndpoint &endpoint,int listenqsize=10 );  

bool getTcpTarget(IHRPCtransport *transport,SocketEndpoint &ep);
                                    


/* Exceptions raised by MakeTcpTransport

HRPCERR_transport_port_in_use


*/


#endif


                                  
