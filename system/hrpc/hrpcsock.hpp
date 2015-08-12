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


                                  
