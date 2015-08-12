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

#ifndef HRPCMP_HPP
#define HRPCMP_HPP

#include "hrpc.hpp"
#include <mpbase.hpp>
#include <mpcomm.hpp>


// Intra Communication. rank is server's rank, server tag is tag server is listening on
IHRPCtransport *MakeClientMpTransport( ICommunicator *comm, rank_t rank, mptag_t servertag ) ;
IHRPCtransport *MakeServerMpTransport( ICommunicator *comm, mptag_t servertag );
                                                                        
// Inter (using InterCommunicator) node is server's node, server tag is tag server is listening on 
IHRPCtransport *MakeClientMpInterTransport( INode *node, mptag_t servertag );
IHRPCtransport *MakeServerMpInterTransport(mptag_t tag);
                                                                        

#endif


                                  
