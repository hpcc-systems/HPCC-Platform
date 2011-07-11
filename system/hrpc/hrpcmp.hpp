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


                                  
