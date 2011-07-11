################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################


# Component: dalibase 

#####################################################
# Description:
# ------------
#    Cmake Input File for dalibase
#####################################################


project( dalibase ) 

set (    SRCS 
         daaudit.cpp 
         daclient.cpp 
         dacoven.cpp 
         dacsds.cpp 
         dadfs.cpp 
         dadiags.cpp 
         dafdesc.cpp 
         danqs.cpp 
         dasds.cpp 
         dasess.cpp 
         dasubs.cpp 
         dautils.cpp 
    )
    
set (    INCLUDES
         daaudit.hpp
         daclient.hpp
         dacoven.hpp
         dadfs.hpp
         dadiags.hpp
         dafdesc.hpp
         danqs.hpp
         dasds.hpp
         dasess.hpp
         dasubs.hpp
         dautils.hpp
    )   

include_directories ( 
         ./../../common/deftype 
         ./../../common/remote 
         ./../server 
         ./../../system/mp 
         ./../../system/include 
         ./../../system/jlib 
    )

ADD_DEFINITIONS( -DLOGMSGCOMPONENT=3 -D_USRDLL -DDALI_EXPORTS )

HPCC_ADD_LIBRARY( dalibase SHARED ${SRCS} ${INCLUDES} )
install ( TARGETS dalibase DESTINATION ${OSSDIR}/lib )
target_link_libraries ( dalibase 
         jlib
         mp 
         hrpc 
         remote 
    )


