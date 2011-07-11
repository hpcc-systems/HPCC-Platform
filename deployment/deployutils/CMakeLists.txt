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


# Component: deployutils
#####################################################
# Description:
# ------------
#    Cmake Input File for deployutils
#####################################################

project( deployutils ) 

set (    SRCS 
         buildset.cpp 
         computerpicker.cpp 
         configenvhelper.cpp
         wizardInputs.cpp 
         deployutils.cpp 
    )

include_directories ( 
         ./../../system/include 
         ./../../system/jlib 
         ./../../system/mp 
         ./../../system/xmllib       
         ./../../common/environment
         ./../../dali/base 
         ./../../esp/esplib
         ../configgen
         ../deploy 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS ( -D_USRDLL -DDEPLOYUTILS_EXPORTS )

HPCC_ADD_LIBRARY( deployutils SHARED ${SRCS} )
install ( TARGETS deployutils DESTINATION ${OSSDIR}/lib )
target_link_libraries ( deployutils
         jlib 
         environment 
         xmllib
         deploy
    )


