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


# Component: dfuXRefLib 

#####################################################
# Description:
# ------------
#    Cmake Input File for dfuXRefLib
#####################################################


project( dfuXRefLib ) 

set (    SRCS 
         dfurdir.cpp 
         dfuxreflib.cpp 
         XRefFilesNode.cpp 
         XRefNodeManager.cpp 
    )

include_directories ( 
         ./../../esp/bindings/SOAP/xpp 
         ./../../common/remote 
         ./../server 
         ./../../system/mp 
         . 
         ./../base 
         ./../ft 
         ./../../system/include 
         ./../../system/jlib 
         ./../../common/environment 
         ./../../common/workunit 
    )

ADD_DEFINITIONS( -D_USRDLL -DDFUXREFLIB_EXPORTS )

HPCC_ADD_LIBRARY( dfuXRefLib SHARED ${SRCS} )
install ( TARGETS dfuXRefLib DESTINATION ${OSSDIR}/lib )
target_link_libraries ( dfuXRefLib
    jlib
    mp 
    hrpc 
    remote 
    dalibase 
    environment 
)

