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

# Component: xmllib 
#####################################################
# Description:
# ------------
#    Cmake Input File for xmllib
#####################################################

project( xmllib ) 

set (    SRCS 
         xmlvalidator.cpp 
         xsdparser.cpp 
         xslcache.cpp 
         xslprocessor.cpp 
    )

include_directories ( 
         ./../../common/deftype 
         ./../../system/include 
         ./../../system/jlib 
         ./../../ecl/hql 
         ${XALAN_INCLUDE_DIR} 
         ${XERCES_INCLUDE_DIR}
    )

ADD_DEFINITIONS( -D_USRDLL -DXMLLIB_EXPORTS )

HPCC_ADD_LIBRARY( xmllib SHARED ${SRCS} )
install ( TARGETS xmllib DESTINATION ${OSSDIR}/lib )
target_link_libraries ( xmllib
         ${XALAN_LIBRARIES} 
         ${XERCES_LIBRARIES}
         jlib
    )


