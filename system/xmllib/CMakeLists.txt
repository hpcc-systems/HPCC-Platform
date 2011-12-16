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

if ( USE_XALAN )
    set(LIB_INCLUDE_DIR ${XALAN_INCLUDE_DIR} ${XERCES_INCLUDE_DIR})
    set(LIB_LIBRARIES ${XALAN_LIBRARIES} ${XERCES_LIBRARIES})
    set (SRCS
        xsdparser.cpp
        xslcache.cpp
        xalan_processor.cpp
        xerces_validator.cpp
    )
elseif (USE_LIBXSLT)
    set(LIB_INCLUDE_DIR ${LIBXSLT_INCLUDE_DIR} ${LIBEXSLT_INCLUDE_DIR} ${LIBXML2_INCLUDE_DIR})
    set(LIB_LIBRARIES ${LIBXSLT_LIBRARIES} ${LIBEXLST_LIBRARIES} ${LIBXML2_LIBRARIES})
    set (SRCS
        xsdparser.cpp
        xslcache.cpp
        libxslt_processor.cpp
        libxml_validator.cpp
    )
else()
    set (SRCS
        xsdparser.cpp
        xslcache.cpp
        xmllib_unsupported.cpp
    )
endif()


include_directories (
         ./../../common/deftype
         ./../../system/include
         ./../../system/jlib
         ./../../ecl/hql
         ${LIB_INCLUDE_DIR}
    )

ADD_DEFINITIONS( -D_USRDLL -DXMLLIB_EXPORTS )

HPCC_ADD_LIBRARY( xmllib SHARED ${SRCS} )
install ( TARGETS xmllib DESTINATION ${OSSDIR}/lib )
target_link_libraries ( xmllib
         ${LIB_LIBRARIES}
         jlib
    )
