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


# Component: hthor

#####################################################
# Description:
# ------------
#    Cmake Input File for hthor
#####################################################


project( hthor )

set (    SRCS
         hthor.cpp
         hthorkey.cpp
         hthorstep.cpp
         ../eclagent/eclagent.cpp
         ../eclagent/eclgraph.cpp
         sourcedoc.xml
    )

set (    INCLUDES
         hthor.ipp
         hthor.hpp
         hthorstep.ipp
         ../eclagent/agentctx.hpp
         ../eclagent/eclagent.ipp
    )

include_directories (
         .
         ${HPCC_SOURCE_DIR}/common/remote
         ${HPCC_SOURCE_DIR}/system/jhtree
         ${HPCC_SOURCE_DIR}/system/hrpc
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/common/deftype
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/ecl/eclagent
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/common/thorhelper
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/roxie/roxiemem
         ${HPCC_SOURCE_DIR}/roxie/roxie
         ${HPCC_SOURCE_DIR}/roxie/ccd
         ${HPCC_SOURCE_DIR}/common/roxiehelper
         ${HPCC_SOURCE_DIR}/common/dllserver
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/ecl/schedulectrl
         ${HPCC_SOURCE_DIR}/common/commonext
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_USRDLL -DHTHOR_EXPORTS -DSTARTQUERY_EXPORTS )

if ( NOT CLIENTTOOLS_ONLY )
    HPCC_ADD_LIBRARY( hthor SHARED ${SRCS} ${INCLUDES} )
    install ( TARGETS hthor DESTINATION ${OSSDIR}/lib )
    target_link_libraries ( hthor
         jlib
         mp
         hrpc
         remote
         dalibase
         environment
         dllserver
         nbcd
         eclrtl
         deftype
         workunit
         jhtree
         securesocket
         thorhelper
         roxiemem
         roxiehelper
         commonext
         schedulectrl
    )
endif()
