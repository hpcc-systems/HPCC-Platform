################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################


# Component: hthorlib

#####################################################
# Description:
# ------------
#    Cmake Input File for hthorlib
#####################################################


project( hthorlib )

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
         ${HPCC_SOURCE_DIR}/common/dllserver
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/ecl/schedulectrl
         ${HPCC_SOURCE_DIR}/ecl/hql
         ${HPCC_SOURCE_DIR}/fs/dafsclient
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/dali/ft
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/common/wuanalysis
    )

ADD_DEFINITIONS( -D_USRDLL -DHTHOR_EXPORTS -DSTARTQUERY_EXPORTS )

HPCC_ADD_LIBRARY( hthorlib SHARED ${SRCS} ${INCLUDES} )
install ( TARGETS hthorlib RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} ARCHIVE DESTINATION componentfiles/cl/lib )
target_link_libraries ( hthorlib
         jlib
         mp
         hrpc
         dafsclient
         dalibase
         environment
         dllserver
         nbcd
         eclrtl
         deftype
         workunit
         jhtree
         thorhelper
         roxiemem
         schedulectrl
         wuanalysis
    )

IF (USE_OPENSSL)
    target_link_libraries ( hthorlib
    	securesocket
    )
ENDIF()
