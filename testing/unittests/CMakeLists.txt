################################################################################
#    Copyright (C) 2012 HPCC Systems®.
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


# Component: unittests
#####################################################
# Description:
# ------------
#    Cmake Input File for unittests
#####################################################

if (USE_CPPUNIT)

if (NOT CONTAINERIZED)
ADD_SUBDIRECTORY(configmgr)
endif()

project( unittests )

set (    SRCS
         unittests.cpp
         remotetests.cpp
         dalitests.cpp
         jlibtests.cpp
         cryptotests.cpp
         hqltests.cpp
         esdltests.cpp
    )

if (NOT CONTAINERIZED)
set (    SRCS
         ${SRCS}
         configmgr/ConfigMgrUnitTests.cpp
         configmgr/ConfigMgrTemplateTests.cpp
         configmgr/ConfigMgrHPCCTests.cpp
    )
endif ()

include_directories (
         .
         ./../../system/include
         ./../../system/jlib
         ./../../system/mp
         ./../../system/libbase58
         ./../../fs/dafsclient
         ./../../common/thorhelper
         ./../../dali/base
         ./../../system/security/shared
         ./../../common/deftype
         ./../../system/security/cryptohelper
         ./../../configuration/configmgr/configmgrlib
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/ecl/hql
         ${HPCC_SOURCE_DIR}/configuration/configmgr/RapidJSON/include
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/esdllib
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/esp/services/common
         ${CMAKE_BINARY_DIR}/generated
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( unittests ${SRCS} )

install ( TARGETS unittests RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( unittests
         jlib
         dafsclient
         dalibase
         deftype
         libbase58
         thorhelper
         esphttp
         esdllib
         ${CPPUNIT_LIBRARIES}
    )

if (NOT CONTAINERIZED)
  target_link_libraries ( unittests configmgr )
endif ()

endif ()
