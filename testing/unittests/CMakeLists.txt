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

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set (    SRCS
         unittests.cpp
         remotetests.cpp
         dalitests.cpp
         jlibtests.cpp
         jlibtests2.cpp
         jstreamtests.cpp
         cryptotests.cpp
         hqltests.cpp
         esdltests.cpp
         loggingtests.cpp
         txSummarytests.cpp
         accessmaptests.cpp
         fxpptests.cpp
         espapicmdtests.cpp
         filetests.cpp
         wutests.cpp
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp/fxpp/FragmentedXmlPullParser.cpp
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp/fxpp/FragmentedXmlAssistant.cpp
         ${CMAKE_BINARY_DIR}/generated/ws_loggingservice_esp.cpp
         datamaskingtests.cpp
         ${HPCC_SOURCE_DIR}/tools/esp-api/espapi.cpp
         ${HPCC_SOURCE_DIR}/tools/esp-api/espshell.cpp
         ${HPCC_SOURCE_DIR}/tools/esp-api/espservice.cpp
         commonJsonWriterTests.cpp
         rtlxmltests.cpp
         jthreadpooltests.cpp
    )

if (NOT CONTAINERIZED)
set (    SRCS
         ${SRCS}
         configmgr/ConfigMgrUnitTests.cpp
         configmgr/ConfigMgrTemplateTests.cpp
         configmgr/ConfigMgrHPCCTests.cpp
         metrics/MetricFrameworkTests.cpp
         metrics/PeriodicSinkTests.cpp
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
         ./../../common/workunit
         ./../../system/security/cryptohelper
         ./../../configuration/configmgr/configmgrlib
         ${HPCC_SOURCE_DIR}/system/masking/include
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/common/dllserver
         ${HPCC_SOURCE_DIR}/common/eventconsumption
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/ecl/hql
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/esp/esdllib
         ${HPCC_SOURCE_DIR}/esp/esdlscriptlib
         ${HPCC_SOURCE_DIR}/esp/logging
         ${HPCC_SOURCE_DIR}/esp/logging/logginglib
         ${HPCC_SOURCE_DIR}/esp/logging/loggingmanager
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/esp/services/common
         ${HPCC_SOURCE_DIR}/tools/hidl
         ${HPCC_SOURCE_DIR}/tools/esp-api
         ${CMAKE_BINARY_DIR}/generated
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/esp/bindings/http/client
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
         logginglib
         workunit
         eventconsumption
         ${CppUnit_LIBRARIES}
    )

if (NOT CONTAINERIZED)
  target_link_libraries ( unittests configmgr )
endif ()

endif ()
