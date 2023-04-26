################################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.
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


# Component: esdl_svc_engine
#####################################################
# Description:
# ------------
#    Cmake Input File for esdl_svc_engine
#####################################################

project( esdl_svc_engine )

set (   SRCS
        esdl_binding.cpp
        esdl_svc_engine_plugin.cpp
        esdl_svc_engine.cpp
        esdl_store.cpp
        esdl_monitor.cpp
        esdl_summary_profile.cpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/esp/esplib
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/esp/services
         ${HPCC_SOURCE_DIR}/esp/services/common
         ${HPCC_SOURCE_DIR}/esp/clients
         ${HPCC_SOURCE_DIR}/esp/clients/wsecl
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/esp/bindings/http/client
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/masking/include
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/common/deftype
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/common/dllserver
         ${HPCC_SOURCE_DIR}/common/thorhelper
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/common/wuwebview
         ${HPCC_SOURCE_DIR}/ecl/hql
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/esp/esdllib
         ${HPCC_SOURCE_DIR}/esp/logging/logginglib
         ${HPCC_SOURCE_DIR}/esp/logging/loggingmanager
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/tools/hidl #TAccessMapGenerator
    )

ADD_DEFINITIONS( -D_USRDLL -DESDL_ENGINE_EXPORTS )

HPCC_ADD_LIBRARY( esdl_svc_engine SHARED ${SRCS} )
install ( TARGETS esdl_svc_engine RUNTIME DESTINATION bin LIBRARY DESTINATION lib )
add_dependencies (esdl_svc_engine espscm)
target_link_libraries ( esdl_svc_engine
         jlib
         xmllib
         esphttp
         mp
         hrpc
         nbcd
         eclrtl
         deftype
         hql
         remote
         dalibase
         esdllib
         dllserver
         dalibase
    )

IF (USE_OPENSSL)
    target_link_libraries ( esdl_svc_engine
    	securesocket
    )
ENDIF()
