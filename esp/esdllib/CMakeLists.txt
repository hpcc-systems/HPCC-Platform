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
# Component: esdllib

project( esdllib )

include_directories (
    ${HPCC_SOURCE_DIR}/system/xmllib
    ${HPCC_SOURCE_DIR}/esp/bindings
    ${HPCC_SOURCE_DIR}/esp/bindings/http/client
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
    ${HPCC_SOURCE_DIR}/esp/clients/wsecl
    ${HPCC_SOURCE_DIR}/esp/platform
    ${HPCC_SOURCE_DIR}/system/include
    ${HPCC_SOURCE_DIR}/esp/esdllib
    ${HPCC_SOURCE_DIR}/esp/services/common
    ${HPCC_SOURCE_DIR}/system/jlib
    ${HPCC_SOURCE_DIR}/system/security/shared
    ${HPCC_SOURCE_DIR}/rtl/eclrtl
    ${HPCC_SOURCE_DIR}/rtl/include #IXMLWriter
    ${HPCC_SOURCE_DIR}/common/thorhelper #JSONWRITER
    ${HPCC_SOURCE_DIR}/common/dllserver #loading plugins
    ${HPCC_SOURCE_DIR}/tools/hidl #TAccessMapGenerator
    ${HPCC_SOURCE_DIR}/system/masking/include
)

ADD_DEFINITIONS ( -D_USRDLL -DESDLLIB_EXPORTS )

set ( SRCS
    esdl_def.cpp
    esdl_def_helper.cpp
    esdl_transformer2.cpp
    esdl_script.cpp
    params2xml.cpp
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp/fxpp/FragmentedXmlPullParser.cpp;
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp/xpp/xpputils.cpp
    ${HPCC_SOURCE_DIR}/esp/services/common/wsexcept.cpp
)

if (USE_LIBXSLT)
    list (APPEND SRCS
        esdl_xpath_extensions_libxml.cpp
    )
else()
    list (APPEND SRCS
        esdl_xpath_extensions_unsupported.cpp
    )
endif()

HPCC_ADD_LIBRARY( esdllib SHARED ${SRCS}
    ${HEADERS}
)

install ( TARGETS esdllib RUNTIME DESTINATION bin LIBRARY DESTINATION lib )

add_dependencies ( esdllib jlib espscm)

target_link_libraries ( esdllib
    jlib
    xmllib
    thorhelper
    esphttp
)

IF (USE_LIBXSLT)
    include_directories (
         ${LIBXML2_INCLUDE_DIR}
    )
ENDIF()
