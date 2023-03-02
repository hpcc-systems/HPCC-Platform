##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
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
##############################################################################

project ( configmgr )

find_package(RapidJSON CONFIG REQUIRED)

SET(  SRCS

        EnvironmentMgr.cpp
        XMLEnvironmentMgr.cpp
        XMLEnvironmentLoader.cpp
        SchemaParser.cpp
        SchemaItem.cpp
        SchemaValue.cpp
        SchemaTypeLimits.cpp
        SchemaTypeIntegerLimits.cpp
        SchemaTypeStringLimits.cpp
        EnvironmentNode.cpp
        EnvironmentValue.cpp
        XSDComponentParser.cpp
        XSDSchemaParser.cpp
        XSDValueSetParser.cpp
        Status.cpp
        EnvironmentEventHandlers.cpp
        Utils.cpp
        InsertableItem.cpp
        ConfigPath.cpp
        EnvSupportLib.cpp
        mod_template_support/EnvModTemplate.cpp
        mod_template_support/Variable.cpp
        mod_template_support/TemplateException.cpp
        mod_template_support/Operation.cpp
        mod_template_support/OperationCreateNode.cpp
        mod_template_support/OperationFindNode.cpp
        mod_template_support/OperationModifyNode.cpp
        mod_template_support/Variables.cpp
        mod_template_support/IPAddressRangeVariable.cpp
        mod_template_support/IPAddressVariable.cpp
        mod_template_support/HostNameVariable.cpp
        mod_template_support/OperationDeleteNode.cpp
)

INCLUDE_DIRECTORIES(
    ${HPCC_SOURCE_DIR}/system/include
    ${HPCC_SOURCE_DIR}/system/jlib
    ${HPCC_SOURCE_DIR}/deployment/deploy
    ${CMAKE_PREFIX_PATH}/mkspecs/linux-g++
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../hpcc_support
    ${CMAKE_BINARY_DIR}
    ${CMAKE_BINARY_DIR}/oss
    ${Boost_INCLUDE_DIRS}
)

ADD_DEFINITIONS( -D_USRDLL -DCFGMGRLIB_EXPORTS)
HPCC_ADD_LIBRARY( configmgr SHARED ${SRCS} )
TARGET_LINK_LIBRARIES( configmgr jlib)

INSTALL ( TARGETS configmgr RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
