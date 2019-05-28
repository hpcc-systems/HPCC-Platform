################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
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


# File      : CMakeLists.txt
# Component: wuanalysis
#####################################################
# Description:
# ------------
#    Cmake Input File for wuanalysis
#####################################################

cmake_policy( SET CMP0011 NEW )

project( wuanalysis )

set (    SRCS
         anacommon.cpp
         anarule.cpp
         anawu.cpp

         anacommon.hpp
         anarule.hpp
         anawu.hpp
    )

include_directories (
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/common/thorhelper
         ${HPCC_SOURCE_DIR}/common/workunit
    )

HPCC_ADD_LIBRARY( wuanalysis SHARED ${SRCS} )

set_target_properties(wuanalysis PROPERTIES
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL WUANALYSIS_EXPORTS )

install ( TARGETS wuanalysis RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )

target_link_libraries ( wuanalysis
         jlib
         mp
         hrpc
         workunit
         thorhelper
    )
