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

# Component: pkencrypt

#####################################################
# Description:
# ------------
#    Cmake Input File for pkencrypt
#####################################################


project( pkencrypt )

set (
        SRCS
        pkencrypt.cpp
    )

include_directories (
        ${HPCC_SOURCE_DIR}/system/security/zcrypt
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/system/jlib
        ${CMAKE_BINARY_DIR}
        ${OPENSSL_INCLUDE_DIR}
    )

ADD_DEFINITIONS ( -DPKENCRYPTIONMGR_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( pkencrypt SHARED ${SRCS} )

install (
        TARGETS pkencrypt
        RUNTIME DESTINATION ${EXEC_DIR}
        LIBRARY DESTINATION ${LIB_DIR}
    )

target_link_libraries (
        pkencrypt
        jlib
    )

if (USE_ZLIB)
      target_link_libraries (
          pkencrypt
          zcrypt
          ${ZLIB_LIBRARIES}
    )
endif()
