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


# Component: dafsstream 
#####################################################
# Description:
# ------------
#    Cmake Input File for dafsstream
#####################################################

project( dafsstream ) 

set (    SRCS 
         dafsstream.cpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/hrpc 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/system/jhtree
         ${HPCC_SOURCE_DIR}/fs/dafsclient
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/security/cryptohelper
         ${HPCC_SOURCE_DIR}/testing/unittests
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/ecl/hql
         ${HPCC_SOURCE_DIR}/common/deftype
         ${HPCC_SOURCE_DIR}/dali/base
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_USRDLL -DDAFSCLIENT_EXPORTS )

HPCC_ADD_LIBRARY( dafsstream SHARED ${SRCS}  )
install ( 
    TARGETS dafsstream 
    RUNTIME DESTINATION ${EXEC_DIR} 
    LIBRARY DESTINATION ${LIB_DIR} 
    COMPONENT Runtime
)

target_link_libraries ( dafsstream 
    eclrtl
    jlib
    jhtree 
    hql   
    mp
    dalibase
    remote
    dafsclient
    )
