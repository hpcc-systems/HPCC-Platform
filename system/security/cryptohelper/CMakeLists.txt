################################################################################
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
################################################################################

# Component: cryptohelper

#####################################################
# Description:
# ------------
#    Cmake Input File for cryptohelper
#####################################################


project( cryptohelper )

set (
        SRCS
        cryptocommon.cpp
        digisign.cpp
        pke.cpp
        ske.cpp
    )

include_directories (
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/system/jlib
        ${CMAKE_BINARY_DIR}
        ${OPENSSL_INCLUDE_DIR}
    )

IF (USE_OPENSSL)
    include_directories(${OPENSSL_INCLUDE_DIR})
ENDIF()

ADD_DEFINITIONS( -D_LIB )
ADD_DEFINITIONS( -D_USRDLL -DJLIB_EXPORTS )

#SET_SOURCE_FILES_PROPERTIES( ${SRCS} PROPERTIES LANGUAGE CXX )
HPCC_ADD_LIBRARY( cryptohelper STATIC ${SRCS} )

