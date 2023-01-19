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


# Component: cryptolib

#####################################################
# Description:
# ------------
#    Cmake Input File for cryptolib
#####################################################

project( cryptolib )

set (    SRCS
         cryptolib.cpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/security/cryptohelper
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${OPENSSL_INCLUDE_DIR}
    )

ADD_DEFINITIONS( -D_USRDLL -DCRYPTOLIB_EXPORTS )

HPCC_ADD_LIBRARY( cryptolib SHARED ${SRCS} )
install ( TARGETS cryptolib DESTINATION plugins CALC_DEPS)
target_link_libraries ( 
        cryptolib
        dalibase
        jlib
        ${ZLIB_LIBRARIES}
    )
