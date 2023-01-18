################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
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


# Component: LdapSecurity
#####################################################
# Description:
# ------------
#    Cmake Input File for htpasswdSecurity
#####################################################

project( htpasswdSecurity )

HPCC_ADD_SUBDIRECTORY (initfiles)

set (    SRCS
         ${HPCC_SOURCE_DIR}/system/security/shared/authmap.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/htpasswdSecurity.cpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/dali/base
         ${APRUTIL_INCLUDE_DIR}
         ${APR_INCLUDE_DIR}
    )

ADD_DEFINITIONS( -DHTPASSWDSECURITY_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( htpasswdSecurity SHARED ${SRCS} )
install ( TARGETS htpasswdSecurity RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} CALC_DEPS)
target_link_libraries ( htpasswdSecurity
         jlib
         dalibase
         ${APRUTIL_LIBRARY}
         ${APR_LIBRARY}
         ${Iconv_LIBRARIES}
    )
