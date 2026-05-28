##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.
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

# Component: sslservices

#############################################################
# Description:
# -----------
# Cmake Input File for sslservices
#############################################################

project(sslservices)

if (PLATFORM AND USE_OPENSSLV3)
    find_package(OpenSSL REQUIRED)

    set(
        SRCS
        sslservices.hpp
        sslservices.cpp
    )

    include_directories(
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/rtl/eclrtl
        ${HPCC_SOURCE_DIR}/rtl/include
        ${HPCC_SOURCE_DIR}/common/deftype
        ${HPCC_SOURCE_DIR}/system/jlib
    )

    add_definitions(-D_USRDLL -DECL_OPENSSL_EXPORTS)
    HPCC_ADD_LIBRARY(sslservices SHARED ${SRCS})

    install(
        TARGETS sslservices
        DESTINATION plugins CALC_DEPS
    )

    set(OPENSSL_INSTALL_LIBS)
    foreach(opensslLib IN LISTS OPENSSL_LIBRARIES)
        if(IS_ABSOLUTE "${opensslLib}" AND EXISTS "${opensslLib}")
            list(APPEND OPENSSL_INSTALL_LIBS "${opensslLib}")
        endif()
    endforeach()

    if(OPENSSL_INSTALL_LIBS)
        install(
            FILES ${OPENSSL_INSTALL_LIBS}
            DESTINATION ${LIB_DIR}
            COMPONENT Runtime
        )
    endif()

    target_link_libraries(
        sslservices
        eclrtl
        jlib
        OpenSSL::SSL
        OpenSSL::Crypto
    )
endif()
