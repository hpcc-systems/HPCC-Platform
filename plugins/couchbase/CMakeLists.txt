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

# Component: couchbaseembed

#####################################################
# Description:
# ------------
#    Cmake Input File for couchbaseembed
#####################################################

project(couchbaseembed)

IF ( COUCHBASEEMBED )
    ADD_PLUGIN(COUCHBASEEMBED PACKAGES COUCHBASE)
    IF ( MAKE_COUCHBASEEMBED )
        set(
            SRCS
            couchbaseembed.cpp
        )

        INCLUDE_DIRECTORIES (
            ${PROJECT_SOURCE_DIR}/libcouchbase-cxx/include
            ./../../esp/platform #for token serialize/deserialize
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../rtl/nbcd
            ./../../common/deftype
            ./../../system/jlib
            ./../../roxie/roxiemem
        )

        ADD_DEFINITIONS(-D_USRDLL -DCOUCHBASEEMBED_EXPORTS)
        HPCC_ADD_LIBRARY(couchbaseembed SHARED ${SRCS})
        install(
            TARGETS couchbaseembed
            DESTINATION plugins)

        target_link_libraries(
            couchbaseembed
            ${LIBCOUCHBASE_LIBRARIES}
            eclrtl
            jlib)
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/couchbase.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
