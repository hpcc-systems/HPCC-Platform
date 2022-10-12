##############################################################################

#    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################

# Component: mongodbembed

#############################################################
# Description:
# -----------
# Cmake Input File for mongodbembed
#############################################################

project(mongodbembed)

IF(MONGODBEMBED)
    ADD_PLUGIN(mongodbembed)
    if(MAKE_MONGODBEMBED)
        find_package(bsoncxx CONFIG REQUIRED)
        find_package(mongocxx CONFIG REQUIRED)

        set(
            SRCS
            mongodbembed.cpp
        )

        INCLUDE_DIRECTORIES(
            ${HPCC_SOURCE_DIR}/esp/platform #for token serialize/deserialize
            ${HPCC_SOURCE_DIR}/system/include
            ${HPCC_SOURCE_DIR}/rtl/eclrtl
            ${HPCC_SOURCE_DIR}/rtl/include
            ${HPCC_SOURCE_DIR}/rtl/nbcd
            ${HPCC_SOURCE_DIR}/common/deftype
            ${HPCC_SOURCE_DIR}/system/jlib
            ${HPCC_SOURCE_DIR}/roxie/roxiemem
        )

        HPCC_ADD_LIBRARY(mongodbembed SHARED ${SRCS})

        install(
            TARGETS mongodbembed
            DESTINATION plugins)

        install(
            FILES ${LIBMONGOCXX_LIB_REAL}
            DESTINATION ${LIB_DIR}
            PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
            COMPONENT Runtime)

        install(
            FILES ${LIBMONGOCXX_LIB} ${LIBMONGOCXX_LIB_ABI}
            DESTINATION ${LIB_DIR}
            COMPONENT Runtime)
            
        install(
            FILES ${LIBBSONCXX_LIB_REAL}
            DESTINATION ${LIB_DIR}
            PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
            COMPONENT Runtime)

        install(
            FILES ${LIBBSONCXX_LIB} ${LIBBSONCXX_LIB_ABI}
            DESTINATION ${LIB_DIR}
            COMPONENT Runtime
            CALC_DEPS)

        target_link_libraries(
            mongodbembed
            PRIVATE mongo::bsoncxx_shared
            PRIVATE mongo::mongocxx_shared
            eclrtl
            jlib
        )
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/mongodb.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
