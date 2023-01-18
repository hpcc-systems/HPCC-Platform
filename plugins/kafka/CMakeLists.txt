################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.
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

# Component: kafka

#####################################################
# Description:
# ------------
#    Cmake Input File for kafka
#####################################################

project(kafka)

if(KAFKA)
    ADD_PLUGIN(kafka)
    if(MAKE_KAFKA)
        find_package(RdKafka REQUIRED)

        set(
            SRCS
            kafka.hpp
            kafka.cpp)

        include_directories(
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../common/deftype
            ./../../system/jlib
            ${LOCAL_LIBRDKAFKA_INCLUDE}
            ${CMAKE_BINARY_DIR}
            ${CMAKE_BINARY_DIR}/oss)

        add_definitions(-D_USRDLL -DECL_KAFKA_EXPORTS)
        HPCC_ADD_LIBRARY(kafka SHARED ${SRCS})

        if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties(kafka PROPERTIES NO_SONAME 1)
        endif()

        install(
            TARGETS kafka
            DESTINATION plugins)

        install(
            FILES ${LIBRDKAFKA_LIB_REAL} ${LIBRDKAFKACPP_LIB_REAL}
            DESTINATION ${LIB_DIR}
            PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
            COMPONENT Runtime)

        install(
            FILES ${LIBRDKAFKA_LIB} ${LIBRDKAFKACPP_LIB}
            DESTINATION ${LIB_DIR}
            COMPONENT Runtime
            CALC_DEPS)

        target_link_libraries(kafka
            eclrtl
            jlib
            RdKafka::rdkafka 
            RdKafka::rdkafka++
            ${ZLIB_LIBRARIES})
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/kafka.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
