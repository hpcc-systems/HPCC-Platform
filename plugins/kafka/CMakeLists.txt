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
        # librdkafka packages do not include everything we need to properly
        # build against it; until/if they are ever fixed we will need to build
        # our own version of librdkafka from source and include it ourselves

        if(NOT EXISTS "${PROJECT_SOURCE_DIR}/librdkafka/configure")
            message(FATAL_ERROR
"   The librdkafka submodule is not available.
   This normally indicates that the git submodule has not been fetched.
   Please run git submodule update --init --recursive")
        endif()

        set(
            LIBRDKAFKA_SRCS
            librdkafka/src/rdkafka.c
            librdkafka/src/rdkafka_broker.c
            librdkafka/src/rdkafka_msg.c
            librdkafka/src/rdkafka_topic.c
            librdkafka/src/rdkafka_defaultconf.c
            librdkafka/src/rdkafka_timer.c
            librdkafka/src/rdkafka_offset.c
            librdkafka/src/rdcrc32.c
            librdkafka/src/rdgz.c
            librdkafka/src/rdaddr.c
            librdkafka/src/rdrand.c
            librdkafka/src/rdthread.c
            librdkafka/src/rdqueue.c
            librdkafka/src/rdlog.c
            librdkafka/src/snappy.c
            librdkafka/src/rdkafka.h)

	set(
            LIBRDKAFKACPP_SRCS
            librdkafka/src-cpp/RdKafka.cpp
            librdkafka/src-cpp/ConfImpl.cpp
            librdkafka/src-cpp/HandleImpl.cpp
            librdkafka/src-cpp/ConsumerImpl.cpp
            librdkafka/src-cpp/ProducerImpl.cpp
            librdkafka/src-cpp/QueueImpl.cpp
            librdkafka/src-cpp/TopicImpl.cpp
            librdkafka/src-cpp/MessageImpl.cpp
            librdkafka/src-cpp/rdkafkacpp.h)

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
            ${PROJECT_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}
            ${CMAKE_BINARY_DIR}/oss
            ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka
            ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka/src
            ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka/src-cpp)

	execute_process(
            COMMAND ./configure
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka
            OUTPUT_QUIET)

	add_custom_target(librdkafka-config ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka/config.h)

	HPCC_ADD_LIBRARY(librdkafka SHARED ${LIBRDKAFKA_SRCS})
        HPCC_ADD_LIBRARY(librdkafkacpp SHARED ${LIBRDKAFKACPP_SRCS})
	add_dependencies(librdkafka librdkafka-config)
	add_dependencies(librdkafkacpp librdkafka-config)

	install(
            TARGETS librdkafka librdkafkacpp
            DESTINATION lib)

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

        target_link_libraries(
            kafka
            librdkafka
            librdkafkacpp
            eclrtl
            jlib
            ${ZLIB_LIBRARIES}) 
    endif()
endif()

if(PLATFORM)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/kafka.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
