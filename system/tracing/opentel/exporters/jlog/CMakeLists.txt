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

# Component: jlogspanexporter
#####################################################
# Description:
# ------------
#    Cmake Input File for jlogspanexporter
#####################################################

project( jlogspanexporter )

#For http exporter
find_package(Protobuf REQUIRED)
#For http exporter
find_package(CURL REQUIRED)

find_package(opentelemetry-cpp CONFIG REQUIRED)

if (opentelemetry-cpp_FOUND)
    MESSAGE(STATUS "Found Opentelemetry")
    include_directories (
         ${HPCC_SOURCE_DIR}/system/win32
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${OPENTELEMETRY_CPP_INCLUDE_DIRS}
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

    SET ( SRCS spanexporterfactory.cpp )
    SET ( INCLUDES spanexporterfactory.hpp )
    SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STRICT_CXX_FLAGS}")

    HPCC_ADD_LIBRARY( jlogspanexporter SHARED ${SRCS} ${INCLUDES} )

    target_link_libraries ( jlogspanexporter
        opentelemetry-cpp::api
        opentelemetry-cpp::ext
        opentelemetry-cpp::sdk
        opentelemetry-cpp::logs
        opentelemetry-cpp::ostream_span_exporter
        opentelemetry-cpp::metrics
        opentelemetry-cpp::otlp_http_exporter
        opentelemetry-cpp::http_client_curl
        jlib)
else()
    MESSAGE(STATUS "Opentelemetry libs/headers not found")
endif()
