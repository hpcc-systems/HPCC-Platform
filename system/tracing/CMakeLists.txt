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

# Component: jlib
#####################################################
# Description:
# ------------
#    Cmake Input File for jlib
#####################################################

project( tracing )

find_package(opentelemetry-cpp CONFIG REQUIRED)

if (opentelemetry-cpp_FOUND)
    MESSAGE(STATUS "Found Opentelemetry")
    MESSAGE("^^^^^^^^^${OPENTELEMETRY_CPP_INCLUDE_DIRS}")
    include_directories (
         ${HPCC_SOURCE_DIR}/system/win32
         ${HPCC_SOURCE_DIR}/system/include
         ${OPENTELEMETRY_CPP_INCLUDE_DIRS}
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )
    SET ( SRCS tracer.cpp )
    SET ( INCLUDES tracer.hpp )
    SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STRICT_CXX_FLAGS}")
    #HPCC_ADD_LIBRARY( tracing SHARED ${SRCS} ${INCLUDES} )
    HPCC_ADD_EXECUTABLE ( tracing ${SRCS} )
    target_link_libraries ( tracing
        opentelemetry-cpp::api
        opentelemetry-cpp::ext
        opentelemetry-cpp::sdk
        opentelemetry-cpp::logs
        opentelemetry-cpp::ostream_span_exporter
        #opentelemetry-cpp::jaeger_trace_exporter
        #opentelemetry-cpp::otlp_grpc_exporter
        #opentelemetry-cpp::otlp_grpc_log_exporter
        #opentelemetry-cpp::otlp_http_exporter
        opentelemetry-cpp::metrics
        opentelemetry-cpp::sdk
    )
else()
    MESSAGE(STATUS "Opentelemetry libs/headers not found")
endif()




