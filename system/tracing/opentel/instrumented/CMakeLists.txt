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

project( instrumentedhttp )

#For http exporter
#find_package(Protobuf REQUIRED)
#For http exporter
#find_package(CURL REQUIRED)

#find_package(opentelemetry-cpp CONFIG REQUIRED)

if (opentelemetry-cpp_FOUND)
    MESSAGE(STATUS "Found Opentelemetry")
    include_directories (
         ${HPCC_SOURCE_DIR}/system/win32
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${OPENTELEMETRY_CPP_INCLUDE_DIRS}
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/system/tracing
    )

    SET ( SERVER_SRCS httpserver.cpp httpclient.cpp)
    SET ( INCLUDES httpserver.hpp )
    SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STRICT_CXX_FLAGS}")

    HPCC_ADD_EXECUTABLE(instrumented_http_client httpclient.cpp)
    HPCC_ADD_EXECUTABLE(instrumented_http_server httpserver.cpp)
    target_link_libraries(instrumented_http_server 
        ${CMAKE_THREAD_LIBS_INIT}
        opentelemetry-cpp::api
        opentelemetry-cpp::ext
        opentelemetry-cpp::sdk
        opentelemetry-cpp::logs
        opentelemetry-cpp::http_client_curl
        opentelemetry-cpp::metrics
        opentelemetry-cpp::otlp_http_exporter
        opentelemetry-cpp::ostream_span_exporter
        jlib)

    target_link_libraries ( instrumented_http_client
        opentelemetry-cpp::api
        opentelemetry-cpp::ext
        opentelemetry-cpp::sdk
        opentelemetry-cpp::logs
        opentelemetry-cpp::ostream_span_exporter
        #opentelemetry-cpp::jaeger_trace_exporter
        #opentelemetry-cpp::otlp_grpc_exporter
        #opentelemetry-cpp::otlp_grpc_log_exporter
        opentelemetry-cpp::metrics
        opentelemetry-cpp::otlp_http_exporter
        opentelemetry-cpp::http_client_curl
        jlib)
else()
    MESSAGE(STATUS "Opentelemetry libs/headers not found")
endif()
