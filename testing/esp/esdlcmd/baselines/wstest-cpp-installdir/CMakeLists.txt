cmake_minimum_required(VERSION 3.0)
project (WsTestServicePlugin)
set(CMAKE_INSTALL_PREFIX "/opt/HPCCSystems")
if (("${HPCC_SOURCE_DIR}" STREQUAL "") OR ("${HPCC_BUILD_DIR}" STREQUAL "") OR ("${CMAKE_BUILD_TYPE}" STREQUAL
 ""))
    message (FATAL_ERROR "Please specify HPCC_SOURCE_DIR, HPCC_BUILD_DIR and CMAKE_BUILD_TYPE")
endif ()

set (CMAKE_CXX_FLAGS "-fPIC -std=c++11")
include_directories ("${HPCC_SOURCE_DIR}/system/jlib"
                     "${HPCC_SOURCE_DIR}/system/include"
                     )
link_directories ("${HPCC_BUILD_DIR}/${CMAKE_BUILD_TYPE}/libs" .)

add_library (WsTestService SHARED WsTestServiceBase.cpp
                                       WsTestServiceBase.hpp
                                       WsTestService.cpp
                                       WsTestService.hpp
                                       )
target_link_libraries (WsTestService jlib)
install(TARGETS WsTestService DESTINATION plugins)
