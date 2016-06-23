################################################################################
#    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.
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

# Component: cassandraembed

#####################################################
# Description:
# ------------
#    Cmake Input File for cassandraembed
#####################################################

project(cassandraembed)

# There is not yet a standard package for cassandra cpp client, and only very modern distros have libuv-dev
# When there is (and the distros catch up) we may want to add them as dependencies here
# until then, we build the required libraries from source
# Build libuv, required by the cassandra driver but not available on all distros
if(USE_CASSANDRA)
  if(APPLE)
    add_custom_command(
      OUTPUT ${PROJECT_BINARY_DIR}/libuv.dylib
      COMMAND make builddir_name=${PROJECT_BINARY_DIR}
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/libuv)
    add_custom_target(
      libuv ALL
      DEPENDS ${PROJECT_BINARY_DIR}/libuv.dylib)
    set(LIBUV_LIBRARY ${PROJECT_BINARY_DIR}/libuv.dylib)
  else()
    add_custom_command(
      OUTPUT ${PROJECT_BINARY_DIR}/libuv.so
      COMMAND make builddir_name=${PROJECT_BINARY_DIR}
      COMMAND mv ${PROJECT_BINARY_DIR}/libuv.so ${PROJECT_BINARY_DIR}/libuv.so.0.10
      COMMAND ln -s ${PROJECT_BINARY_DIR}/libuv.so.0.10 ${PROJECT_BINARY_DIR}/libuv.so
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/libuv)
    add_custom_target(
      libuv ALL
      DEPENDS ${PROJECT_BINARY_DIR}/libuv.so)
    set(LIBUV_LIBRARY ${PROJECT_BINARY_DIR}/libuv.so)
  endif()
  set(LIBUV_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/libuv/include/)

  # Build the Cassandra cpp driver, only presently available as source
  if(NOT EXISTS "${PROJECT_SOURCE_DIR}/cpp-driver/CMakeLists.txt")
    message(FATAL_ERROR
      "   The cpp-driver submodule is not available.
      This normally indicates that the git submodule has not been fetched.
      Please run git submodule update --init --recursive")
  endif()
  option(CASS_INSTALL_HEADER "Install header file" OFF)
  option(CASS_BUILD_STATIC "Build static library" OFF)
  option(CASS_BUILD_EXAMPLES "Build examples" OFF)
  set_property(GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS FALSE)
  set(_SAVE_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  if(NOT WIN32)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-format-nonliteral") # Work around cassandra build error
  endif()
  add_subdirectory(cpp-driver ${PROJECT_BINARY_DIR}/cassandra)
  add_dependencies(cassandra libuv)
  set(CMAKE_CXX_FLAGS "${_SAVE_CMAKE_CXX_FLAGS}")

  install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${PROJECT_BINARY_DIR}:${PROJECT_BINARY_DIR}/cassandra\")")
  if(APPLE)
    install(
      FILES ${PROJECT_BINARY_DIR}/libuv.dylib
      DESTINATION ${LIB_DIR}
      COMPONENT Runtime)
  else()
    install(
      FILES ${PROJECT_BINARY_DIR}/libuv.so.0.10
      DESTINATION ${LIB_DIR}
      COMPONENT Runtime)
  endif()

  ADD_PLUGIN(cassandraembed)

  set(
    SRCS
    cassandraembed.cpp
    cassandrawu.cpp)

  include_directories(
    ./../../system/include
    ./../../rtl/eclrtl
    ./../../roxie/roxiemem
    ./../../rtl/include
    ./../../rtl/nbcd
    ./../../common/deftype
    ./../../common/workunit
    ./../../system/jlib
    ./../../system/security/shared 
    ./../../system/mp
    ./../../dali/base 
    ./cpp-driver/include)

  add_definitions(-D_USRDLL -DCASSANDRAEMBED_EXPORTS)

  HPCC_ADD_LIBRARY(cassandraembed SHARED ${SRCS})
  if(NOT FORCE_WORKUNITS_TO_CASSANDRA)
    if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
      message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
    elseif(NOT APPLE)
      set_target_properties(cassandraembed PROPERTIES NO_SONAME 1)
    endif()
  endif()

  install(
    TARGETS cassandraembed
    DESTINATION plugins
    COMPONENT Runtime )

  install(
    TARGETS cassandraembed
    DESTINATION ${LIB_DIR}
    COMPONENT Runtime )

  target_link_libraries(
    cassandraembed
    cassandra
    eclrtl
    roxiemem
    dalibase
    workunit
    jlib)

  if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
      FILES ${CMAKE_CURRENT_SOURCE_DIR}/cassandra.ecllib
      DESTINATION plugins
      COMPONENT Runtime)
  endif()
endif(USE_CASSANDRA)
