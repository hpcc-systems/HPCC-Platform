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
	  OUTPUT ${PROJECT_BINARY_DIR}/.libs/libuv${CMAKE_SHARED_LIBRARY_SUFFIX}
	  COMMAND sh ${PROJECT_SOURCE_DIR}/libuv/autogen.sh
	  COMMAND ${PROJECT_SOURCE_DIR}/libuv/configure --libdir=${CMAKE_INSTALL_PREFIX}/${LIB_DIR}
	  COMMAND make
	  WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
  else()
	add_custom_command(
	  OUTPUT ${PROJECT_BINARY_DIR}/.libs/libuv${CMAKE_SHARED_LIBRARY_SUFFIX}
	  COMMAND sh ${PROJECT_SOURCE_DIR}/libuv/autogen.sh
	  COMMAND ${PROJECT_SOURCE_DIR}/libuv/configure
	  COMMAND make
	  WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
  endif()
  add_custom_target(
    libuv ALL
    DEPENDS ${PROJECT_BINARY_DIR}/.libs/libuv${CMAKE_SHARED_LIBRARY_SUFFIX})
  set(LIBUV_LIBRARY ${PROJECT_BINARY_DIR}/.libs/libuv${CMAKE_SHARED_LIBRARY_SUFFIX})
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
  option(CASS_INSTALL_PKG_CONFIG "Install pkg-config files" OFF)
  set_property(GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS FALSE)
  set(_SAVE_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  if (CMAKE_COMPILER_IS_GNUCXX)
  # From time to time need to add flags in here to disable warnings, if a new compiler picks up something new.
  # e.g.
    if (NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.2.0)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-copy")
    endif ()
  endif()
  if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
    remove_definitions (-fsanitize=undefined -fno-sanitize=alignment -fsanitize-trap=undefined)
  endif()
  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cpp-driver/cmake/modules/")
  set(CMAKE_INSTALL_LIBDIR "${CMAKE_INSTALL_PREFIX}/${LIB_DIR}")
  set(CASS_ROOT_DIR "${PROJECT_SOURCE_DIR}/cpp-driver")
  add_subdirectory(cpp-driver ${PROJECT_BINARY_DIR}/cassandra)
  add_dependencies(cassandra libuv)
  set(CMAKE_CXX_FLAGS "${_SAVE_CMAKE_CXX_FLAGS}")

  install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${PROJECT_BINARY_DIR}:${PROJECT_BINARY_DIR}/.libs:${PROJECT_BINARY_DIR}/cassandra\")")
  if(APPLE)
    install(
      FILES ${LIBUV_LIBRARY} ${PROJECT_BINARY_DIR}/.libs/libuv.1${CMAKE_SHARED_LIBRARY_SUFFIX}
      DESTINATION ${LIB_DIR}
      COMPONENT Runtime)
  else()
    install(
      FILES ${LIBUV_LIBRARY} ${LIBUV_LIBRARY}.1 ${LIBUV_LIBRARY}.1.0.0
      DESTINATION ${LIB_DIR}
      PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
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
