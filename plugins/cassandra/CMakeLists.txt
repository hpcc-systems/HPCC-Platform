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

if(CASSANDRAEMBED OR USE_CASSANDRA)
  ADD_PLUGIN(cassandraembed)
  if(MAKE_CASSANDRAEMBED OR USE_CASSANDRA)
    find_path(CPP_DRIVER_INCLUDE_DIRS "cassandra.h" REQUIRED)
    find_library(CPP_DRIVER_LIBRARY NAMES "cassandra" REQUIRED)
    find_package(libuv CONFIG REQUIRED)

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
      ${CPP_DRIVER_INCLUDE_DIRS}
      )

    add_definitions(-D_USRDLL -DCASSANDRAEMBED_EXPORTS)

    HPCC_ADD_LIBRARY(cassandraembed SHARED ${SRCS})

    install(
      TARGETS cassandraembed
      DESTINATION plugins
      COMPONENT Runtime )

    install(
      TARGETS cassandraembed
      DESTINATION ${LIB_DIR}
      COMPONENT Runtime CALC_DEPS)

    target_link_libraries(
      cassandraembed
      eclrtl
      roxiemem
      dalibase
      workunit
      jlib
      ${CPP_DRIVER_LIBRARY}
      $<IF:$<TARGET_EXISTS:uv_a>,uv_a,uv>)

  endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
  install(
    FILES ${CMAKE_CURRENT_SOURCE_DIR}/cassandra.ecllib
    DESTINATION plugins
    COMPONENT Runtime)
endif()
