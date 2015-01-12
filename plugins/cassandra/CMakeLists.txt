################################################################################
#    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.
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


project( cassandraembed )

if (USE_CASSANDRA)
  # There is not yet a standard package for cassandra cpp client, and only very modern distros have libuv-dev
  # When there is (and the distros catch up) we may want to add them as dependencies here
  ADD_PLUGIN(cassandraembed PACKAGES OPTION MAKE_CASSANDRAEMBED)
  if ( MAKE_CASSANDRAEMBED )

    # until then, we build the required libraries from source

    # Build libuv, required by the cassandra driver but not available on all distros
    add_custom_command(OUTPUT ${PROJECT_BINARY_DIR}/libuv.so
                       COMMAND make builddir_name=${PROJECT_BINARY_DIR}
                       COMMAND mv ${PROJECT_BINARY_DIR}/libuv.so ${PROJECT_BINARY_DIR}/libuv.so.0.10
                       COMMAND ln -s ${PROJECT_BINARY_DIR}/libuv.so.0.10 ${PROJECT_BINARY_DIR}/libuv.so
                       WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/libuv)
    add_custom_target ( libuv ALL DEPENDS ${PROJECT_BINARY_DIR}/libuv.so )

    set(LIBUV_LIBRARY ${PROJECT_BINARY_DIR}/libuv.so)
    set(LIBUV_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/libuv/include/)

    # Build the Cassandra cpp driver, only presently available as source
    if( NOT EXISTS "${PROJECT_SOURCE_DIR}/cpp-driver/CMakeLists.txt" )
     message( FATAL_ERROR
"   The cpp-driver submodule is not available.
   This normally indicates that the git submodule has not been fetched.
   Please run git submodule update --init --recursive")
    endif()

    option(CASS_INSTALL_HEADER "Install header file" OFF)
    option(CASS_BUILD_STATIC "Build static library" OFF)
    option(CASS_BUILD_EXAMPLES "Build examples" OFF)
    set_property(GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS FALSE)
    SET (_SAVE_Boost_FOUND "${Boost_FOUND}")  # Working around a bug in the cassandra cmake file
    UNSET(Boost_FOUND)
    add_subdirectory (cpp-driver ${PROJECT_BINARY_DIR}/cassandra)
    add_dependencies( cassandra libuv )

    SET (Boost_FOUND "${_SAVE_Boost_FOUND}")

    set(SRCS cassandraembed.cpp)

    include_directories (
         ./../../system/include
         ./../../rtl/eclrtl
         ./../../roxie/roxiemem
         ./../../rtl/include
         ./../../rtl/nbcd
         ./../../common/deftype
         ./../../system/jlib

         ./cpp-driver/include
       )

    ADD_DEFINITIONS( -D_USRDLL -DCASSANDRAEMBED_EXPORTS)

    HPCC_ADD_LIBRARY( cassandraembed SHARED ${SRCS} )
    if (${CMAKE_VERSION} VERSION_LESS "2.8.9")
      message("WARNING: Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
    else()
      set_target_properties( cassandraembed PROPERTIES NO_SONAME 1 )
    endif()

    install ( TARGETS cassandraembed DESTINATION plugins COMPONENT Runtime )
    # until such time as the cassandra cpp driver and libuv are available as standard in all distros we want to support,
    # include them in our rpm
    # Note that the cassandra driver CMake file already includes the relevant install commands

    install ( FILES ${PROJECT_BINARY_DIR}/libuv.so.0.10 DESTINATION ${LIB_DIR} COMPONENT Runtime )

    target_link_libraries ( cassandraembed
        cassandra
        eclrtl
        roxiemem
        jlib
        )
  endif()
endif()

# Even if not making the Cassandra plugin, we want to install the header
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/cassandra.ecllib DESTINATION plugins COMPONENT Runtime)
