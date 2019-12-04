################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems.
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

# Component: azure

#####################################################
# Description:
# ------------
#    Cmake Input File for azure support libraries
#####################################################

project( azure )

if(USE_AZURE)

    if (CMAKE_COMPILER_IS_GNUCXX AND NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.0.0)
      set(EXTERNAL_AZURE_CXX_FLAGS "-Wno-error=implicit-fallthrough -Wno-error=int-in-bool-context -Wno-error=format-nonliteral -Wno-error=sign-compare" )
      #Extra checking, but disable the unhelpful errors.
      set(EXTERNAL_AZURE_CXX_FLAGS "${EXTERNAL_AZURE_CXX_FLAGS} -Wall -Wextra -Wno-unused-value -Wno-switch -Wno-unused-parameter -Wno-reorder -Wno-sign-compare -Wno-unknown-pragmas" )
      #these ones really should be fixed.
      set(EXTERNAL_AZURE_CXX_FLAGS "${EXTERNAL_AZURE_CXX_FLAGS} -Wno-catch-value -Wno-unused-local-typedefs" )
    endif()
    if (CMAKE_COMPILER_IS_GNUCXX AND NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 8.0.0)
      # currently some uninitialised booleans that cause issues
      set(EXTERNAL_AZURE_CXX_FLAGS "${EXTERNAL_AZURE_CXX_FLAGS} -fno-sanitize=all" )
    endif()
    if (CMAKE_COMPILER_IS_GNUCXX AND NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0.0)
      set(EXTERNAL_AZURE_CXX_FLAGS "${EXTERNAL_AZURE_CXX_FLAGS} -Wno-error=deprecated-copy -Wno-redundant-move" )
    endif()
    include(ExternalProject)
    ExternalProject_Add(
        cpprestsdk
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/cpprestsdk
        BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/cpprestsdk
        CMAKE_ARGS -DCMAKE_CXX_FLAGS=${EXTERNAL_AZURE_CXX_FLAGS} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} LDFLAGS=-Wl,-rpath-link,${LIB_PATH}
        INSTALL_COMMAND "")

    set(CASABLANCA_OPTS -DCASABLANCA_INCLUDE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/cpprestsdk/Release/include -DCASABLANCA_LIBRARY=${CMAKE_CURRENT_BINARY_DIR}/cpprestsdk/Release/Binaries/libcpprest.so )
    ExternalProject_Add(
        azure-storage-cpp
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/azure-storage-cpp/Microsoft.WindowsAzure.Storage
        BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/azure-storage-cpp
        CMAKE_ARGS -DCMAKE_CXX_FLAGS=${EXTERNAL_AZURE_CXX_FLAGS} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ${CASABLANCA_OPTS}
        BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} LDFLAGS=-Wl,-rpath-link,${LIB_PATH}
        INSTALL_COMMAND "")

    add_library(azure-core SHARED IMPORTED GLOBAL)
    set_property(TARGET azure-core
        PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/cpprestsdk/Release/Binaries/libcpprest.so )

    add_library(azure-storage SHARED IMPORTED GLOBAL)
    set_property(TARGET azure-storage
        PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/azure-storage-cpp/Binaries/libazurestorage.so )

    add_dependencies(azure-storage-cpp cpprestsdk)
endif()
