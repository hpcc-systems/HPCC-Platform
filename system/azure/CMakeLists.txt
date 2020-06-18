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

    include(ExternalProject)

    ExternalProject_Add(
        azure-storage-cpplite
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/azure-storage-cpplite
        BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/azure-storage-cpplite
        CMAKE_ARGS -DCMAKE_CXX_FLAGS=${EXTERNAL_AZURE_CXX_FLAGS} -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF -DBUILD_SAMPLES=OFF -DBUILD_SHARED_LIBS=ON
        BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} LDFLAGS=-Wl,-rpath-link,${LIB_PATH} azure-storage-lite
        INSTALL_COMMAND "")

    add_library(azure-storage-lite SHARED IMPORTED GLOBAL)
    set_property(TARGET azure-storage-lite
        PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/azure-storage-cpplite/libazure-storage-lite${CMAKE_SHARED_LIBRARY_SUFFIX} )

    if(NOT PLUGIN)
        install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/azure-storage-cpplite/libazure-storage-lite${CMAKE_SHARED_LIBRARY_SUFFIX}
                DESTINATION ${LIB_DIR})
        #I do not think this is needed...
        install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/azure-storage-cpplite/LICENSE
            RENAME azure-LICENSE.txt
            DESTINATION ${LIB_DIR})
    endif(NOT PLUGIN)

endif()
