################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
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


# Component: azurefile
#####################################################
# Description:
# ------------
#    Cmake Input File for azure direct reading hook
#####################################################

if (NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.0.0)
  set(EXTERNAL_AZURE_CXX_FLAGS "-fno-sanitize=all" )
endif()

find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
find_package(azure-storage-files-shares-cpp CONFIG REQUIRED)
find_package(azure-storage-common-cpp CONFIG REQUIRED)

project( azurefile )

set (   SRCS
        azurefile.cpp
        azurefile.hpp
    )

include_directories (
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/system/jlib
    )

ADD_DEFINITIONS( -D_USRDLL -DAZURE_FILE_EXPORTS )

HPCC_ADD_LIBRARY( azurefile SHARED ${SRCS}  )
install ( TARGETS azurefile DESTINATION filehooks CALC_DEPS)

target_link_libraries ( azurefile
        jlib
        CURL::libcurl
        Azure::azure-storage-common
        Azure::azure-storage-blobs
        Azure::azure-storage-files-shares
    )
