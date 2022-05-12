###############################################################################
#    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.
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

project(AzureBlobLogAccess)

#set (azure-storage-blobs-cpp_DIR "/home/ubuntu/GIT/HPCC-Platform/vcpkg/packages/azure-storage-blobs-cpp_x64-linux/share/azure-storage-blobs-cpp")
#find_package(azure-storage-blobs-cpp)
find_package (azure-storage-blobs-cpp CONFIG REQUIRED)

set(srcs
  ${CMAKE_CURRENT_SOURCE_DIR}/AzureBlobLogAccess.cpp
)

include_directories(
  ${HPCC_SOURCE_DIR}/system/include
  ${HPCC_SOURCE_DIR}/system/jlib
)

add_definitions(-DAZUREBLOB_LOGACCESS_EXPORTS)
HPCC_ADD_LIBRARY(AzureBlobLogAccess SHARED ${srcs})

target_link_libraries(AzureBlobLogAccess
 Azure::azure-storage-blobs
  jlib
)