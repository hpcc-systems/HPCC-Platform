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

project(AzureBloblogaccess)
set(CMAKE_CXX_STANDARD 14)

#set (azure-storage-blobs-cpp_DIR "/home/ubuntu/GIT/HPCC-Platform/vcpkg/packages/azure-storage-blobs-cpp_x64-linux/share/azure-storage-blobs-cpp")
#set (azure-storage-common-cpp_DIR "/home/ubuntu/GIT/HPCC-Platform/vcpkg/packages/azure-storage-common-cpp_x64-linux/share/azure-storage-common-cpp/")
#set (azure-core-cpp_DIR "/home/ubuntu/GIT/HPCC-Platform/vcpkg/packages/azure-core-cpp_x64-linux/share/azure-core-cpp/")

message("$$$$ Finding azure-storage-blobs-cpp")
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)

if (azure-storage-blobs-cpp_FOUND)
    message("$$$$ Found azure-storage-blobs-cpp")
#    message("$$$$ Found azure-storage-blobs-cpp: ${AZURESTORAGE_INCLUDE_DIRS}")
#    message("$$$$ Found azure-storage-blobs-cpp: ${AZURESTORAGE_LIBRARY}")
#    message("$$$$ Found azure-storage-blobs-cpp: ${AZURESTORAGE_LIBRARIES}")
#    include_directories(${AZURESTORAGE_INCLUDE_DIRS})
#    set(azure-storage-blobs-cpp_LIBRARY Azure::azure-storage-blobs)
else(azure-storage-blobs-cpp_FOUND)
    message(FATAL_ERROR "$$$$ Could not find azure-storage-blobs-cpp! You need to install azure-sdk-for-cpp library ^${azure-storage-blobs-cpp_DIR}^")
endif(azure-storage-blobs-cpp_FOUND)

set(azure-storage-blobs-cpp_LIBRARY Azure::azure-storage-blobs)
message( "$$$$ azure-storage-blobs-cpp_LIBRARY --${azure-storage-blobs-cpp_LIBRARY}--")

set(srcs
  ${CMAKE_CURRENT_SOURCE_DIR}/AzureBlobLogAccess.cpp
)

include_directories(
  ${HPCC_SOURCE_DIR}/system/include
  ${HPCC_SOURCE_DIR}/system/jlib
  PRIVATE ${AZURESTORAGE_INCLUDE_DIRS}
)

add_definitions(-DAZUREBLOB_LOGACCESS_EXPORTS)
HPCC_ADD_LIBRARY(${PROJECT_NAME} SHARED ${srcs})

target_link_libraries(${PROJECT_NAME}
   PRIVATE ${azure-storage-blobs-cpp_LIBRARY}
   #get-env-helper
   jlib
)

install(TARGETS ${PROJECT_NAME}
  RUNTIME DESTINATION ${EXEC_DIR}
  LIBRARY DESTINATION ${LIB_DIR}
)
#message("^^^^^^^^^^^^^^^^^^^^ CMAKE_CXX_FLAGS" "${CMAKE_CXX_FLAGS}")
