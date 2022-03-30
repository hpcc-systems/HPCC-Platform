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


project( libgit2helper )
SET(CMAKE_UNITY_BUILD FALSE)

remove_definitions(-fvisibility=hidden)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/libgit2 ${CMAKE_CURRENT_BINARY_DIR}/libgit2 EXCLUDE_FROM_ALL)

#Install the libraries that were created by the git2 sub project into the appropriate location
install(TARGETS git2
    LIBRARY DESTINATION ${LIB_DIR}
)
