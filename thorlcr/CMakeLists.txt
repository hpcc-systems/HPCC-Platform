################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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

if (USE_UNITY_BUILD)
    SET (CMAKE_UNITY_BUILD TRUE)
endif ()

if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
    SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-reorder -Wno-unused-parameter -Wno-overloaded-virtual -Wno-delete-non-virtual-dtor")
endif()

add_subdirectory (activities)
add_subdirectory (graph)
add_subdirectory (master)
add_subdirectory (mfilemanager)
add_subdirectory (msort)
add_subdirectory (slave)
add_subdirectory (thorcodectx)
