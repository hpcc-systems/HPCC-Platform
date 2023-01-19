################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
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

# - Try to find the Numa library
# Once done this will define
#
#  NUMA_FOUND - system has the numa library
#  NUMA_INCLUDE_DIR - the numa include directory
#  NUMA_LIBRARIES - The libraries needed to use numa

IF (NOT NUMA_FOUND)
  SET (numa_lib "numa")

  FIND_PATH (NUMA_INCLUDE_DIR NAMES numa.h)
  FIND_LIBRARY (NUMA_LIBRARIES NAMES ${numa_lib})

  find_package_handle_standard_args(NUMA DEFAULT_MSG
    NUMA_LIBRARIES
    NUMA_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(NUMA_INCLUDE_DIR NUMA_LIBRARIES)
ENDIF()
