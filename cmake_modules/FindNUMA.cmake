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

# - Try to find the numa includes and libraries
# Once done this will define
#
#  NUMA_FOUND
#  NUMA_INCLUDE_DIRS
#  NUMA_LIBRARIES

IF (NOT NUMA_FOUND AND NOT APPLE AND NOT WIN32)

  FIND_PATH (NUMA_INCLUDE_NUMA NAMES numa.h)
  FIND_PATH (NUMA_INCLUDE_NUMAIF NAMES numaif.h)
  FIND_LIBRARY (NUMA_LIBRARY NAMES numa)

  SET(NUMA_INCLUDE_DIRS ${NUMA_INCLUDE_NUMA} ${NUMA_INCLUDE_NUMAIF})
  SET(NUMA_LIBRARIES ${NUMA_LIBRARY})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(NUMA DEFAULT_MSG
    NUMA_INCLUDE_DIRS
    NUMA_LIBRARIES
  )

  IF(NUMA_INCLUDE_NUMA STREQUAL "NUMA_INCLUDE_NUMA-NOTFOUND" OR NUMA_INCLUDE_NUMAIF STREQUAL "NUMA_INCLUDE_NUMAIF-NOTFOUND")
    MESSAGE(FATAL_ERROR "NUMA requested but include files not found")
  ELSEIF(NUMA_LIBRARY STREQUAL "NUMA_LIBRARY-NOTFOUND")
    MESSAGE(FATAL_ERROR "NUMA requested but libraries not found")
  ENDIF()

  MESSAGE(STATUS "adding NUMA support")

  SET(OPTIONAL_DEB_PKGS "${OPTIONAL_DEB_PKGS}, libnuma1")
  SET(OPTIONAL_RPM_PKGS "${OPTIONAL_RPM_PKGS}, numactl")

  MARK_AS_ADVANCED(NUMA_INCLUDE_DIRS NUMA_LIBRARIES)
ENDIF()
