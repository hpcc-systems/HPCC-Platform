################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.
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


# - Try to find the TBB malloc proxy library
# Once done this will define
#
#  TBBMALLOC_FOUND - system has the TBBMALLOC library
#  TBBMALLOC_LIBRARIES - The libraries needed to use TBBMALLOC

IF (NOT TBBMALLOC_FOUND)
  SET (tbbmalloc_lib "tbbmalloc_proxy")

  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
    IF (WIN32)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "Win64")
      ELSE()
        SET (osdir "Win32")
      ENDIF()
      SET (tbbver "1.2.8")
    ELSE()
      SET (osdir "unknown")
      SET (tbbver "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_LIBRARY (TBBMALLOC_LIBRARIES NAMES ${tbbmalloc_lib} PATHS "${EXTERNALS_DIRECTORY}/tbb/${tbbver}/lib/${osdir}" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_LIBRARY (TBBMALLOC_LIBRARIES NAMES ${tbbmalloc_lib})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(TBBMALLOC DEFAULT_MSG
    TBBMALLOC_LIBRARIES
  )

  MARK_AS_ADVANCED(TBBMALLOC_LIBRARIES)
ENDIF()
