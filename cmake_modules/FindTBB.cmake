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


# - Try to find the TBB compression library
# Once done this will define
#
#  TBB_FOUND - system has the TBB library
#  TBB_INCLUDE_DIR - the TBB include directory
#  TBB_LIBRARIES - The libraries needed to use TBB

IF (NOT TBB_FOUND)
  SET (tbb_lib "tbb")

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
      FIND_PATH (TBB_INCLUDE_DIR NAMES tbb/tbb.h PATHS "${EXTERNALS_DIRECTORY}/tbb/${tbbver}/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (TBB_LIBRARIES NAMES ${tbb_lib} PATHS "${EXTERNALS_DIRECTORY}/tbb/${tbbver}/lib/${osdir}" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (TBB_INCLUDE_DIR NAMES tbb/tbb.h)
    FIND_LIBRARY (TBB_LIBRARIES NAMES ${tbb_lib})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(TBB DEFAULT_MSG
    TBB_LIBRARIES
    TBB_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(TBB_INCLUDE_DIR TBB_LIBRARIES)
ENDIF()
