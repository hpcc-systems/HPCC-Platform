################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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


# - Try to find the BZIP2 compression library
# Once done this will define
#
#  BZIP2_FOUND - system has the BZIP2 library
#  BZIP2_INCLUDE_DIR - the BZIP include directory
#  BZIP2_LIBRARIES - The libraries needed to use BZIP2

IF (NOT BZIP2_FOUND)
  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
    IF(WIN32)
      SET (osdir "win32")
      SET (bzip2ver "1.0.6")
      SET (bzip2_lib "libbz2")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (BZIP2_INCLUDE_DIR NAMES bzlib.h PATHS "${EXTERNALS_DIRECTORY}/bzip2/${bzip2ver}/include"
NO_DEFAULT_PATH)
      FIND_LIBRARY (BZIP2_LIBRARIES NAMES ${bzip2_lib} PATHS "${EXTERNALS_DIRECTORY}/bzip2/${bzip2ver}/lib/${osdir}" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  IF (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (BZIP2_INCLUDE_DIR NAMES bzlib.h)
    FIND_LIBRARY (BZIP2_LIBRARIES NAMES ${bzip2_lib})
  ENDIF()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(BZip2 DEFAULT_MSG
    BZIP2_LIBRARIES
    BZIP2_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(BZIP2_INCLUDE_DIR BZIP2_LIBRARIES)
ENDIF()
