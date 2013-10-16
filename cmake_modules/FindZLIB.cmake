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

    
# - Try to find the ZLib compression library
# Once done this will define
#
#  ZLIB_FOUND - system has the ZLib library
#  ZLIB_INCLUDE_DIR - the ZLib include directory
#  ZLIB_LIBRARIES - The libraries needed to use ZLib

IF (NOT ZLIB_FOUND)
  IF (WIN32)
    SET (zlib_lib "zlib")
  ELSE()
    SET (zlib_lib "z")
  ENDIF()

  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux64_gcc4.1.1")
      ELSE()
        SET (osdir "linux32_gcc4.1.1")
      ENDIF()
      SET (zlibver "1.2.5")
    ELSEIF(WIN32)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "win64")
      ELSE()
        SET (osdir "win32")
      ENDIF()  
      SET (zlibver "1.2.5")
    ELSE()
      SET (osdir "unknown")
      SET (zlibver "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (ZLIB_INCLUDE_DIR NAMES zlib.h PATHS "${EXTERNALS_DIRECTORY}/zlib/${zlibver}/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (ZLIB_LIBRARIES NAMES ${zlib_lib} PATHS "${EXTERNALS_DIRECTORY}/zlib/${zlibver}/lib/${osdir}" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (ZLIB_INCLUDE_DIR NAMES zlib.h)
    FIND_LIBRARY (ZLIB_LIBRARIES NAMES ${zlib_lib})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(ZLib DEFAULT_MSG
    ZLIB_LIBRARIES 
    ZLIB_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(ZLIB_INCLUDE_DIR ZLIB_LIBRARIES)
ENDIF()
