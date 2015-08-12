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

# - Try to find the libarchive archive decompression library
# Once done this will define
#
#  LIBARCHIVE_FOUND - system has the libarchive library
#  LIBARCHIVE_INCLUDE_DIR - the libarchive include directory
#  LIBARCHIVE_LIBRARIES - The libraries needed to use libarchive

IF (NOT LIBARCHIVE_FOUND)
  IF (WIN32)
    SET (libarchive_ver "3.1.2")
    SET (libarchive_lib "archive_static")
    IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "Win64")
    ELSE ()
        SET (osdir "Win32")
    ENDIF ()

    FIND_PATH (LIBARCHIVE_INCLUDE_DIR NAMES archive.h PATHS "${EXTERNALS_DIRECTORY}/libarchive/${libarchive_ver}/include" NO_DEFAULT_PATH)
    FIND_LIBRARY (LIBARCHIVE_LIBRARIES NAMES ${libarchive_lib} PATHS "${EXTERNALS_DIRECTORY}/libarchive/${libarchive_ver}/lib/${osdir}" NO_DEFAULT_PATH)
  ELSE()
    SET (libarchive_lib "archive")
    FIND_PATH (LIBARCHIVE_INCLUDE_DIR NAMES archive.h)
    FIND_LIBRARY (LIBARCHIVE_LIBRARIES NAMES ${libarchive_lib})
  ENDIF()



  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(libarchive DEFAULT_MSG
    LIBARCHIVE_LIBRARIES
    LIBARCHIVE_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(LIBARCHIVE_INCLUDE_DIR LIBARCHIVE_LIBRARIES)
ENDIF()
