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


# - Try to find the libxml2 xml library
# Once done this will define
#
#  LIBXML2_FOUND - system has the libxml2 library
#  LIBXML2_INCLUDE_DIR - the libxml2 include directory
#  LIBXML2_LIBRARIES - The libraries needed to use libxml2

if (NOT LIBXML2_FOUND)
  IF (WIN32)
    SET (libxml2_libs "xml2 libxml2")
  ELSE()
    SET (libxml2_libs "xml2 libxml2")
  ENDIF()

  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux64_gcc4.1.1")
      ELSE()
        SET (osdir "linux32_gcc4.1.1")
      ENDIF()
    ELSEIF(WIN32)
      SET (osdir "lib")
    ELSE()
      SET (osdir "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (LIBXML2_INCLUDE_DIR NAMES libxml/xpath.h PATHS "${EXTERNALS_DIRECTORY}/libxml2/include/" NO_DEFAULT_PATH)
      FIND_LIBRARY (LIBXML2_LIBRARIES NAMES ${libxml2_libs} PATHS "${EXTERNALS_DIRECTORY}/libxml2/${osdir}" NO_DEFAULT_PATH)
    ENDIF() 
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (LIBXML2_INCLUDE_DIR NAMES libxml/xpath.h PATH_SUFFIXES libxml2)
    FIND_LIBRARY (LIBXML2_LIBRARIES NAMES xml2 libxml2)
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(Libxml2 DEFAULT_MSG
    LIBXML2_LIBRARIES 
    LIBXML2_INCLUDE_DIR
  )
  MARK_AS_ADVANCED(LIBXML2_INCLUDE_DIR LIBXML2_LIBRARIES)
ENDIF()

