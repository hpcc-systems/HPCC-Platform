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


# - Try to find the ICU unicode library
# Once done this will define
#
#  ICU_FOUND - system has the ICU library
#  ICU_INCLUDE_DIR - the ICU include directory
#  ICU_LIBRARIES - The libraries needed to use ICU
#  ICU_VERSION - Version of unicode library

IF (NOT ICU_FOUND)
  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (libdir "lib/linux64_gcc4.1.1")
      ELSE()
        SET (libdir "lib/linux32_gcc4.1.1")
      ENDIF()
    ELSEIF(WIN32)
      IF (${ARCH64BIT} EQUAL 1)
        SET (libdir "lib64")
      ELSE()
        SET (libdir "lib")
      ENDIF()
    ELSE()
      SET (libdir "unknown")
    ENDIF()
    IF (NOT ("${libdir}" STREQUAL "unknown"))
      FIND_PATH (ICU_INCLUDE_DIR NAMES unicode/uchar.h PATHS "${EXTERNALS_DIRECTORY}/icu/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (ICU_LIBRARIES NAMES icuuc PATHS "${EXTERNALS_DIRECTORY}/icu/${libdir}" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (ICU_INCLUDE_DIR PATHS /usr/local/opt/icu4c/include NAMES unicode/uchar.h)
    FIND_LIBRARY (ICU_LIBRARIES PATHS /usr/local/opt/icu4c/lib NAMES icuuc)
  endif()

  if(EXISTS ${ICU_INCLUDE_DIR}/unicode/uchar.h)
    file(STRINGS ${ICU_INCLUDE_DIR}/unicode/uchar.h __ICU_VERSION REGEX
        "^#define U_UNICODE_VERSION*")
    string(REPLACE "#define U_UNICODE_VERSION" "" __ICU_VERSION
        ${__ICU_VERSION})
    string(REPLACE "\"" "" __ICU_VERSION ${__ICU_VERSION})
    string(STRIP "${__ICU_VERSION}" __ICU_VERSION)
    set(ICU_VERSION "${__ICU_VERSION}" CACHE STRING "unicode library version")
  endif()

  if(EXISTS ${ICU_INCLUDE_DIR}/unicode/uvernum.h)
    file(STRINGS ${ICU_INCLUDE_DIR}/unicode/uvernum.h __ICU_MAJOR_VERSION REGEX
        "^#define U_ICU_VERSION_MAJOR_NUM*")
    string(REPLACE "#define U_ICU_VERSION_MAJOR_NUM" "" __ICU_MAJOR_VERSION
        ${__ICU_MAJOR_VERSION})
    string(REPLACE "\"" "" __ICU_MAJOR_VERSION ${__ICU_MAJOR_VERSION})
    string(STRIP "${__ICU_MAJOR_VERSION}" __ICU_MAJOR_VERSION)
    set(ICU_MAJOR_VERSION "${__ICU_MAJOR_VERSION}" CACHE STRING "icu library version")
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(ICU DEFAULT_MSG
    ICU_LIBRARIES 
    ICU_INCLUDE_DIR
  )
  message(STATUS "  version: ${ICU_MAJOR_VERSION} unicode: ${ICU_VERSION}")
  IF (ICU_FOUND)
    IF (UNIX)
      STRING(REPLACE "icuuc" "icui18n" ICU_EXTRA1 "${ICU_LIBRARIES}")
      STRING(REPLACE "icuuc" "icudata" ICU_EXTRA2 "${ICU_LIBRARIES}")
    ELSE()
      STRING(REPLACE "icuuc" "icuin" ICU_EXTRA1 "${ICU_LIBRARIES}")
    ENDIF()
    # The order is important for lib2 processing:
    # depender, such as icuil8n, should be placed before the dependee
    set (ICU_LIBRARIES ${ICU_EXTRA1} ${ICU_LIBRARIES} ${ICU_EXTRA2} )
  ENDIF()

  MARK_AS_ADVANCED(ICU_INCLUDE_DIR ICU_LIBRARIES ICU_MAJOR_VERSION ICU_VERSION)
ENDIF()
