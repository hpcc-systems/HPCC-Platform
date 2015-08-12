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


# - Try to find the LIBXSLT xml library
# Once done this will define
#
#  LIBXSLT_FOUND - system has the LIBXSLT library
#  LIBXSLT_INCLUDE_DIR - the LIBXSLT include directory
#  LIBXSLT_LIBRARIES - The libraries needed to use LIBXSLT

if (NOT LIBXSLT_FOUND)
  IF (WIN32)
    SET (libxslt_libs "xslt libxslt")
    SET (libexslt_libs "exslt libexslt")
  ELSE()
    SET (libxslt_libs "xslt libxslt")
    SET (libexslt_libs "exslt libexslt")
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
      FIND_PATH (LIBXSLT_INCLUDE_DIR NAMES libxslt/xslt.h PATHS "${EXTERNALS_DIRECTORY}/libxslt/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (LIBXSLT_LIBRARIES NAMES ${libxslt_libs} PATHS "${EXTERNALS_DIRECTORY}/libxslt/${osdir}" NO_DEFAULT_PATH)
      FIND_PATH (LIBEXSLT_INCLUDE_DIR NAMES libexslt/exslt.h PATHS "${EXTERNALS_DIRECTORY}/libexslt/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (LIBEXSLT_LIBRARIES NAMES ${libexslt_libs} PATHS "${EXTERNALS_DIRECTORY}/libexslt/${osdir}" NO_DEFAULT_PATH)
    ENDIF() 
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (LIBXSLT_INCLUDE_DIR NAMES libxslt/xslt.h )
    FIND_LIBRARY (LIBXSLT_LIBRARIES NAMES xslt libxslt)
    FIND_PATH (LIBEXSLT_INCLUDE_DIR NAMES libexslt/exslt.h )
    FIND_LIBRARY (LIBEXSLT_LIBRARIES NAMES exslt libexslt)
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(Libxslt DEFAULT_MSG
    LIBXSLT_LIBRARIES 
    LIBXSLT_INCLUDE_DIR
    LIBEXSLT_LIBRARIES 
    LIBEXSLT_INCLUDE_DIR
  )
  MARK_AS_ADVANCED(LIBXSLT_INCLUDE_DIR LIBEXSLT_INCLUDE_DIR LIBXSLT_LIBRARIES LIBEXSLT_LIBRARIES)
ENDIF()

