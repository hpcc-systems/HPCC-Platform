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


# - Try to find the MYSQL library
# Once done this will define
#
#  MYSQL_FOUND - system has the MYSQL library
#  MYSQL_INCLUDE_DIR - the MYSQL include directory
#  MYSQL_LIBRARIES - The libraries needed to use MYSQL

IF (NOT MYSQL_FOUND)
  IF (WIN32)
    SET (mysql_lib "libmysql")
  ELSE()
    SET (mysql_lib "mysqlclient_r")    # Use the re-entrant version if it exists
    SET (mysql_lib_alt "mysqlclient")  # (but newer versions do not make the distinction and supply just the one)
  ENDIF()
  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux64_gcc4.1.1")
      ELSE()
        SET (osdir "linux32_gcc4.1.1")
      ENDIF()
    ELSEIF(WIN32)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "win64")
      ELSE()
        SET (osdir "win32")
      ENDIF()
    ELSE()
      SET (osdir "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      IF( "${MYSQL_INCLUDE_DIR}" STREQUAL "" )
        FIND_PATH (MYSQL_INCLUDE_DIR NAMES mysql.h PATHS "${EXTERNALS_DIRECTORY}/mysql/${osdir}/include"
          "${EXTERNALS_DIRECTORY}/mysql/include" NO_DEFAULT_PATH)
      ENDIF()
      IF( "${MYSQL_LIBRARIES}" STREQUAL "" )
        FIND_LIBRARY (MYSQL_LIBRARIES NAMES ${mysql_lib} PATHS "${EXTERNALS_DIRECTORY}/mysql/${osdir}/lib"
          "${EXTERNALS_DIRECTORY}/mysql/${osdir}" "${EXTERNALS_DIRECTORY}/mysql/lib" NO_DEFAULT_PATH)
        FIND_LIBRARY (MYSQL_LIBRARIES NAMES ${mysql_lib_alt} PATHS "${EXTERNALS_DIRECTORY}/mysql/${osdir}/lib"
          "${EXTERNALS_DIRECTORY}/mysql/${osdir}" "${EXTERNALS_DIRECTORY}/mysql/lib" NO_DEFAULT_PATH)
      ENDIF()
    ENDIF()
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (MYSQL_INCLUDE_DIR NAMES mysql.h PATH_SUFFIXES mysql)
    FIND_LIBRARY (MYSQL_LIBRARIES NAMES ${mysql_lib} PATH_SUFFIXES mysql)
    FIND_LIBRARY (MYSQL_LIBRARIES NAMES ${mysql_lib_alt} PATH_SUFFIXES mysql)
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(MYSQL DEFAULT_MSG
    MYSQL_LIBRARIES 
    MYSQL_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(MYSQL_INCLUDE_DIR MYSQL_LIBRARIES)
ENDIF()
