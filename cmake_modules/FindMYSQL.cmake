################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    SET (mysql_lib "mysqlclient")
  ENDIF()
  IF (NOT ${EXTERNALS_DIRECTORY} STREQUAL "")
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
      FIND_PATH (MYSQL_INCLUDE_DIR NAMES mysql.h PATHS "${EXTERNALS_DIRECTORY}/mysql/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (MYSQL_LIBRARIES NAMES ${mysql_lib} PATHS "${EXTERNALS_DIRECTORY}/mysql/${osdir}" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (MYSQL_INCLUDE_DIR NAMES mysql.h PATH_SUFFIXES mysql)
    FIND_LIBRARY (MYSQL_LIBRARIES NAMES ${mysql_lib} PATH_SUFFIXES mysql)
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(MYSQL DEFAULT_MSG
    MYSQL_LIBRARIES 
    MYSQL_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(MYSQL_INCLUDE_DIR MYSQL_LIBRARIES)
ENDIF()
