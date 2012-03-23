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


# - Try to find the LIBHDFS xml library
# Once done this will define
#
#  LIBHDFS_FOUND - system has the LIBHDFS library
#  LIBHDFS_INCLUDE_DIR - the LIBHDFS include directory
#  LIBHDFS_LIBRARIES - The libraries needed to use LIBHDFS

if (NOT LIBHDFS_FOUND)
  IF (WIN32)
    SET (libhdfs_libs "hdfs" "libhdfs")
  ELSE()
    SET (libhdfs_libs "hdfs" "libhdfs")
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
      FIND_PATH (LIBHDFS_INCLUDE_DIR NAMES libhdfs/hdfs.h PATHS "${EXTERNALS_DIRECTORY}/${HADOOP_PATH}/src/c++" NO_DEFAULT_PATH)
      FIND_LIBRARY (LIBHDFS_LIBRARIES NAMES ${libhdfs_libs} PATHS "${EXTERNALS_DIRECTORY}/${HADOOP_PATH}/c++/${osdir}" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (LIBHDFS_INCLUDE_DIR NAMES hdfs.h PATHS "${HADOOP_PATH}/src/c++/libhdfs" )
    FIND_LIBRARY (LIBHDFS_LIBRARIES NAMES ${libhdfs_libs} PATHS "${HADOOP_PATH}/c++/Linux-amd64-64/lib" )
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(Libhdfs DEFAULT_MSG
    LIBHDFS_LIBRARIES
    LIBHDFS_INCLUDE_DIR
  )
  MARK_AS_ADVANCED(LIBHDFS_INCLUDE_DIR LIBHDFS_LIBRARIES )
ENDIF()
