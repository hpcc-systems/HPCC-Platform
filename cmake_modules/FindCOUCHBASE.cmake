################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
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


# - Try to find the Couchbase c library
# Once done this will define
#
#  LIBCOUCHBASE_FOUND, if false, do not try to link with libev
#  LIBCOUCHBASE_LIBRARIES, Library path and libs
#  LIBCOUCHBASE_INCLUDE_DIR, where to find the libev headers


IF (NOT LIBCOUCHBASE_FOUND)

  #couchbase.h
  #libcouchbase

  IF (UNIX)
    IF (${ARCH64BIT} EQUAL 1)
      SET (osdir "x86_64-linux-gnu")
    ELSE()
      SET (osdir "unknown")
    ENDIF()
  ELSEIF(WIN32)
    IF (${ARCH64BIT} EQUAL 1)
      SET (osdir "unknown")
    ELSE()
      SET (osdir "unknown")
    ENDIF()
  ELSE()
    SET (osdir "unknown")
  ENDIF()

  IF (NOT ("${osdir}" STREQUAL "unknown"))
    FIND_PATH(
      LIBCOUCHBASE_INCLUDE_DIR
      NAMES couchbase.h
      PATHS "${EXTERNALS_DIRECTORY}/couchbase/${osdir}/include" "${EXTERNALS_DIRECTORY}/couchbase/include"
      PATH_SUFFIXES include libcouchbase
      NO_DEFAULT_PATH
    )

    FIND_LIBRARY(
      MYSQL_LIBRARIES NAMES ${mysql_lib} PATHS "${EXTERNALS_DIRECTORY}/mysql/${osdir}/lib" "${EXTERNALS_DIRECTORY}/mysql/${osdir}" "${EXTERNALS_DIRECTORY}/mysql/lib"
      NO_DEFAULT_PATH
    )
    FIND_LIBRARY (
      MYSQL_LIBRARIES NAMES ${mysql_lib_alt} PATHS "${EXTERNALS_DIRECTORY}/mysql/${osdir}/lib" "${EXTERNALS_DIRECTORY}/mysql/${osdir}" "${EXTERNALS_DIRECTORY}/mysql/lib"
      NO_DEFAULT_PATH
    )
  ENDIF()

  FIND_PATH (
    LIBCOUCHBASE_INCLUDE_DIR couchbase.h
    PATHS
      /usr
      /usr/include
      /usr/include/libcouchbase
      /opt/local
      /opt
   )

   FIND_LIBRARY (
     LIBCOUCHBASE_LIBRARIES
     NAMES couchbase libcouchbase
     PATH_SUFFIXES lib lib/${osdir} libcouchbase libcouchbase/${osdir}
     PATHS
       /usr
     NO_DEFAULT_PATH
   )

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(
    couchbase DEFAULT_MSG
    LIBCOUCHBASE_LIBRARIES
    LIBCOUCHBASE_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(LIBCOUCHBASE_INCLUDE_DIR LIBCOUCHBASE_LIBRARIES)
ENDIF()
