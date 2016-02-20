################################################################################
#    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.
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

# - Try to find the libmemcached library
# Once done this will define
#
#  LIBMEMCACHED_FOUND - system has the libmemcached library
#  LIBMEMCACHED_INCLUDE_DIR - the libmemcached include directory(s)
#  LIBMEMCACHED_LIBRARIES - The libraries needed to use libmemcached

IF (NOT LIBMEMCACHED_FOUND)
  IF (WIN32)
    SET (libmemcached_lib "libmemcached")
    SET (libmemcachedUtil_lib "libmemcachedutil")
  ELSE()
    SET (libmemcached_lib "memcached")
    SET (libmemcachedUtil_lib "memcachedutil")
  ENDIF()

  FIND_PATH(LIBMEMCACHED_INCLUDE_DIR libmemcached/memcached.hpp PATHS /usr/include /usr/share/include /usr/local/include PATH_SUFFIXES libmemcached)

  FIND_LIBRARY (LIBMEMCACHEDCORE_LIBRARY NAMES ${libmemcached_lib} PATHS /usr/lib usr/lib/libmemcached /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64)
  FIND_LIBRARY (LIBMEMCACHEDUTIL_LIBRARY NAMES ${libmemcachedUtil_lib} PATHS /usr/lib /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64)

  #IF (LIBMEMCACHED_LIBRARY STREQUAL "LIBMEMCACHED_LIBRARY-NOTFOUND")
  #  SET (LIBMEMCACHEDCORE_LIBRARY "")    # Newer versions of libmemcached are header-only, with no associated library.
  #ENDIF()

  SET (LIBMEMCACHED_LIBRARIES ${LIBMEMCACHEDCORE_LIBRARY} ${LIBMEMCACHEDUTIL_LIBRARY})

  FILE (STRINGS "${LIBMEMCACHED_INCLUDE_DIR}/libmemcached-1.0/configure.h" version REGEX "#define LIBMEMCACHED_VERSION_STRING")
  STRING(REGEX REPLACE "#define LIBMEMCACHED_VERSION_STRING " "" version "${version}")
  STRING(REGEX REPLACE "\"" "" version "${version}")
  SET (LIBMEMCACHED_VERSION_STRING ${version})
  IF ("${LIBMEMCACHED_VERSION_STRING}" VERSION_EQUAL "${LIBMEMCACHED_FIND_VERSION}" OR "${LIBMEMCACHED_VERSION_STRING}" VERSION_GREATER "${LIBMEMCACHED_FIND_VERSION}")  
    SET (LIBMEMCACHED_VERSION_OK 1)
    SET (MSG "${DEFAULT_MSG}")
  ELSE()
    SET (LIBMEMCACHED_VERSION_OK 0)
    SET(MSG "libmemcached version '${LIBMEMCACHED_VERSION_STRING}' incompatible with min version>=${LIBMEMCACHED_FIND_VERSION}")
  ENDIF()
      
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(libmemcached ${MSG}
    LIBMEMCACHED_LIBRARIES
    LIBMEMCACHED_INCLUDE_DIR
    LIBMEMCACHED_VERSION_OK
  )

  MARK_AS_ADVANCED(LIBMEMCACHED_INCLUDE_DIRS LIBMEMCACHED_LIBRARIES)
ENDIF()

