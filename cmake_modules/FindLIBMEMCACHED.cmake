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

# - Try to find the libmemcached library # Once done this will define #
#  LIBMEMCACHED_FOUND - system has the libmemcached library
#  LIBMEMCACHED_INCLUDE_DIR - the libmemcached include directory(s)
#  LIBMEMCACHED_LIBRARIES - The libraries needed to use libmemcached

#  If the memcached libraries are found on the system, we assume they exist natively and dependencies
#  can be handled through package management.  If the libraries are not found, and if
#  MEMCACHED_USE_EXTERNAL_LIBRARY is ON, we will fetch, build, and include a copy of the neccessary
#  Libraries.

# Search for native library to build against
if(WIN32)
    set(libmemcached_lib "libmemcached")
    set(libmemcachedUtil_lib "libmemcachedutil")
else()
    set(libmemcached_lib "memcached")
    set(libmemcachedUtil_lib "memcachedutil")
endif()

find_path(LIBMEMCACHED_INCLUDE_DIR NAMES libmemcached/memcached.hpp)
find_library(LIBMEMCACHEDCORE_LIBRARY NAMES ${libmemcached_lib})
find_library(LIBMEMCACHEDUTIL_LIBRARY NAMES ${libmemcachedUtil_lib})

set(LIBMEMCACHED_LIBRARIES ${LIBMEMCACHEDCORE_LIBRARY} ${LIBMEMCACHEDUTIL_LIBRARY})


include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    LIBMEMCACHED DEFAULT_MSG
    LIBMEMCACHEDCORE_LIBRARY
    LIBMEMCACHEDUTIL_LIBRARY
    LIBMEMCACHED_INCLUDE_DIR
)
mark_as_advanced(LIBMEMCACHED_INCLUDE_DIR LIBMEMCACHED_LIBRARIES LIBMEMCACHEDCORE_LIBRARY LIBMEMCACHEDUTIL_LIBRARY)
