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

  FIND_PATH (
    LIBCOUCHBASE_INCLUDE_DIR 
    NAMES couchbase.h
    PATH_SUFFIXES libcouchbase
   )

   FIND_LIBRARY (
     LIBCOUCHBASE_LIBRARIES
     NAMES couchbase libcouchbase
     PATHS /usr/lib64
     PATH_SUFFIXES libcouchbase
   )

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(
    couchbase DEFAULT_MSG
    LIBCOUCHBASE_LIBRARIES
    LIBCOUCHBASE_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(LIBCOUCHBASE_INCLUDE_DIR LIBCOUCHBASE_LIBRARIES)
ENDIF()
