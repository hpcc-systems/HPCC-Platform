################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.
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

# - Try to find the libhiredislibrary
# Once done this will define
#
#  LIBHEDIS_FOUND - system has the libhiredis library
#  LIBHIREDIS_INCLUDE_DIR - the libhiredis include directory(s)
#  LIBHIREDIS_LIBRARY - The library needed to use hiredis

IF (NOT LIBREDIS_FOUND)
  IF (WIN32)
    SET (libhiredis "libhiredis")
  ELSE()
    SET (libhiredis "hiredis")
  ENDIF()

  FIND_PATH(LIBHIREDIS_INCLUDE_DIR hiredis/hiredis.h PATHS /usr/include /usr/share/include /usr/local/include PATH_SUFFIXES hiredis)
  FIND_LIBRARY(LIBHIREDIS_LIBRARY NAMES ${libhiredis} PATHS /usr/lib /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64)

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(redis DEFAULT_MSG
    LIBHIREDIS_LIBRARY
    LIBHIREDIS_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(LIBHIREDIS_INCLUDE_DIR LIBHIREDIS_LIBRARY)
ENDIF()

