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

# - Try to find the hiredis library
# Once done this will define
#
#  HIREDIS_FOUND - system has the hiredis library
#  HIREDIS_INCLUDE_DIR - the hiredis include directory(s)
#  HIREDIS_LIBRARY - The library needed to use hiredis

IF (NOT HIREDIS_FOUND)
  IF (WIN32)
    SET (libhiredis "libhiredis")
  ELSE()
    SET (libhiredis "hiredis")
  ENDIF()

  FIND_PATH(HIREDIS_INCLUDE_DIR hiredis/hiredis.h PATHS /usr/include /usr/share/include /usr/local/include PATH_SUFFIXES hiredis)
  FIND_LIBRARY(HIREDIS_LIBRARY NAMES ${libhiredis} PATHS /usr/lib /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64)

  IF(HIREDIS_INCLUDE_DIR)
    #MAJOR
    FILE (STRINGS "${HIREDIS_INCLUDE_DIR}/hiredis/hiredis.h" major REGEX "#define HIREDIS_MAJOR")
    STRING(REGEX REPLACE "#define HIREDIS_MAJOR " "" major "${major}")
    STRING(REGEX REPLACE "\"" "" major "${major}")
    #MINOR
    FILE (STRINGS "${HIREDIS_INCLUDE_DIR}/hiredis/hiredis.h" minor REGEX "#define HIREDIS_MINOR")
    STRING(REGEX REPLACE "#define HIREDIS_MINOR " "" minor "${minor}")
    STRING(REGEX REPLACE "\"" "" minor "${minor}")
    #PATCH
    FILE (STRINGS "${HIREDIS_INCLUDE_DIR}/hiredis/hiredis.h" patch REGEX "#define HIREDIS_PATCH")
    STRING(REGEX REPLACE "#define HIREDIS_PATCH " "" patch "${patch}")
    STRING(REGEX REPLACE "\"" "" patch "${patch}")

    SET(HIREDIS_VERSION_STRING "${major}.${minor}.${patch}")
    IF ("${HIREDIS_VERSION_STRING}" VERSION_LESS "${HIREDIS_FIND_VERSION}")
      MESSAGE("WARNING - connection caching not avaliable with libhiredis version '${HIREDIS_VERSION_STRING}' as incompatible with min version>=${HIREDIS_FIND_VERSION}")
    ENDIF()
  ENDIF()


  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(hiredis DEFAULT_MSG
    HIREDIS_LIBRARY
    HIREDIS_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(HIREDIS_INCLUDE_DIR HIREDIS_LIBRARY)
ENDIF()

