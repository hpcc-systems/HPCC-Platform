################################################################################
#    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.
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

# - Try to find the Sqlite3 headers and libraries
# Once done this will define
#
#  SQLITE3_FOUND - system has the SQLITE3 headers and library
#  SQLITE3_INCLUDE_DIR - the SQLITE3 include directory
#  SQLITE3_LIBRARIES - The libraries needed to use SQLITE3

IF (NOT SQLITE3_FOUND)
  IF (WIN32)
    SET (sqlite3_lib "libsqlite3")
  ELSE()
    SET (sqlite3_lib "sqlite3")
  ENDIF()

  FIND_PATH (SQLITE3_INCLUDE_DIR NAMES sqlite3.h)
  FIND_LIBRARY (SQLITE3_LIBRARIES NAMES ${sqlite3_lib})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(SQLITE3 DEFAULT_MSG
    SQLITE3_LIBRARIES
    SQLITE3_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(SQLITE3_INCLUDE_DIR SQLITE3_LIBRARIES)
ENDIF()
