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

# - Try to find the V8 JavaScript library
# Once done this will define
#
#  V8_FOUND - system has the b8 javascript library
#  V8_INCLUDE_DIR - the V8 include directory
#  V8_LIBRARIES - The libraries needed to use V8

IF (NOT V8_FOUND)
  IF (WIN32)
    SET (v8_lib "libv8")
  ELSE()
    SET (v8_lib "v8")
  ENDIF()

  FIND_PATH (V8_INCLUDE_DIR NAMES v8.h)
  FIND_LIBRARY (V8_LIBRARIES NAMES ${v8_lib})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(v8 DEFAULT_MSG
    V8_LIBRARIES
    V8_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(V8_INCLUDE_DIR V8_LIBRARIES)
ENDIF()
