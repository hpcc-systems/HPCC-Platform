################################################################################
#    Copyright (C) 2012 HPCC Systems®.
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

# - Try to find the uriparser uri parsing library
# Once done this will define
#
#  URIPARSER_FOUND - system has the uriparser library
#  URIPARSER_INCLUDE_DIR - the uriparser include directory
#  URIPARSER_LIBRARIES - The libraries needed to use uriparser

IF (NOT URIPARSER_FOUND)
  IF (WIN32)
    SET (uriparser_lib "liburiparser")
  ELSE()
    SET (uriparser_lib "uriparser")
  ENDIF()

  FIND_PATH (URIPARSER_INCLUDE_DIR NAMES uriparser/Uri.h)
  FIND_LIBRARY (URIPARSER_LIBRARIES NAMES ${uriparser_lib})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(uriparser DEFAULT_MSG
    URIPARSER_LIBRARIES
    URIPARSER_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(URIPARSER_INCLUDE_DIR URIPARSER_LIBRARIES)
ENDIF()
