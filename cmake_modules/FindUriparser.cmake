################################################################################
#    Copyright (C) 2012 HPCC Systems.
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
