################################################################################
#    Copyright (C) 2011 HPCC Systems.
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

# - Try to find the libarchive archive decompression library
# Once done this will define
#
#  LIBARCHIVE_FOUND - system has the libarchive library
#  LIBARCHIVE_INCLUDE_DIR - the libarchive include directory
#  LIBARCHIVE_LIBRARIES - The libraries needed to use libarchive

IF (NOT LIBARCHIVE_FOUND)
  IF (WIN32)
    SET (libarchive_lib "libarchive")
  ELSE()
    SET (libarchive_lib "archive")
  ENDIF()

  FIND_PATH (LIBARCHIVE_INCLUDE_DIR NAMES archive.h)
  FIND_LIBRARY (LIBARCHIVE_LIBRARIES NAMES ${libarchive_lib})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(libarchive DEFAULT_MSG
    LIBARCHIVE_LIBRARIES
    LIBARCHIVE_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(LIBARCHIVE_INCLUDE_DIR LIBARCHIVE_LIBRARIES)
ENDIF()
