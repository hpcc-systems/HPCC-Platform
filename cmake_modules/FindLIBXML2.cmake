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


# - Try to find the libxml2 xml library
# Once done this will define
#
#  LIBXML2_FOUND - system has the libxml2 library
#  LIBXML2_INCLUDE_DIR - the libxml2 include directory
#  LIBXML2_LIBRARIES - The libraries needed to use libxml2

if (NOT LIBXML2_FOUND)
  IF (WIN32)
    SET (libxml2_libs "xml2 libxml2")
  ELSE()
    SET (libxml2_libs "xml2 libxml2")
  ENDIF()

  IF (NOT ${EXTERNALS_DIRECTORY} STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux64_gcc4.1.1")
      ELSE()
        SET (osdir "linux32_gcc4.1.1")
      ENDIF()
    ELSEIF(WIN32)
      SET (osdir "lib")
    ELSE()
      SET (osdir "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (LIBXML2_INCLUDE_DIR NAMES libxml/xpath.h PATHS "${EXTERNALS_DIRECTORY}/libxml2/include/" NO_DEFAULT_PATH)
      FIND_LIBRARY (LIBXML2_LIBRARIES NAMES ${libxml2_libs} PATHS "${EXTERNALS_DIRECTORY}/libxml2/${osdir}" NO_DEFAULT_PATH)
    ENDIF() 
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (LIBXML2_INCLUDE_DIR NAMES libxml2/libxml/xpath.h )
    FIND_LIBRARY (LIBXML2_LIBRARIES NAMES xml2 libxml2)
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(Libxml2 DEFAULT_MSG
    LIBXML2_LIBRARIES 
    LIBXML2_INCLUDE_DIR
  )
  MARK_AS_ADVANCED(LIBXML2_INCLUDE_DIR LIBXML2_LIBRARIES)
ENDIF()

