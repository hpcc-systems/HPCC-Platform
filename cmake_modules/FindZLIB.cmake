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

    
# - Try to find the ZLib compression library
# Once done this will define
#
#  ZLIB_FOUND - system has the ZLib library
#  ZLIB_INCLUDE_DIR - the ZLib include directory
#  ZLIB_LIBRARIES - The libraries needed to use ZLib

IF (NOT ZLIB_FOUND)
  IF (WIN32)
    SET (zlib_lib "zlib")
  ELSE()
    SET (zlib_lib "z")
  ENDIF()

  IF (NOT ${EXTERNALS_DIRECTORY} STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux64_gcc4.1.1")
      ELSE()
        SET (osdir "linux32_gcc4.1.1")
      ENDIF()
      SET (zlibver "1.2.5")
    ELSEIF(WIN32)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "win64")
        SET (zlibver "1.2.5")
      ELSE()
        SET (osdir "")
        SET (zlibver "1.2.1")
      ENDIF()  
    ELSE()
      SET (osdir "unknown")
      SET (zlibver "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (ZLIB_INCLUDE_DIR NAMES zlib.h PATHS "${EXTERNALS_DIRECTORY}/zlib/${zlibver}/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (ZLIB_LIBRARIES NAMES ${zlib_lib} PATHS "${EXTERNALS_DIRECTORY}/zlib/${zlibver}/lib/${osdir}" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (ZLIB_INCLUDE_DIR NAMES zlib.h)
    FIND_LIBRARY (ZLIB_LIBRARIES NAMES ${zlib_lib})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(ZLib DEFAULT_MSG
    ZLIB_LIBRARIES 
    ZLIB_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(ZLIB_INCLUDE_DIR ZLIB_LIBRARIES)
ENDIF()
