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


# - Try to find the OPENSSL library
# Once done this will define
#
#  OPENSSL_FOUND - system has the OPENSSL library
#  OPENSSL_INCLUDE_DIR - the OPENSSL include directory
#  OPENSSL_LIBRARIES - The libraries needed to use OPENSSL

IF (NOT OPENSSL_FOUND)
  IF (WIN32)
    SET (ssl_lib "ssleay32.lib")
  ELSEIF ("${CMAKE_SYSTEM_NAME}" STREQUAL "Darwin" )
    SET (ssl_lib "libssl.dylib")
  ELSE()
    SET (ssl_lib "libssl.so")
  ENDIF()
  IF (NOT ${EXTERNALS_DIRECTORY} STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux64_gcc4.1.1")
      ELSE()
        SET (osdir "linux32_gcc4.1.1")
      ENDIF()
    ELSEIF(WIN32)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "win64")
      ELSE()
        SET (osdir "win32")
      ENDIF()
    ELSE()
      SET (osdir "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (OPENSSL_INCLUDE_DIR NAMES openssl/ssl.h PATHS "${EXTERNALS_DIRECTORY}/openssl/${osdir}/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (OPENSSL_LIBRARIES NAMES ${ssl_lib} PATHS "${EXTERNALS_DIRECTORY}/openssl/${osdir}/lib" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (OPENSSL_INCLUDE_DIR NAMES openssl/ssl.h)
    FIND_LIBRARY (OPENSSL_LIBRARIES NAMES ${ssl_lib})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(OPENSSL DEFAULT_MSG
    OPENSSL_LIBRARIES 
    OPENSSL_INCLUDE_DIR
  )
  IF (OPENSSL_FOUND)
    STRING(REPLACE "/${ssl_lib}" "" OPENSSL_LIBRARY_DIR "${OPENSSL_LIBRARIES}")
    IF (NOT WIN32)
      STRING(REPLACE "libssl" "libcrypto" OPENSSL_EXTRA "${OPENSSL_LIBRARIES}")
    ELSE()
      STRING(REPLACE "ssleay32" "libeay32" OPENSSL_EXTRA "${OPENSSL_LIBRARIES}")
    ENDIF()
    set (OPENSSL_LIBRARIES ${OPENSSL_LIBRARIES} ${OPENSSL_EXTRA} )
  ENDIF()

  MARK_AS_ADVANCED(OPENSSL_INCLUDE_DIR OPENSSL_LIBRARIES)
ENDIF()
