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


# - Try to find the OPENSSL library
# Once done this will define
#
#  OPENSSL_FOUND - system has the OPENSSL library
#  OPENSSL_INCLUDE_DIR - the OPENSSL include directory
#  OPENSSL_LIBRARIES - The libraries needed to use OPENSSL

IF (NOT OPENSSL_FOUND)
  IF (WIN32)
    SET (ssl_lib "libssl.lib")
  ELSEIF (APPLE)
    SET (ssl_lib "libssl.dylib")
  ELSE()
    SET (ssl_lib "libssl.so")
  ENDIF()
  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
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
    IF (APPLE)
      # We want to ignore any libraries provided by the OS
      FIND_PATH (OPENSSL_INCLUDE_DIR NAMES openssl/ssl.h PATHS /opt/local/include /usr/local/include NO_DEFAULT_PATH)
      FIND_LIBRARY (OPENSSL_LIBRARIES NAMES ${ssl_lib} PATHS /opt/local/lib /usr/local/lib NO_DEFAULT_PATH)
    ELSEIF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (OPENSSL_INCLUDE_DIR NAMES openssl/ssl.h PATHS "${EXTERNALS_DIRECTORY}/openssl/1.1/${osdir}/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (OPENSSL_LIBRARIES NAMES ${ssl_lib} PATHS "${EXTERNALS_DIRECTORY}/openssl/1.1/${osdir}/lib" NO_DEFAULT_PATH)
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
    STRING(REPLACE "libssl" "libcrypto" OPENSSL_EXTRA "${OPENSSL_LIBRARIES}")
    set (OPENSSL_LIBRARIES ${OPENSSL_LIBRARIES} ${OPENSSL_EXTRA} )
  ENDIF()

  MARK_AS_ADVANCED(OPENSSL_INCLUDE_DIR OPENSSL_LIBRARIES)
ENDIF()
