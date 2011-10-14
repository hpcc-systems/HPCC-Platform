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

# File  : commonSetup.cmake
#
#########################################################
# Description:
# ------------
# sets up various cmake options. 
#########################################################

IF ("${COMMONSETUP_DONE}" STREQUAL "")
  SET (COMMONSETUP_DONE 1)

  MACRO (MACRO_ENSURE_OUT_OF_SOURCE_BUILD _errorMessage)
    STRING(COMPARE EQUAL "${CMAKE_SOURCE_DIR}" "${CMAKE_BINARY_DIR}" insource)
    IF (insource)
      MESSAGE(FATAL_ERROR "${_errorMessage}")
    ENDIF(insource)
  ENDMACRO (MACRO_ENSURE_OUT_OF_SOURCE_BUILD)

  macro_ensure_out_of_source_build("The LexisNexis Hpcc requires an out of source build.
    Please remove the directory ${CMAKE_BINARY_DIR}/CMakeFiles 
    and the file ${CMAKE_BINARY_DIR}/CMakeCache.txt,
    then create a separate build directory and run 'cmake path_to_source [options]' there.")

  cmake_policy ( SET CMP0011 NEW )

  # For options that we don't presently support setting to off, we have a SET() below rather than an option...

  option(USE_CPPUNIT "Enable unit tests (requires cppunit)" OFF)
  set (USE_OPENLDAP 1)     # option(USE_OPENLDAP "Enable OpenLDAP support (requires OpenLDAP)" ON)
  set (USE_ICU 1)          # option(USE_ICU "Enable unicode support (requires ICU)" ON)
  set (USE_XALAN 1)        # option(USE_XALAN "Configure use of xalan" ON)
  set (USE_XERCES 1)       # option(USE_XERCES "Configure use of xerces" ON)
  option(USE_BOOST_REGEX "Configure use of boost regex" ON)
  option(Boost_USE_STATIC_LIBS "Use boost_regex static library for RPM BUILD" OFF)
  set (USE_OPENSSL 1)      # option(USE_OPENSSL "Configure use of OpenSSL" ON)
  option(USE_ZLIB "Configure use of zlib" ON)
  option(USE_NATIVE_LIBRARIES "Search standard OS locations for thirdparty libraries" ON)

  option(PORTALURL "Set url to hpccsystems portal download page")

  if ( NOT PORTALURL )
    set( PORTALURL "http://hpccsystems.com/download" )
  endif()

  set(CMAKE_MODULE_PATH "${HPCC_SOURCE_DIR}/cmake_modules")

  ##########################################################

  # common compiler/linker flags

  if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
    set ( CMAKE_BUILD_TYPE "Release" )
  elseif (NOT "${CMAKE_BUILD_TYPE}" MATCHES "Debug|Release")
    message (FATAL_ERROR "Unknown build type $ENV{CMAKE_BUILD_TYPE}")
  endif ()
  message ("-- Making ${CMAKE_BUILD_TYPE} system")

  if (CMAKE_SIZEOF_VOID_P EQUAL 8)
    set ( ARCH64BIT 1 )
  else ()
    set ( ARCH64BIT 0 )
  endif ()
  message ("-- 64bit architecture is ${ARCH64BIT}")

  set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_DEBUG -DDEBUG")

  set (CMAKE_THREAD_PREFER_PTHREAD 1)
  find_package(Threads)
  IF (NOT THREADS_FOUND)
    message(FATAL_ERROR "No threading support found")
  ENDIF()

  if (WIN32)
    # On windows, the vcproj generator generates both windows and debug build capabilities, and the release mode is appended to the directory later
    # This output location matches what our existing windows release scripts expect - might make sense to move out of tree sometime though
    set ( EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/bin" )
    set ( LIBRARY_OUTPUT_PATH "${CMAKE_BINARY_DIR}/bin" )
    # Workaround CMake's odd decision to default windows stack size to 10000000
    STRING(REGEX REPLACE "/STACK:[0-9]+" "" CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}")
    SET (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /LARGEADDRESSAWARE")

    if (${ARCH64BIT} EQUAL 0)
      add_definitions(/Zc:wchar_t-)
    endif ()      
      
    if ("${CMAKE_BUILD_TYPE}" MATCHES "Debug")
      add_definitions(/ZI)
    endif ()
  else ()
    if (NOT CMAKE_USE_PTHREADS_INIT)
      message (FATAL_ERROR "pthreads support not detected")
    endif ()
    set ( EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/bin" )
    set ( LIBRARY_OUTPUT_PATH "${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/libs" )
    if (${CMAKE_COMPILER_IS_GNUCXX})
      SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -frtti -fPIC -fmessage-length=0 -Wformat -Wformat-security -Wformat-nonliteral -pthread")
      SET (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,-export-dynamic")
      SET (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -fno-default-inline -fno-inline-functions")
    endif ()
    # All of these are defined in platform.h too, but need to be defned before any system header is included
    ADD_DEFINITIONS (-D_LARGEFILE_SOURCE=1 -D_LARGEFILE64_SOURCE=1 -D_FILE_OFFSET_BITS=64 -D__USE_LARGEFILE64=1 -D__USE_FILE_OFFSET64=1)
  endif ()

  macro(HPCC_ADD_LIBRARY target)
    add_library(${target} ${ARGN})
  endmacro(HPCC_ADD_LIBRARY target)

  set ( SCM_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated )
  include_directories (${SCM_GENERATED_DIR})

  ##################################################################

  # Build tag generation

  set(projname ${HPCC_PROJECT})
  set(majorver ${HPCC_MAJOR})
  set(minorver ${HPCC_MINOR})
  set(point ${HPCC_POINT})
  if ( "${HPCC_MATURITY}" STREQUAL "release" )
    set(stagever "${HPCC_SEQUENCE}")
  else()
    set(stagever "${HPCC_SEQUENCE}${HPCC_MATURITY}")
  endif()
  set(version ${majorver}.${minorver}.${point})

  IF ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
    set( stagever "${stagever}-Debug" )
  ENDIF ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")

  ###########################################################################

  if (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
    message ("-- Using externals directory at ${EXTERNALS_DIRECTORY}")
  endif()

  IF ("${EXTERNALS_DIRECTORY}" STREQUAL "")
    SET(bisoncmd "bison")
    SET(flexcmd "flex")
  ELSE()
    IF (WIN32)
      SET(bisoncmdprefix "call")
      SET(flexcmdprefix "call")
      SET(bisoncmd "${EXTERNALS_DIRECTORY}\\bison\\bison.bat")
      SET(flexcmd "${EXTERNALS_DIRECTORY}\\bison\\flex.bat")
    ELSE ()
      SET(bisoncmd "${EXTERNALS_DIRECTORY}/bison/bison")
      SET(flexcmd "${EXTERNALS_DIRECTORY}/bison/flex")
    ENDIF()
  ENDIF()

  IF ("${BISON_VERSION}" STREQUAL "")
    IF (WIN32)
      # cmake bug workaround - it converts path separators fine in add_custom_command but not here
      STRING(REPLACE "/" "\\" BISON_exename "${bisoncmd}")  
    ELSE()
      SET(BISON_exename "${bisoncmd}")  
    ENDIF()
    EXECUTE_PROCESS(COMMAND ${BISON_exename} --version
      OUTPUT_VARIABLE BISON_version_output
      ERROR_VARIABLE BISON_version_error
      RESULT_VARIABLE BISON_version_result
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    STRING(REGEX REPLACE "^[^0-9]*([0-9.]+).*" "\\1" BISON_VERSION "${BISON_version_output}")
  ENDIF()

  IF ("${FLEX_VERSION}" STREQUAL "")
    IF (WIN32)
      # cmake bug workaround - it converts path separators fine in add_custom_command but not here
      STRING(REPLACE "/" "\\" FLEX_exename "${flexcmd}")  
    ELSE()
      SET(FLEX_exename "${flexcmd}")  
    ENDIF()
    EXECUTE_PROCESS(COMMAND ${FLEX_exename} --version
      OUTPUT_VARIABLE FLEX_version_output
      ERROR_VARIABLE FLEX_version_error
      RESULT_VARIABLE FLEX_version_result
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    STRING(REGEX REPLACE "^[^0-9]*([0-9.]+).*" "\\1" FLEX_VERSION "${FLEX_version_output}")
  ENDIF()

  IF ("${BISON_VERSION}" VERSION_LESS "2.4.1")
    MESSAGE(FATAL_ERROR "You need bison version 2.4.1 or later to build this project (version ${BISON_VERSION} detected)")
  ENDIF()

  IF ("${FLEX_VERSION}" VERSION_LESS "2.5.35")
    MESSAGE(FATAL_ERROR "You need flex version 2.5.35 or later to build this project (version ${FLEX_VERSION} detected)")
  ENDIF()

  IF (CMAKE_COMPILER_IS_GNUCXX)
    EXECUTE_PROCESS(COMMAND ${CMAKE_CXX_COMPILER} -dumpversion OUTPUT_VARIABLE GNUCXX_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)
    IF ("${GNUCXX_VERSION}" VERSION_LESS "4.1.1")
      MESSAGE(FATAL_ERROR "You need Gnu c++ version 4.1.1 or later to build this project (version ${GNUCXX_VERSION} detected)")
    ENDIF()
  ENDIF()

  ###########################################################################

  # External library setup - some can be optionally selected based on USE_xxx flags, some are required

  IF (NOT WIN32)
    find_package(BINUTILS)
    IF (NOT BINUTILS_FOUND)
      message(FATAL_ERROR "BINUTILS package not found")
    ENDIF()
  ENDIF()

  IF (USE_OPENLDAP)
    find_package(OPENLDAP)
    IF (OPENLDAP_FOUND)
      add_definitions (-D_USE_OPENLDAP)
    ELSE()
      message(FATAL_ERROR "OPENLDAP requested but package not found")
    ENDIF()
  ENDIF(USE_OPENLDAP)

  IF (USE_CPPUNIT)
    find_package(CPPUNIT)
    IF (CPPUNIT_FOUND)
      add_definitions (-D_USE_CPPUNIT)
      include_directories(${CPPUNIT_INCLUDE_DIR})
    ELSE()
      message(FATAL_ERROR "CPPUNIT requested but package not found")
    ENDIF()
  ENDIF(USE_CPPUNIT)

  IF (USE_ICU)
    find_package(ICU)
    IF (ICU_FOUND)
      add_definitions (-D_USE_ICU)
      include_directories(${ICU_INCLUDE_DIR})
    ELSE()
      message(FATAL_ERROR "ICU requested but package not found")
    ENDIF()
  ENDIF(USE_ICU)

  if(USE_XALAN)
    find_package(XALAN)
    if (XALAN_FOUND)
      add_definitions (-D_USE_XALAN)
    else()
      message(FATAL_ERROR "XALAN requested but package not found")
    endif()
  endif(USE_XALAN)

  if(USE_XERCES)
    find_package(XERCES)
    if (XERCES_FOUND)
      add_definitions (-D_USE_XERCES)
    else()
      message(FATAL_ERROR "XERCES requested but package not found")
    endif()
  endif(USE_XERCES)

  if(USE_ZLIB)
    find_package(ZLIB)
    if (ZLIB_FOUND)
      add_definitions (-D_USE_ZLIB)
    else()
      message(FATAL_ERROR "ZLIB requested but package not found")
    endif()
  endif(USE_ZLIB)

  if(USE_BOOST_REGEX)
    find_package(BOOST_REGEX)
    if (BOOST_REGEX_FOUND)
      add_definitions (-D_USE_BOOST_REGEX)
    else()
      message(FATAL_ERROR "BOOST_REGEX requested but package not found")
    endif()
  endif(USE_BOOST_REGEX)

  if(USE_OPENSSL)
    find_package(OPENSSL)
    if (OPENSSL_FOUND)
      add_definitions (-D_USE_OPENSSL)
      include_directories(${OPENSSL_INCLUDE_DIR})
      link_directories(${OPENSSL_LIBRARY_DIR})
    else()
      message(FATAL_ERROR "OPENSSL requested but package not found")
    endif()
  endif(USE_OPENSSL)

  if(USE_MYSQL)
    find_package(MYSQL)
    if (MYSQL_FOUND)
      add_definitions (-D_USE_MYSQL)
    else()
      message(FATAL_ERROR "MYSQL requested but package not found")
    endif()
  else()
    add_definitions (-D_NO_MYSQL)
  endif(USE_MYSQL)

  ###########################################################################
endif ("${COMMONSETUP_DONE}" STREQUAL "")
