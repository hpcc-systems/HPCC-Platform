################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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


# - Try to find the Boost regex library
# Once done this will define
#
#  BOOST_REGEX_FOUND - system has the Boost regex library
#  BOOST_REGEX_INCLUDE_DIR - the Boost regex include directory
#  BOOST_REGEX_LIBRARIES - The libraries needed to use Boost regex
#  BOOST_REGEX_LIBRARY_DIR - The directory containing libraries needed to use Boost regex

IF (NOT BOOST_REGEX_FOUND)
  IF (WIN32 AND USE_NATIVE_LIBRARIES)
    set ( Boost_USE_STATIC_LIBS ON )
    set ( Boost_USE_MULTITHREADED ON )
    set ( Boost_USE_STATIC_RUNTIME OFF )
    find_package( Boost COMPONENTS regex )
    IF (Boost_FOUND)
      set(BOOST_REGEX_FOUND TRUE)
      set(BOOST_REGEX_LIBRARIES ${Boost_LIBRARIES})
      set(BOOST_REGEX_INCLUDE_DIR ${Boost_INCLUDE_DIRS})
      set(BOOST_REGEX_LIBRARY_DIR ${Boost_LIBRARY_DIRS})
      link_directories(${BOOST_REGEX_LIBRARY_DIR})
    ENDIF (Boost_FOUND)
  ELSE (WIN32 AND USE_NATIVE_LIBRARIES)
    IF (UNIX)
      SET (boost_regex_lib "boost_regex-mt")
      IF(Boost_USE_STATIC_LIBS)
        SET (boost_regex_lib "libboost_regex-mt.a")
      ENDIF()
    ELSEIF(WIN32)
      SET (boost_regex_lib "libboost_regex-vc90-mt.lib") # note - this may not be the lib we need, but should be in same place as it...
    ENDIF()
    IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
      IF (UNIX)
        IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux-x86_64-gcc4")
        ELSE()
        SET (osdir "linux-i686-gcc4")
        ENDIF()
      ELSEIF(WIN32)
        SET (osdir "windows-i386-vc90")
      ELSE()
        SET (osdir "unknown")
      ENDIF()
      IF (NOT ("${osdir}" STREQUAL "unknown"))
        FIND_PATH (BOOST_REGEX_INCLUDE_DIR NAMES boost/regex.h PATHS "${EXTERNALS_DIRECTORY}/boost/include" NO_DEFAULT_PATH)
        FIND_LIBRARY (BOOST_REGEX_LIBRARIES NAMES ${boost_regex_lib} PATHS "${EXTERNALS_DIRECTORY}/boost/${osdir}/lib" NO_DEFAULT_PATH)
      ENDIF() 
    ENDIF()

    # if we didn't find in externals, look in system include path
    if (USE_NATIVE_LIBRARIES)
       set(Boost_ADDITIONAL_VERSIONS "1.41" "1.41.0" "1.44.0")
       set(Boost_USE_MULTITHREADED ON)
       set (Boost_DETAILED_FAILURE_MSG ON)
    # On centos we may have both boost-devel and boost141-devel installed. We need to look for the former first.
       set(BOOST_ROOT "/usr/include/boost141")
       set(BOOST_INCLUDEDIR "/usr/include/boost141")
       if (${ARCH64BIT} EQUAL 1)
         set(BOOST_LIBRARYDIR "/usr/lib64/boost141")
       else()
         set(BOOST_LIBRARYDIR "/usr/lib/boost141")
       endif()
       find_package( Boost 1.34.0 COMPONENTS regex )
       if(Boost_FOUND)
         set(BOOST_REGEX_LIBRARIES ${Boost_LIBRARIES})
         set(BOOST_REGEX_INCLUDE_DIR ${Boost_INCLUDE_DIRS})
       endif()
    endif()

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(BOOST_REGEX DEFAULT_MSG
      BOOST_REGEX_LIBRARIES 
      BOOST_REGEX_INCLUDE_DIR
    )

    IF (BOOST_REGEX_FOUND)
      IF (WIN32)
        STRING(REPLACE "/${boost_regex_lib}" "" BOOST_REGEX_LIBRARY_DIR "${BOOST_REGEX_LIBRARIES}")
        link_directories(${BOOST_REGEX_LIBRARY_DIR})
        set (BOOST_REGEX_LIBRARIES "")  # the actual library to use is controlled by boost header files
      ENDIF()
    ENDIF()
    MARK_AS_ADVANCED(BOOST_REGEX_INCLUDE_DIR BOOST_REGEX_LIBRARIES)
  ENDIF (WIN32 AND USE_NATIVE_LIBRARIES)
ENDIF()
