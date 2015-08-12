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


# - Try to find the CppUnit unit testing library
# Once done this will define
#
#  CPPUNIT_FOUND - system has the CppUnit library
#  CPPUNIT_INCLUDE_DIR - the CppUnit include directory
#  CPPUNIT_LIBRARIES - The libraries needed to use CppUnit

IF (NOT CPPUNIT_FOUND)
  IF (WIN32)
    SET (cppunit_dll "cppunit_dll")
  ELSE()
    SET (cppunit_dll "cppunit")
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
    
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (CPPUNIT_INCLUDE_DIR NAMES cppunit/TestFixture.h PATHS "${EXTERNALS_DIRECTORY}/cppunit/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (CPPUNIT_LIBRARIES NAMES ${cppunit_dll} PATHS "${EXTERNALS_DIRECTORY}/cppunit/lib/${osdir}" NO_DEFAULT_PATH)
    ENDIF() 
    
  ENDIF()

  # if we didn't find in externals, look in system include path
  if (USE_NATIVE_LIBRARIES)
    FIND_PATH (CPPUNIT_INCLUDE_DIR NAMES cppunit/TestFixture.h)
    FIND_LIBRARY (CPPUNIT_LIBRARIES NAMES ${cppunit_dll})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(CppUnit DEFAULT_MSG
    CPPUNIT_LIBRARIES 
    CPPUNIT_INCLUDE_DIR
  )

  IF (CPPUNIT_FOUND AND WIN32)
    STRING(REPLACE "cppunit_dll" "cppunitd_dll" CPPUNIT_DEBUG_LIBRARIES "${CPPUNIT_LIBRARIES}")
    set (CPPUNIT_LIBRARIES optimized ${CPPUNIT_LIBRARIES} debug ${CPPUNIT_DEBUG_LIBRARIES})
  ENDIF()
  MARK_AS_ADVANCED(CPPUNIT_INCLUDE_DIR CPPUNIT_LIBRARIES)
ENDIF()
