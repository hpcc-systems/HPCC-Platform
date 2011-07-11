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
