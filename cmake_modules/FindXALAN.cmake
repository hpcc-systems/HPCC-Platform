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


# - Try to find the Xalan xml library
# Once done this will define
#
#  XALAN_FOUND - system has the Xalan library
#  XALAN_INCLUDE_DIR - the Xalan include directory
#  XALAN_LIBRARIES - The libraries needed to use Xalan

IF (NOT XALAN_FOUND)
  IF (WIN32)
    SET (xalan_libs "Xalan-C_1")
  ELSE()
    SET (xalan_libs "xalan-c")
  ENDIF()

  IF (NOT ${EXTERNALS_DIRECTORY} STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux64_gcc4.1.1")
      ELSE()
        SET (osdir "linux32_gcc4.1.1")
      ENDIF()
    ELSEIF(WIN32)
      SET (osdir "xalan-c/lib")
    ELSE()
      SET (osdir "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (XALAN_INCLUDE_DIR NAMES xalanc/XPath/XObjectFactory.hpp PATHS "${EXTERNALS_DIRECTORY}/xalan/xalan-c/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (XALAN_LIBRARIES NAMES ${xalan_libs} PATHS "${EXTERNALS_DIRECTORY}/xalan/${osdir}" NO_DEFAULT_PATH)
    ENDIF() 
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (XALAN_INCLUDE_DIR NAMES xalanc/XPath/XObjectFactory.hpp )
    FIND_LIBRARY (XALAN_LIBRARIES NAMES ${xalan_libs})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(Xalan DEFAULT_MSG
    XALAN_LIBRARIES 
    XALAN_INCLUDE_DIR
  )

  IF (XALAN_FOUND)
    IF (WIN32)
      STRING(REPLACE "Xalan-C_1" "Xalan-C_1D" XALAN_DEBUG_LIBRARIES "${XALAN_LIBRARIES}")
      set (XALAN_LIBRARIES optimized ${XALAN_LIBRARIES} debug ${XALAN_DEBUG_LIBRARIES})
    ELSE()
      STRING(REPLACE "xalan-c" "xalanMsg" XALAN_EXTRA_LIBRARIES "${XALAN_LIBRARIES}")
      IF (EXISTS ${XALAN_EXTRA_LIBRARIES})
        set (XALAN_LIBRARIES ${XALAN_LIBRARIES} ${XALAN_EXTRA_LIBRARIES})
      ENDIF()
    ENDIF()
  ENDIF()
  MARK_AS_ADVANCED(XALAN_INCLUDE_DIR XALAN_LIBRARIES)
ENDIF()
