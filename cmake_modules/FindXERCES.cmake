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


# - Try to find the Xerces xml library
# Once done this will define
#
#  XERCES_FOUND - system has the Xerces library
#  XERCES_INCLUDE_DIR - the Xerces include directory
#  XERCES_LIBRARIES - The libraries needed to use Xerces

if (NOT XERCES_FOUND)
  IF (WIN32)
    SET (xerces_libs "xerces-c_2")
  ELSE()
    SET (xerces_libs "xerces-c")
  ENDIF()

  IF (NOT ${EXTERNALS_DIRECTORY} STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux64_gcc4.1.1")
      ELSE()
        SET (osdir "linux32_gcc4.1.1")
      ENDIF()
    ELSEIF(WIN32)
      SET (osdir "xerces-c/lib")
    ELSE()
      SET (osdir "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (XERCES_INCLUDE_DIR NAMES xercesc/util/XercesDefs.hpp PATHS "${EXTERNALS_DIRECTORY}/xalan/xerces-c/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (XERCES_LIBRARIES NAMES ${xerces_libs} PATHS "${EXTERNALS_DIRECTORY}/xalan/${osdir}" NO_DEFAULT_PATH)
    ENDIF() 
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (XERCES_INCLUDE_DIR NAMES xercesc/util/XercesDefs.hpp )
    FIND_LIBRARY (XERCES_LIBRARIES NAMES ${xerces_libs})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(Xerces DEFAULT_MSG
    XERCES_LIBRARIES 
    XERCES_INCLUDE_DIR
  )

  IF (XERCES_FOUND AND WIN32)
    STRING(REPLACE "xerces-c_2" "xerces-c_2D" XERCES_DEBUG_LIBRARIES "${XERCES_LIBRARIES}")
    set (XERCES_LIBRARIES optimized ${XERCES_LIBRARIES} debug ${XERCES_DEBUG_LIBRARIES})
  ENDIF()
  MARK_AS_ADVANCED(XERCES_INCLUDE_DIR XERCES_LIBRARIES)
ENDIF()

