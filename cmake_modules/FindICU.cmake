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


# - Try to find the ICU unicode library
# Once done this will define
#
#  ICU_FOUND - system has the ICU library
#  ICU_INCLUDE_DIR - the ICU include directory
#  ICU_LIBRARIES - The libraries needed to use ICU

IF (NOT ICU_FOUND)
  IF (NOT ${EXTERNALS_DIRECTORY} STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux64_gcc4.1.1")
      ELSE()
        SET (osdir "linux32_gcc4.1.1")
      ENDIF()
    ELSEIF(WIN32)
      SET (osdir )
    ELSE()
      SET (osdir "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (ICU_INCLUDE_DIR NAMES unicode/uchar.h PATHS "${EXTERNALS_DIRECTORY}/icu/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (ICU_LIBRARIES NAMES icuuc PATHS "${EXTERNALS_DIRECTORY}/icu/lib/${osdir}" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (ICU_INCLUDE_DIR NAMES unicode/uchar.h)
    FIND_LIBRARY (ICU_LIBRARIES NAMES icuuc)
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(ICU DEFAULT_MSG
    ICU_LIBRARIES 
    ICU_INCLUDE_DIR
  )
  IF (ICU_FOUND)
    IF (UNIX)
      STRING(REPLACE "icuuc" "icui18n" ICU_EXTRA1 "${ICU_LIBRARIES}")
      STRING(REPLACE "icuuc" "icudata" ICU_EXTRA2 "${ICU_LIBRARIES}")
    ELSE()
      STRING(REPLACE "icuuc" "icuin" ICU_EXTRA1 "${ICU_LIBRARIES}")
    ENDIF()
    set (ICU_LIBRARIES ${ICU_LIBRARIES} ${ICU_EXTRA1} ${ICU_EXTRA2} )
  ENDIF()


  MARK_AS_ADVANCED(ICU_INCLUDE_DIR ICU_LIBRARIES)
ENDIF()
