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


# - Try to find the BinUtils development library Once done this will define
#
#  BINUTILS_FOUND - system has the BinUtils library BINUTILS_INCLUDE_DIR - the BinUtils include directory BINUTILS_LIBRARIES - The libraries needed 
#  to use BinUtils
IF (NOT BINUTILS_FOUND)
  IF (NOT ${EXTERNALS_DIRECTORY} STREQUAL "")
    IF (${ARCH64BIT} EQUAL 1)
      SET (osdir "linux64_gcc4.1.1")
    ELSE()
      SET (osdir "linux32_gcc4.1.1")
    ENDIF()
    FIND_PATH (BINUTILS_INCLUDE_DIR NAMES bfd.h PATHS "${EXTERNALS_DIRECTORY}/binutils/${osdir}/include" NO_DEFAULT_PATH)
    FIND_LIBRARY (BINUTILS_LIBRARIES NAMES bfd PATHS "${EXTERNALS_DIRECTORY}/binutils/${osdir}/lib" NO_DEFAULT_PATH)
    # We also need libiberty - but that is complicated by the fact that some distros ship libibery_pic that you have to use in .so's, while on others libiberty.a is PIC-friendly
    FIND_LIBRARY (IBERTY_LIBRARIES NAMES iberty_pic PATHS "${EXTERNALS_DIRECTORY}/binutils/${osdir}/lib" NO_DEFAULT_PATH)
    FIND_LIBRARY (IBERTY_LIBRARIES NAMES iberty PATHS "${EXTERNALS_DIRECTORY}/binutils/${osdir}/lib" NO_DEFAULT_PATH)
  ENDIF()
  # if we didn't find in externals, look in system include path
  if (USE_NATIVE_LIBRARIES)
    FIND_PATH (BINUTILS_INCLUDE_DIR NAMES bfd.h)
    FIND_LIBRARY (BINUTILS_LIBRARIES NAMES bfd)
    FIND_LIBRARY (IBERTY_LIBRARIES NAMES iberty_pic)
    FIND_LIBRARY (IBERTY_LIBRARIES NAMES iberty)
  endif()
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(BinUtils DEFAULT_MSG
    BINUTILS_LIBRARIES
    BINUTILS_INCLUDE_DIR
    IBERTY_LIBRARIES
  )
  IF (BINUTILS_FOUND)
    set (BINUTILS_LIBRARIES ${BINUTILS_LIBRARIES} ${IBERTY_LIBRARIES} )
  ENDIF()
  MARK_AS_ADVANCED(BINUTILS_INCLUDE_DIR BINUTILS_LIBRARIES)
ENDIF()
