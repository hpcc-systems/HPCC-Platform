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


# - Try to find the fop executable
#
#  FOP_FOUND - system has the fop executable.
#  FOP_EXECUTABLE - the runtime path of the fop executable

if (NOT FOP_FOUND)
  IF (WIN32)
    SET (fop_n "fop.bat")
  ELSE()
    SET (fop_n "fop")
  ENDIF()

  IF (NOT ${EXTERNALS_DIRECTORY} STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux64_gcc4.1.1")
      ELSE()
        SET (osdir "linux32_gcc4.1.1")
      ENDIF()
    ELSEIF(WIN32)
      SET (osdir "lib")
    ELSE()
      SET (osdir "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PROGRAM(FOP_EXECUTABLE ${fop_n} PATHS "${EXTERNALS_DIRECTORY}/xsltproc/${osdir}")
    ENDIF() 
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PROGRAM(FOP_EXECUTABLE ${fop_n})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(FOP DEFAULT_MSG
    FOP_EXECUTABLE
  )
  MARK_AS_ADVANCED(FOP_EXECUTABLE)
ENDIF()

