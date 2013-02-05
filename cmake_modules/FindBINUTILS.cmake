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


# - Try to find the BinUtils development library Once done this will define
#
#  BINUTILS_FOUND - system has the BinUtils library BINUTILS_INCLUDE_DIR - the BinUtils include directory BINUTILS_LIBRARIES - The libraries needed 
#  to use BinUtils
IF (NOT BINUTILS_FOUND)
  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
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
    if ( NOT APPLE )
      FIND_LIBRARY (IBERTY_LIBRARIES NAMES iberty_pic)
      FIND_LIBRARY (IBERTY_LIBRARIES NAMES iberty)
    endif ( NOT APPLE )
  endif()
  include(FindPackageHandleStandardArgs)
  if ( NOT APPLE )
    find_package_handle_standard_args(BinUtils DEFAULT_MSG
      BINUTILS_LIBRARIES
      BINUTILS_INCLUDE_DIR
      IBERTY_LIBRARIES
    )
    IF (BINUTILS_FOUND)
      set (BINUTILS_LIBRARIES ${BINUTILS_LIBRARIES} ${IBERTY_LIBRARIES} )
    ENDIF()
  else ( NOT APPLE )
    find_package_handle_standard_args(BinUtils DEFAULT_MSG
      BINUTILS_LIBRARIES
      BINUTILS_INCLUDE_DIR
    )
    IF (BINUTILS_FOUND)
      set (BINUTILS_LIBRARIES ${BINUTILS_LIBRARIES} )
    ENDIF()
  endif ( NOT APPLE )
  MARK_AS_ADVANCED(BINUTILS_INCLUDE_DIR BINUTILS_LIBRARIES)
ENDIF()
