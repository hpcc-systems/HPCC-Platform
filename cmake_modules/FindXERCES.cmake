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


# - Try to find the Xerces xml library
# Once done this will define
#
#  XERCES_FOUND - system has the Xerces library
#  XERCES_INCLUDE_DIR - the Xerces include directory
#  XERCES_LIBRARIES - The libraries needed to use Xerces

if (NOT XERCES_FOUND)
  IF (WIN32)
    SET (xerces_libs "xerces-c_3")
  ELSE()
    SET (xerces_libs "xerces-c")
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
        SET (osdir "xerces-c/win64")
      ELSE()
        SET (osdir "xerces-c/win32")
      ENDIF()
    ELSE()
      SET (osdir "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (XERCES_INCLUDE_DIR NAMES xercesc/util/XercesDefs.hpp PATHS "${EXTERNALS_DIRECTORY}/xalan/${osdir}/include"
              "${EXTERNALS_DIRECTORY}/xalan/xerces-c/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (XERCES_LIBRARIES NAMES ${xerces_libs} PATHS "${EXTERNALS_DIRECTORY}/xalan/${osdir}/lib" NO_DEFAULT_PATH)
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
    STRING(REPLACE "xerces-c_3" "xerces-c_3D" XERCES_DEBUG_LIBRARIES "${XERCES_LIBRARIES}")
    set (XERCES_LIBRARIES optimized ${XERCES_LIBRARIES} debug ${XERCES_DEBUG_LIBRARIES})
  ENDIF()
  MARK_AS_ADVANCED(XERCES_INCLUDE_DIR XERCES_LIBRARIES)
ENDIF()

