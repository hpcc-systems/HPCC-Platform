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


# - Try to find the xsltproc executable
#
#  XSLTPROC_FOUND - system has the xsltproc executable.
#  XSLTPROC_EXECUTABLE - the runtime path of the xsltproc executable

if (NOT XSLTPROC_FOUND)
  IF (WIN32)
    SET (xsltproc_n "xsltproc.exe")
  ELSE()
    SET (xsltproc_n "xsltproc")
  ENDIF()

  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
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
      FIND_PROGRAM(XSLTPROC_EXECUTABLE ${xsltproc_n} PATHS "${EXTERNALS_DIRECTORY}/xsltproc/${osdir}")
    ENDIF() 
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PROGRAM(XSLTPROC_EXECUTABLE ${xsltproc_n})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(XSLTPROC DEFAULT_MSG
    XSLTPROC_EXECUTABLE
  )
  MARK_AS_ADVANCED(XSLTPROC_EXECUTABLE)
ENDIF()

