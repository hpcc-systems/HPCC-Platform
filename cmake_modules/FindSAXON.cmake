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
#  SAXON_FOUND - system has the saxon executable.
#  SAXON_EXECUTABLE - the runtime path of the saxon executable

if (NOT SAXON_FOUND)
  IF (WIN32)
    SET (saxon "transform.exe")
  ELSE()
    SET (saxon "saxonb-xslt")
  ENDIF()

  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
    FIND_PROGRAM(SAXON_EXECUTABLE ${saxon} PATHS "${EXTERNALS_DIRECTORY}")
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PROGRAM(SAXON_EXECUTABLE ${saxon})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(SAXON DEFAULT_MSG
    SAXON_EXECUTABLE
  )
  MARK_AS_ADVANCED(SAXON_EXECUTABLE)
ENDIF()
