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

  FIND_PROGRAM(XSLTPROC_EXECUTABLE NAMES ${xsltproc_n})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(XSLTPROC DEFAULT_MSG
    XSLTPROC_EXECUTABLE
  )
  MARK_AS_ADVANCED(XSLTPROC_EXECUTABLE)
ENDIF()

