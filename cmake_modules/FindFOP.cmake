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

  FIND_PROGRAM(FOP_EXECUTABLE NAMES ${fop_n})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(FOP DEFAULT_MSG
    FOP_EXECUTABLE
  )
  MARK_AS_ADVANCED(FOP_EXECUTABLE)
ENDIF()

