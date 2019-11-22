################################################################################
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems.
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

################################################################################
# Description:
# ------------
#     This directory is the last add_subdirectory and will be the last entry
#     in the cmake_install.cmake file. This behavior is necessary for running
#     setup scripts for a developers build and installation.
################################################################################

if(NOT "${DESTDIR}" STREQUAL "")
  message(STATUS "Development build and installation")
  message(STATUS "    DESTDIR:       ${DESTDIR}")
  message(STATUS "    RUNTIME_USER:  ${RUNTIME_USER}")
  message(STATUS "    RUNTIME_GROUP: ${RUNTIME_GROUP}")
  include(pki_setup.cmake)
endif()
