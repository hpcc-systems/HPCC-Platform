###############################################################################
#    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.
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

#
#########################################################
# Description:
# ------------
#     Perform tasks required by the platform and most, if
#     not all top level projects with platform dependencies,
#     before the first use of PROJECT().
#########################################################

if ("${HPCC_SOURCE_DIR}" STREQUAL "")
    set(HPCC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
endif()
include(${HPCC_SOURCE_DIR}/version.cmake)
include(${HPCC_SOURCE_DIR}/cmake_modules/options.cmake)
include(${HPCC_SOURCE_DIR}/cmake_modules/plugins.cmake)
include(${HPCC_SOURCE_DIR}/cmake_modules/vcpkg.cmake)
