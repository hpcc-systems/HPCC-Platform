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
HPCC_ADD_SUBDIRECTORY (deftype)
HPCC_ADD_SUBDIRECTORY (dllserver)
if (NOT CONTAINERIZED)
    HPCC_ADD_SUBDIRECTORY (environment)
ENDIF()
if ("${BUILD_LEVEL}" STREQUAL "INTERNAL")
    HPCC_ADD_SUBDIRECTORY (fileview2)
else ()
    HPCC_ADD_SUBDIRECTORY (fileview2 "PLATFORM")
endif ()
HPCC_ADD_SUBDIRECTORY (monitoring "PLATFORM")
HPCC_ADD_SUBDIRECTORY (remote)
HPCC_ADD_SUBDIRECTORY (roxiecommlib)
HPCC_ADD_SUBDIRECTORY (thorhelper)
HPCC_ADD_SUBDIRECTORY (pkgfiles)
HPCC_ADD_SUBDIRECTORY (workunit)
HPCC_ADD_SUBDIRECTORY (wuanalysis)
HPCC_ADD_SUBDIRECTORY (wuwebview "PLATFORM")
HPCC_ADD_SUBDIRECTORY (eventconsumption)
