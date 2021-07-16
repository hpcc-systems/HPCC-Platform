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
HPCC_ADD_SUBDIRECTORY (ccd)
IF (NOT WIN32)
  HPCC_ADD_SUBDIRECTORY (ccdcache "PLATFORM")
ENDIF()
HPCC_ADD_SUBDIRECTORY (roxie "PLATFORM")
HPCC_ADD_SUBDIRECTORY (topo "PLATFORM")
HPCC_ADD_SUBDIRECTORY (roxiemem)
HPCC_ADD_SUBDIRECTORY (roxiepipe "PLATFORM")
HPCC_ADD_SUBDIRECTORY (udplib)
