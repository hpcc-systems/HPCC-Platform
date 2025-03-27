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
project (tools)
HPCC_ADD_SUBDIRECTORY (esdlcomp)
HPCC_ADD_SUBDIRECTORY (hidl)
HPCC_ADD_SUBDIRECTORY (esdlcmd-xml)
HPCC_ADD_SUBDIRECTORY (esdlcmd)
HPCC_ADD_SUBDIRECTORY (esp-api)
SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STRICT_CXX_FLAGS}")
HPCC_ADD_SUBDIRECTORY (backupnode "PLATFORM")
HPCC_ADD_SUBDIRECTORY (swapnode "PLATFORM")
IF (USE_OPENLDAP)
HPCC_ADD_SUBDIRECTORY (initldap "PLATFORM")
ENDIF(USE_OPENLDAP)
HPCC_ADD_SUBDIRECTORY (combine "PLATFORM")
HPCC_ADD_SUBDIRECTORY (dumpkey "PLATFORM")
HPCC_ADD_SUBDIRECTORY (keydiff "PLATFORM")
HPCC_ADD_SUBDIRECTORY (pstart "PLATFORM")
HPCC_ADD_SUBDIRECTORY (pskill "PLATFORM")
HPCC_ADD_SUBDIRECTORY (testsocket)
HPCC_ADD_SUBDIRECTORY (vkey "PLATFORM")
HPCC_ADD_SUBDIRECTORY (wuget "CLIENTTOOLS")
HPCC_ADD_SUBDIRECTORY (wutool "PLATFORM")
HPCC_ADD_SUBDIRECTORY (copyexp "PLATFORM")
HPCC_ADD_SUBDIRECTORY (genht "PLATFORM")
HPCC_ADD_SUBDIRECTORY (evtool "PLATFORM")

# start-stop-daemon is linux and apple only.
if ( CMAKE_SYSTEM MATCHES Linux OR APPLE)
    message ("-- Building start-stop-daemon")
    HPCC_ADD_SUBDIRECTORY (start-stop-daemon)
endif()
if ( PLATFORM )
    INSTALL ( PROGRAMS "doperf/doperf" DESTINATION bin COMPONENT Runtime )
    INSTALL ( PROGRAMS "doperf/stackcollapse-elfutils.pl" DESTINATION bin COMPONENT Runtime )
    INSTALL ( PROGRAMS "doperf/flamegraph.pl" DESTINATION bin COMPONENT Runtime )
endif()