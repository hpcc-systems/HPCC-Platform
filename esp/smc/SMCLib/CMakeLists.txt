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


# Component: SMCLib 
#####################################################
# Description:
# ------------
#    Cmake Input File for SMCLib
#####################################################

project( SMCLib ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS
         ${HPCC_SOURCE_DIR}/dali/sasha/sacmd.cpp
         ${ESPSCM_GENERATED_DIR}/ws_topology_esp.cpp 
         LogicFileWrapper.cpp 
         TpWrapper.cpp 
         WUXMLInfo.cpp
         InfoCacheReader.cpp
    )

include_directories ( 
         ./../../../system/mp 
         ./../../platform 
         ./../../../system/jlib 
         ./../../bindings/SOAP/client 
         ./../../../common/environment 
         ./../../services 
         ./../../../dali/ft 
         ./../../bindings/SOAP/Platform 
         ./../../../system/xmllib 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared
         ./../../../system/include 
         ./../../../common/workunit 
         ./../../../common/remote 
         ./../../clients 
         ./../../../dali/base
         ./../../../dali/sasha
         ./../../clients/LoggingClient 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
         ./.. 
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_USRDLL -DSMCLIB_EXPORTS )

HPCC_ADD_LIBRARY( SMCLib SHARED ${SRCS} )
add_dependencies(SMCLib espscm)
install ( TARGETS SMCLib RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( SMCLib
         jlib
         remote 
         xmllib 
         esphttp 
         dalibase 
         environment 
         dalift 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
    )
