################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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


# Component: deploy 

#####################################################
# Description:
# ------------
#    Cmake Input File for deploy
#####################################################


project( deploy ) 

set (    SRCS 
         configgenengine.cpp 
         dalideploymentengine.cpp 
         deploy.cpp 
         DeployLog.cpp 
         DeploymentEngine.cpp 
         DeployTask.cpp 
         EspDeploymentEngine.cpp 
         espconfiggenengine.cpp 
         RoxieDeploymentEngine.cpp 
         thorconfiggenengine.cpp 
         ThorDeploymentEngine.cpp 
         XslFunctions.cpp
         ../deployutils/confighelper.cpp
    )

include_directories ( 
         ./../../system/include 
         ./../../system/xmllib 
         ./../../system/jlib 
         ./../../system/mp 
         ./../../common/environment
         ./../../dali/base 
         ./../../system/security/securesocket 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ./../deployutils
    )

ADD_DEFINITIONS ( -D_USRDLL -DDEPLOY_EXPORTS )

HPCC_ADD_LIBRARY( deploy SHARED ${SRCS} )
install ( TARGETS deploy RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( deploy 
         jlib 
         environment 
         xmllib 
         securesocket
    )

if ( PLATFORM )
    install ( PROGRAMS configesp.install DESTINATION etc/init.d/install COMPONENT Runtime )
    install ( PROGRAMS configesp.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime )
endif()
