################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
#        HoleDeploymentEngine.cpp 
         RoxieDeploymentEngine.cpp 
         thorconfiggenengine.cpp 
         ThorDeploymentEngine.cpp 
         XslFunctions.cpp 
    )

include_directories ( 
         ./../../system/include 
         ./../../system/xmllib 
         ./../../system/jlib 
         ./../../system/mp 
         ./../../common/environment
         ./../../dali/base 
         ./../../system/xalan/xml-xerces/c/src 
         ./../../system/xalan/xml-xalan/c/src 
         ./../../system/security/securesocket 
    )

ADD_DEFINITIONS ( -D_USRDLL -DDEPLOY_EXPORTS )

HPCC_ADD_LIBRARY( deploy SHARED ${SRCS} )
install ( TARGETS deploy DESTINATION ${OSSDIR}/lib )
target_link_libraries ( deploy 
         jlib 
         environment 
         xmllib 
         securesocket
    )


