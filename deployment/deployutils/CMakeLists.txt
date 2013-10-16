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


# Component: deployutils
#####################################################
# Description:
# ------------
#    Cmake Input File for deployutils
#####################################################

project( deployutils ) 

set (    SRCS 
         buildset.cpp 
         computerpicker.cpp 
         configenvhelper.cpp
         wizardInputs.cpp 
         deployutils.cpp 
         confighelper.cpp
    )

include_directories ( 
         ./../../system/include 
         ./../../system/jlib 
         ./../../system/mp 
         ./../../system/xmllib       
         ./../../common/environment
         ./../../dali/base 
         ./../../esp/esplib
         ../configgen
         ../deploy 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS ( -D_USRDLL -DDEPLOYUTILS_EXPORTS )

HPCC_ADD_LIBRARY( deployutils SHARED ${SRCS} )
install ( TARGETS deployutils RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( deployutils
         jlib 
         environment 
         xmllib
         deploy
    )


