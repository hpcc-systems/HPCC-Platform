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


# Component: configgen 
#####################################################
# Description:
# ------------
#    Cmake Input File for configgen
#####################################################

project( configgen ) 

set ( SRCS main.cpp )

include_directories ( 
         ./../../system/include 
         ./../../system/jlib 
         ./../../system/mp 
         ./../../common/environment 
         ./../deploy/ 
         ./../../dali/base 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ./../../system/security/shared
    )

ADD_DEFINITIONS ( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( configgen ${SRCS} )
install ( TARGETS configgen DESTINATION sbin CALC_DEPS)
target_link_libraries ( configgen 
        deploy 
        jlib 
        environment 
        )

if (CONTAINERIZED)
    message(FATAL_ERROR "Configgen is not included in container builds")
endif()

IF (USE_OPENSSL)
    target_link_libraries ( configgen
    	securesocket
    )
ENDIF()
