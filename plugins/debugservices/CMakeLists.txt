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


# Component: debugservices 
#####################################################
# Description:
# ------------
#    Cmake Input File for debugservices
#####################################################

project( debugservices ) 

set (    SRCS 
         debugservices.cpp 
    )

include_directories ( 
         ./../../system/include 
    )

ADD_DEFINITIONS ( -DDEBUGSERVICES_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( debugservices SHARED ${SRCS} )
install ( TARGETS debugservices DESTINATION plugins )


if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_debugservices.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
