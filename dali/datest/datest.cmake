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

# Component: datest 
#####################################################
# Description:
# ------------
#    Cmake Input File for datest
#####################################################

project( datest ) 

set (    SRCS 
         datest.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/dali/server 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/esp/clients/wsdfuaccess
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
    )

HPCC_ADD_EXECUTABLE ( datest ${SRCS} )
set_target_properties (datest PROPERTIES COMPILE_FLAGS -D_CONSOLE)
target_link_libraries ( datest 
         jlib
         mp 
         hrpc 
         remote
         eclrtl
         wsdfuaccess 
         dalibase 
         ${CPPUNIT_LIBRARIES}
    )


