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

# Component: dfuwutest 
#####################################################
# Description:
# ------------
#    Cmake Input File for dfuwutest
#####################################################

project( dfuwutest ) 

set (    SRCS 
         ../dfu/dfuutil.cpp 
         dfuwutest.cpp 
    )

include_directories ( 
         ./../dfu 
         ./../base 
         ./../../fs/dafsclient 
         ./../../system/mp 
         . 
         ./../../system/include 
         ./../../system/jlib 
         ./../../common/workunit 
         ../../common/environment 
    )

HPCC_ADD_EXECUTABLE ( dfuwutest ${SRCS} )
set_target_properties (dfuwutest PROPERTIES COMPILE_FLAGS -D_CONSOLE)
target_link_libraries ( dfuwutest
         workunit
         jlib
         mp 
         hrpc 
         dafsclient 
         dalibase 
         dfuwu 
    )

if (NOT CONTAINERIZED)
    target_link_libraries ( dfuwutest environment )
endif()
