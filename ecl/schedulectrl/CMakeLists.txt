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


# Component: schedulectrl 

#####################################################
# Description:
# ------------
#    Cmake Input File for schedulectrl
#####################################################


project( schedulectrl ) 

set (    SRCS 
         eventqueue.cpp 
         schedulectrl.cpp 
         scheduleread.cpp 
    )

include_directories ( 
         ./../../system/mp 
         ./../../system/include 
         ./../../dali/base 
         ./../../system/jlib 
         ./../schedulectrl 
         ./../../common/workunit
         ./../../system/security/shared
    )

ADD_DEFINITIONS( -D_USRDLL -DSCHEDULECTRL_EXPORTS )

HPCC_ADD_LIBRARY( schedulectrl SHARED ${SRCS} )
install ( TARGETS schedulectrl RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( schedulectrl
         jlib
         remote 
         dalibase 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
    )


