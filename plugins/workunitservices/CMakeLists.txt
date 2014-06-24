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


# Component: workunitservices 
#####################################################
# Description:
# ------------
#    Cmake Input File for workunitservices
#####################################################

project( workunitservices ) 

set (    SRCS 
         workunitservices.cpp 
         ../../dali/sasha/sacmd.cpp 
    )

include_directories ( 
         ./../../common/environment 
         ./../../common/remote 
         ./../../system/mp 
         ./../../common/workunit 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../ecl/eclagent 
         ./../../common/thorhelper 
         ./../../common/deftype 
         ./../../rtl/eclrtl 
         ./../../system/jhtree 
         ./../../system/jlib 
         ./../../dali/sasha 
    )

ADD_DEFINITIONS( -D_USRDLL -DWORKUNITSERVICES_EXPORTS )

HPCC_ADD_LIBRARY( workunitservices SHARED ${SRCS} )
install ( TARGETS workunitservices DESTINATION plugins )
target_link_LIBRARies ( workunitservices 
         jlib
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         workunit 
    )


