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


# Component: roxiehelper 
#####################################################
# Description:
# ------------
#    Cmake Input File for roxiehelper
#####################################################

project( roxiehelper ) 

include_directories (            ./../../system/security/securesocket 
         ./../../common/remote 
         ./../../system/jhtree 
         ./../../rtl/eclrtl 
         ./../../common/thorhelper 
         ./../../roxie/roxiemem 
         ./../../roxie/roxie
         ./../../roxie/ccd
         ./../../system/include 
         ./../workunit 
         ./../../system/jlib 
         ./../deftype 
         ./../../rtl/include
         ./../../system/mp 
         ./../../dali/base 
    )

ADD_DEFINITIONS( -DROXIEHELPER_EXPORTS -D_USRDLL -DMODULE_PRIORITY=5 )

set ( SRCS roxiehelper.cpp roxiedebug.cpp
           roxiehelper.hpp roxiedebug.hpp roxielmj.hpp)

HPCC_ADD_LIBRARY( roxiehelper SHARED ${SRCS} )
install ( TARGETS roxiehelper RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( roxiehelper
         jlib 
         eclrtl 
         thorhelper 
         roxiemem 
         dalibase 
 ) 

