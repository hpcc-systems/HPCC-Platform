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


# Component: workunit 
#####################################################
# Description:
# ------------
#    Cmake Input File for workunit
#####################################################

project( workunit ) 

set (    SRCS 
         workunit.cpp 
         wujobq.cpp
         package.cpp
                 
         workunit.hpp 
         wuerror.hpp
         wujobq.hpp
         package.hpp
    )

include_directories ( 
         ./../../common/workunit
         ./../../system/mp 
         ./../../dali/ft 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../../system/jlib
         ./../../system/security/shared 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../../rtl/nbcd
    )

HPCC_ADD_LIBRARY( workunit SHARED ${SRCS} )
set_target_properties(workunit PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL WORKUNIT_EXPORTS )
install ( TARGETS workunit RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( workunit 
         jlib 
         dalibase 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
    )


