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


# Component: wuwebview
#####################################################
# Description:
# ------------
#    Cmake Input File for wuwebview
#####################################################

project( wuwebview )

set (    SRCS
         wuwebview.cpp
         wuwebview.hpp
         wuweberror.hpp
    )

include_directories (
         ./../../common/workunit
         ./../../common/fileview2
         ./../../system/include
         ./../../common/dllserver
         ./../../system/jlib
         ./../../system/xmllib
    )

HPCC_ADD_LIBRARY( wuwebview SHARED ${SRCS} )
set_target_properties(wuwebview PROPERTIES
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL WUWEBVIEW_EXPORTS )
install ( 
    TARGETS wuwebview 
    RUNTIME DESTINATION ${EXEC_DIR} 
    LIBRARY DESTINATION ${LIB_DIR} 
    COMPONENT Runtime
)
target_link_libraries ( wuwebview
         jlib
         workunit
         fileview2
         dllserver
         xmllib
    )
