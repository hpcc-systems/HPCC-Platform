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

# Component: swapnodelib
#####################################################
# Description:
# ------------
#    Cmake Input File for swapnodelib
#####################################################

project( swapnodelib )

set (    SRCS
         swapnodelib.cpp
    )

include_directories (
         ./../../common/remote
         ./../../fs/dafsclient
         ./../../system/mp
         ./../../system/include
         ./../../dali/base
         ./../../system/jlib
         ./../../common/environment
         ./../../common/workunit
    )

HPCC_ADD_LIBRARY( swapnodelib SHARED ${SRCS} )
set_target_properties(swapnodelib PROPERTIES
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL SWAPNODELIB_EXPORTS )
install ( TARGETS swapnodelib RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( swapnodelib
         jlib
         remote
         dafsclient
         dalibase
         workunit
    )
