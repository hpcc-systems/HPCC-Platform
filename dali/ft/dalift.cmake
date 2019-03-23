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

# Component: dalift 
#####################################################
# Description:
# ------------
#    Cmake Input File for dalift
#####################################################

project( dalift ) 

set (    SRCS 
         daft.cpp 
         daftdir.cpp 
         daftformat.cpp 
         daftmc.cpp 
         daftprogress.cpp 
         daftsize.cpp 
         filecopy.cpp 
         ftbase.cpp 
         fttransform.cpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../../fs/dafsclient
         ./../../system/mp 
         ./../base 
         ./../../system/include 
         ./../../system/jlib 
         ./../../common/environment
         ./../../system/security/shared
    )

HPCC_ADD_LIBRARY( dalift SHARED ${SRCS} )
set_target_properties (dalift PROPERTIES 
    COMPILE_FLAGS -D_CONSOLE
    DEFINE_SYMBOL DALIFT_EXPORTS
    )
install ( TARGETS dalift RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( dalift 
         jlib
         mp 
         hrpc 
         dafsclient
         dalibase 
         environment 
    )

