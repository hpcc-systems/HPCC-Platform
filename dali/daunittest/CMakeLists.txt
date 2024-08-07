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


# Component: daunittest 
#####################################################
# Description:
# ------------
#    Cmake Input File for daunittest
#####################################################

project( daunittest ) 

set (    SRCS 
         daunittest.cpp 
         dautdfs.cpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../server 
         ./../base 
         ./../../system/mp 
         . 
         ./../../system/include 
         ./../../system/jlib
         ./../../system/security/shared
    )

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( daunittest ${SRCS} )
target_link_libraries ( daunittest
         jlib
         mp 
         hrpc 
         remote 
         dalibase 
         ${CppUnit_LIBRARIES}
    )


