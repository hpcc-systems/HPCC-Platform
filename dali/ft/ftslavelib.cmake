################################################################################
#    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.
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


# Component: ftslavelib
#####################################################
# Description:
# ------------
#    Cmake Input File for ftslavelib
#####################################################

project( ftslavelib ) 

set (    SRCS 
         ftslavelib.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/dali/base 
         ${HPCC_SOURCE_DIR}/include 
         ${HPCC_SOURCE_DIR}/jlib
         ${HPCC_SOURCE_DIR}/security/shared
         ${HPCC_SOURCE_DIR}/system/mp 
    )

ADD_DEFINITIONS( -D_USRDLL -DFTSLAVELIB_EXPORTS )

HPCC_ADD_LIBRARY ( ftslavelib SHARED ${SRCS} )
install ( TARGETS ftslavelib RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ftslavelib
         dalibase 
         dalift 
         hrpc
         jlib
         mp 
         remote 
    )
