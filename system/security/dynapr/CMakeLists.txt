################################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.
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


# Component: dynapr
#####################################################
# Description:
# ------------
#    Cmake Input File for dynapr
#####################################################

project( dynapr )

set (    SRCS
         apr.cpp
         aprshared.cpp
         apushared.cpp
         dynshared.cpp
    )

include_directories (
         .
         ./../../include
         ./../../jlib
    )

ADD_DEFINITIONS( -DDYNAPR_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( dynapr SHARED ${SRCS} )
install ( TARGETS dynapr RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( dynapr
         jlib
    )
