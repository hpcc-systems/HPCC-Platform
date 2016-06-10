################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
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


# Component: pseudorandomlib
#####################################################
# Description:
# ------------
#    Cmake Input File for pseudorandomlib
#####################################################

project( pseudorandomlib )

set (    SRCS
         pseudorandomlib.cpp
    )

include_directories (
         ./../../system/include
         ./../../system/jlib
    )

ADD_DEFINITIONS ( -DPSEUDO_RANDOMLIB_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( pseudorandomlib SHARED ${SRCS} )
install ( TARGETS pseudorandomlib DESTINATION plugins )
target_link_libraries(
	pseudorandomlib
        jlib
)

