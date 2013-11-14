################################################################################
#    Copyright (C) 2012 HPCC Systems.
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


# Component: unittests
#####################################################
# Description:
# ------------
#    Cmake Input File for unittests
#####################################################

if (USE_CPPUNIT)

project( unitests )

set (    SRCS
         unittests.cpp
         remotetests.cpp
         dalitests.cpp
         jlibtests.cpp
    )

include_directories (
         .
         ./../../system/include
         ./../../system/jlib
         ./../../system/mp
         ./../../common/remote
         ./../../dali/base
    )

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( unittests ${SRCS} )

install ( TARGETS unittests DESTINATION ${OSSDIR}/bin )
target_link_libraries ( unittests
         jlib
         remote
         dalibase
         ${CPPUNIT_LIBRARIES}
    )

endif ()