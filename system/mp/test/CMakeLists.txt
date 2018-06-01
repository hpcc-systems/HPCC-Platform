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

# Component: mptest 

#####################################################
# Description:
# ------------
#    Cmake Input File for mptest
#####################################################


project( mptest ) 

set (    SRCS 
         mptest_mpi.cpp 
    )

include_directories ( 
         ./../../mp 
         . 
         ./../../jlib 
         ./../../include 
    )

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE (mptest_rank ${SRCS} ) 
SET_TARGET_PROPERTIES(mptest_rank PROPERTIES COMPILE_FLAGS "-DRANK_TEST")
target_link_libraries (mptest_rank jlib mp)

HPCC_ADD_EXECUTABLE (mptest_single ${SRCS} ) 
SET_TARGET_PROPERTIES(mptest_single PROPERTIES COMPILE_FLAGS "-DSINGLE_SEND_TEST")
target_link_libraries (mptest_single jlib mp)

HPCC_ADD_EXECUTABLE (mptest_rshift ${SRCS} ) 
SET_TARGET_PROPERTIES(mptest_rshift PROPERTIES COMPILE_FLAGS "-DRIGHT_SHIFT_TEST")
target_link_libraries (mptest_rshift jlib mp)

HPCC_ADD_EXECUTABLE (mptest_custom ${SRCS} ) 
SET_TARGET_PROPERTIES(mptest_custom PROPERTIES COMPILE_FLAGS "-DCUSTOM_SEND_TEST")
target_link_libraries (mptest_custom jlib mp)

HPCC_ADD_EXECUTABLE (mptest_to_all ${SRCS} ) 
SET_TARGET_PROPERTIES(mptest_to_all PROPERTIES COMPILE_FLAGS "-DSEND_ONE_TO_ALL_TEST")
target_link_libraries (mptest_to_all jlib mp)

HPCC_ADD_EXECUTABLE (mptest_from_all ${SRCS} ) 
SET_TARGET_PROPERTIES(mptest_from_all PROPERTIES COMPILE_FLAGS "-DRECEIVE_ONE_FROM_ALL_TEST")
target_link_libraries (mptest_from_all jlib mp)

HPCC_ADD_EXECUTABLE (mptest_recv_any ${SRCS} ) 
SET_TARGET_PROPERTIES(mptest_recv_any PROPERTIES COMPILE_FLAGS "-DRECEIVE_FROM_ANY_TEST")
target_link_libraries (mptest_recv_any jlib mp)


