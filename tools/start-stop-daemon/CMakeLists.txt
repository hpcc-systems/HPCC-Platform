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

project(start-stop-daemon)

add_definitions( -Wno-return-type )

set( SRC start-stop-daemon.c )
set( HEADER macros.h )

include_directories( ${CMAKE_CURRENT_SOURCE_DIR} )

HPCC_ADD_EXECUTABLE( start-stop-daemon ${SRC} ${HEADER})
install ( 
    TARGETS start-stop-daemon RUNTIME DESTINATION ${EXEC_DIR} 
    COMPONENT Runtime
)

