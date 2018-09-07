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

configure_file(setup setup COPYONLY)
configure_file(add-node add-node COPYONLY)
configure_file(mod-node mod-node COPYONLY)
configure_file(rmv-node rmv-node COPYONLY)
configure_file(esp-service esp-service COPYONLY)
configure_file(topology topology COPYONLY)
configure_file(misc misc COPYONLY)
configure_file(regression regression COPYONLY)
configure_file(run_valgrind.in run_valgrind @ONLY)
configure_file(test.sh.in test.sh @ONLY)

execute_process ( WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} COMMAND ${CMAKE_COMMAND} -E make_directory  src )
execute_process ( WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/src ./src/ )
