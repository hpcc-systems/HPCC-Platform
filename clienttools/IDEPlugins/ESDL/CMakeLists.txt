################################################################################
#    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.
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

project(IDEPlugins-ESDL)

set(SRC
    esdl.bat.in
    )

foreach(source ${SRC})
    string(REGEX REPLACE "\\.in$" "" configured_source ${source})
    configure_file(${source} ${configured_source} @ONLY)
    install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${configured_source} DESTINATION IDEPlugins/ESDL)
endforeach()
