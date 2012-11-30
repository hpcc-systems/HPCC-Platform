################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

set ( CSS_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/ecl.css
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch.css
    ${CMAKE_CURRENT_SOURCE_DIR}/espdefault.css
    ${CMAKE_CURRENT_SOURCE_DIR}/graph.css
    ${CMAKE_CURRENT_SOURCE_DIR}/headerFooter.css
    ${CMAKE_CURRENT_SOURCE_DIR}/hpcc.css
    ${CMAKE_CURRENT_SOURCE_DIR}/list.css
    ${CMAKE_CURRENT_SOURCE_DIR}/rightSideBar.css
    ${CMAKE_CURRENT_SOURCE_DIR}/sortabletable.css
    ${CMAKE_CURRENT_SOURCE_DIR}/tabs.css
    ${CMAKE_CURRENT_SOURCE_DIR}/winclassic.css
)

FOREACH( iFile ${CSS_FILES} )
    Install( FILES ${iFile} DESTINATION componentfiles/files/css COMPONENT Runtime )
ENDFOREACH ( iFile )
