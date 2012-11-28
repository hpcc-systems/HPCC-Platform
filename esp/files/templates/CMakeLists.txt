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
set ( TEMPLATES_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/ECLPlaygroundWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/ECLSourceWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/GraphPageWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/GraphWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/InfoGridWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/LFDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/LogsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/ResultsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/SampleSelectWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/TargetSelectWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/TimingGridWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/TimingPageWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/TimingTreeMapWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/WUDetailsWidget.html
)
set ( TEMPLATES_FILES ${TEMPLATES_FILES} PARENT_SCOPE)

FOREACH( iFile ${TEMPLATES_FILES} )
    Install( FILES ${iFile} DESTINATION componentfiles/files/templates COMPONENT Runtime )
ENDFOREACH ( iFile )

