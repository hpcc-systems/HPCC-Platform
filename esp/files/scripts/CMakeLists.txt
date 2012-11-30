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
add_subdirectory (configmgr)

set ( SCRIPTS_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/bpsreport.js
    ${CMAKE_CURRENT_SOURCE_DIR}/builder.js
    ${CMAKE_CURRENT_SOURCE_DIR}/CMultiSelect.js
    ${CMAKE_CURRENT_SOURCE_DIR}/controls.js
    ${CMAKE_CURRENT_SOURCE_DIR}/DFUSearchWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/DFUWUDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/dragdrop.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ECLPlaygroundWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ECLSourceWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/effects.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ESPBase.js
    ${CMAKE_CURRENT_SOURCE_DIR}/espdefault.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ESPDFUWorkunit.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ESPLogicalFile.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ESPResult.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ESPWorkunit.js
    ${CMAKE_CURRENT_SOURCE_DIR}/FilePartsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/fixedTables.js
    ${CMAKE_CURRENT_SOURCE_DIR}/graphgvc.js
    ${CMAKE_CURRENT_SOURCE_DIR}/GraphPageWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/GraphsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/GraphWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/InfoGridWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/LFDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/LogsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/multiselect.js
    ${CMAKE_CURRENT_SOURCE_DIR}/objtree.js
    ${CMAKE_CURRENT_SOURCE_DIR}/prototype.js
    ${CMAKE_CURRENT_SOURCE_DIR}/prototype_helpers.js
    ${CMAKE_CURRENT_SOURCE_DIR}/range.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ResultsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ResultWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/rightSideBar.js
    ${CMAKE_CURRENT_SOURCE_DIR}/SampleSelectControl.js
    ${CMAKE_CURRENT_SOURCE_DIR}/SampleSelectWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/scriptaculous.js
    ${CMAKE_CURRENT_SOURCE_DIR}/slider.js
    ${CMAKE_CURRENT_SOURCE_DIR}/sortabletable.js
    ${CMAKE_CURRENT_SOURCE_DIR}/tabularForm.js
    ${CMAKE_CURRENT_SOURCE_DIR}/TargetSelectWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/timer.js
    ${CMAKE_CURRENT_SOURCE_DIR}/TimingGridWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/TimingPageWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/TimingTreeMapWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/tooltip.js
    ${CMAKE_CURRENT_SOURCE_DIR}/tree.js
    ${CMAKE_CURRENT_SOURCE_DIR}/tree_template.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ui_engine.js
    ${CMAKE_CURRENT_SOURCE_DIR}/WsWorkunits.js
    ${CMAKE_CURRENT_SOURCE_DIR}/WUDetailsWidget.js
)

FOREACH( iFile ${SCRIPTS_FILES} )
    Install( FILES ${iFile} DESTINATION componentfiles/files/scripts COMPONENT Runtime )
ENDFOREACH ( iFile )

