################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    This program is free software: you can redistribute it and/or All rights
#    reserved. This program is NOT PRESENTLY free software: you can NOT
#    redistribute
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################
add_subdirectory (configmgr)

FOREACH( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/espdefault.js
    ${CMAKE_CURRENT_SOURCE_DIR}/CMultiSelect.js
    ${CMAKE_CURRENT_SOURCE_DIR}/bpsreport.js
    ${CMAKE_CURRENT_SOURCE_DIR}/builder.js
    ${CMAKE_CURRENT_SOURCE_DIR}/controls.js
    ${CMAKE_CURRENT_SOURCE_DIR}/dragdrop.js
    ${CMAKE_CURRENT_SOURCE_DIR}/effects.js
    ${CMAKE_CURRENT_SOURCE_DIR}/fixedTables.js
    ${CMAKE_CURRENT_SOURCE_DIR}/graphgvc.js
    ${CMAKE_CURRENT_SOURCE_DIR}/multiselect.js
    ${CMAKE_CURRENT_SOURCE_DIR}/objtree.js
    ${CMAKE_CURRENT_SOURCE_DIR}/prototype.js
    ${CMAKE_CURRENT_SOURCE_DIR}/range.js
    ${CMAKE_CURRENT_SOURCE_DIR}/timer.js
    ${CMAKE_CURRENT_SOURCE_DIR}/slider.js
    ${CMAKE_CURRENT_SOURCE_DIR}/prototype_helpers.js
    ${CMAKE_CURRENT_SOURCE_DIR}/rightSideBar.js
    ${CMAKE_CURRENT_SOURCE_DIR}/scriptaculous.js
    ${CMAKE_CURRENT_SOURCE_DIR}/sortabletable.js
    ${CMAKE_CURRENT_SOURCE_DIR}/tabularForm.js
    ${CMAKE_CURRENT_SOURCE_DIR}/tooltip.js
    ${CMAKE_CURRENT_SOURCE_DIR}/tree.js
    ${CMAKE_CURRENT_SOURCE_DIR}/tree_template.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ui_engine.js
    ${CMAKE_CURRENT_SOURCE_DIR}/EclEditorControl.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ESPBase.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ESPWorkunit.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ESPResult.js
    ${CMAKE_CURRENT_SOURCE_DIR}/WsWorkunits.js
    ${CMAKE_CURRENT_SOURCE_DIR}/GraphControl.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ResultsControl.js
    ${CMAKE_CURRENT_SOURCE_DIR}/SampleSelectControl.js
)
    Install ( FILES ${iFILES} DESTINATION componentfiles/files/scripts COMPONENT Runtime )
ENDFOREACH ( iFILES )
