################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    This program is free software: you can redistribute it and/or All rights
#    reserved. This program is NOT PRESENTLY free software: you can NOT
#    redistribute
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    ${CMAKE_CURRENT_SOURCE_DIR}/GraphControl.js
    ${CMAKE_CURRENT_SOURCE_DIR}/ResultsControl.js
    ${CMAKE_CURRENT_SOURCE_DIR}/SampleSelectControl.js
)
    Install ( FILES ${iFILES} DESTINATION ${OSSDIR}/componentfiles/files/scripts COMPONENT Runtime )
ENDFOREACH ( iFILES )
