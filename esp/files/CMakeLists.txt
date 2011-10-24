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

FOREACH( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/default.css
    ${CMAKE_CURRENT_SOURCE_DIR}/gen_form.css
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl2_form.css
    ${CMAKE_CURRENT_SOURCE_DIR}/configmgr.html
    ${CMAKE_CURRENT_SOURCE_DIR}/GraphViewCtl.cab
    ${CMAKE_CURRENT_SOURCE_DIR}/npGraphViewCtl.xpi
    ${CMAKE_CURRENT_SOURCE_DIR}/update.rdf
    ${CMAKE_CURRENT_SOURCE_DIR}/base64.js
    ${CMAKE_CURRENT_SOURCE_DIR}/qmalert.html
    ${CMAKE_CURRENT_SOURCE_DIR}/calendar.html
    ${CMAKE_CURRENT_SOURCE_DIR}/calendar_xs.js
    ${CMAKE_CURRENT_SOURCE_DIR}/components.xml
    ${CMAKE_CURRENT_SOURCE_DIR}/empty.svg
    ${CMAKE_CURRENT_SOURCE_DIR}/gen_form.js
    ${CMAKE_CURRENT_SOURCE_DIR}/get_input.js
    ${CMAKE_CURRENT_SOURCE_DIR}/graph.js
    ${CMAKE_CURRENT_SOURCE_DIR}/groupadd.html
    ${CMAKE_CURRENT_SOURCE_DIR}/hashtable.js
    ${CMAKE_CURRENT_SOURCE_DIR}/hint.js
    ${CMAKE_CURRENT_SOURCE_DIR}/joblist.js
    ${CMAKE_CURRENT_SOURCE_DIR}/jobqueue.js
    ${CMAKE_CURRENT_SOURCE_DIR}/minus.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/plus.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/popup.js
    ${CMAKE_CURRENT_SOURCE_DIR}/req_array.js
    ${CMAKE_CURRENT_SOURCE_DIR}/select.js
    ${CMAKE_CURRENT_SOURCE_DIR}/stack.js
    ${CMAKE_CURRENT_SOURCE_DIR}/stringbuffer.js
    ${CMAKE_CURRENT_SOURCE_DIR}/submitNavForm.html
    ${CMAKE_CURRENT_SOURCE_DIR}/tabularForm.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/transformDlg.html
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_app_tree.html
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_app.html
)
    Install( FILES ${iFILES} DESTINATION ${OSSDIR}/componentfiles/files COMPONENT Runtime )
ENDFOREACH ( iFILES )

Install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/popup.js DESTINATION ${OSSDIR}/componentfiles/files/scripts COMPONENT Runtime )

Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/yui DESTINATION ${OSSDIR}/componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )

add_subdirectory (css)
add_subdirectory (html)
add_subdirectory (img)
add_subdirectory (logo)
add_subdirectory (scripts)
