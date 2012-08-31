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
    ${CMAKE_CURRENT_SOURCE_DIR}/useradd.html
    ${CMAKE_CURRENT_SOURCE_DIR}/groupadd.html
    ${CMAKE_CURRENT_SOURCE_DIR}/ECLPlayground.htm
    ${CMAKE_CURRENT_SOURCE_DIR}/ECLPlaygroundResults.htm
    ${CMAKE_CURRENT_SOURCE_DIR}/ECLPlayground.css
    ${CMAKE_CURRENT_SOURCE_DIR}/ECLPlayground.js
)
    Install( FILES ${iFILES} DESTINATION componentfiles/files COMPONENT Runtime )
ENDFOREACH ( iFILES )

Install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/popup.js DESTINATION componentfiles/files/scripts COMPONENT Runtime )

Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/yui DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )

Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/dojo DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )
Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/dojox DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )
Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/dijit DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )
Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/CodeMirror2 DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )
Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/ecl DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )

add_subdirectory (css)
add_subdirectory (html)
add_subdirectory (img)
add_subdirectory (logo)
add_subdirectory (scripts)
