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

project( web_files ) 

add_subdirectory (css)
source_group(css FILES ${CSS_FILES})
add_subdirectory (html)
source_group(html FILES ${HTML_FILES})
add_subdirectory (img)
source_group(img FILES ${IMG_FILES})
add_subdirectory (logo)
source_group(logo FILES ${LOGO_FILES})
add_subdirectory (scripts)
source_group(scripts FILES ${SCRIPTS_FILES})
add_subdirectory (templates)
source_group(templates FILES ${TEMPLATES_FILES})

set ( MAIN_FILES 
    base64.js
    calendar.html
    calendar_xs.js
    components.xml
    configmgr.html
    default.css
    empty.svg
    esp_app.html
    esp_app_tree.html
    gen_form.css
    gen_form.js
    get_input.js
    graph.js
    GraphViewCtl.cab
    groupadd.html
    hashtable.js
    hint.js
    joblist.js
    jobqueue.js
    minus.gif
    npGraphViewCtl.xpi
    plus.gif
    popup.js
    qmalert.html
    req_array.js
    select.js
    stack.js
    stringbuffer.js
    stub.htm
    stub.js
    submitNavForm.html
    tabularForm.xslt
    transformDlg.html
    update.rdf
    useradd.html
    wsecl2_form.css
)

FOREACH( iFile ${MAIN_FILES} )
    Install( FILES ${iFile} DESTINATION componentfiles/files COMPONENT Runtime )
ENDFOREACH ( iFile )

Install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/popup.js DESTINATION componentfiles/files/scripts COMPONENT Runtime )

Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/yui DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )

Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/dojo DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )
Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/dojox DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )
Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/dijit DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )
Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/CodeMirror2 DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )
Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/ecl DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )
