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

set ( HTML_FILES
    html/searchfile.html
)
source_group(html FILES ${HTML_FILES})

set ( IMG_FILES
    img/collapse.gif
    img/expand.gif
    img/save.gif
    img/cloud.jpg
    img/wizard.gif
    img/tooltip.jpg
    img/hpcc_logo.png
    img/favicon.ico
    img/base.gif
    img/blank.png
    img/information.png
    img/cal.gif
    img/close_wnd.gif
    img/config.png
    img/copyurl.png
    img/downsimple.png
    img/espbtns_tiled.gif
    img/folder.gif
    img/folderopen.gif
    img/form_minus.gif
    img/form_more.gif
    img/form_plus.gif
    img/home.png
    img/join.gif
    img/joinbottom.gif
    img/keyfile.png
    img/hpccsystemsECLWatch.png
    img/hpccsystemsWsECL.png
    img/line.gif
    img/loading.gif
    img/locked.gif
    img/menu1.png
    img/menudown.png
    img/menuup.png
    img/minus.gif
    img/minusbottom.gif
    img/next.gif
    img/next_year.gif
    img/outlet.png
    img/page.gif
    img/pixel.gif
    img/plus.gif
    img/plusbottom.gif
    img/prev.gif
    img/prev_year.gif
    img/refresh.png
    img/refreshdisabled.png
    img/refreshenabled.png
    img/relogin.png
    img/reqxml.gif
    img/respxml.gif
    img/selectall.gif
    img/tab_bottom.gif
    img/topurl.png
    img/unselectall.gif
    img/upsimple.png
    img/wsdl.gif
    img/xsd.gif
    img/zip.gif
    img/zipbig.gif
    img/accept.png
    img/bullet_green.png
    img/bullet_orange.png
    img/cog.png
    img/delete.png
    img/folder_table.png
    img/control_pause_blue.png
    img/bullet_error.png
    img/bullet_red.png
    img/bullet_white.png
    img/bullet_yellow.png
    img/loadingAnimation.gif
)
source_group(img FILES ${IMG_FILES})

set ( LOGO_FILES
    logo/ln_small.gif
    logo/seisint_small.gif
    logo/slogo.gif
)
source_group(logo FILES ${LOGO_FILES})

set ( CSS_FILES
    css/espdefault.css
    css/eclwatch.css
    css/graph.css
    css/winclassic.css
    css/headerFooter.css
    css/list.css
    css/rightSideBar.css
    css/sortabletable.css
    css/tabs.css
    css/hpcc.css
    css/ecl.css
)
source_group(css FILES ${CSS_FILES})

set ( TEMPLATES_FILES
    templates/ECLPlaygroundWidget.html
    templates/GraphPageWidget.html
    templates/GraphWidget.html
    templates/ResultsWidget.html
    templates/SampleSelectWidget.html
    templates/TargetSelectWidget.html
    templates/TimingTreeMapWidget.html
    templates/ECLSourceWidget.html
)
source_group(templates FILES ${TEMPLATES_FILES})

set ( SCRIPTS_FILES
    scripts/espdefault.js
    scripts/CMultiSelect.js
    scripts/bpsreport.js
    scripts/builder.js
    scripts/controls.js
    scripts/dragdrop.js
    scripts/effects.js
    scripts/fixedTables.js
    scripts/graphgvc.js
    scripts/multiselect.js
    scripts/objtree.js
    scripts/prototype.js
    scripts/range.js
    scripts/timer.js
    scripts/slider.js
    scripts/prototype_helpers.js
    scripts/rightSideBar.js
    scripts/scriptaculous.js
    scripts/sortabletable.js
    scripts/tabularForm.js
    scripts/tooltip.js
    scripts/tree.js
    scripts/tree_template.js
    scripts/ui_engine.js

    scripts/ESPBase.js
    scripts/ESPWorkunit.js
    scripts/ESPResult.js
    scripts/WsWorkunits.js

    scripts/ResultsControl.js

    scripts/ECLPlaygroundWidget.js
    scripts/GraphPageWidget.js
    scripts/GraphWidget.js
    scripts/ResultsWidget.js
    scripts/SampleSelectWidget.js
    scripts/TargetSelectWidget.js
    scripts/TimingTreeMapWidget.js
    scripts/ECLSourceWidget.js
)
source_group(scripts FILES ${SCRIPTS_FILES})

set ( MAIN_FILES 
	dummy.cpp
    default.css
    gen_form.css
    wsecl2_form.css
    configmgr.html
    GraphViewCtl.cab
    npGraphViewCtl.xpi
    update.rdf
    base64.js
    qmalert.html
    calendar.html
    calendar_xs.js
    components.xml
    empty.svg
    gen_form.js
    get_input.js
    graph.js
    hashtable.js
    hint.js
    joblist.js
    jobqueue.js
    minus.gif
    plus.gif
    popup.js
    req_array.js
    select.js
    stack.js
    stringbuffer.js
    submitNavForm.html
    tabularForm.xslt
    transformDlg.html
    esp_app_tree.html
    esp_app.html
    useradd.html
    groupadd.html
    stub.htm
    stub.js
    )
source_group("Source Files" FILES ${MAIN_FILES})

set ( SRCS ${MAIN_FILES} ${HTML_FILES} ${IMG_FILES} ${LOGO_FILES} ${CSS_FILES} ${SCRIPTS_FILES} ${TEMPLATES_FILES})
HPCC_ADD_LIBRARY( web_files ${SRCS} )

Install ( FILES html/searchfile.html DESTINATION componentfiles/files COMPONENT Runtime )
FOREACH( iFile ${IMG_FILES} )
    Install( FILES ${iFile} DESTINATION componentfiles/files/img COMPONENT Runtime )
ENDFOREACH ( iFile )
Install ( FILES img/favlogo.png DESTINATION componentfiles/files COMPONENT Runtime )

FOREACH( iFile ${LOGO_FILES} )
    Install( FILES ${iFile} DESTINATION componentfiles/files/logo COMPONENT Runtime )
ENDFOREACH ( iFile )

FOREACH( iFile ${CSS_FILES} )
    Install( FILES ${iFile} DESTINATION componentfiles/files/css COMPONENT Runtime )
ENDFOREACH ( iFile )

FOREACH( iFile ${SCRIPTS_FILES} )
    Install( FILES ${iFile} DESTINATION componentfiles/files/scripts COMPONENT Runtime )
ENDFOREACH ( iFile )

FOREACH( iFile ${TEMPLATES_FILES} )
    Install( FILES ${iFile} DESTINATION componentfiles/files/templates COMPONENT Runtime )
ENDFOREACH ( iFile )

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
