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

set ( IMG_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/accept.png
    ${CMAKE_CURRENT_SOURCE_DIR}/base.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/blank.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_error.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_green.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_orange.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_red.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_white.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_yellow.png
    ${CMAKE_CURRENT_SOURCE_DIR}/cal.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/close_wnd.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/cloud.jpg
    ${CMAKE_CURRENT_SOURCE_DIR}/cog.png
    ${CMAKE_CURRENT_SOURCE_DIR}/collapse.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/config.png
    ${CMAKE_CURRENT_SOURCE_DIR}/control_pause_blue.png
    ${CMAKE_CURRENT_SOURCE_DIR}/copyurl.png
    ${CMAKE_CURRENT_SOURCE_DIR}/delete.png
    ${CMAKE_CURRENT_SOURCE_DIR}/downsimple.png
    ${CMAKE_CURRENT_SOURCE_DIR}/error-icon.png
    ${CMAKE_CURRENT_SOURCE_DIR}/espbtns_tiled.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/expand.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/favicon.ico
    ${CMAKE_CURRENT_SOURCE_DIR}/folder.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/folderopen.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/folder_table.png
    ${CMAKE_CURRENT_SOURCE_DIR}/form_minus.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/form_more.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/form_plus.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/home.png
    ${CMAKE_CURRENT_SOURCE_DIR}/hpccsystemsECLWatch.png
    ${CMAKE_CURRENT_SOURCE_DIR}/hpccsystemsWsECL.png
    ${CMAKE_CURRENT_SOURCE_DIR}/hpcc_logo.png
    ${CMAKE_CURRENT_SOURCE_DIR}/information.png
    ${CMAKE_CURRENT_SOURCE_DIR}/join.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/joinbottom.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/keyfile.png
    ${CMAKE_CURRENT_SOURCE_DIR}/line.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/loading.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/loadingAnimation.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/locked.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/locked.png
    ${CMAKE_CURRENT_SOURCE_DIR}/menu1.png
    ${CMAKE_CURRENT_SOURCE_DIR}/menudown.png
    ${CMAKE_CURRENT_SOURCE_DIR}/menuup.png
    ${CMAKE_CURRENT_SOURCE_DIR}/minus.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/minusbottom.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/next.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/next_year.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/outlet.png
    ${CMAKE_CURRENT_SOURCE_DIR}/page.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/pixel.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/plus.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/plusbottom.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/prev.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/prev_year.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/refresh.png
    ${CMAKE_CURRENT_SOURCE_DIR}/refresh2.png
    ${CMAKE_CURRENT_SOURCE_DIR}/refreshdisabled.png
    ${CMAKE_CURRENT_SOURCE_DIR}/refreshenabled.png
    ${CMAKE_CURRENT_SOURCE_DIR}/relogin.png
    ${CMAKE_CURRENT_SOURCE_DIR}/reqxml.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/respxml.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/save.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/saveasimg.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/selectall.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/tab_bottom.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/tooltip.jpg
    ${CMAKE_CURRENT_SOURCE_DIR}/topurl.png
    ${CMAKE_CURRENT_SOURCE_DIR}/unlocked.png
    ${CMAKE_CURRENT_SOURCE_DIR}/unselectall.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/upsimple.png
    ${CMAKE_CURRENT_SOURCE_DIR}/warning.png
    ${CMAKE_CURRENT_SOURCE_DIR}/wizard.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/workunit.png
    ${CMAKE_CURRENT_SOURCE_DIR}/workunit_aborting.png
    ${CMAKE_CURRENT_SOURCE_DIR}/workunit_completed.png
    ${CMAKE_CURRENT_SOURCE_DIR}/workunit_deleted.png
    ${CMAKE_CURRENT_SOURCE_DIR}/workunit_edit.png
    ${CMAKE_CURRENT_SOURCE_DIR}/workunit_failed.png
    ${CMAKE_CURRENT_SOURCE_DIR}/workunit_running.png
    ${CMAKE_CURRENT_SOURCE_DIR}/workunit_submitted.png
    ${CMAKE_CURRENT_SOURCE_DIR}/workunit_warning.png
    ${CMAKE_CURRENT_SOURCE_DIR}/wsdl.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/xsd.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/zip.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/zipbig.gif
)

FOREACH( iFile ${IMG_FILES} )
    Install( FILES ${iFile} DESTINATION componentfiles/files/img COMPONENT Runtime )
ENDFOREACH ( iFile )

Install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/favlogo.png DESTINATION componentfiles/files COMPONENT Runtime )
