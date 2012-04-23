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
    ${CMAKE_CURRENT_SOURCE_DIR}/collapse.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/expand.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/save.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/cloud.jpg
    ${CMAKE_CURRENT_SOURCE_DIR}/wizard.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/tooltip.jpg
    ${CMAKE_CURRENT_SOURCE_DIR}/hpcc_logo.png
    ${CMAKE_CURRENT_SOURCE_DIR}/favicon.ico
    ${CMAKE_CURRENT_SOURCE_DIR}/base.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/blank.png
    ${CMAKE_CURRENT_SOURCE_DIR}/information.png
    ${CMAKE_CURRENT_SOURCE_DIR}/cal.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/close_wnd.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/config.png
    ${CMAKE_CURRENT_SOURCE_DIR}/copyurl.png
    ${CMAKE_CURRENT_SOURCE_DIR}/downsimple.png
    ${CMAKE_CURRENT_SOURCE_DIR}/espbtns_tiled.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/folder.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/folderopen.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/form_minus.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/form_more.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/form_plus.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/home.png
    ${CMAKE_CURRENT_SOURCE_DIR}/join.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/joinbottom.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/keyfile.png
    ${CMAKE_CURRENT_SOURCE_DIR}/hpccsystemsECLWatch.png
    ${CMAKE_CURRENT_SOURCE_DIR}/hpccsystemsWsECL.png
    ${CMAKE_CURRENT_SOURCE_DIR}/line.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/loading.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/locked.gif
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
    ${CMAKE_CURRENT_SOURCE_DIR}/refreshdisabled.png
    ${CMAKE_CURRENT_SOURCE_DIR}/refreshenabled.png
    ${CMAKE_CURRENT_SOURCE_DIR}/relogin.png
    ${CMAKE_CURRENT_SOURCE_DIR}/reqxml.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/respxml.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/selectall.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/tab_bottom.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/topurl.png
    ${CMAKE_CURRENT_SOURCE_DIR}/unselectall.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/upsimple.png
    ${CMAKE_CURRENT_SOURCE_DIR}/wsdl.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/xsd.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/zip.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/zipbig.gif
    ${CMAKE_CURRENT_SOURCE_DIR}/accept.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_green.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_orange.png
    ${CMAKE_CURRENT_SOURCE_DIR}/cog.png
    ${CMAKE_CURRENT_SOURCE_DIR}/delete.png
    ${CMAKE_CURRENT_SOURCE_DIR}/folder_table.png
    ${CMAKE_CURRENT_SOURCE_DIR}/control_pause_blue.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_error.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_red.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_white.png
    ${CMAKE_CURRENT_SOURCE_DIR}/bullet_yellow.png
    ${CMAKE_CURRENT_SOURCE_DIR}/loadingAnimation.gif
)
    Install ( FILES ${iFILES} DESTINATION ${OSSDIR}/componentfiles/files/img COMPONENT Runtime)
ENDFOREACH( iFILES )

Install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/favlogo.png DESTINATION ${OSSDIR}/componentfiles/files COMPONENT Runtime )
