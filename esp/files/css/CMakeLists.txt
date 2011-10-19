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
    ${CMAKE_CURRENT_SOURCE_DIR}/espdefault.css
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch.css
    ${CMAKE_CURRENT_SOURCE_DIR}/graph.css
    ${CMAKE_CURRENT_SOURCE_DIR}/winclassic.css
    ${CMAKE_CURRENT_SOURCE_DIR}/headerFooter.css
    ${CMAKE_CURRENT_SOURCE_DIR}/list.css
    ${CMAKE_CURRENT_SOURCE_DIR}/rightSideBar.css
    ${CMAKE_CURRENT_SOURCE_DIR}/sortabletable.css
    ${CMAKE_CURRENT_SOURCE_DIR}/tabs.css
)
    Install( FILES ${iFILES} DESTINATION ${OSSDIR}/componentfiles/files COMPONENT Runtime )
ENDFOREACH ( iFILES )
