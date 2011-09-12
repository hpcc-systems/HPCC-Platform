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
    ${CMAKE_CURRENT_SOURCE_DIR}/ui_configmgr.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/appframe.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/dict_sort.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/env2jstree.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/esdl2ecl.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/esdl_method.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/espheader.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esxdl2req.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/esxdl2xsd.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/gen_form.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/multistatus.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/nav.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/navigation.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/soap_page.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/sso_create_session.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/ui_engine.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/ui_overrides.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/yuitree.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/xmlformatter.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_form.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_links.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_tabview.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_xmltest.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_jsontest.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_result.xslt
)
    Install ( FILES ${iFILES} DESTINATION ${OSSDIR}/componentfiles/xslt COMPONENT Runtime )
ENDFOREACH ( iFILES )

FOREACH ( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl2_form.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl_tabview.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl_tree.xsl
)
    Install ( FILES ${iFILES} DESTINATION ${OSSDIR}/componentfiles/files COMPONENT Runtime )
ENDFOREACH ( iFILES )