################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    ${CMAKE_CURRENT_SOURCE_DIR}/ui_configmgr.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/appframe.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/dict_sort.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/env2jstree.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/esdl_method.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/espheader.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esxdl2req.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/gen_form.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/multistatus.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/nav.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/navigation.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/soap_page.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/sso_create_session.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/ui_overrides.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/yuitree.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/xmlformatter.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_form.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_boxform.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_links.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_url.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_tabview.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_xmltest.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_jsontest.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl3_result.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/passwordupdate.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esdl2ecl.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/esxdl2xsd.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/esdl2java_srvbase.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/esdl2java_srvdummy.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxie_page.xsl
)
    Install ( FILES ${iFILES} DESTINATION componentfiles/xslt COMPONENT Runtime )
ENDFOREACH ( iFILES )

FOREACH ( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl2_form.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl_tabview.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl_tree.xsl
)
    Install ( FILES ${iFILES} DESTINATION componentfiles/files COMPONENT Runtime )
ENDFOREACH ( iFILES )
