################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems(r).
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

DOCBOOK_TO_PDF( ${FO_XSL} DynamicESDL_Includer.xml "DynamicESDL" "DESDL-Mods")
DOCBOOK_TO_PDF( ${FO_XSL} ESDL_LangRef_Includer.xml "ESDL_LangRef" "DESDL-Mods")

IF(MAKE_DOCS)
  SET(HELP_DEPENDENCIES)
  GET_PROPERTY(Current_Targets GLOBAL PROPERTY DOC_TARGETS)
  FOREACH(T ${Current_Targets})
    IF("${T}" MATCHES ".*ESDL_LangRef.*")
      LIST(APPEND HELP_DEPENDENCIES  ${T})
    ENDIF()
  ENDFOREACH()

  DOCBOOK_TO_HTML(${ECLIPSE_HTML_XSL} "${CMAKE_CURRENT_BINARY_DIR}/ESDL_LangRef_Includer.xml" ${PORTAL_HTML_DIR}/ESDL_LangRef  "esdl_langref_html" "${HPCC_SOURCE_DIR}/docs/common/eclipsehelp.css" "")
ENDIF(MAKE_DOCS)
