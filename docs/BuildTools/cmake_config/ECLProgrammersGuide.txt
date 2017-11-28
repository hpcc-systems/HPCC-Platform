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


DOCBOOK_TO_PDF( ${FO_XSL} PrGd-Includer.xml "ECLProgrammersGuide_${DOC_LANG}" "PRG_Mods")

IF(MAKE_DOCS)
  set(zip_out_dir ${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/docs/${DOC_LANG})
  set(ecl_code_name ECL_Code_Files_${DOC_LANG})

  ADD_CUSTOM_COMMAND(
	COMMAND mkdir -p ${zip_out_dir}
	COMMAND mkdir -p ${ecl_code_name}
	COMMAND rm -rf ${ecl_code_name}/*
        COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/PRG_Mods/"\"ECL Code Files\""/*.ecl ${ecl_code_name}/
        COMMAND zip -r  ${zip_out_dir}/${ecl_code_name}.zip ${ecl_code_name}
        OUTPUT ${zip_out_dir}/${ecl_code_name}.zip
	)

  ADD_CUSTOM_TARGET(${ecl_code_name} ALL DEPENDS  ${zip_out_dir}/${ecl_code_name}.zip)

  SET(HELP_DEPENDENCIES)
  GET_PROPERTY(Current_Targets GLOBAL PROPERTY DOC_TARGETS)
  FOREACH(T ${Current_Targets})
    IF("${T}" MATCHES ".*ECLProgrammersGuide_${DOC_LANG}.*")
      LIST(APPEND HELP_DEPENDENCIES  ${T})
     ENDIF()
  ENDFOREACH()

  DOCBOOK_TO_HTML(${ECLIPSE_HTML_XSL} "${CMAKE_CURRENT_BINARY_DIR}/PrGd-Includer.xml" ${PORTAL_HTML_DIR}/ProgrammersGuide_${DOC_LANG}  "programmers_guide_html_${DOC_LANG}" "${HPCC_SOURCE_DIR}/docs/common/eclipsehelp.css" "")
ENDIF(MAKE_DOCS)
