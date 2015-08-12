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


DOCBOOK_TO_PDF( ${FO_XSL} PrGd-Includer.xml "ECLProgrammersGuide" "PRG_Mods")

IF(MAKE_DOCS)
  set(zip_out_dir ${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/docs)
  ADD_CUSTOM_COMMAND(
	COMMAND mkdir -p ${zip_out_dir}
	COMMAND mkdir -p ECL_Code_Files
	COMMAND rm -rf ECL_Code_Files/*
        COMMAND cp ${HPCC_SOURCE_DIR}/docs/ECLProgrammersGuide/PRG_Mods/"\"ECL Code Files\""/*.ecl ECL_Code_Files/
        COMMAND zip -r  ${zip_out_dir}/ECL_Code_Files.zip ECL_Code_Files
        OUTPUT ${zip_out_dir}/ECL_Code_Files.zip
	)

  ADD_CUSTOM_TARGET(ECL_Code_Files ALL DEPENDS  ${zip_out_dir}/ECL_Code_Files.zip)
ENDIF(MAKE_DOCS)
