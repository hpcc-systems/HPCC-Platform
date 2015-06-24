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
GET_PROPERTY(Current_Targets GLOBAL PROPERTY DOC_TARGETS)

SET(XSLTPROC_DEPENDENCIES)
FOREACH(T ${Current_Targets})
   IF("${T}" MATCHES "xsd_to_xml" )
      LIST(APPEND XSLTPROC_DEPENDENCIES  ${T})
   ENDIF()
ENDFOREACH()
########################################################################
# commented out per issue # HPCC-10267
#
#
DOCBOOK_TO_PDF( ${FO_XSL} UsingConfigManager.xml "UsingConfigManager")
#
#
########################################################################
