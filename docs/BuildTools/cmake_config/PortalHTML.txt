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
IF(MAKE_DOCS)
  SET(HELP_DEPENDENCIES)
  GET_PROPERTY(Current_Targets GLOBAL PROPERTY DOC_TARGETS)
  FOREACH(T ${Current_Targets})
    IF("${T}" MATCHES ".*_html")
      LIST(APPEND HELP_DEPENDENCIES  ${T})
    ENDIF()
  ENDFOREACH()
  SET(PORTAL_HTML_ZIP "portal_html_${DOC_LANG}_zip")
  ADD_CUSTOM_TARGET(${PORTAL_HTML_ZIP} ALL
           COMMAND zip -r "portal_html_${DOC_LANG}.zip" ${PORTAL_HTML_DIR}
           WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/
           DEPENDS ${HELP_DEPENDENCIES}
           )
  set_property(GLOBAL APPEND PROPERTY DOC_TARGETS ${PORTAL_HTML_ZIP})

ENDIF(MAKE_DOCS)
