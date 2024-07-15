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

PROJECT(docs 
  VERSION ${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}.${HPCC_SEQUENCE}
)

define_property(GLOBAL PROPERTY DOC_TARGETS BRIEF_DOCS "docs" FULL_DOCS "docs")
mark_as_advanced(DOC_TARGETS)

include(${CMAKE_MODULE_PATH}/docMacros.cmake)

set (DOCBOOK_RESOURCES ${CMAKE_CURRENT_BINARY_DIR}/resources)
set (DOCBOOK_XSL ${CMAKE_CURRENT_BINARY_DIR}/resources/docbook-xsl)
#set (DOC_IMAGES ${CMAKE_CURRENT_SOURCE_DIR}/)
set (XML_CATALOG ${CMAKE_CURRENT_BINARY_DIR}/BuildTools/catalog.xml)
set (DOC_VERSION "${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}")
set (FO_XSL ${CMAKE_CURRENT_BINARY_DIR}/BuildTools/fo.xsl)
set (VERSION_DIR ${CMAKE_CURRENT_BINARY_DIR})
set (ECLIPSE_HTML_XSL ${CMAKE_CURRENT_BINARY_DIR}/BuildTools/EclipseHelp.xsl)
#set (ECL_REFERENCE_XML ${CMAKE_CURRENT_BINARY_DIR}/ECLReference/ECLReference.xml)
set (HTML_HELP_XSL ${CMAKE_CURRENT_BINARY_DIR}/resources/docbook-xsl/htmlhelp/htmlhelp.xsl)
set (PORTAL_GEN_XSL ${CMAKE_CURRENT_BINARY_DIR}/BuildTools/PortalGen.xsl)



if ( "${HPCC_MATURITY}" STREQUAL "release" )
	set (DOC_VERSION "${DOC_VERSION}-${HPCC_SEQUENCE}")
	set (DOC_VERSION_ONLY "${DOC_VERSION}")
else()
   if (DOCTAG)
	set (DOC_VERSION "${majorver}-${minorver}-${DOCTAG}")
        string (REGEX REPLACE "-#.*" "" DOC_VERSION_ONLY ${DOC_VERSION})
   else()
	set (DOC_VERSION "${DOC_VERSION}-${HPCC_MATURITY}${HPCC_SEQUENCE}")
	set (DOC_VERSION_ONLY "${DOC_VERSION}")
  endif()
endif()

# Build image file list to add to source watch.
#file(GLOB_RECURSE DOC_IMAGES_LIST images/*)


# Required directories to add.
add_subdirectory(portal-js)
add_subdirectory(common)
add_subdirectory(BuildTools)
add_subdirectory(resources)

# Docs automation. 
# Must before each language dir since UsingConfigManager depends on it
if (DOCS_AUTO)
   add_subdirectory(XMLGeneration)
endif()

# GHacked up to test for Int'l.
#
if (NOT DOC_LANGS)
   set(DOC_LANGS EN_US)
endif()

string(TOUPPER "${DOC_LANGS}" DOC_LANGS)

if ( ${DOC_LANGS} STREQUAL "ALL" )
  set(DOC_LANGS "EN_US;PT_BR")
endif ()

foreach(LANG ${DOC_LANGS})
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${LANG}") 
    add_subdirectory("${LANG}")
  endif()
endforeach()


GET_PROPERTY(_targets GLOBAL PROPERTY DOC_TARGETS)
add_custom_target(docs ALL DEPENDS ${_targets})
