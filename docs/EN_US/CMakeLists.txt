################################################################################
#    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
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

###
#  This CMakeLists intended to be used in docs/EN
#    Refactored for internationalization
###
# Last Mod GP 11/2023
###
#  Removed AWS
#  Removed HPCC in a VM
#  Removed Monitoring  
#  Removed ECLWatch  
#########################################
#
# Doc dirs to include.
#


PROJECT(docs
  VERSION ${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}.${HPCC_SEQUENCE}
)

#define_property(GLOBAL PROPERTY DOC_TARGETS BRIEF_DOCS "docs_en_us" FULL_DOCS "docs_en_us")
#mark_as_advanced(DOC_TARGETS)

#include(${CMAKE_MODULE_PATH}/docMacros.cmake)

set (DOC_LANG EN_US)
#set (DOCBOOK_RESOURCES ${CMAKE_CURRENT_BINARY_DIR}/../resources)
#set (DOCBOOK_XSL ${CMAKE_CURRENT_BINARY_DIR}/../resources/docbook-xsl)
set (DOC_IMAGES ${CMAKE_CURRENT_SOURCE_DIR}/)
#set (XML_CATALOG ${CMAKE_CURRENT_BINARY_DIR}/../BuildTools/catalog.xml)
#set (DOC_VERSION "${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}")
#set (FO_XSL ${CMAKE_CURRENT_BINARY_DIR}/../BuildTools/fo.xsl)
#set (VERSION_DIR ${CMAKE_CURRENT_BINARY_DIR}/)
#set (ECLIPSE_HTML_XSL ${CMAKE_CURRENT_BINARY_DIR}/../BuildTools/EclipseHelp.xsl)
set (ECL_REFERENCE_XML ${CMAKE_CURRENT_BINARY_DIR}/ECLReference/ECLReference.xml)
#set (HTML_HELP_XSL ${CMAKE_CURRENT_BINARY_DIR}/../resources/docbook-xsl/htmlhelp/htmlhelp.xsl)

set (PORTAL_HTML_DIR "${CMAKE_CURRENT_BINARY_DIR}/PortalHTML")

# Build image file list to add to source watch.
file(GLOB_RECURSE DOC_IMAGES_LIST images/*)

# Required directories to add.
#add_subdirectory(../common common)
#add_subdirectory(../BuildTools BuildTools)
#add_subdirectory(../resources resources)


add_subdirectory(DynamicESDL)
add_subdirectory(ECLLanguageReference)
add_subdirectory(ECLProgrammersGuide)
add_subdirectory(ECLStandardLibraryReference)
add_subdirectory(ECLReference)

add_subdirectory(HTMLHelp)
add_subdirectory(ECLPlayground)
add_subdirectory(HPCCClientTools)
add_subdirectory(HPCCCertify)
add_subdirectory(HPCCDataHandling)
add_subdirectory(HPCCDataTutorial)

add_subdirectory(HPCCSystemAdmin)
add_subdirectory(HPCCSpark)
add_subdirectory(HPCC4J)
add_subdirectory(Installing_and_RunningTheHPCCPlatform)
add_subdirectory(JDBC-driver)
add_subdirectory(RoxieReference)
add_subdirectory(ECLScheduler)
#add_subdirectory(ECLWatch)
add_subdirectory(VisualizingECL)
add_subdirectory(WsSQLUG)
add_subdirectory(PortalHTML)
add_subdirectory(ContainerizedHPCC)



# Docs automation
if (DOCS_AUTO)
#   add_subdirectory(XMLGeneration)
   add_subdirectory(ConfiguringHPCC)
endif()


#WIP -  Docs
#add_subdirectory(RuningHPCCinAmazonWebServicesEC2)


#GET_PROPERTY(_targets GLOBAL PROPERTY DOC_TARGETS)
#add_custom_target(docs_en_us ALL DEPENDS ${_targets})
