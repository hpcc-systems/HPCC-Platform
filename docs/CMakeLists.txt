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

cmake_minimum_required(VERSION 2.8)
PROJECT(docs)

define_property(GLOBAL PROPERTY DOC_TARGETS BRIEF_DOCS "docs" FULL_DOCS "docs")
mark_as_advanced(DOC_TARGETS)

include(${CMAKE_MODULE_PATH}/docMacros.cmake)

set (DOCBOOK_RESOURCES ${CMAKE_CURRENT_BINARY_DIR}/resources)
set (DOCBOOK_XSL ${CMAKE_CURRENT_BINARY_DIR}/resources/docbook-xsl)
set (DOC_IMAGES ${CMAKE_CURRENT_SOURCE_DIR}/)
set (XML_CATALOG ${CMAKE_CURRENT_BINARY_DIR}/BuildTools/catalog.xml)
set (DOC_VERSION "${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}")
set (FO_XSL ${CMAKE_CURRENT_BINARY_DIR}/BuildTools/fo.xsl)
set (VERSION_DIR ${CMAKE_CURRENT_BINARY_DIR}/)


if ( ${HPCC_MATURITY} STREQUAL "release" )
	set (DOC_VERSION "${DOC_VERSION}.${HPCC_SEQUENCE}")
else()
	set (DOC_VERSION "${DOC_VERSION}.${HPCC_SEQUENCE}${HPCC_MATURITY}")
endif()

# Build image file list to add to source watch.
file(GLOB_RECURSE DOC_IMAGES_LIST images/*)

# Required directories to add.
add_subdirectory(common)
add_subdirectory(BuildTools)
add_subdirectory(resources)


# Doc dirs to include.
add_subdirectory(ECLLanguageReference)
add_subdirectory(ECLProgrammersGuide)
#add_subdirectory(ECLReference)
add_subdirectory(ECLStandardLibraryReference)
add_subdirectory(HPCCClientTools)
add_subdirectory(HPCCCertify)
add_subdirectory(HPCCDataHandling)
add_subdirectory(HPCCDataTutorial)
add_subdirectory(IMDB)
add_subdirectory(InstantCloud)
add_subdirectory(Installing_and_RunningTheHPCCPlatform)
add_subdirectory(RDDERef)
add_subdirectory(RunningHPCCinaVirtualMachine)
add_subdirectory(UsingConfigManager)
add_subdirectory(ECLPlayground)
add_subdirectory(ECLPluginForEclipse)
add_subdirectory(ECLScheduler)



#WIP -  Docs
#add_subdirectory(RuningHPCCinAmazonWebServicesEC2)

GET_PROPERTY(_targets GLOBAL PROPERTY DOC_TARGETS)
add_custom_target(docs ALL DEPENDS ${_targets})
