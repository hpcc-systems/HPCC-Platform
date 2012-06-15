################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
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
add_subdirectory(HPCCDataHandling)
add_subdirectory(HPCCDataTutorial)
add_subdirectory(IMDB)
add_subdirectory(Installing_and_RunningTheHPCCPlatform)
add_subdirectory(RDDERef)
add_subdirectory(RunningHPCCinaVirtualMachine)
add_subdirectory(UsingConfigManager)
add_subdirectory(HDFSConnector)
add_subdirectory(ECLPlayground)



#WIP -  Docs
#add_subdirectory(RuningHPCCinAmazonWebServicesEC2)

GET_PROPERTY(_targets GLOBAL PROPERTY DOC_TARGETS)
add_custom_target(docs ALL DEPENDS ${_targets})
