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
SET(XSD_SOURCE_DIR ${HPCC_SOURCE_DIR}/initfiles/componentfiles/configxml)
SET(XSD_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR}/XSD )
SET(XML_TARGET_DIR ${CMAKE_CURRENT_SOURCE_DIR}/xml )

FILE(MAKE_DIRECTORY ${XSD_TARGET_DIR})

# get all *.xsd.in and *.xsd files
FILE(GLOB XSD_TEMPLATE_FILES RELATIVE "${XSD_SOURCE_DIR}" "${XSD_SOURCE_DIR}/*.xsd.in")
FILE(GLOB XSD_FILES RELATIVE "${XSD_SOURCE_DIR}" "${XSD_SOURCE_DIR}/*.xsd")

# Skip some files which cause problem
LIST(REMOVE_ITEM XSD_FILES "esp_service_wsecl2.xsd")
LIST(REMOVE_ITEM XSD_FILES "ftslave_linux.xsd")

# process and copy *.xsd.in files
FOREACH( XSD_TEMPLATE_FILE  ${XSD_TEMPLATE_FILES} )
   STRING(REGEX REPLACE "(.*).in" "\\1" XSD_FILE "${XSD_TEMPLATE_FILE}")
   CONFIGURE_FILE(${XSD_SOURCE_DIR}/${XSD_TEMPLATE_FILE} ${XSD_TARGET_DIR}/${XSD_FILE})
ENDFOREACH()

# copy *.xsd.in files
FOREACH( XSD_FILE  ${XSD_FILES} )
   CONFIGURE_FILE(${XSD_SOURCE_DIR}/${XSD_FILE} ${XSD_TARGET_DIR}/${XSD_FILE} COPYONLY)
ENDFOREACH()

# create complete *.xsd file list
FOREACH( XSD_TEMPLATE_FILE  ${XSD_TEMPLATE_FILES} )
   STRING(REGEX REPLACE "(.*).in" "\\1" XSD_FILE "${XSD_TEMPLATE_FILE}")
   SET(XSD_FILES ${XSD_FILES} ${XSD_FILE})
ENDFOREACH()

# Set dependencies
GET_PROPERTY(Current_Targets GLOBAL PROPERTY DOC_TARGETS)

SET(XSLTPROC_DEPENDENCIES)
FOREACH(T ${Current_Targets})
   IF("${T}" MATCHES "xsd_to_xml" )
      LIST(APPEND XSLTPROC_DEPENDENCIES  ${T})
   ENDIF()
ENDFOREACH()


# create MakeFiles to generate xsd file to xml file
XSD_TO_XML("${XSD_FILES}" ${XSD_TARGET_DIR} ${XML_TARGET_DIR})
