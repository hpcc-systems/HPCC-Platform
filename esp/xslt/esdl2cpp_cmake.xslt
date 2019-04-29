<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.
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
##############################################################################
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" xmlns:http="http://schemas.xmlsoap.org/wsdl/http/" xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/" xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">
    <xsl:output method="text" omit-xml-declaration="yes" indent="no"/>
    <xsl:param name="installdir"/>
<xsl:template match="esxdl">
<xsl:apply-templates select="EsdlService"/>
</xsl:template>

<xsl:template match="EsdlService">
    <xsl:variable name="servicename" select="@name"/>
<xsl:text></xsl:text>cmake_minimum_required(VERSION 3.0)
project (<xsl:value-of select="$servicename"/>ServicePlugin)
set(CMAKE_INSTALL_PREFIX "<xsl:value-of select="$installdir"/>")
if (("${HPCC_SOURCE_DIR}" STREQUAL "") OR ("${HPCC_BUILD_DIR}" STREQUAL "") OR ("${CMAKE_BUILD_TYPE}" STREQUAL
 ""))
    message (FATAL_ERROR "Please specify HPCC_SOURCE_DIR, HPCC_BUILD_DIR and CMAKE_BUILD_TYPE")
endif ()

set (CMAKE_CXX_FLAGS "-fPIC -std=c++11")
include_directories ("${HPCC_SOURCE_DIR}/system/jlib"
                     "${HPCC_SOURCE_DIR}/system/include"
                     )
link_directories ("${HPCC_BUILD_DIR}/${CMAKE_BUILD_TYPE}/libs" .)

add_library (<xsl:value-of select="$servicename"/>Service SHARED <xsl:value-of select="$servicename"/>ServiceBase.cpp
                                       <xsl:value-of select="$servicename"/>ServiceBase.hpp
                                       <xsl:value-of select="$servicename"/>Service.cpp
                                       <xsl:value-of select="$servicename"/>Service.hpp
                                       )
target_link_libraries (<xsl:value-of select="$servicename"/>Service jlib)
install(TARGETS <xsl:value-of select="$servicename"/>Service DESTINATION plugins)<xsl:text>
</xsl:text>
</xsl:template>
</xsl:stylesheet>
