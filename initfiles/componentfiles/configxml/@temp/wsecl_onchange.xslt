<?xml version="1.0" encoding="UTF-8"?>
<!--
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
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" omit-xml-declaration="yes"/>
<xsl:param name="attribName"/>
<xsl:param name="newValue"/>
<xsl:param name="oldValue"/>

<xsl:template match="/EspService">
    <xsl:copy>
        <xsl:if test="string(@roxieAddress)!='' and (string(@eclServer)!='' or string(@attributeServer)!='')">
            <xsl:message terminate="yes">Please specify either a Roxie address or an ECL server and Attribute server pair!</xsl:message>
        </xsl:if>
        <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
</xsl:template>

<xsl:template match="Properties/@defaultPort">
    <xsl:attribute name="defaultPort">
        <xsl:choose>
            <xsl:when test="string(/EspService/@eclServer)!=''">8002</xsl:when>
            <xsl:otherwise>8022</xsl:otherwise>
        </xsl:choose>
    </xsl:attribute>
</xsl:template>

<xsl:template match="Properties/@defaultSecurePort">
    <xsl:attribute name="defaultSecurePort">
        <xsl:choose>
            <xsl:when test="string(/EspService/@eclServer)!=''">18002</xsl:when>
            <xsl:otherwise>18022</xsl:otherwise>
        </xsl:choose>
    </xsl:attribute>
</xsl:template>

<xsl:template match="@*|node()">
    <xsl:copy>
        <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
</xsl:template>
</xsl:stylesheet>
