<?xml version="1.0" encoding="UTF-8"?>
<!--
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
