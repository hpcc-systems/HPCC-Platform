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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format" xml:space="default">
    <xsl:strip-space elements="*"/>
    <xsl:output method="text" indent="no" omit-xml-declaration="yes"/>
    <xsl:template match="text()"/>
    <xsl:param name="process" select="'unknown'"/>
    <xsl:param name="isLinuxInstance" select="0"/>
    
    <xsl:variable name="oldPathChars">
        <xsl:choose>
            <xsl:when test="$isLinuxInstance = 1">'\:'</xsl:when>
            <xsl:otherwise>'/$'</xsl:otherwise>
        </xsl:choose>   
    </xsl:variable>
    
    <xsl:variable name="newPathChars">
        <xsl:choose>
            <xsl:when test="$isLinuxInstance = 1">'/$'</xsl:when>
            <xsl:otherwise>'\:'</xsl:otherwise>
        </xsl:choose>   
    </xsl:variable>
    
    
    <xsl:variable name="processNode" select="/Environment/Software/EclPlusProcess[@name=$process]"/>
    
    
    <xsl:template match="/">
       <xsl:if test="not($processNode)">
          <xsl:message terminate="yes">The ECL Plus '<xsl:value-of select="$process"/>' is undefined!</xsl:message>
       </xsl:if>
       <xsl:apply-templates select="$processNode"/>
    </xsl:template>
    
    
    <xsl:template match="EclPlusProcess">
# ECLPLUS INI

server=<xsl:call-template name="GetEspBindingAddress"><xsl:with-param name="espBindingInfo" select="@server"/></xsl:call-template>
    </xsl:template>


<xsl:template name="GetEspBindingAddress">
    <xsl:param name="espBindingInfo"/><!--format is "esp_name/binding_name" -->
    <xsl:param name="addProtocolPrefix" select="'true'"/>

    <xsl:variable name="espName" select="substring-before($espBindingInfo, '/')"/>
    <xsl:variable name="bindingName" select="substring-after($espBindingInfo, '/')"/>
    <xsl:variable name="espNode" select="/Environment/Software/EspProcess[@name=$espName]"/>
    <xsl:variable name="bindingNode" select="$espNode/EspBinding[@name=$bindingName]"/>
      
    <xsl:if test="not($espNode) or not($bindingNode)">
        <xsl:message terminate="yes">Invalid ESP process and/or ESP binding information in'<xsl:value-of select="$espBindingInfo"/>'.</xsl:message>
    </xsl:if>
    <xsl:variable name="espComputer" select="$espNode/Instance[1]/@computer"/>
    <xsl:variable name="espIP" select="/Environment/Hardware/Computer[@name=$espComputer]/@netAddress"/>
    <xsl:if test="string($espIP) = ''">
        <xsl:message terminate="yes">The ESP server defined in '<xsl:value-of select="$espBindingInfo"/>' has invalid instance!</xsl:message>
    </xsl:if>
      
    <xsl:if test="string($bindingNode/@port) = ''">
        <xsl:message terminate="yes">The ESP binding defined in '<xsl:value-of select="$espBindingInfo"/>' has invalid port!</xsl:message>
    </xsl:if>
      
    <xsl:if test="boolean($addProtocolPrefix)">
        <xsl:if test="string($bindingNode/@protocol) = ''">
            <xsl:message terminate="yes">The ESP binding defined in '<xsl:value-of select="$espBindingInfo"/>' has no protocol!</xsl:message>
        </xsl:if>
        <xsl:value-of select="concat($bindingNode/@protocol, '://')"/>
    </xsl:if>
    <xsl:value-of select="concat($espIP, ':', $bindingNode/@port)"/>
</xsl:template>

</xsl:stylesheet>
