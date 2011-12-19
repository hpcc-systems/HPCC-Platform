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
        
<xsl:param name="process" select="'eclccserver'"/>
<xsl:param name="instance" select="'s1'"/>
<xsl:param name="isLinuxInstance" select="0"/>
<xsl:param name="outputFilePath" select="'c:\dep\ecl\eclccserver.xml'"/>
<xsl:param name="tempPath" select="'c:\temp\'"/>

<xsl:variable name="oldPathSeparator">
    <xsl:choose>
        <xsl:when test="$isLinuxInstance = 1">\:</xsl:when>
        <xsl:otherwise>'/$'</xsl:otherwise>
    </xsl:choose>   
</xsl:variable>


<xsl:variable name="newPathSeparator">
    <xsl:choose>
        <xsl:when test="$isLinuxInstance = 1">/$</xsl:when>
        <xsl:otherwise>'\:'</xsl:otherwise>
    </xsl:choose>   
</xsl:variable>


<xsl:variable name="newPathSeparatorChar">
    <xsl:choose>
        <xsl:when test="$isLinuxInstance = 1">'/'</xsl:when>
        <xsl:otherwise>'\'</xsl:otherwise>
    </xsl:choose>   
</xsl:variable>


<xsl:variable name="eclccserverNode" select="/Environment/Software/EclCCServerProcess[@name=$process]"/>
<xsl:variable name="instanceNode" select="$eclccserverNode/Instance[@name=$instance]"/>


<xsl:template match="/">
   <xsl:if test="not($eclccserverNode)">
      <xsl:message terminate="yes">ECL server '<xsl:value-of select="$process"/>' is undefined!</xsl:message>
   </xsl:if>
   <xsl:if test="not($instanceNode)">
      <xsl:message terminate="yes">Invalid instance '<xsl:value-of select="$instance"/></xsl:message>
   </xsl:if>
   <xsl:apply-templates select="$eclccserverNode"/>
</xsl:template>


<xsl:template match="EclCCServerProcess">
   <EclCCserver>

      <xsl:apply-templates select="@*[string(.) != '']"/>
      
     
      <xsl:apply-templates select="Option[string(@name) != '']" mode="copy"/>
   </EclCCserver>
</xsl:template>


<!--propagate the attributes that need to be copied verbatim -->
<xsl:template match="@build|@traceLevel|@name">
   <xsl:copy-of select="."/>
</xsl:template>


<!--eat attributes not needed to be generated or those which would be processed somewhere else -->
<xsl:template match="@buildSet|@description"/>


<!--handle attributes that need some form of translation/processing-->      
<xsl:template match="@logDir">
   <xsl:attribute name="{name()}">
      <xsl:value-of select="translate(., $oldPathSeparator, $newPathSeparator)"/>
   </xsl:attribute>
</xsl:template>

<xsl:template match="@daliServers">
   <xsl:attribute name="daliServers">
      <xsl:call-template name="getDaliServers">
         <xsl:with-param name="daliservers" select="."/>
      </xsl:call-template>
   </xsl:attribute>
</xsl:template>

<!--propagate any unrecognized attributes to output-->
<xsl:template match="@*">
   <xsl:copy-of select="."/>
</xsl:template>

<xsl:template match="text()"/>

<xsl:template name="getDaliServers">
   <xsl:param name="daliservers"/>
   <xsl:variable name="daliServerNode" select="/Environment/Software/DaliServerProcess[@name=$daliservers]"/>
   <xsl:if test="string($daliServerNode/Instance/@computer) = ''">
      <xsl:message terminate="yes">Dali server process '<xsl:value-of select="$daliservers"/>' or its instances are not defined!</xsl:message>
   </xsl:if>
   <xsl:for-each select="$daliServerNode/Instance">
     <xsl:value-of select="/Environment/Hardware/Computer[@name=current()/@computer]/@netAddress"/>
    <xsl:if test="string(@port) != ''">:<xsl:value-of select="@port"/>
    </xsl:if>
    <xsl:if test="position() != last()">, </xsl:if>
   </xsl:for-each>
</xsl:template>

<xsl:template name="GetPathName">
<xsl:param name="path"/>
   <xsl:if test="contains($path, '\')">
      <xsl:variable name="prefix" select="substring-before($path, '\')"/>
      <xsl:value-of select="concat($prefix, '\')"/>
      <xsl:call-template name="GetPathName">
         <xsl:with-param name="path" select="substring-after($path, '\')"/>
      </xsl:call-template>
   </xsl:if>
</xsl:template>

<xsl:template match="*" mode="copy">
 <xsl:element name="{local-name()}"><!--get rid of namespace, if any-->
   <!-- go process attributes and children -->
   <xsl:apply-templates select="@*[string(.) != '']|node()" mode="copy"/>
 </xsl:element>
</xsl:template>

<xsl:template match="@*" mode="copy">
 <xsl:attribute name="{local-name()}"><!--get rid of namespace, if any-->
   <xsl:value-of select="."/>
 </xsl:attribute>
</xsl:template>


</xsl:stylesheet>
