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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
 xmlns:xalan="http://xml.apache.org/xalan" exclude-result-prefixes="xalan" xmlns:fo="http://www.w3.org/1999/XSL/Format" xml:space="default">
    <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" omit-xml-declaration="yes"/>
    <xsl:param name="process" select="'thor'"/>
    <xsl:param name="isLinuxInstance" select="0"/>
    <xsl:param name="tempPath" select="'c:\temp\'"/>
    <xsl:param name="outputFilePath"/>

  <xsl:variable name="oldPathSeparator">
    <xsl:choose>
      <xsl:when test="$isLinuxInstance = 1">'\:'</xsl:when>
      <xsl:otherwise>'/$'</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="newPathSeparator">
    <xsl:choose>
      <xsl:when test="$isLinuxInstance = 1">'/$'</xsl:when>
      <xsl:otherwise>'\:'</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:template match="/">
        <xsl:apply-templates select="/Environment/Software/ThorCluster[@name=$process]"/>
    </xsl:template>

  <!-- directories -->
  <xsl:template match="@externalProgDir">
    <xsl:attribute name="{name()}">
      <xsl:variable name="tempdirvar" select="translate(., $oldPathSeparator, $newPathSeparator)"/>
      <xsl:if test="$isLinuxInstance=1 and not(starts-with($tempdirvar, '/'))">/</xsl:if>
      <xsl:value-of select="$tempdirvar"/>
    </xsl:attribute>
  </xsl:template>

  <!-- pluginsPath is a relative path handled differently from above directories -->
  <xsl:template match="@pluginsPath">
    <xsl:attribute name="pluginsPath">
      <xsl:variable name="path" select="translate(., '/$', '\:')"/>
      <xsl:variable name="path2">
        <xsl:choose>
          <xsl:when test="starts-with($path, '.\')">
            <!--skip .\ prefix -->
            <xsl:value-of select="substring($path, 3)"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="$path"/>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:variable>
      <xsl:variable name="len" select="string-length($path2)"/>
      <xsl:variable name="path3">
        <xsl:choose>
          <xsl:when test="$len > 0">
            <xsl:value-of select="$path2"/>
            <xsl:if test="not(substring($path2, number($len)-1, 1) = '\')">\</xsl:if>
          </xsl:when>
          <xsl:otherwise>plugins\</xsl:otherwise>
        </xsl:choose>
      </xsl:variable>
      <xsl:value-of select="translate($path3, $oldPathSeparator, $newPathSeparator)"/>
    </xsl:attribute>
  </xsl:template>
  
  <xsl:template match="@daliServers">
    <xsl:attribute name="{name()}">
      <xsl:variable name="dali" select="."/>
      <xsl:for-each select="/Environment/Software/DaliServerProcess[@name=$dali]/Instance">
        <xsl:call-template name="getNetAddress">
          <xsl:with-param name="computer" select="@computer"/>
        </xsl:call-template>
        <xsl:if test="string(@port) != ''">:<xsl:value-of select="@port"/>
        </xsl:if>
        <xsl:if test="position() != last()">, </xsl:if>
      </xsl:for-each>
    </xsl:attribute>
  </xsl:template>

  <!--eat attributes not needed to be generated or those which would be processed somewhere else -->
  <xsl:template match="@build|@buildSet|@computer|@description|@directory"/>

  <xsl:template match="@*">
    <xsl:copy-of select="."/>
  </xsl:template>

  <xsl:template match="ThorCluster">
    <Thor>
      <xsl:attribute name="name">
        <xsl:value-of select="@name"/>
      </xsl:attribute> 
      <xsl:attribute name="slaves">
        <xsl:value-of select="count(ThorSlaveProcess)"/>
      </xsl:attribute>
      <xsl:attribute name="nodeGroup">
        <xsl:choose>
          <xsl:when test="string(@nodeGroup) = ''">
            <xsl:value-of select="@name"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="@nodeGroup"/>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:attribute>
      <xsl:attribute name="outputNodeGroup">
        <xsl:choose>
          <xsl:when test="string(@outputNodeGroup) = ''">
            <xsl:choose>
              <xsl:when test="string(@nodeGroup) = ''">
                <xsl:value-of select="@name"/>
              </xsl:when>
              <xsl:otherwise>
                <xsl:value-of select="@nodeGroup"/>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="@outputNodeGroup"/>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:attribute>

      <xsl:apply-templates select="@*[string(.) != '']"/>
      
      <xsl:copy-of select="/Environment/Software/Directories"/> 
      <Debug>
        <xsl:for-each select="Debug/@*">
          <xsl:if test="string(.) != ''">
            <xsl:copy-of select="."/>
          </xsl:if>
        </xsl:for-each>
      </Debug>
      <SSH>
        <xsl:for-each select="SSH/@*">
          <xsl:if test="string(.) != ''">
            <xsl:copy-of select="."/>
          </xsl:if>
        </xsl:for-each>
      </SSH>
      <SwapNode>
        <xsl:for-each select="SwapNode/@*">
          <xsl:if test="string(.) != ''">
            <xsl:copy-of select="."/>
          </xsl:if>
        </xsl:for-each>
      </SwapNode>
    </Thor>
  </xsl:template>

  <xsl:template name="getNetAddress">
    <xsl:param name="computer"/>
    <xsl:value-of select="/Environment/Hardware/Computer[@name=$computer]/@netAddress"/>
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
  
</xsl:stylesheet>
