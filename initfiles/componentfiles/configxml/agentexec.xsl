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

  <xsl:param name="process" select="'unknown'"/>
  <xsl:param name="instance" select="'s1'"/>
  <xsl:param name="isLinuxInstance" select="0"/>

  <xsl:variable name="oldPathSeparator">
    <xsl:choose>
      <xsl:when test="$isLinuxInstance = 1">\:</xsl:when>
      <xsl:otherwise>/$</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="newPathSeparator">
    <xsl:choose>
      <xsl:when test="$isLinuxInstance = 1">/$</xsl:when>
      <xsl:otherwise>\:</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="pathSep">
    <xsl:choose>
      <xsl:when test="$isLinuxInstance = 1">/</xsl:when>
      <xsl:otherwise>\</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="processNode" select="/Environment/Software/EclAgentProcess[@name=$process]"/>


  <xsl:template match="/">
    <xsl:if test="not($processNode)">
      <xsl:message terminate="yes">
        The eclagent execution process '<xsl:value-of select="$process"/>' is undefined!
      </xsl:message>
    </xsl:if>
    <xsl:apply-templates select="$processNode"/>
  </xsl:template>


  <xsl:template match="EclAgentProcess">
    <agentexec>
      <xsl:attribute name="allowedPipePrograms">
        <xsl:value-of select="@allowedPipePrograms"/>
      </xsl:attribute>

      <xsl:attribute name="daliServers">
        <xsl:call-template name="getDaliServers">
          <!--xsl:with-param name="daliServers" select="."/-->
          <xsl:with-param name="daliservers" select="@daliServers"/>
        </xsl:call-template>
      </xsl:attribute>

      <xsl:attribute name="logDir">
        <xsl:call-template name="makeAbsolutePath">
          <xsl:with-param name="path" select="@logDir"/>
        </xsl:call-template>
      </xsl:attribute>

      <xsl:attribute name="name">
        <xsl:value-of select="@name"/>
      </xsl:attribute>

      <xsl:attribute name="pluginDirectory">
        <xsl:call-template name="makeAbsolutePath">
          <xsl:with-param name="path" select="@pluginDirectory"/>
        </xsl:call-template>
      </xsl:attribute>

      <xsl:attribute name="traceLevel">
        <xsl:value-of select="@traceLevel"/>
      </xsl:attribute>

      <xsl:attribute name="thorConnectTimeout">
        <xsl:value-of select="@thorConnectTimeout"/>
      </xsl:attribute>

      <xsl:copy-of select="/Environment/Software/Directories"/>  

    </agentexec>
  </xsl:template>


  <xsl:template name="getDaliServers">
    <xsl:param name="daliservers"/>
    <xsl:variable name="daliServerNode" select="/Environment/Software/DaliServerProcess[@name=$daliservers]"/>
    <xsl:if test="string($daliServerNode/Instance/@computer) = ''">
      <xsl:message terminate="yes">
        Dali server process '<xsl:value-of select="$daliservers"/>' or its instances are not defined! You must select one under the 'AgentExec' tab.
      </xsl:message>
    </xsl:if>
    <xsl:for-each select="$daliServerNode/Instance">
      <xsl:value-of select="/Environment/Hardware/Computer[@name=current()/@computer]/@netAddress"/>
      <xsl:if test="string(@port) != ''">:<xsl:value-of select="@port"/>
      </xsl:if>
      <xsl:if test="position() != last()">, </xsl:if>
    </xsl:for-each>
  </xsl:template>


  <xsl:template name="makeAbsolutePath">
    <xsl:param name="path"/>
    <xsl:variable name="newPath" select="translate($path, $oldPathSeparator, $newPathSeparator)"/>
    <xsl:choose>
      <xsl:when test="$isLinuxInstance=1">
        <xsl:value-of select="$newPath"/>
      </xsl:when>
      <xsl:when test="starts-with($newPath, '\')">
        <!--windows-->
        <xsl:value-of select="substring($newPath, 2)"/>
      </xsl:when>
      <xsl:otherwise>
        <!--windows-->
        <xsl:value-of select="$newPath"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>


</xsl:stylesheet>
