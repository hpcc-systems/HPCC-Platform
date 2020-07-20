<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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

      <xsl:attribute name="defaultMemoryLimitMB">
        <xsl:value-of select="@defaultMemoryLimitMB"/>
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

      <xsl:attribute name="analyzeWorkunit">
        <xsl:value-of select="@analyzeWorkunit"/>
      </xsl:attribute>

      <xsl:attribute name="thorConnectTimeout">
        <xsl:value-of select="@thorConnectTimeout"/>
      </xsl:attribute>
      <xsl:if test="@httpGlobalIdHeader">
        <xsl:attribute name="httpCallerIdHeader">
          <xsl:value-of select="@httpCallerIdHeader"/>
        </xsl:attribute>
        <xsl:attribute name="httpGlobalIdHeader">
          <xsl:value-of select="@httpGlobalIdHeader"/>
        </xsl:attribute>
      </xsl:if>
      <xsl:copy-of select="analyzerOptions"/>
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
