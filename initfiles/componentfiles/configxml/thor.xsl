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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
 xmlns:fo="http://www.w3.org/1999/XSL/Format" xml:space="default">
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
    <xsl:variable name="masterport" select="@masterport"/>
    <xsl:variable name="slaveport" select="@slaveport"/>
    <xsl:variable name="masternode" select="ThorMasterProcess/@computer"/>
    <xsl:for-each select="/Environment/Software/ThorCluster[@name!=string($process)]">
      <xsl:variable name="thismasterport" select="@masterport"/>
      <xsl:variable name="thisthor" select="@name"/>
        <xsl:for-each select="ThorMasterProcess[@computer=$masternode]">
          <xsl:if test="string($thismasterport)=string($masterport)">
            <xsl:message terminate="yes">
              There cannot be more than one ThorCluster ('<xsl:value-of select="$process"/>' and '<xsl:value-of select="$thisthor"/>') with the same thor master '"<xsl:value-of select="$masternode"/>' and same thor master port '<xsl:value-of select="$masterport"/>!
            </xsl:message>
          </xsl:if>
        </xsl:for-each>
    </xsl:for-each>

    <xsl:for-each select="ThorSlaveProcess">
      <xsl:variable name="slavenode" select="@computer"/>
      <xsl:if test=" count(/Environment/Software/DafilesrvProcess/Instance[@computer=$slavenode]) = 0">
         <xsl:message terminate="yes">
            '<xsl:value-of select="$slavenode"/>' slave is not assigned to a Dafilesrv process.
          </xsl:message>
       </xsl:if>
        <xsl:if test=" count(/Environment/Software/FTSlaveProcess/Instance[@computer=$slavenode]) = 0">
          <xsl:message terminate="yes">
            '<xsl:value-of select="$slavenode"/>' slave is not assigned to a FT Slave process.
          </xsl:message>
      </xsl:if>
      <xsl:for-each select="/Environment/Software/ThorCluster[@name!=string($process)]">
        <xsl:variable name="thisslaveport" select="@slaveport"/>
        <xsl:if test="string($thisslaveport)=string($slaveport)">
            <xsl:if test="count(ThorSlaveProcess[@computer=$slavenode]) > 0">
              <xsl:message terminate="yes">
                There cannot be more than one ThorCluster ('<xsl:value-of select="$process"/>' and '<xsl:value-of select="@name"/>') with the same thor slave '"<xsl:value-of select="$slavenode"/>' and thor slave port '<xsl:value-of select="$slaveport"/>'!
              </xsl:message>
          </xsl:if>
        </xsl:if>
      </xsl:for-each>
    </xsl:for-each>

    <xsl:for-each select="ThorSpareProcess">
      <xsl:variable name="thorSpareNode" select="@computer"/>
            <xsl:if test=" count(/Environment/Software/DafilesrvProcess/Instance[@computer=$thorSpareNode]) = 0">
              <xsl:message terminate="yes">
                '<xsl:value-of select="$thorSpareNode"/>' spare is not assigned to a Dafilesrv process.
              </xsl:message>
          </xsl:if>
          <xsl:if test=" count(/Environment/Software/FTSlaveProcess/Instance[@computer=$thorSpareNode]) = 0">
            <xsl:message terminate="yes">
              '<xsl:value-of select="$thorSpareNode"/>' spare is not assigned to a FT Slave process.
            </xsl:message>
        </xsl:if>
    </xsl:for-each>

    <Thor>
      <xsl:attribute name="name">
        <xsl:value-of select="@name"/>
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
      <xsl:copy-of select="/Environment/Hardware/cost"/>
      @XSL_PLUGIN_DEFINITION@
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
