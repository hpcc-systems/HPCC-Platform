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
xmlns:xs="http://www.w3.org/2001/XMLSchema" xml:space="default"
xmlns:seisint="http://seisint.com" exclude-result-prefixes="seisint">
  <xsl:output method="xml" indent="yes"/>

  <xsl:variable name="apos">'</xsl:variable>

  <xsl:template match="/Environment">
    <xsl:apply-templates select="Hardware/Computer"/>
    <xsl:apply-templates select="Software/ThorCluster"/>
  </xsl:template>

  <xsl:template match="Computer">
    <xsl:variable name="instanceIp" select="@netAddress"/>
    <xsl:variable name="hasInstance" select="/Environment/DeployComponents/*/Instance[@netAddress=$instanceIp]"/>
    <xsl:choose>
      <xsl:when test="$hasInstance">
        <xsl:variable name="dafileSrvNode" select="/Environment/DeployComponents/*[name()='DafilesrvProcess']/Instance[@netAddress=$instanceIp]"/>
        <xsl:choose>
          <xsl:when test="not($dafileSrvNode)">
            <xsl:call-template name="validationMessage">
              <xsl:with-param name="msg" select="concat('The computer ', $apos, $instanceIp, $apos, ' does not have a DafileSrvProcess Instance!')"/>
            </xsl:call-template>
          </xsl:when>
        </xsl:choose>
      </xsl:when>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="ThorCluster">
    <xsl:variable name="thorMasterNode" select="./ThorMasterProcess[@computer]"/>
    <xsl:choose>
      <xsl:when test="$thorMasterNode">
        <xsl:if test="string(@localThor) = 'true' and count(./ThorSlaveProcess[@computer!=$thorMasterNode/@computer]) > 0">
          <xsl:call-template name="validationMessage">
            <xsl:with-param name="msg" select="'Thor attribute localThor cannot be true when the master and slave processes are on different nodes!'"/>
          </xsl:call-template>
        </xsl:if>
      </xsl:when>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="validationMessage">
    <xsl:param name="type" select="'error'"/>
    <xsl:param name="compType"/>
    <xsl:param name="compName"/>
    <xsl:param name="msg"/>
    <!--ask deployment tool to display this validation error -->
    <!--format is like: error:EspProcess:esp1:This is a message.-->
    <xsl:variable name="encodedMsg" select="concat($type, ':', $compType, ':', $compName, ':', $msg)"/>
    <xsl:choose>
      <xsl:when test="function-available('seisint:validationMessage')">
        <xsl:variable name="dummy" select="seisint:validationMessage($encodedMsg)"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="message">
          <xsl:with-param name="text" select="concat('Validation for ', $compType, ' named ', $compName, ': ', $msg)"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="message">
    <xsl:param name="text"/>
    <xsl:choose>
      <xsl:when test="function-available('seisint:message')">
        <xsl:variable name="dummy" select="seisint:message($text)"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:message terminate="no">
          <xsl:value-of select="$text"/>
        </xsl:message>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>
