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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
xmlns:xs="http://www.w3.org/2001/XMLSchema" xml:space="default"
xmlns:seisint="http://seisint.com" exclude-result-prefixes="seisint">
  <xsl:output method="xml" indent="yes"/>

  <xsl:variable name="apos">'</xsl:variable>

  <xsl:template match="/Environment">
    <xsl:apply-templates select="Hardware/Computer"/>
    <xsl:apply-templates select="Software/ThorCluster"/>
    <xsl:apply-templates select="Software/Topology"/>
    <xsl:apply-templates select="Software"/>
  </xsl:template>

  <xsl:template match="Environment/Software/*">
    <xsl:for-each select="descendant-or-self::*[name()='Cluster']">
    <xsl:variable select="@name" name="elem"/>
      <xsl:if test= "count(preceding-sibling::*[@name=$elem]) = 0 and count(following-sibling::*[@name=$elem]) &gt; 0">
        <xsl:variable select="count(following-sibling::*[@name=$elem])+1" name="numOccurrences"/>
        <xsl:call-template name="validationMessage">
            <xsl:with-param name="msg" select="concat('/Environment/Software/Topology/Cluster[@name=',$elem,']',' occurs ', $numOccurrences, ' times. Max occurrence must be 1.')"/>
        </xsl:call-template>        
      </xsl:if> 
    </xsl:for-each>
    <xsl:variable select="@name" name="elem1"/>
        <xsl:if test="(translate($elem1,'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_',''))" >
      <xsl:call-template name="validationMessage">
        <xsl:with-param name="msg" select=" concat('Invalid character[@name=' ,$elem1, ']') "/>
      </xsl:call-template>
    </xsl:if>
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
