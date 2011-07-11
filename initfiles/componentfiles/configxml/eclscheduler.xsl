<?xml version="1.0" encoding="UTF-8"?>
<!--

## Copyright © 2011 HPCC Systems.  All rights reserved.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
 xmlns:xalan="http://xml.apache.org/xalan" exclude-result-prefixes="xalan">
 
<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" omit-xml-declaration="yes"/>
        
<xsl:param name="process" select="'eclscheduler'"/>
<xsl:param name="instance" select="'s1'"/>
<xsl:param name="isLinuxInstance" select="0"/>
<xsl:param name="outputFilePath" select="'eclscheduler.xml'"/>
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


<xsl:variable name="eclschedulerNode" select="/Environment/Software/EclSchedulerProcess[@name=$process]"/>
<xsl:variable name="instanceNode" select="$eclschedulerNode/Instance[@name=$instance]"/>


<xsl:template match="/">
   <xsl:if test="not($eclschedulerNode)">
      <xsl:message terminate="yes">EclScheduler '<xsl:value-of select="$process"/>' is undefined!</xsl:message>
   </xsl:if>
   <xsl:if test="not($instanceNode)">
      <xsl:message terminate="yes">Invalid instance '<xsl:value-of select="$instance"/></xsl:message>
   </xsl:if>
   <xsl:apply-templates select="$eclschedulerNode"/>
</xsl:template>


<xsl:template match="EclSchedulerProcess">
   <EclSchedulerProcess>
      <xsl:apply-templates select="@*[string(.) != '']"/>
      <xsl:copy-of select="/Environment/Software/Directories"/>
   </EclSchedulerProcess>
</xsl:template>


<!--propagate the attributes that need to be copied verbatim -->
<xsl:template match="@build|@name">
   <xsl:copy-of select="."/>
</xsl:template>


<!--eat attributes not needed to be generated or those which would be processed somewhere else -->
<xsl:template match="@buildSet|@description"/>

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

</xsl:stylesheet>
