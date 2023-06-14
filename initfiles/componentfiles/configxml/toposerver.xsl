<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
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
        
<xsl:param name="process" select="'toposerver'"/>
<xsl:param name="instance" select="'s1'"/>
<xsl:param name="isLinuxInstance" select="0"/>
<xsl:param name="outputFilePath" select="'toposerver.xml'"/>
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


<xsl:variable name="toposerverNode" select="/Environment/Software/TopoServerProcess[@name=$process]"/>
<xsl:variable name="instanceNode" select="$toposerverNode/Instance[@name=$instance]"/>


<xsl:template match="/">
   <xsl:if test="not($toposerverNode)">
      <xsl:message terminate="yes">Topology Server '<xsl:value-of select="$process"/>' is undefined!</xsl:message>
   </xsl:if>
   <xsl:if test="not($instanceNode)">
      <xsl:message terminate="yes">Invalid instance '<xsl:value-of select="$instance"/></xsl:message>
   </xsl:if>
   <xsl:apply-templates select="$toposerverNode"/>
</xsl:template>


<xsl:template match="TopoServerProcess">
   <TopoServerProcess>
      <xsl:apply-templates select="@*[string(.) != '']"/>
      <xsl:copy-of select="/Environment/Software/Directories"/>
      <!--
      # Generated for configuration info. accessed by getGlobalConfig()
      -->
      <global>
        <expert>
          <xsl:copy-of select="/Environment/Software/Globals/@* | /Environment/Software/Globals/*"/>
        </expert>
	<xsl:copy-of select="/Environment/Hardware/cost"/>
      </global>
   </TopoServerProcess>
</xsl:template>


<!--propagate the attributes that need to be copied verbatim -->
<xsl:template match="@build|@name">
   <xsl:copy-of select="."/>
</xsl:template>


<!--eat attributes not needed to be generated or those which would be processed somewhere else -->
<xsl:template match="@buildSet|@description"/>

<!--propagate any unrecognized attributes to output-->
<xsl:template match="@*">
   <xsl:copy-of select="."/>
</xsl:template>

<xsl:template match="text()"/>

</xsl:stylesheet>
