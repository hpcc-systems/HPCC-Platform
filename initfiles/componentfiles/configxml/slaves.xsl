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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format" xml:space="default">
<xsl:strip-space elements="*"/>
<xsl:output method="text" indent="no"/>
<xsl:param name="process" select="'unknown'"/>
<xsl:template match="text()" />

<xsl:template  match="/Environment">
   <xsl:apply-templates select="Software/RoxieCluster[@name=$process]"/>
</xsl:template>

<xsl:template  match="RoxieCluster">
   <xsl:for-each select="RoxieServerProcess">
    <xsl:if test="string(@computer) != '' and not(@computer = preceding-sibling::RoxieServerProcess/@computer)">
        <xsl:value-of select="/Environment/Hardware/Computer[@name=current()/@computer]/@netAddress"/>
        <xsl:text>
</xsl:text>
    </xsl:if>           
   </xsl:for-each>
</xsl:template>
        
</xsl:stylesheet>

