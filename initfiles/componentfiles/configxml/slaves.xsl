<?xml version="1.0" encoding="UTF-8"?>
<!--

## Copyright © 2011 HPCC Systems.  All rights reserved.
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
   <xsl:for-each select="RoxieServerProcess|RoxieSlaveProcess">
    <xsl:if test="string(@computer) != '' and not(@computer = preceding-sibling::RoxieServerProcess/@computer) and not(@computer = preceding-sibling::RoxieSlaveProcess/@computer)">
        <xsl:value-of select="/Environment/Hardware/Computer[@name=current()/@computer]/@netAddress"/>
        <xsl:text>
</xsl:text>
    </xsl:if>           
   </xsl:for-each>
</xsl:template>
        
</xsl:stylesheet>

