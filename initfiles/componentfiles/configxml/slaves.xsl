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

