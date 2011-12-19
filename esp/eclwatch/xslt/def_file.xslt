<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
-->

<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
                              xmlns:xlink="http://www.w3.org/1999/xlink"
                              xmlns:svg="http://www.w3.org/2000/svg">
<xsl:output method="text" indent="no"/>
<xsl:param name="filename"/>

<xsl:template match="Data">
<xsl:text/>FileName:    <xsl:value-of select="$filename"/>
RecordLen:   <xsl:value-of select="sum(./Field/@size)"/>
Compressed:  N
HeaderSize:  0
Live:        N
####################################################################################
fields:
#################################################################################
<xsl:apply-templates select="Field">
    <xsl:sort select="@position" data-type="number"/>

</xsl:apply-templates>
<xsl:text/>#################################################################################<xsl:text/>
</xsl:template>

<xsl:template match="Field">
    <xsl:value-of select="format-number(@size,'###')"/><xsl:text> </xsl:text>
    <xsl:choose>
        <xsl:when test="@type='string'">char</xsl:when>
        <xsl:otherwise>byte</xsl:otherwise>
    </xsl:choose><xsl:text> </xsl:text>
    <xsl:value-of select="@name"/><xsl:text disable-output-escaping="yes">&#013;&#010;</xsl:text>
</xsl:template>

<xsl:template match="text()|comment()"/>

</xsl:stylesheet>
