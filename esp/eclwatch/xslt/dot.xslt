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
<xsl:output method="xml" indent="yes" media-type="image/svg"/>


<xsl:template match="@*|node()">
  <xsl:copy>
    <xsl:apply-templates select="@*|node()" />
  </xsl:copy>
</xsl:template>

<xsl:template match="comment()"/>

<xsl:template match="svg:svg">
    <xsl:copy>
        <xsl:copy-of select="@*" />
        <xsl:attribute name="onload">resize_graph('<xsl:value-of select="@width"/>','<xsl:value-of select="@height"/>')</xsl:attribute>
        <xsl:attribute name="width">0</xsl:attribute>
        <xsl:attribute name="height">0</xsl:attribute>
        <xsl:apply-templates/>
    </xsl:copy>
</xsl:template>

<xsl:template match="svg:g[@class='node'] | svg:g[@class='edge']">
    <xsl:variable name="nodeid">
        <xsl:value-of select="svg:a/@xlink:href"/>
    </xsl:variable>    
    <xsl:copy>
        <xsl:copy-of select="@*" />
        <xsl:attribute name="id"><xsl:value-of select="$nodeid"/></xsl:attribute>
        <xsl:attribute name="onmouseover">show_popup('<xsl:value-of select="$nodeid"/>')</xsl:attribute>
        <xsl:attribute name="onmouseout">hide_popup()</xsl:attribute>
        <xsl:apply-templates/>
    </xsl:copy>
</xsl:template>

<xsl:template match="svg:g[@class='graph']">
    <xsl:copy>
        <xsl:copy-of select="@*" />
        <xsl:attribute name="style">font-family:Century, Times;font-size:14;</xsl:attribute>
        <xsl:apply-templates/>
    </xsl:copy>
</xsl:template>

<xsl:template match="svg:a">
<xsl:apply-templates/>
</xsl:template>

<xsl:template match="svg:title"/>

</xsl:stylesheet>
