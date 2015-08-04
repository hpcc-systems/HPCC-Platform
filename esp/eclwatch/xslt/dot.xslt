<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
