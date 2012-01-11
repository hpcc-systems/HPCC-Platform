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

<xsl:stylesheet version="1.0" 
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:exsl="http://exslt.org/common"
    exclude-result-prefixes="exsl"
    >
    <xsl:output method="html"/>
    <xsl:param name="attribute"/>
    <xsl:param name="module"/>
    <xsl:param name="showCount" select="1"/>
    <xsl:param name="showHeader" select="1"/>
    <xsl:param name="rowStart" select="0"/>

<xsl:template match="/Result">
    <html>
        <head>
        <title>Result <xsl:value-of select="AttrName"/></title>
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
          <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
          <style type="text/css">
            body { background-color: #white;}
            table { border-collapse: collapse; border: double solid #0066CC; font: 10pt arial, helvetica, sans-serif; }
            table th { border: 2 solid #0066CC; background-color: #ddd;} 
            table td { border: 2 solid #0066CC; text-align:center;} 
        </style>
            
        </head>
    <body class="yui-skin-sam" onload="nof5();">
            <xsl:apply-templates/>
        </body>
    </html>
</xsl:template>

<xsl:template match="Dataset">
    <table cellspacing="0" cellpadding="2">
    <xsl:if test="boolean($showHeader)">
        <thead>
        <xsl:variable name="headers">
            <xsl:call-template name="grab-snodes">
                <xsl:with-param name="schema" select="../XmlSchema[@name=current()/@xmlSchema]//xs:element[@name='Row']" />
            </xsl:call-template>
        </xsl:variable>

        <xsl:variable name="height">
            <xsl:for-each select="exsl:node-set($headers)/*/@height">
                <xsl:sort data-type="number" order="descending"/>
                <xsl:if test="position() = 1">
                    <xsl:value-of select="."/>
                </xsl:if>
            </xsl:for-each>
        </xsl:variable>

        <xsl:call-template name="show-header">
            <xsl:with-param name="headers" select="exsl:node-set($headers)" />
            <xsl:with-param name="height" select="$height"/>
        </xsl:call-template>

        </thead>
    </xsl:if>
    <xsl:apply-templates>
        <xsl:with-param name="dataset-schema" select="../XmlSchema[@name=current()/@xmlSchema]"/>
    </xsl:apply-templates>
    </table>
</xsl:template>

<xsl:template match="Row">
    <xsl:param name="dataset-schema"/>
    <tr>
    <xsl:if test="boolean($showCount)"><th><xsl:value-of select="$rowStart+position()"/></th></xsl:if>
    <xsl:call-template name="show-results">
        <xsl:with-param name="schema" select="$dataset-schema//xs:element[@name='Row']"/>
        <xsl:with-param name="results" select="."/>
    </xsl:call-template>
    </tr>
</xsl:template>

<xsl:template name="show-results">
    <xsl:param name="schema"/>
    <xsl:param name="results"/>

    <xsl:if test="not(count($schema/xs:complexType))">
        <td>
            <xsl:value-of select="$results"/>
        </td>
    </xsl:if>

    <xsl:for-each select="$schema/xs:complexType/xs:sequence/xs:element">
        <xsl:call-template name="show-results">
            <xsl:with-param name="schema" select="current()"/>
            <xsl:with-param name="results" select="$results/*[name(.)=current()/@name]"/>
        </xsl:call-template>
    </xsl:for-each>
</xsl:template>


<xsl:template name="grab-snodes">
    <xsl:param name="schema"/>
    <xsl:param name="height" select="1"/>


    <xsl:for-each select="$schema/xs:complexType/xs:sequence/xs:element">
        <h height="{$height}" name="{@name}" width="{count(.//xs:element[count(./xs:complexType)=0])}" leaf="{count(./xs:complexType)}"/>
        <xsl:call-template name="grab-snodes">
            <xsl:with-param name="schema" select="."/>
            <xsl:with-param name="height" select="number($height)+1"/>
        </xsl:call-template>
    </xsl:for-each>

</xsl:template>

<xsl:template name="show-header">
    <xsl:param name="headers"/>
    <xsl:param name="height"/>
    <xsl:param name="level">1</xsl:param>
    
    <tr valign="bottom">
        <xsl:if test="number($level)=1 and boolean($showCount)">
            <th rowspan="{$height}"></th>
        </xsl:if>

        <xsl:for-each select="$headers/*[@height=$level]">
            <th>
                <xsl:if test="not(number(@leaf))">
                    <xsl:attribute name="rowspan"><xsl:value-of select="1+($height)-number(@height)"/></xsl:attribute>
                </xsl:if>
                <xsl:if test="number(@width)">
                    <xsl:attribute name="colspan"><xsl:value-of select="@width"/></xsl:attribute>
                </xsl:if>
                <xsl:value-of select="@name"/>
            </th>
        </xsl:for-each>
    </tr>


    <xsl:if test="number($height)>number($level)">
        <xsl:call-template name="show-header">
            <xsl:with-param name="headers" select="$headers"/>
            <xsl:with-param name="level" select="number($level)+1"/>
            <xsl:with-param name="height" select="$height"/>
        </xsl:call-template>
    </xsl:if>
</xsl:template>

<xsl:template match="text()|comment()"/>

</xsl:stylesheet>
