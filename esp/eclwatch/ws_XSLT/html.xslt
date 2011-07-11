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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <xsl:output method="html"/>
    <xsl:param name="attribute"/>
    <xsl:param name="module"/>
    <xsl:param name="showCount" select="1"/>
    <xsl:param name="showHeader" select="1"/>

<xsl:template match="/">
    <html>
        <head>
        <title>Result
        <xsl:if test="string-length($module)">
            <xsl:value-of select="$module"/>.<xsl:value-of select="$attribute"/>
        </xsl:if>
        </title>
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
          <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
          <style type="text/css">
            body { background-color: #white;}
            table { border-collapse: collapse; border: double solid #0066CC; font: 10pt arial, helvetica, sans-serif; }
            table th { background-color: #0066CC; color: #DFEFFF; } 
            table td { border: 2 solid #0066CC; text-align:center;} 
            .blue    { background-color: #DFEFFF; }
        </style>
        </head>
    <body class="yui-skin-sam" onload="nof5();">
            <xsl:apply-templates/>
        </body>
    </html>
</xsl:template>

<xsl:template match="/Dataset">
    <table cellspacing="0" cellpadding="2">
    <xsl:if test="boolean($showHeader)">
        <thead>
        <tr>
        <xsl:if test="boolean($showCount)"><th></th></xsl:if>
        <xsl:for-each select="Row[1]/*"><th><xsl:value-of select="name()"/></th></xsl:for-each>
        </tr>
        </thead>
    </xsl:if>
    <xsl:apply-templates/>
    </table>
</xsl:template>

<xsl:template match="/Dataset/Row">
    <tr>
    <xsl:if test="(position() mod 2)=0"><xsl:attribute name="class">blue</xsl:attribute></xsl:if>
    <xsl:if test="boolean($showCount)"><th><xsl:value-of select="position()"/></th></xsl:if>
    <xsl:apply-templates select="*"/>
    </tr>
</xsl:template>

<xsl:template match="*">
    <td>
    <xsl:value-of select="."/>
    </td>
</xsl:template>

<xsl:template match="did">
    <td>
    <a href="/DOXIE/HEADERFILESEARCHSERVICE?DID={current()}"><xsl:value-of select="."/></a>
    </td>
</xsl:template>

<xsl:template match="ssn">
    <td>
    <a href="/DOXIE/HEADERFILESEARCHSERVICE?SSN={current()}"><xsl:value-of select="."/></a>
    </td>
</xsl:template>

<xsl:template match="text()|comment()"/>

</xsl:stylesheet>
