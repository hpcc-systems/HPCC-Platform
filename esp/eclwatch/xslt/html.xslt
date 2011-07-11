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
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <title>Result
        <xsl:if test="string-length($module)">
            <xsl:value-of select="$module"/>.<xsl:value-of select="$attribute"/>
        </xsl:if>
        </title>
        <style type="text/css">
            body { background-color: #white;}
            table { border-collapse: collapse; border: double solid #0066CC; font: 10pt arial, helvetica, sans-serif; }
            table th { background-color: #0066CC; color: #DFEFFF; } 
            table td { border: 2 solid #0066CC; text-align:center;} 
            .blue    { background-color: #DFEFFF; }
        </style>
        </head>
        <body>
            <xsl:apply-templates/>
        </body>
    </html>
</xsl:template>

<xsl:template match="Exception">
    Exception: <br/>
    Reported by: <xsl:value-of select="Source"/><br/>
    Message: <xsl:value-of select="Message"/><br/>
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
