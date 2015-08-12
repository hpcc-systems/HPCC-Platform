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
