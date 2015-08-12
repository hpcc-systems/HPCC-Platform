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
