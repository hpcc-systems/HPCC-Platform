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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default">
<xsl:output method="text"/>

<xsl:param name="method" select="'EspMethod'"/>

<!--by default, this stylesheet translates response from 3rd party server.
    Set the following to 0 to generate request to be sent to 3rd party server-->
<xsl:param name="response" select="1"/>
<xsl:param name="def" select="1"/>
<xsl:param name="rename" select="0"/>

<xsl:include href="xml2common.xsl"/>


<xsl:template match="/">
    <xsl:choose>
        <xsl:when test="$response=0">
            <xsl:call-template name="generate"/>
        </xsl:when>
        <xsl:otherwise>
            <xsl:for-each select="$root">
                <xsl:call-template name="generate"/>
            </xsl:for-each>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>


<xsl:template name="generate">
    <xsl:choose>
        <xsl:when test="$def=1">
            <xsl:text>fields:
</xsl:text>
            <xsl:apply-templates select="*[1]" mode="def"/>
            <xsl:text>1     char      lf
</xsl:text>
        </xsl:when>
        <xsl:otherwise>
            <xsl:apply-templates select="*[1]" mode="map"/>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template match="*[*]" mode="def">
    <xsl:apply-templates select="*" mode="def"/>
</xsl:template>

<xsl:template match="*[*]" mode="map">
    <xsl:apply-templates select="*" mode="map"/>
</xsl:template>

<xsl:template match="*" mode="def">
    <xsl:choose>
        <xsl:when test="starts-with(., '[')">
            <xsl:variable name="slen" select="translate(., '[]', '')"/>
            <xsl:variable name="spaces" select="'          '"/>
            <xsl:value-of select="concat($slen, substring($spaces, 5+string-length($slen)), 'char      ', name(), $eol)"/>
        </xsl:when>
    </xsl:choose>
</xsl:template>

<xsl:template match="*" mode="map">
    <xsl:variable name="outName">
        <xsl:choose>
            <xsl:when test="$rename=1">
                <xsl:call-template name="getOutName">
                    <xsl:with-param name="inName" select="name()"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="name()"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    <xsl:value-of select="concat(name(), ',', $outName, ',', '1', $eol)"/>
</xsl:template>

<xsl:template match="@*|text()" mode="def"/>
<xsl:template match="@*|text()" mode="map"/>

</xsl:stylesheet>
