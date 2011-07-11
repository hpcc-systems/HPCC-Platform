<?xml version="1.0" encoding="UTF-8"?>
<!--

## Copyright © 2011 HPCC Systems.  All rights reserved.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
 xmlns:seisint="http://seisint.com" exclude-result-prefixes="seisint">
<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" omit-xml-declaration="yes"/>

<!--Note that this file no longer has service specific data, which has been moved into WsOnDemandServices.xml.
    This file loads the data file and picks the data it needs for the selected service and mode-->

<xsl:template match="/">
    <xsl:for-each select="Configuration">
        <xsl:choose>
            <xsl:when test="string(@mode)!=''">
                <xsl:apply-templates select="."/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:call-template name="message">
                    <xsl:with-param name="text">Please define a valid mode for <xsl:value-of select="@name"/> service. </xsl:with-param>
                </xsl:call-template>
                <xsl:copy>
                    <xsl:copy-of select="@name"/>
                </xsl:copy>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:for-each>
</xsl:template>

<xsl:template match="/Configuration">
    <xsl:variable name="mode" select="@mode"/>
    <xsl:variable name="status" select="@status"/>
    <xsl:if test="$mode != 'Normal' and $mode != 'ESDL' and $mode != 'Boolean' and $mode!='ESDLBoolean' and $mode != 'Focus' and $mode!='ESDLFocus'">
        <xsl:message terminate="yes">Configuration has invalid mode '<xsl:value-of select="$mode"/>'.  Valid choices are Normal, ESDL, Boolean, ESDLBoolean, Focus and ESDLFocus.</xsl:message>
    </xsl:if>
    <xsl:variable name="realMode">
        <xsl:choose>
            <xsl:when test="$mode='Normal' or $mode='Focus' or $mode='Boolean'">Normal</xsl:when>
            <xsl:otherwise>ESDL</xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    <xsl:variable name="svcDefs" select="document('WsOnDemandServices.xml')/Configurations"/>
    <xsl:if test="not($svcDefs)">
        <xsl:message terminate="yes">Service definition file 'WsOnDemandServices.xml' is missing or is invalid!</xsl:message>
    </xsl:if>
    <xsl:variable name="config" select="$svcDefs/*[name()=$realMode]/Configuration[@name=current()/@name]"/>
    <xsl:if test="not($config)">
        <xsl:message terminate="yes">Service '<xsl:value-of select="@name"/>' is not defined for mode '<xsl:value-of select="$realMode"/>' in the service definition file!</xsl:message>
    </xsl:if>
    <xsl:for-each select="$config">
        <xsl:copy>
            <xsl:variable name="path" select="normalize-space(@path)"/>
            <xsl:variable name="query" select="normalize-space(@queryname)"/>
            <xsl:variable name="queryname">
                <xsl:if test="$query!='' and not(contains($query, '.'))">
                    <xsl:value-of select="concat($path, '.')"/>
                </xsl:if>
                <xsl:value-of select="$query"/>
            </xsl:variable>
            <xsl:attribute name="name">
                <xsl:value-of select="normalize-space(@name)"/>
            </xsl:attribute>
            <xsl:attribute name="path">
                <xsl:value-of select="$path"/>
            </xsl:attribute>
            <xsl:attribute name="queryname">
                <xsl:choose>
                    <xsl:when test="string($mode)=$realMode">
                        <xsl:value-of select="$queryname"/>
                    </xsl:when>
                    <xsl:when test="$mode='Focus' or $mode='ESDLFocus'">
                        <xsl:value-of select="concat($path, '.FocusSearchService')"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="concat($path, '.TextSearchService')"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
            <xsl:attribute name="mode">
                <xsl:value-of select="$mode"/>
            </xsl:attribute>
            <xsl:attribute name="status">
                <xsl:choose>
                    <xsl:when test="$status!=''">
                        <xsl:value-of select="$status"/>
                    </xsl:when>
                    <xsl:otherwise>available</xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
            <xsl:copy-of select="@roxieSince"/>
        </xsl:copy>
    </xsl:for-each>
</xsl:template>

    <xsl:template name="message">
        <xsl:param name="text"/>
        <xsl:choose>
            <xsl:when test="function-available('seisint:message')">
            <xsl:variable name="dummy" select="seisint:message($text)"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:message terminate="no">
                    <xsl:value-of select="$text"/>
                </xsl:message>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
</xsl:stylesheet>
