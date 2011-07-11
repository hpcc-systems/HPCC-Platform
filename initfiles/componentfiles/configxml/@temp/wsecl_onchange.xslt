<?xml version="1.0" encoding="UTF-8"?>
<!--

## Copyright © 2011 HPCC Systems.  All rights reserved.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" omit-xml-declaration="yes"/>
<xsl:param name="attribName"/>
<xsl:param name="newValue"/>
<xsl:param name="oldValue"/>

<xsl:template match="/EspService">
    <xsl:copy>
        <xsl:if test="string(@roxieAddress)!='' and (string(@eclServer)!='' or string(@attributeServer)!='')">
            <xsl:message terminate="yes">Please specify either a Roxie address or an ECL server and Attribute server pair!</xsl:message>
        </xsl:if>
        <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
</xsl:template>

<xsl:template match="Properties/@defaultPort">
    <xsl:attribute name="defaultPort">
        <xsl:choose>
            <xsl:when test="string(/EspService/@eclServer)!=''">8002</xsl:when>
            <xsl:otherwise>8022</xsl:otherwise>
        </xsl:choose>
    </xsl:attribute>
</xsl:template>

<xsl:template match="Properties/@defaultSecurePort">
    <xsl:attribute name="defaultSecurePort">
        <xsl:choose>
            <xsl:when test="string(/EspService/@eclServer)!=''">18002</xsl:when>
            <xsl:otherwise>18022</xsl:otherwise>
        </xsl:choose>
    </xsl:attribute>
</xsl:template>

<xsl:template match="@*|node()">
    <xsl:copy>
        <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
</xsl:template>
</xsl:stylesheet>
