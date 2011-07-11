<?xml version="1.0" encoding="utf-8"?>
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

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output indent="yes"/>
<xsl:param name="doc2" select="'d:/scapps_dev/xmldiffs/sparse2.xml'"/>

<xsl:variable name="d" select="document($doc2)"/>

<xsl:template match="/">
    <xsl:if test="string($doc2)='' or not($d)">
        <xsl:message terminate="yes">The second document must be specified with the 'doc' parameter!</xsl:message>
    </xsl:if>   
    <xsl:apply-templates select="*|$d/*">
        <xsl:with-param name="n" select="*|$d/*"/>
    </xsl:apply-templates>
</xsl:template>

<xsl:template match="*">
<xsl:param name="n"/>
     <xsl:variable name="name" select="name()"/>
    <xsl:variable name="s" select="$n[name()=$name]"/>
    <xsl:if test="count(.|$s[1])=1">
        <xsl:copy>
            <xsl:copy-of select="@*"/>
            <xsl:apply-templates select="$s/*">
                <xsl:with-param name="n" select="$s/*"/>
            </xsl:apply-templates>
        </xsl:copy>
    </xsl:if>
</xsl:template>

</xsl:stylesheet>