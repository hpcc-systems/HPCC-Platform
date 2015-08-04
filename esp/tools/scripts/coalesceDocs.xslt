<?xml version="1.0" encoding="utf-8"?>
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