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

<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
                              xmlns:xlink="http://www.w3.org/1999/xlink"
                              xmlns:svg="http://www.w3.org/2000/svg">
<xsl:output method="text" indent="yes"/>
      
<xsl:template match="Data">
<xsl:text/>FileName:    <xsl:value-of select="//filename"/>
<br>
RecordLen:   <xsl:value-of select="sum(./Field/@size)"/>
</br>
<br>Compressed:  N</br>
HeaderSize:  0
Live:        N
####################################################################################
fields:
#################################################################################
<xsl:apply-templates select="Field">
    <xsl:sort select="@position" data-type="number"/>
</xsl:apply-templates>
<xsl:text/>#################################################################################<xsl:text/>
</xsl:template>

<xsl:template match="Field">
    <xsl:param name="indent" select="''"/>
    <xsl:value-of select="$indent"/>
    <xsl:variable name="thesize" select="format-number(@size,'###')"/>
    <xsl:choose>
       <xsl:when test="$thesize='-15'">UNKNOWN</xsl:when>
       <xsl:otherwise><xsl:value-of select="$thesize"/></xsl:otherwise>
    </xsl:choose>
    <xsl:text> </xsl:text>
    <xsl:choose>
        <xsl:when test="@type='string'">char</xsl:when>
        <xsl:otherwise>byte</xsl:otherwise>
    </xsl:choose><xsl:text> </xsl:text>
    <xsl:value-of select="@name"/><xsl:text disable-output-escaping="yes">&#013;&#010;</xsl:text>
    <xsl:if test="Field">
    <xsl:apply-templates select="Field">
            <xsl:with-param name="indent" select="concat($indent, '    ')"/>
        <xsl:sort select="@position" data-type="number"/>
    </xsl:apply-templates>
    </xsl:if>

</xsl:template>

<xsl:template match="text()|comment()"/>

</xsl:stylesheet>
