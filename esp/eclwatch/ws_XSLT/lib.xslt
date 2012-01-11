<xsl:stylesheet version="1.0" 
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    >

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

<xsl:template name="id2string">
    <xsl:param name="toconvert"/>

    <xsl:call-template name="_id2string">
        <xsl:with-param name="toconvert" select="translate($toconvert, '_', ' ')"/>
    </xsl:call-template>

</xsl:template>


<xsl:template name="_id2string">
    <xsl:param name="toconvert"/>
    <xsl:param name="hadcap" select="false"/>
    <xsl:if test="string-length($toconvert) > 0">
        <xsl:variable name="f" select="substring($toconvert, 1, 1)"/>
        <xsl:variable name="iscap" select="contains('ABCDEFGHIJKLMNOPQRSTUVWXYZ', $f)"/>
        <xsl:variable name="nextcap" select="contains('ABCDEFGHIJKLMNOPQRSTUVWXYZ', substring($toconvert, 2, 1))"/>
        <xsl:if test="$iscap and (($hadcap=false) or ($nextcap=false))">
            <xsl:text> </xsl:text>
        </xsl:if>
        <xsl:value-of select="$f"/>
        <xsl:call-template name="_id2string">
            <xsl:with-param name="toconvert" select="substring($toconvert, 2)"/>
            <xsl:with-param name="hadcap" select="$iscap"/>
        </xsl:call-template>
    </xsl:if>
</xsl:template>

<xsl:template name="min">
   <xsl:param name="nodes" select="/.." />
   <xsl:choose>
      <xsl:when test="not($nodes)">
         <xsl:value-of select="number('NaN')" />
      </xsl:when>
      <xsl:otherwise>
         <xsl:for-each select="$nodes">
            <xsl:sort data-type="number" />
            <xsl:if test="position() = 1">
               <xsl:value-of select="number(.)" />
            </xsl:if>
         </xsl:for-each>
      </xsl:otherwise>
   </xsl:choose>
</xsl:template>

<xsl:template name="max">
   <xsl:param name="nodes" select="/.." />
   <xsl:choose>
      <xsl:when test="not($nodes)">
         <xsl:value-of select="number('NaN')" />
      </xsl:when>
      <xsl:otherwise>
         <xsl:for-each select="$nodes">
            <xsl:sort data-type="number" order="descending" />
            <xsl:if test="position() = 1">
               <xsl:value-of select="number(.)" />
            </xsl:if>
         </xsl:for-each>
      </xsl:otherwise>
   </xsl:choose>
</xsl:template>

<xsl:template name="comma_separated">
   <xsl:param name="value"/>
   <xsl:param name="pos" select="string-length($value)+1"/>
      <xsl:if test="$pos > 4">
        <xsl:call-template name="comma_separated">
            <xsl:with-param name="value" select="$value"/>
            <xsl:with-param name="pos" select="$pos - 3"/>
        </xsl:call-template>
        <xsl:text>,</xsl:text>
      </xsl:if>
   <xsl:value-of select="substring($value,$pos - 3,3)" />
</xsl:template>

</xsl:stylesheet>
