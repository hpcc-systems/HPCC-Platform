<xsl:stylesheet version="1.0" 
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    >

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
