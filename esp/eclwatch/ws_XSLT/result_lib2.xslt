<?xml version="1.0" encoding="UTF-8"?>
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

<xsl:stylesheet version="1.0" 
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
    xmlns:xs="http://www.w3.org/2001/XMLSchema" 
    xmlns:xalan="http://xml.apache.org/xalan" 
    xmlns:msxsl="urn:schemas-microsoft-com:xslt" 
    exclude-result-prefixes="xalan">
    
    <xsl:param name="showCount" select="1"/>
    <xsl:param name="showHeader" select="1"/>
    <xsl:param name="escapeResults" select="count(/DFUBrowseDataResponse)"/>
    
    <xsl:include href="lib.xslt"/>
    
    
    <xsl:template match="Result">
        <xsl:param name="wsecl" select="false()"/>
        <xsl:apply-templates select="Exception"/>
        <xsl:apply-templates select="Dataset">
            <xsl:with-param name="wsecl" select="$wsecl"/>
        </xsl:apply-templates>
    </xsl:template>
    
    
    <xsl:template match="Dataset">
        <xsl:param name="wsecl" select="false()"/>
        <xsl:variable name="schema" select="../XmlSchema[@name=current()/@xmlSchema or @name=current()/@name]/*/xs:element[@name='Dataset']/*/*/xs:element[@name='Row']"/>
        <xsl:choose>
            <!--xsl:when test="$stage1Only"--><!--produce intermediate nodeset only-->
            <xsl:when test="0"><!--produce intermediate nodeset only-->
                <xsl:for-each select="Row">
                    <xsl:call-template name="grab-dataset">
                        <xsl:with-param name="data" select="."/>
                        <xsl:with-param name="schema" select="$schema"/>
                        <xsl:with-param name="level" select="1"/>
                        <xsl:with-param name="top" select="1"/>
                    </xsl:call-template>
                </xsl:for-each>
            </xsl:when>
            <xsl:otherwise>
                <xsl:if test="$wsecl">
                    <hr/>
                    <xsl:if test="@name">
                        <xsl:text>Dataset: </xsl:text>
                        <xsl:value-of select="@name"/>
                    </xsl:if>
                </xsl:if> 
                <table id="dataset_table" class="results-table" cellspacing="0">
                    <xsl:if test="boolean($showHeader)">
                        <xsl:call-template name="show-headers">
                            <xsl:with-param name="schema" select="$schema"/>
                            <xsl:with-param name="showCount" select="$showCount"/>
                        </xsl:call-template>
                    </xsl:if>
                    <xsl:for-each select="Row">
                        <xsl:variable name="nodes">
                            <xsl:call-template name="grab-dataset">
                                <xsl:with-param name="data" select="."/>
                                <xsl:with-param name="schema" select="$schema"/>
                                <xsl:with-param name="level" select="1"/>
                                <xsl:with-param name="top" select="1"/>
                            </xsl:call-template>
                        </xsl:variable>
                        <xsl:choose>
                            <xsl:when test="function-available('xalan:nodeset')">
                                <xsl:call-template name="show-row">
                                    <xsl:with-param name="nodes" select="xalan:nodeset($nodes)"/>
                                    <xsl:with-param name="level" select="1"/>
                                </xsl:call-template>
                            </xsl:when>
                            <xsl:when test="function-available('msxsl:node-set')">
                                <xsl:call-template name="show-row">
                                    <xsl:with-param name="nodes" select="msxsl:node-set($nodes)"/>
                                    <xsl:with-param name="level" select="1"/>
                                </xsl:call-template>
                            </xsl:when>
                            <xsl:otherwise>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:for-each>
                </table>
                <xsl:if test="$wsecl">
                    <br/>
                </xsl:if>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    
    <xsl:template name="show-headers">
        <xsl:param name="schema"/>
        <xsl:param name="showCount" select="0"/>
        <xsl:variable name="headers">
            <xsl:call-template name="grab-snodes">
                <xsl:with-param name="schema" select="$schema"/>
            </xsl:call-template>
        </xsl:variable>
        <colgroup>
           <col/>
           <xsl:for-each select="/DFUBrowseDataResponse/ColumnsHidden/ColumnHidden">
              <col id="_col_{position()}">
                <xsl:choose>
                    <xsl:when test="ColumnSize=0">
                       <xsl:attribute name="style">display:none</xsl:attribute>
                    </xsl:when>
                    <xsl:otherwise>
                       <xsl:attribute name="style">display:block</xsl:attribute>
                    </xsl:otherwise>
                </xsl:choose>
              </col>
           </xsl:for-each>
        </colgroup>
        <thead>
            <xsl:choose>
                <xsl:when test="function-available('xalan:nodeset')">
                    <xsl:call-template name="show-header">
                        <xsl:with-param name="headers" select="xalan:nodeset($headers)"/>
                        <xsl:with-param name="showCount" select="$showCount"/>
                    </xsl:call-template>
                </xsl:when>
                <xsl:when test="function-available('msxsl:node-set')">
                    <xsl:call-template name="show-header">
                        <xsl:with-param name="headers" select="msxsl:node-set($headers)"/>
                        <xsl:with-param name="showCount" select="$showCount"/>
                    </xsl:call-template>
                </xsl:when>
                <xsl:otherwise>
            </xsl:otherwise>
            </xsl:choose>
        </thead>
    </xsl:template>
    
    
    <xsl:template name="show-row">
        <xsl:param name="nodes"/>
        <xsl:param name="level"/>
        <tr BGCOLOR="#AAEFAA">
            <xsl:if test="($level mod 2)=0">
                <xsl:attribute name="class">blue</xsl:attribute>
            </xsl:if>
            <xsl:for-each select="$nodes//data[../@level=$level]">
                <xsl:variable name="pos" select="position()"/>
                <xsl:variable name="hide" select="/DFUBrowseDataResponse/ColumnsHidden/ColumnHidden[position() = $pos]/ColumnSize = 0"/>
                <td>
                    <xsl:variable name="p" select="../ancestor::row[not(@last)][1]"/>
                    <xsl:variable name="h" select="$p/@height"/>
                    <xsl:variable name="l" select="$p/@level"/>
                    <xsl:variable name="rowSpan">
                        <xsl:choose>
                            <xsl:when test="../@last and $l+$h > $level+../@height">
                                <xsl:value-of select="$l + $h - $level"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="../@height"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:variable>
                    <xsl:if test="$rowSpan>1">
                        <xsl:attribute name="rowspan">
                            <xsl:value-of select="$rowSpan"/>
                        </xsl:attribute>
                    </xsl:if>
                    <xsl:choose>
                        <xsl:when test="@escape">
                            <xsl:value-of select="." disable-output-escaping="no"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="." disable-output-escaping="yes"/>
                        </xsl:otherwise>
                    </xsl:choose>
                    <!--xsl:if test="$debug">
                        <xsl:value-of select="concat(' [', @debug, ' ', $level, ' H', ../@height, ' ', $h)"/>
                        <xsl:if test="../@last"> L</xsl:if>
                        <xsl:text>]</xsl:text>
                    </xsl:if-->
                </td>
            </xsl:for-each>
        </tr>
        <xsl:if test="$nodes//data[../@level=($level + 1)]">
            <xsl:call-template name="show-row">
                <xsl:with-param name="nodes" select="$nodes"/>
                <xsl:with-param name="level" select="1 + $level"/>
            </xsl:call-template>
        </xsl:if>
    </xsl:template>
    
    
    <xsl:template name="show-header">
        <xsl:param name="headers"/>
        <xsl:param name="showCount" select="0"/>
        <xsl:param name="height">
            <xsl:call-template name="max">
                <xsl:with-param name="nodes" select="$headers//*/@height"/>
            </xsl:call-template>
        </xsl:param>
        <xsl:param name="level">1</xsl:param>
        <tr valign="bottom">
            <xsl:if test="number($showCount)">
                <th rowspan="{$height}"/>
            </xsl:if>
            <xsl:for-each select="$headers//*[@height=$level]">
                <xsl:variable name="width" select="count(.//*[@leaf=1])"/>
                <th>
                    <xsl:if test="number(@leaf)">
                        <xsl:attribute name="rowspan"><xsl:value-of select="1+($height)-number(@height)"/></xsl:attribute>
                    </xsl:if>
                    <xsl:if test="number($width)">
                        <xsl:attribute name="colspan"><xsl:value-of select="$width"/></xsl:attribute>
                    </xsl:if>
                    <xsl:call-template name="id2string">
                        <xsl:with-param name="toconvert" select="translate(@name, '_', ' ')"/>
                    </xsl:call-template>
                </th>
            </xsl:for-each>
        </tr>
        <xsl:if test="number($height)>number($level)">
            <xsl:call-template name="show-header">
                <xsl:with-param name="headers" select="$headers"/>
                <xsl:with-param name="level" select="number($level)+1"/>
                <xsl:with-param name="height" select="$height"/>
            </xsl:call-template>
        </xsl:if>
    </xsl:template>
    
    
    <xsl:template name="grab-snodes">
        <xsl:param name="schema"/>
        <xsl:param name="height" select="1"/>
        <xsl:for-each select="$schema/xs:complexType/xs:sequence/xs:element">
            <xsl:if test="@name!='Row'">
                <h name="{@name}" height="{$height}">
                    <xsl:if test="not(./xs:complexType)">
                        <xsl:attribute name="leaf">1</xsl:attribute>
                    </xsl:if>
                    <xsl:call-template name="grab-snodes">
                        <xsl:with-param name="schema" select="."/>
                        <xsl:with-param name="height" select="number($height)+1"/>
                    </xsl:call-template>
                </h>
            </xsl:if>
            <xsl:if test="@name='Row'">
                <xsl:call-template name="grab-snodes">
                    <xsl:with-param name="schema" select="."/>
                    <xsl:with-param name="height" select="number($height)+1"/>
                </xsl:call-template>
            </xsl:if>
        </xsl:for-each>
    </xsl:template>
    
    
    <xsl:template name="getMaxLevel">
        <xsl:param name="nodes"/>
        <xsl:param name="level"/>
        <xsl:choose>
            <xsl:when test="$nodes//row">
                <xsl:call-template name="max">
                    <xsl:with-param name="nodes" select="$nodes//row/@level"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$level"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    
    <xsl:template name="grab-dataset">
        <xsl:param name="schema"/>
        <xsl:param name="data"/>
        <xsl:param name="level"/>
        <xsl:param name="top" select="0"/>
        <!--xsl:if test="$data"-->
            <xsl:variable name="nodes">
                <xsl:call-template name="grab-row">
                    <xsl:with-param name="level" select="$level"/>
                    <xsl:with-param name="data" select="$data"/>
                    <xsl:with-param name="schema" select="$schema"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:variable name="newLevel">
                <xsl:choose>
                    <xsl:when test="function-available('xalan:nodeset')">
                        <xsl:call-template name="getMaxLevel">
                            <xsl:with-param name="nodes" select="xalan:nodeset($nodes)"/>
                            <xsl:with-param name="level" select="$level"/>
                        </xsl:call-template>
                    </xsl:when>
                    <xsl:when test="function-available('msxsl:node-set')">
                        <xsl:call-template name="getMaxLevel">
                            <xsl:with-param name="nodes" select="msxsl:node-set($nodes)"/>
                            <xsl:with-param name="level" select="$level"/>
                        </xsl:call-template>
                    </xsl:when>
                    <xsl:otherwise>
                </xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <row level="{$level}" height="{1 + $newLevel - $level}">
                <xsl:if test="$top=0 and not($data/following-sibling::Row[1])">
                    <xsl:attribute name="last">1</xsl:attribute>
                </xsl:if>
                <!--xsl:if test="$debug">
                    <xsl:attribute name="debug">
                        <xsl:value-of select="$schema/../../../@name"/>
                    </xsl:attribute>
                </xsl:if-->
                <xsl:if test="number($top) and number($showCount)">
                    <data>
                        <!--xsl:if test="$debug">
                            <xsl:attribute name="debug">SNo</xsl:attribute>
                        </xsl:if-->
                        <xsl:value-of select="$rowStart+position()"/>
                    </data>
                </xsl:if>
                <xsl:choose>
                    <xsl:when test="function-available('xalan:nodeset')">
                        <xsl:copy-of select="xalan:nodeset($nodes)/*"/>
                    </xsl:when>
                    <xsl:when test="function-available('msxsl:node-set')">
                        <xsl:copy-of select="msxsl:node-set($nodes)/*"/>
                    </xsl:when>
                    <xsl:otherwise>
                </xsl:otherwise>
                </xsl:choose>
            </row>
            <xsl:if test="number($top)=0 and $data/following-sibling::Row[1]">
                <xsl:call-template name="grab-dataset">
                    <xsl:with-param name="level" select="1 + $newLevel"/>
                    <xsl:with-param name="data" select="$data/following-sibling::Row[1]"/>
                    <xsl:with-param name="schema" select="$schema"/>
                </xsl:call-template>
            </xsl:if>
        <!--/xsl:if-->
    </xsl:template>
    
    
    <xsl:template name="grab-row">
        <xsl:param name="schema"/>
        <xsl:param name="data"/>
        <xsl:param name="level"/>
        <xsl:for-each select="$schema/xs:complexType/xs:sequence/xs:element">
            <xsl:variable name="rowSchema" select="."/>
            <xsl:variable name="name" select="@name"/>
            <xsl:variable name="matchingData" select="$data/*[name()=$name]"/>          
            <xsl:choose>
                <xsl:when test="$matchingData">
                    <xsl:for-each select="$matchingData">
                        <xsl:call-template name="grab-data">
                            <xsl:with-param name="level" select="$level"/>
                            <xsl:with-param name="schema" select="$rowSchema"/>
                            <xsl:with-param name="data" select="."/>
                        </xsl:call-template>
                    </xsl:for-each>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:call-template name="grab-data">
                        <xsl:with-param name="level" select="$level"/>
                        <xsl:with-param name="schema" select="$rowSchema"/>
                        <xsl:with-param name="data" select="/.."/><!--empty node set-->
                    </xsl:call-template>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:for-each>
                
    </xsl:template>
    
    
    <xsl:template name="grab-data">
        <xsl:param name="schema"/>
        <xsl:param name="data"/>
        <xsl:param name="level"/>
        <xsl:choose>
            <!--xsl:when test="not($data)">
            </xsl:when-->
            <xsl:when test="$schema/xs:complexType/xs:sequence[@maxOccurs='unbounded']">
                <xsl:variable name="row-schema" select="$schema/*/*/xs:element[@name='Row']"/>
                <xsl:call-template name="grab-dataset">
                    <xsl:with-param name="level" select="$level"/>
                    <xsl:with-param name="data" select="$data/Row[1]"/>
                    <xsl:with-param name="schema" select="$row-schema"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:when test="$schema/xs:complexType">
                <xsl:call-template name="grab-row">
                    <xsl:with-param name="level" select="$level"/>
                    <xsl:with-param name="data" select="$data"/>
                    <xsl:with-param name="schema" select="$schema"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:when test="starts-with($schema/@type, 'setof_')">
                <data>
                    <!--xsl:if test="$debug">
                        <xsl:attribute name="debug">
                            <xsl:value-of select="$schema/@name"/>
                        </xsl:attribute>
                    </xsl:if-->
                    <xsl:choose>
                        <xsl:when test="$data/All">
                            <b>All</b>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:for-each select="$data/Item">
                                <xsl:value-of select="."/>
                                <xsl:if test="position()!=last()">, </xsl:if>
                            </xsl:for-each>
                        </xsl:otherwise>
                    </xsl:choose>
                </data>
            </xsl:when>
            <xsl:otherwise>
                <data>
                    <xsl:if test="number($escapeResults) and not(starts-with($schema/@name, '__html__'))">
                        <xsl:attribute name="escape">1</xsl:attribute>
                    </xsl:if>
                    <xsl:copy-of select="$data"/>
                </data>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    
    <xsl:template match="text()|comment()"/>
    
</xsl:stylesheet>
