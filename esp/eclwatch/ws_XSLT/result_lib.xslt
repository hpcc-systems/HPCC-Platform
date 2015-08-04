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

<xsl:stylesheet version="1.0" 
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
    xmlns:xs="http://www.w3.org/2001/XMLSchema" 
    xmlns:exsl="http://exslt.org/common" 
    xmlns:msxsl="urn:schemas-microsoft-com:xslt" 
    exclude-result-prefixes="exsl">
    
    <xsl:param name="showCount" select="1"/>
    <xsl:param name="showHeader" select="1"/>
    <xsl:param name="escapeResults" select="count(/WUResultResponse)"/>

    <!--the following is enabled for debugging only to 
          produce intermediate nodeset only-->
    <xsl:param name="stage1Only" select="0"/>
    
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
        <xsl:variable name="schema" select="../XmlSchema[@name=current()/@xmlSchema or @name=current()/@name]/*/xs:element[@name='Dataset']/*[1]/*[1]/xs:element[1]"/>
        <xsl:variable name="rowName">
            <xsl:call-template name="getMatchingName">
                <xsl:with-param name="schema" select="$schema"/>
                <xsl:with-param name="parent" select="."/>
            </xsl:call-template>
        </xsl:variable>
        <xsl:choose>
            <xsl:when test="$stage1Only"><!--produce intermediate nodeset only-->
                <xsl:for-each select="*[name()=$rowName]">
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
                    <xsl:if test="@name">
                        <b>
                            <xsl:text>Dataset: </xsl:text>
                            <xsl:value-of select="@name"/>
                        </b>
                    </xsl:if>
                </xsl:if>
                <table class="results" cellspacing="0" frame="box" rules="all">
                    <xsl:if test="boolean($showHeader)">
                        <xsl:call-template name="show-headers">
                            <xsl:with-param name="schema" select="$schema"/>
                            <xsl:with-param name="showCount" select="$showCount"/>
                        </xsl:call-template>
                    </xsl:if>
                    <xsl:for-each select="*[name()=$rowName]">
                        <xsl:variable name="nodes">
                            <xsl:call-template name="grab-dataset">
                                <xsl:with-param name="data" select="."/>
                                <xsl:with-param name="schema" select="$schema"/>
                                <xsl:with-param name="level" select="1"/>
                                <xsl:with-param name="top" select="1"/>
                            </xsl:call-template>
                        </xsl:variable>
                        <xsl:choose>
                            <xsl:when test="function-available('exsl:node-set')">
                                <xsl:call-template name="show-row">
                                    <xsl:with-param name="nodes" select="exsl:node-set($nodes)"/>
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
                <xsl:with-param name="node" select="*[1]"/>
            </xsl:call-template>
        </xsl:variable>
        <thead>
            <xsl:choose>
                <xsl:when test="function-available('exsl:node-set')">
                    <xsl:call-template name="show-header">
                        <xsl:with-param name="headers" select="exsl:node-set($headers)"/>
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
        <tr>
            <xsl:if test="($level mod 2)=0">
                <xsl:attribute name="class">blue</xsl:attribute>
            </xsl:if>
            <xsl:for-each select="$nodes//data[../@level=$level]">
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
                        <xsl:with-param name="toconvert" select="translate(@name, '_-', '  ')"/>
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
    
    <xsl:template name="grab-snodes-item">
        <xsl:param name="schema"/>
        <xsl:param name="node"/>
        <xsl:param name="height" select="1"/>
        <xsl:param name="supportsAll" select="0"/>
            <xsl:variable name="name">
                <xsl:call-template name="getMatchingName">
                    <xsl:with-param name="schema" select="."/>
                    <xsl:with-param name="parent" select="$node"/>
                    <xsl:with-param name="supportsAll" select="$supportsAll"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:choose>
                <xsl:when test="name()='xs:attribute'">
                    <h name="{concat('@',$name)}" height="{$height}" leaf="1"/>
                </xsl:when>
                <xsl:when test="string(../@maxOccurs)!='unbounded'">
                    <xsl:variable name="childNode" select="$node/*[name()=$name]"/>
                    <h name="{$name}" height="{$height}">
                        <xsl:if test="not(./xs:complexType)">
                            <xsl:attribute name="leaf">1</xsl:attribute>
                        </xsl:if>
                        <xsl:call-template name="grab-snodes">
                            <xsl:with-param name="schema" select="."/>
                            <xsl:with-param name="node" select="$childNode"/>
                            <xsl:with-param name="height" select="number($height)+1"/>
                        </xsl:call-template>
                    </h>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:choose>
                        <xsl:when test="@type">
                            <h name="{$name}" height="{$height}" leaf="1"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:call-template name="grab-snodes">
                                <xsl:with-param name="schema" select="."/>
                                <xsl:with-param name="node" select="$node/*[name()=$name][1]"/>
                                <xsl:with-param name="height" select="$height"/>
                            </xsl:call-template>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:otherwise>
            </xsl:choose>
    </xsl:template>

    <xsl:template name="grab-snodes">
        <xsl:param name="schema"/>
        <xsl:param name="node"/>
        <xsl:param name="height" select="1"/>
        <xsl:for-each select="$schema/xs:complexType/xs:attribute">
            <xsl:call-template name="grab-snodes-item">
                <xsl:with-param name="schema" select="$schema"/>
                <xsl:with-param name="node" select="$node"/>
                <xsl:with-param name="height" select="$height"/>
            </xsl:call-template>
        </xsl:for-each>
        <xsl:variable name="supportsAll" select="$schema/xs:complexType/xs:sequence/xs:element[not(@type) and not(node()) and @name='All']"/>
        <xsl:for-each select="$schema/xs:complexType/xs:sequence/xs:element">
            <xsl:if test="@type or node() or @name!='All'">
		    <xsl:call-template name="grab-snodes-item">
		        <xsl:with-param name="schema" select="$schema"/>
		        <xsl:with-param name="node" select="$node"/>
		        <xsl:with-param name="height" select="$height"/>
		        <xsl:with-param name="supportsAll" select="$supportsAll"/>
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
                    <xsl:when test="function-available('exsl:node-set')">
                        <xsl:call-template name="getMaxLevel">
                            <xsl:with-param name="nodes" select="exsl:node-set($nodes)"/>
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
                <xsl:if test="$top=0 and not($data/following-sibling::*[1])">
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
                    <xsl:when test="function-available('exsl:node-set')">
                        <xsl:copy-of select="exsl:node-set($nodes)/*"/>
                    </xsl:when>
                    <xsl:when test="function-available('msxsl:node-set')">
                        <xsl:copy-of select="msxsl:node-set($nodes)/*"/>
                    </xsl:when>
                    <xsl:otherwise>
                </xsl:otherwise>
                </xsl:choose>
            </row>
            <xsl:if test="number($top)=0 and $data/following-sibling::*[1]">
                <xsl:call-template name="grab-dataset">
                    <xsl:with-param name="level" select="1 + $newLevel"/>
                    <xsl:with-param name="data" select="$data/following-sibling::*[1]"/>
                    <xsl:with-param name="schema" select="$schema"/>
                </xsl:call-template>
            </xsl:if>
        <!--/xsl:if-->
    </xsl:template>
    
    <xsl:template name="grab-column">
            <xsl:param name="schema"/>
            <xsl:param name="level"/>
            <xsl:param name="rowSchema"/>
            <xsl:param name="name"/>
            <xsl:param name="dname"/>
            <xsl:param name="hasAll" select="0"/>
            <xsl:param name="matchingData"/>
            <xsl:param name="matchingData2"/>
            <xsl:choose>
                <xsl:when test="$hasAll">
                    <row level="{$level}" height="1">
                        <data>
                            <b>All</b>
                       </data>
                   </row>
               </xsl:when>
               <xsl:when test="$matchingData|$matchingData2">
                    <xsl:choose>
                        <xsl:when test="$rowSchema/@maxOccurs">
                              <xsl:call-template name="grab-dataset">
                                    <xsl:with-param name="level" select="$level"/>
                                    <xsl:with-param name="schema" select="$rowSchema"/>
                                    <xsl:with-param name="data" select="$matchingData[1]"/>
                                </xsl:call-template>
                        </xsl:when>
                        <xsl:when test="$rowSchema/xs:complexType">
                            <xsl:for-each select="$matchingData|$matchingData2">
                                <xsl:call-template name="grab-data">
                                    <xsl:with-param name="level" select="$level"/>
                                    <xsl:with-param name="schema" select="$rowSchema"/>
                                    <xsl:with-param name="data" select="."/>
                                </xsl:call-template>
                            </xsl:for-each>
                        </xsl:when>
                        <xsl:when test="starts-with($rowSchema/@type, 'setof_')">
                            <xsl:for-each select="$matchingData|$matchingData2">
                                <xsl:call-template name="grab-data">
                                    <xsl:with-param name="level" select="$level"/>
                                    <xsl:with-param name="schema" select="$rowSchema"/>
                                    <xsl:with-param name="data" select="."/>
                                </xsl:call-template>
                            </xsl:for-each>
                        </xsl:when>
                        <xsl:otherwise>
                            <data>
                                <xsl:if test="number($escapeResults) and not(starts-with($rowSchema/@name, '__html__'))">
                                    <xsl:attribute name="escape">1</xsl:attribute>
                                </xsl:if>
                                <xsl:for-each select="$matchingData|$matchingData2">
                                    <xsl:value-of select="."/>
                                    <xsl:if test="position()!=last()">, </xsl:if>
                                </xsl:for-each>
                            </data>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:call-template name="grab-data">
                        <xsl:with-param name="level" select="$level"/>
                        <xsl:with-param name="schema" select="$rowSchema"/>
                        <xsl:with-param name="data" select="/.."/><!--empty node set-->
                    </xsl:call-template>
                </xsl:otherwise>
            </xsl:choose>
    </xsl:template>

    <xsl:template name="grab-row">
        <xsl:param name="schema"/>
        <xsl:param name="data"/>
        <xsl:param name="level"/>
        <xsl:choose>
            <xsl:when test="$schema/@type">
                    <xsl:variable name="name" select="$schema/@name"/>
                    <xsl:variable name="dname" select="translate($name, '_', '-')"/>
                    <xsl:variable name="matchingData" select="$data[name()=$name]"/>            
                    <xsl:variable name="matchingData2" select="$data[not($matchingData)]/*[name()=$dname]"/>            
                    <xsl:choose>
                        <xsl:when test="$matchingData|$matchingData2">
                            <xsl:choose>
                                <xsl:when test="starts-with($schema/@type, 'setof_')">
                                    <xsl:for-each select="$matchingData|$matchingData2">
                                        <xsl:call-template name="grab-data">
                                            <xsl:with-param name="level" select="$level"/>
                                            <xsl:with-param name="schema" select="$schema"/>
                                            <xsl:with-param name="data" select="."/>
                                        </xsl:call-template>
                                    </xsl:for-each>
                                </xsl:when>
                                <xsl:otherwise>
                                    <data>
                                        <xsl:for-each select="$matchingData|$matchingData2">
                                            <xsl:value-of select="."/>
                                            <xsl:if test="position()!=last()">, </xsl:if> 
                                        </xsl:for-each>
                                    </data>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:call-template name="grab-data">
                                <xsl:with-param name="level" select="$level"/>
                                <xsl:with-param name="schema" select="$schema"/>
                                <xsl:with-param name="data" select="/.."/><!--empty node set-->
                            </xsl:call-template>
                        </xsl:otherwise>
                    </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
                <xsl:for-each select="$schema/xs:complexType/xs:attribute">
                    <xsl:variable name="name" select="@name"/>
                    <xsl:variable name="matchingData" select="$data/@*[name()=$name]"/>
                    <xsl:variable name="dname" select="translate($name, '_', '-')"/>
                    <xsl:call-template name="grab-column">
                        <xsl:with-param name="level" select="$level"/>
                        <xsl:with-param name="schema" select="$schema"/>
                        <xsl:with-param name="rowSchema" select="."/>
                        <xsl:with-param name="name" select="$name"/>
                        <xsl:with-param name="dname" select="$dname"/>
                        <xsl:with-param name="matchingData" select="$matchingData"/>
                        <xsl:with-param name="matchingData2" select="$data[not($matchingData)]/@*[name()=$dname]"/>
                    </xsl:call-template>
                </xsl:for-each>
                <xsl:for-each select="$schema/xs:complexType/xs:sequence">
                  <xsl:variable name="supportsAll" select="xs:element[not(@type) and not(node()) and @name='All']"/>
                  <xsl:variable name="hasAll" select="$supportsAll and $data/All"/>
                  <xsl:for-each select="xs:element">
                      <xsl:variable name="name" select="@name"/>
                      <xsl:variable name="matchingData" select="$data/*[name()=$name]"/>
                      <xsl:variable name="dname" select="translate($name, '_', '-')"/>
                      <xsl:if test="not($supportsAll) or $name!='All'">
                          <xsl:call-template name="grab-column">
                              <xsl:with-param name="level" select="$level"/>
                              <xsl:with-param name="schema" select="$schema"/>
                              <xsl:with-param name="rowSchema" select="."/>
                              <xsl:with-param name="name" select="$name"/>
                              <xsl:with-param name="dname" select="$dname"/>
                              <xsl:with-param name="hasAll" select="$hasAll"/>
                              <xsl:with-param name="matchingData" select="$matchingData"/>
                              <xsl:with-param name="matchingData2" select="$data[not($matchingData)]/*[name()=$dname]"/>
                          </xsl:call-template>
                       </xsl:if>
                  </xsl:for-each>
                </xsl:for-each>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <xsl:template name="grab-data">
        <xsl:param name="schema"/>
        <xsl:param name="data"/>
        <xsl:param name="level"/>
        <xsl:choose>
            <!--xsl:when test="not($data)">
            </xsl:when-->
            <xsl:when test="$schema/xs:complexType/xs:sequence[@maxOccurs='unbounded']">
                <xsl:variable name="row-schema" select="$schema/*/*/xs:element[1]"/>
                <xsl:call-template name="grab-dataset">
                    <xsl:with-param name="level" select="$level"/>
                    <xsl:with-param name="data" select="$data/*[1]"/>
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
                    <!--xsl:if test="$debug">
                        <xsl:attribute name="debug">
                            <xsl:value-of select="$schema/@name"/>
                        </xsl:attribute>
                    </xsl:if-->
                    <xsl:copy-of select="$data"/>
                </data>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template name="getMatchingName">
    <xsl:param name="schema"/>
    <xsl:param name="parent"/>
    <xsl:param name="supportsAll" select="0"/>

        <xsl:variable name="sRowName" select="$schema/@name"/>
        <xsl:choose>
            <xsl:when test="$parent/*[name()=$sRowName][1]">
                <xsl:value-of select="$sRowName"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:variable name="dashedName" select="translate($sRowName, '_', '-')"/>
                <xsl:choose>
                    <xsl:when test="$parent/*[name()=$dashedName][1]">
                        <xsl:value-of select="$dashedName"/>
                    </xsl:when>
                    <xsl:when test="$schema/@maxOccurs='unbounded' and (not($supportsAll) or name($parent/*[1])!='All')">
                        <xsl:value-of select="name($parent/*[1])"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="$sRowName"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template> 
    
    <xsl:template match="text()|comment()"/>
    
</xsl:stylesheet>
