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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:import href="exceptions.xslt"/>
    
    <xsl:output method="xml" omit-xml-declaration="yes" indent="yes"/>
    <xsl:param name="svg_exceptions" select="0"/>
        
    <xsl:template match="/">
        <xsl:choose>
            <xsl:when test="$svg_exceptions=0">
                <xsl:apply-imports/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:apply-templates mode="svg"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <xsl:template match="/Exceptions" mode="svg">
        <svg xmlns="http://www.w3.org/2000/svg" width="0" height="0" onload="resize_graph('100%', '100%')">
            <g>
                <title>Exceptions</title>
                <text font-family="Verdana">
                    <tspan x="0" dy="1em" font-weight="bold" font-size="15">
                        <xsl:text>Exception(s) encountered:</xsl:text>
                    </tspan>
                    <xsl:if test="Source">
                        <tspan x="0" dy="2em" font-weight="bold">Reporter: </tspan>
                        <tspan x="75" dy="0em">
                            <xsl:value-of select="Source"/>
                        </tspan>
                    </xsl:if>
                    <tspan x="20" dy="2em" font-weight="bold">Code</tspan>
                    <tspan x="75" dy="0em" font-weight="bold">Message</tspan>
                    <xsl:for-each select="Exception">
                        <tspan x="30" dy="1em">
                            <xsl:value-of select="Code"/>
                        </tspan>
                        <tspan x="75" dy="0em">
                            <xsl:value-of select="Message"/>
                        </tspan>                                                
                    </xsl:for-each>
                </text>
            </g>
        </svg>
    </xsl:template>
</xsl:stylesheet>
