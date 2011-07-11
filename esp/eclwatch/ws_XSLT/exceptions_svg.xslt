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
