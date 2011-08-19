<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
##############################################################################
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
    <xsl:output method="html" omit-xml-declaration="yes"/>
    <xsl:template match="EspApplicationFrame">
        <html>
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title><xsl:value-of select="@title"/><xsl:if test="@title!=''"> - </xsl:if>Enterprise Services Platform</title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="shortcut icon" href="/esp/files/img/favicon.ico" />
      </head>
      <frameset rows="62,*" FRAMEPADDING="0" PADDING="0" SPACING="0" FRAMEBORDER="0">
                <frame src="esp/titlebar" name="header" target="main" scrolling="no"/>
                <frameset FRAMEPADDING="0" PADDING="0" SPACING="0" FRAMEBORDER="{@navResize}" BORDERCOLOR="black" FRAMESPACING="1">
                    <xsl:attribute name="cols"><xsl:value-of select="@navWidth"/>,*</xsl:attribute>
                    <frame name="nav" target="main">
                        <xsl:attribute name="src"><xsl:value-of select="concat('esp/nav',@params)"/></xsl:attribute> 

                        <xsl:attribute name="scrolling">
                            <xsl:choose>
                                <xsl:when test="@navScroll=1">auto</xsl:when>
                                <xsl:otherwise>no</xsl:otherwise>
                            </xsl:choose>
                        </xsl:attribute>
                        <xsl:if test="@navResize=0">
                            <xsl:attribute name="noresize">true</xsl:attribute>
                        </xsl:if>   
                    </frame>
                    <frame id="main" name="main" frameborder="0" scrolling="auto">
                        <xsl:attribute name="src">
                            <xsl:value-of select="@inner"/>
                            <!-- TODO: get rid off duplicate params from @inner and @params -->
                            <xsl:choose>
                                <xsl:when test="contains(@inner, '?')">
                                    <xsl:value-of select="concat('&amp;', substring(@params,2))"/>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:value-of select="@params"/>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:attribute>
                    </frame>
                </frameset>
            </frameset>
        </html>
    </xsl:template>
</xsl:stylesheet>
