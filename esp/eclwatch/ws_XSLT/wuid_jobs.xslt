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

<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
                              xmlns:svg="http://www.w3.org/2000/svg"
                              xmlns:date="http://www.ora.com/XSLTCookbook/namespaces/date"
                              >
<xsl:output method="xml" encoding="UTF-8" media-type="image/svg"
    doctype-public="-//W3C//DTD SVG 1.0//EN" 
    doctype-system="http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd"/>

<xsl:include href="/esp/xslt/date-time.xslt"/>
<xsl:include href="/esp/xslt/lib.xslt"/>

  <xsl:variable name="start-date">
    <xsl:choose>
        <xsl:when test="string-length(/WUJobListResponse/StartDate)">
            <xsl:value-of select="/WUJobListResponse/StartDate"/>
        </xsl:when>
        <xsl:otherwise>
             <xsl:for-each select="//ECLJob/StartedDate">
                <xsl:sort data-type="text" />
                <xsl:if test="position() = 1">
                   <xsl:value-of select="." />
                </xsl:if>
             </xsl:for-each>
        </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="end-date">
    <xsl:choose>
        <xsl:when test="string-length(/WUJobListResponse/EndDate)">
            <xsl:value-of select="/WUJobListResponse/EndDate"/>
        </xsl:when>
        <xsl:otherwise>
             <xsl:for-each select="//ECLJob/FinishedDate">
                <xsl:sort data-type="text" order="descending"/>
                <xsl:if test="position() = 1">
                   <xsl:value-of select="." />
                </xsl:if>
             </xsl:for-each>
        </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="minX" select="0"/>
  <xsl:variable name="maxX" select="24"/>

  <xsl:variable name="Y1">
    <xsl:call-template name="get-date">
        <xsl:with-param name="date-time" select="$start-date"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="Y2">
    <xsl:call-template name="get-date">
        <xsl:with-param name="date-time" select="$end-date"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="minY" select="$Y1"/>
  <xsl:variable name="maxY" select="$Y2 + 1"/>

  <xsl:variable name="offsetX" select="150"/>
  <xsl:variable name="offsetY" select="20"/>

  <xsl:variable name="width" select="900"/>
  <xsl:variable name="height" select="20*($maxY - $minY) + $offsetY + 20"/>

  <xsl:variable name="pwidth" select="$width - $offsetX - 50"/>
  <xsl:variable name="pheight" select="$height - $offsetY - 20"/>

<xsl:template match="/WUJobListResponse">
    <svg width="{$width}" height="{$height}">
        <xsl:attribute name="onload">resize_graph('<xsl:value-of select="$width"/>','<xsl:value-of select="$height"/>')</xsl:attribute>
        <xsl:if test="$maxY > $minY">
        <g transform="translate({$offsetX},{$offsetY}) scale({$pwidth div ($maxX - $minX)},{$pheight div ($maxY - $minY)})">
            <g stroke-width="0.01" stroke="black"  font-size="0.5" font-family="Serpentine-Light" alignment-baseline="middle" >
                <xsl:call-template name="drawYTicks">
                    <xsl:with-param name="x1" select="$minX"/>
                    <xsl:with-param name="y1" select="0"/>
                    <xsl:with-param name="x2" select="$maxX"/>
                    <xsl:with-param name="y2" select="$maxY - $minY"/>
                </xsl:call-template>
                <g text-anchor="middle" >
                    <xsl:call-template name="drawXTicks">
                        <xsl:with-param name="x1" select="$minX"/>
                        <xsl:with-param name="y1" select="0"/>
                        <xsl:with-param name="x2" select="$maxX"/>
                        <xsl:with-param name="y2" select="$maxY - $minY"/>
                    </xsl:call-template>
                </g>
            </g>
            <xsl:apply-templates/>
        </g>
        </xsl:if>
    </svg>
</xsl:template>

<xsl:template match="Jobs">

        <xsl:for-each select="ECLJob">
            <g stroke-width="1" onmouseout="hide_popup()" onclick="on_click('{Wuid}')">
                <xsl:variable name="d">
                    <xsl:call-template name="get-duration">
                        <xsl:with-param name="from" select="StartedDate"/>
                        <xsl:with-param name="to" select="FinishedDate"/>
                    </xsl:call-template>
                </xsl:variable>

                <xsl:attribute name="onmouseover">show_popup(evt, window.evt.screenX,window.evt.screenY,'<xsl:value-of select="Wuid"/>','<xsl:value-of select="Graph"/>','<xsl:value-of select="StartedDate"/>','<xsl:value-of select="FinishedDate"/>','<xsl:value-of select="normalize-space($d)"/>','<xsl:value-of select="Cluster"/>')</xsl:attribute>

                <xsl:attribute name="stroke">
                    <xsl:choose>
                        <xsl:when test="State='failed'">red</xsl:when>
                        <xsl:when test="State='running'">black</xsl:when>
                        <xsl:otherwise>blue</xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>

                <xsl:call-template name="drawSegment">
                    <xsl:with-param name="x1">
                        <xsl:call-template name="get-time">
                            <xsl:with-param name="date-time" select="StartedDate"/>
                        </xsl:call-template>
                    </xsl:with-param>
                    <xsl:with-param name="y1">
                        <xsl:call-template name="get-date">
                            <xsl:with-param name="date-time" select="StartedDate"/>
                        </xsl:call-template>
                    </xsl:with-param>
                    <xsl:with-param name="x2">
                        <xsl:call-template name="get-time">
                            <xsl:with-param name="date-time" select="FinishedDate"/>
                        </xsl:call-template>
                    </xsl:with-param>
                    <xsl:with-param name="y2">
                        <xsl:call-template name="get-date">
                            <xsl:with-param name="date-time" select="FinishedDate"/>
                        </xsl:call-template>
                    </xsl:with-param>
                </xsl:call-template>
            </g>
        </xsl:for-each>
</xsl:template>

<xsl:template name="drawSegment">
    <xsl:param name="x1"/>
    <xsl:param name="y1"/>
    <xsl:param name="x2"/>
    <xsl:param name="y2"/>

    <xsl:if test="($y1=$y2) and ($y1>=$minY) and ($maxY>$y1)">
        <line x1="{$x1 div 3600}" y1="{$y1 + 0.5 - $minY}" x2="{$x2 div 3600}" y2="{$y1 + 0.5 - $minY}"/> 
    </xsl:if>
    <xsl:if test="($y2>$y1) and ($maxY>$y1)">
        <xsl:if test="$y1>=$minY">
            <line x1="{$x1 div 3600}" y1="{$y1 + 0.5 - $minY}" x2="{$maxX}" y2="{$y1 + 0.5 - $minY}"/>
        </xsl:if>
        <xsl:call-template name="drawSegment">
            <xsl:with-param name="x1" select="$minX"/>
            <xsl:with-param name="y1" select="1+$y1"/>
            <xsl:with-param name="x2" select="$x2"/>
            <xsl:with-param name="y2" select="$y2"/>
        </xsl:call-template>
    </xsl:if>
</xsl:template>

<xsl:template name="drawXTicks">
    <xsl:param name="x1"/>
    <xsl:param name="y1"/>
    <xsl:param name="x2"/>
    <xsl:param name="y2"/>
    <xsl:param name="x" select="$x1"/>
    <line x1="{$x}" y1="{$y1}" x2="{$x}" y2="{$y2}">
        <xsl:if test="$x=$x1 or $x=$x2">
            <xsl:attribute name="stroke-width">0.05</xsl:attribute>
        </xsl:if>
    </line>  

    <svg:text x="{$x}" y="{$y1 - 0.5}">
         <xsl:value-of select="$x"/>
    </svg:text>

    <svg:text x="{$x}" y="{$y2 + 0.5}">
         <xsl:value-of select="$x"/>
    </svg:text>

    <xsl:if test="$x!=$x2">
        <xsl:call-template name="drawXTicks">
            <xsl:with-param name="x1" select="$x1"/>
            <xsl:with-param name="y1" select="$y1"/>
            <xsl:with-param name="x2" select="$x2"/>
            <xsl:with-param name="y2" select="$y2"/>
            <xsl:with-param name="x" select="1+$x"/>
        </xsl:call-template>
    </xsl:if>
</xsl:template>

<xsl:template name="drawYTicks">
    <xsl:param name="x1"/>
    <xsl:param name="y1"/>
    <xsl:param name="x2"/>
    <xsl:param name="y2"/>
    <xsl:param name="y" select="$y1"/>

    <xsl:if test="$y!=$y2">
        <xsl:variable name="print-day">
            <xsl:call-template name="date:format-julian-day">
                <xsl:with-param name="julian-day" select="$minY + $y - $y1"/>
                <xsl:with-param name="format" select="'%a, %b %d'"/>
            </xsl:call-template>
        </xsl:variable>

        <xsl:if test="substring($print-day,1,1)='S'">
            <line x1="{$x1}" y1="{$y + 0.5}" x2="{$x2}" y2="{$y + 0.5}" stroke="#ccc" stroke-width="1"/>
        </xsl:if>

        <text y="{$y + 0.5}" x="-0.5" text-anchor="end">
            <xsl:value-of select="$print-day"/>
        </text>
        <text y="{$y + 0.5}" x="{$x2 + 0.5}" text-anchor="begin">
            <xsl:call-template name="dayly-usage">
                <xsl:with-param name="day" select="$minY + $y - $y1"/>
            </xsl:call-template>
        </text>

        <xsl:call-template name="drawYTicks">
            <xsl:with-param name="x1" select="$x1"/>
            <xsl:with-param name="y1" select="$y1"/>
            <xsl:with-param name="x2" select="$x2"/>
            <xsl:with-param name="y2" select="$y2"/>
            <xsl:with-param name="y" select="1+$y"/>
        </xsl:call-template>
    </xsl:if>

    <line x1="{$x1}" y1="{$y}" x2="{$x2}" y2="{$y}">
        <xsl:if test="$y=$y1 or $y=$y2">
            <xsl:attribute name="stroke-width">0.05</xsl:attribute>
        </xsl:if>
    </line>  

</xsl:template>

<xsl:template name="dayly-usage">
    <xsl:param name="day"/>

    <xsl:variable name="d">
        <xsl:call-template name="date:format-julian-day">
            <xsl:with-param name="julian-day" select="$day"/>
            <xsl:with-param name="format" select="'%Y%m%d'"/>
        </xsl:call-template>
    </xsl:variable>

    <xsl:call-template name="sum-interval">
        <xsl:with-param name="nodes" select="Jobs/ECLJob[($d >= translate(substring-before(StartedDate,'T'),'-','') ) and (translate(substring-before(FinishedDate,'T'),'-','') >= $d)]"/>
        <xsl:with-param name="day" select="$d"/>
    </xsl:call-template>
</xsl:template>

<xsl:template name="sum-interval">
    <xsl:param name="nodes"/>
    <xsl:param name="day"/>
    <xsl:param name="sum" select="0"/>

    <xsl:choose>
        <xsl:when test="count($nodes)=0">
            <xsl:value-of select="floor($sum div 864)"/>%
        </xsl:when>
        <xsl:otherwise>

            <xsl:variable name="t1">
                <xsl:choose>
                    <xsl:when test="$day > translate(substring-before($nodes[1]/StartedDate,'T'),'-','')">0</xsl:when>
                    <xsl:otherwise>
                        <xsl:call-template name="get-time">
                            <xsl:with-param name="date-time" select="$nodes[1]/StartedDate"/>
                        </xsl:call-template>
                    </xsl:otherwise>
                 </xsl:choose>
            </xsl:variable>

            <xsl:variable name="t2">
                <xsl:choose>
                    <xsl:when test="translate(substring-before($nodes[1]/FinishedDate,'T'),'-','') > $day">86400</xsl:when>
                    <xsl:otherwise>
                        <xsl:call-template name="get-time">
                            <xsl:with-param name="date-time" select="$nodes[1]/FinishedDate"/>
                        </xsl:call-template>
                    </xsl:otherwise>
                 </xsl:choose>
            </xsl:variable>


            <xsl:call-template name="sum-interval">
                <xsl:with-param name="nodes" select="$nodes[position()!=1]"/>
                <xsl:with-param name="day" select="$day"/>
                <xsl:with-param name="sum" select="$sum + $t2 - $t1"/>
            </xsl:call-template>

        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="get-time">
    <xsl:param name="date-time"/>

    <xsl:variable name="time">
        <xsl:value-of select="substring-after($date-time,'T')"/>
    </xsl:variable>
    <xsl:variable name="hour" select="number(substring($time,1,2))"/>
    <xsl:variable name="minute" select="number(substring($time,4,2))"/>
    <xsl:variable name="second" select="number(substring($time,7,2))"/>
    <xsl:value-of select="$hour * 3600 + $minute * 60 + $second"/>
</xsl:template>

<xsl:template name="get-date">
    <xsl:param name="date-time"/>
    <xsl:call-template name="date:date-to-julian-day">
        <xsl:with-param name="date-time" select="$date-time"/>
    </xsl:call-template>
</xsl:template>

<xsl:template name="get-duration">
    <xsl:param name="from"/>
    <xsl:param name="to"/>

    <xsl:variable name="d1">
        <xsl:call-template name="get-date">
            <xsl:with-param name="date-time" select="$from"/>
        </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="t1">
        <xsl:call-template name="get-time">
            <xsl:with-param name="date-time" select="$from"/>
        </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="d2">
        <xsl:call-template name="get-date">
            <xsl:with-param name="date-time" select="$to"/>
        </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="t2">
        <xsl:call-template name="get-time">
            <xsl:with-param name="date-time" select="$to"/>
        </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="dt" select="86400*($d2 - $d1) + $t2 - $t1"/>

    <xsl:variable name="hours" select="floor($dt div 3600)"/>
    <xsl:variable name="tt" select="$dt - $hours*3600"/>
    <xsl:variable name="minutes" select="floor($tt div 60)"/>
    <xsl:variable name="seconds" select="$tt - $minutes*60"/>

    <xsl:if test="$hours"><xsl:value-of select="$hours"/>h </xsl:if>
    <xsl:if test="$minutes"><xsl:value-of select="$minutes"/>m </xsl:if>
    <xsl:value-of select="$seconds"/>s

</xsl:template>


<xsl:template match="text()|comment()"/>

</xsl:stylesheet>

