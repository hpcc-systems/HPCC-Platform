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
                              xmlns:xlink="http://www.w3.org/1999/xlink"
                              xmlns:svg="http://www.w3.org/2000/svg"
                              exclude-result-prefixes="svg">
<!--note that we don't want to turn indentation on to conserve memory in generated SVG
    graphs since every white space segment generates a separate text node -->
<xsl:output method="xml" indent="no" media-type="image/svg" omit-xml-declaration="yes"/>
<xsl:param name="wuid" select="''"/>
<xsl:param name="graphName" select="''"/>


<xsl:template match="/svg:svg">
    <xsl:variable name="width">
        <xsl:choose>
            <xsl:when test="contains(@width,'px')">
                <xsl:value-of select="substring-before(@width, 'px')"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="@width"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    <xsl:variable name="height">
        <xsl:choose>
            <xsl:when test="contains(@height,'px')">
                <xsl:value-of select="substring-before(@height, 'px')"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="@height"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    <xsl:choose>
        <xsl:when test="$width>=40000 or $height>=40000">
        <svg:svg>
            <xsl:attribute name="onload">onGraphLoaded(evt)</xsl:attribute>
            <svg:text x="20" y="40" font-size="14">The graph is too complicated to be displayable.</svg:text>
        </svg:svg>
     </xsl:when>
        <xsl:otherwise>
        <xsl:copy>
           <xsl:copy-of select="@*[name()!='width' and name()!='height']"/>
           <xsl:attribute name="width">
            <xsl:value-of select="$width"/>
           </xsl:attribute>
           <xsl:attribute name="height">
            <xsl:value-of select="$height"/>
           </xsl:attribute>
           <xsl:attribute name="viewBox">
            <xsl:value-of select="concat('0 0 ', string($width), ' ', string($height))"/>
           </xsl:attribute>
           <xsl:attribute name="onload">onLoad(evt)</xsl:attribute>
                 <svg:style type="text/css">
                    <xsl:apply-templates select="svg:g" mode="getNodeStyle"/>
                    <xsl:text disable-output-escaping="yes">
                         <![CDATA[
                            text {
                                text-anchor: middle;
                            }
                         ]]>
                    </xsl:text>
                 </svg:style>
                 <!--NOTE: if the following javascript section is changed then please replicate it 
                    in graphResult.xslt, which maintains a copy -->
           <svg:script type="text/ecmascript">
                 <![CDATA[
                    function onLoad(evt)
                    {
                        if (typeof onGraphLoaded != 'undefined')
                            onGraphLoaded(evt)
                    }
                    //note that the names of the following functions have been 
                    //intentionally kept small to minimize memory usage 
                    //since they get called from ALL nodes/edges
                    //
                    function getId(obj)
                    {
                        var id;                     
                        do {
                            id = obj.getAttribute('id');
                            if (id && id!='' && id!='undefined')
                                break;
                            obj = obj.parentNode;
                        } while (obj);
                        return id;
                    }
                    function t(evt) //shows tooltip for node or edge
                    {
                        if (typeof show_popup != 'undefined')
                        {
                            var id = getId(evt.getTarget());
                        show_popup(evt, id, evt.screenX, evt.screenY);
                    }
                    }
                    function p(evt) //shows node attributes in a popup window
                    {
                        if (typeof open_new_window != 'undefined')              
                        {
                            var id = getId(evt.getTarget());
                            open_new_window(id);
                        }
                    }
                 ]]>
           </svg:script>           
           <xsl:apply-templates/>
        </xsl:copy>
     </xsl:otherwise>
    </xsl:choose>    
</xsl:template>

<xsl:template match="svg:g[@class='graph']">
    <xsl:copy>
        <xsl:copy-of select="@*"/>
        <xsl:apply-templates/>
    </xsl:copy>
</xsl:template>

<xsl:template match="svg:g[@class='node'] | svg:g[@class='edge']">
    <xsl:variable name="nodeid">
        <xsl:value-of select="svg:a/@xlink:href"/>
    </xsl:variable>
    <xsl:copy>
        <xsl:copy-of select="@*" />
        <xsl:attribute name="id"><xsl:value-of select="$nodeid"/></xsl:attribute>
        <xsl:attribute name="onmouseover">t(evt)</xsl:attribute>
        <xsl:apply-templates/>
    </xsl:copy>
</xsl:template>

<xsl:template match="svg:g[@class='node']/svg:a/svg:polygon/@style"/>

<xsl:template match="svg:g[@class='node']/svg:a/svg:text/@style"/>

<xsl:template match="svg:a/@xlink:href">
    <xsl:attribute name="onclick">p(evt)</xsl:attribute>
</xsl:template>

<xsl:template match="svg:g[@class!='cluster']/svg:title"/>

<xsl:template match="svg:text">
    <xsl:copy>
        <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
</xsl:template>

<xsl:template match="svg:text/@text-anchor">
    <xsl:copy-of select="self::node()[.!='middle']"/>
</xsl:template>

<xsl:template match="svg:text/@startOffset">
    <xsl:copy-of select="self::node()[.!='0']"/>
</xsl:template>

<xsl:template match="text()">
    <!--eat white space since each one generates a text node in svg-->
    <xsl:copy-of select="normalize-space(.)"/>
</xsl:template>

<xsl:template match="comment()|processing-instruction()"/>

<xsl:template match="@*|*">
  <xsl:copy>
    <xsl:apply-templates select="@*|node()" />
  </xsl:copy>
</xsl:template>

<xsl:template match="svg:g[@class='graph' or @class='cluster']" mode="getNodeStyle">
    <xsl:variable name="firstNode" select="svg:g[@class='node'][1]"/>
    <xsl:choose>
        <xsl:when test="$firstNode">
            <xsl:apply-templates select="$firstNode" mode="getNodeStyle"/>
        </xsl:when>
        <xsl:otherwise>
            <xsl:apply-templates select="svg:g" mode="getNodeStyle"/>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template match="svg:g[@class='node'][1]" mode="getNodeStyle">
    <xsl:for-each select="svg:a[1]">
        <xsl:text disable-output-escaping="yes">
            g.node > a > polygon {
        </xsl:text>
        <xsl:value-of select="svg:polygon[1]/@style"/>
        <xsl:text disable-output-escaping="yes">
            } 
            g.node > a > text { 
        </xsl:text>
        <xsl:value-of select="svg:text[1]/@style"/>
        } 
    </xsl:for-each>
</xsl:template>

</xsl:stylesheet>
