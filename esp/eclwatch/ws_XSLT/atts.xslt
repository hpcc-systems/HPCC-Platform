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

<!DOCTYPE xsl:stylesheet [
    <!--define the HTML non-breaking space:-->
    <!ENTITY nbsp "<xsl:text disable-output-escaping='yes'>&amp;nbsp;</xsl:text>">
]>
<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:param name="wuid" select="''"/>
<xsl:param name="graphName" select="''"/>

<xsl:output method="xhtml"/>
<xsl:include href="/esp/xslt/lib.xslt"/>
    

<xsl:template match="/">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <style type="text/css">
            body    { margin:0; }
            </style>
            <script defer="defer">
                function popups_loaded()
                {
                    var top = window.top;
                    if (!top.show_popup || !top.activepopup)
                        return;
                    top.show_popup(null, top.activepopup);
                }
            </script>
        </head>
        <body class="yui-skin-sam" onload="nof5();popups_loaded()">     
            <xsl:apply-templates select="//node|//edge[att]"/>
        </body>
    </html>
</xsl:template>


<xsl:template match="graph">
    <xsl:apply-templates/>
</xsl:template>


<xsl:template match="node|edge">
    <xsl:if test="att[string-length(@name) and not(starts-with(@name, '_'))]">
        <div id="popup_{@id}" style="overflow:auto">
            <table id="tab" style="font:menu">
                <colgroup>
                    <col align="left" valign="top"/>
                </colgroup>
                <tr id="captionRow" style="display:none">
                    <th><xsl:value-of select="name()"/></th>
                    <td align="right">
                        <a href="" onclick="javascript:parent.hide_popup()">
                            <img border="0" src="../esp/files/img/close_wnd.gif"/>
                        </a>
                    </td>
                </tr>
                <tr id="wuRow" style="display:none">
                    <th>workunit</th>
                    <td><xsl:value-of select="$wuid"/></td>
                </tr>
                <tr id="graphNameRow" style="display:none">
                    <th>graph</th>
                    <td>
                        <xsl:value-of select="$graphName"/>
                    </td>
                </tr>
                <tr>
                    <xsl:if test="name()='edge'">
                        <xsl:attribute name="id">idRow</xsl:attribute>
                        <xsl:attribute name="style">display:none</xsl:attribute>
                    </xsl:if>
                    <th>id</th>
                    <td>
                        <xsl:value-of select="@id"/>
                    </td>
                </tr>
                <xsl:apply-templates select="att[string-length(@name) and not(starts-with(@name, '_'))]"/>
            </table>
        </div>
    </xsl:if>
</xsl:template>


<xsl:template match="att">
    <tr>
        <th>
            <xsl:value-of select="@name"/>
        </th>
        <xsl:choose>
            <xsl:when test="Dataset[1]">
                <td>
                    <xsl:apply-templates select="Dataset"/>
                </td>
            </xsl:when>
            <xsl:when test="@name='count' or @name='max' or @name='min'">
                <td>
                    <xsl:call-template name="comma_separated">
                        <xsl:with-param name="value" select="@value"/>
                    </xsl:call-template>
                </td>
            </xsl:when>
            <xsl:otherwise>
                <td>
                    <xsl:value-of select="@value"/>
                </td>
            </xsl:otherwise>
        </xsl:choose>
    </tr>
</xsl:template>


<xsl:template match="Dataset">
    <table style="font:menu;" border="1" cellspacing="0">
        <thead>
            <tr>
                <xsl:for-each select="Group[1]/Row[1]/*">
                    <th>
                        <xsl:value-of select="name()"/>
                    </th>
                </xsl:for-each>
            </tr>
        </thead>
        <xsl:apply-templates select="Group"/>
    </table>
</xsl:template>


<xsl:template match="Group">
    <tbody>
        <xsl:apply-templates select="Row"/>
    </tbody>
</xsl:template>


<xsl:template match="Row">
    <tr>
        <xsl:apply-templates select="*"/>
    </tr>
</xsl:template>


<xsl:template match="*">
    <td>
        <xsl:choose>
            <xsl:when test=".!=''">
                <xsl:value-of select="."/>
            </xsl:when>
            <xsl:otherwise>&nbsp;</xsl:otherwise>
        </xsl:choose>
    </td>
</xsl:template>

</xsl:stylesheet>
