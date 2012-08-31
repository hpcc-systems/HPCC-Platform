<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    <xsl:output method="html"/>
    <xsl:template match="/RoxieQueryDetailsResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>QueryFiles</title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
                <script language="JavaScript1.2">
                     <xsl:text disable-output-escaping="yes"><![CDATA[
                        function onLoad()
                        {
                            initSelection('resultsTable');
                        }

                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <h3>Query Details for <xsl:value-of select="QueryID"/></h3>
                <br/>
                <form>
                    <table style="text-align:left;" cellspacing="10">
                        <colgroup style="vertical-align:top;padding-right:10px;" span="2"/>
                        <tr>
                            <th>WUID:</th>
                            <td><xsl:value-of select="WUID"/></td>
                        </tr>
                        <tr>
                            <th>Roxie Cluster:</th>
                            <td><xsl:value-of select="Cluster"/></td>
                        </tr>
                        <tr>
                            <th>Associated Name:</th>
                            <td><xsl:value-of select="AssociatedName"/></td>
                        </tr>
                        <tr>
                            <th>High Priority:</th>
                            <td><xsl:value-of select="HighPriority"/></td>
                        </tr>
                        <tr>
                            <th>Deployed By:</th>
                            <td><xsl:value-of select="DeployedBy"/></td>
                        </tr>
                        <tr>
                            <th>Suspended:</th>
                            <td><xsl:value-of select="Suspended"/></td>
                        </tr>
                        <xsl:if test="string-length(UpdatedBy)">
                            <tr>
                                <th>Suspended By:</th>
                                <td><xsl:value-of select="UpdatedBy"/></td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="string-length(Label)">
                            <tr>
                                <th>Label:</th>
                                <td><xsl:value-of select="Label"/></td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="string-length(Error)">
                            <tr>
                                <th>Error:</th>
                                <td><xsl:value-of select="Error"/></td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="string-length(Comment)">
                            <tr>
                                <th>Comment:</th>
                                <td><xsl:value-of select="Comment"/></td>
                            </tr>
                        </xsl:if>
                    </table>
                </form>
                <xsl:if test="Aliases/RoxieQueryAlias[1]">
                    <form>
                        <table>
                            <tr>
                                <td><h4>Alias list:</h4></td>
                            </tr>
                            <tr>
                                <td><xsl:apply-templates/></td>
                            </tr>
                        </table>
                    </form>
                </xsl:if>
                <input id="backBtn" type="button" value="Go Back" onclick="history.go(-1)"> </input>
            </body>
        </html>
    </xsl:template>
    

    <xsl:template match="Aliases">
        <form id="listitems">
            <table class="sort-table" id="resultsTable">
                <colgroup>
                    <col/>
                    <col/>
                </colgroup>
                <thead>
                <tr class="grey">
                    <th>Alias Name</th>
                    <th>Original Name</th>
                </tr>
                </thead>
                <tbody>
                <xsl:apply-templates select="RoxieQueryAlias"/>
                </tbody>
            </table>
        </form>
    </xsl:template>
    
    <xsl:template match="RoxieQueryAlias">
        <tr onmouseenter="this.bgColor = '#F0F0FF'">
            <xsl:choose>
                <xsl:when test="position() mod 2">
                    <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
                    <xsl:attribute name="onmouseleave">this.bgColor = '#FFFFFF'</xsl:attribute>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
                    <xsl:attribute name="onmouseleave">this.bgColor = '#F0F0F0'</xsl:attribute>
                </xsl:otherwise>
            </xsl:choose>
            <td align="left">
                <xsl:value-of select="ID"/>
            </td>
            <td>
                <xsl:value-of select="Original"/>
            </td>
        </tr>
    </xsl:template>
    
    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
