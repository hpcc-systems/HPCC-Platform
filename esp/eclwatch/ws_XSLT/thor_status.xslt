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
    <xsl:output method="html"/>
    <xsl:variable name="autoRefresh" select="TpThorStatusResponse/AutoRefresh"/>
    <xsl:variable name="thorName" select="TpThorStatusResponse/Name"/>
    <xsl:template match="TpThorStatusResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>ThorStatus</title>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
                <link rel="StyleSheet" type="text/css" href="/esp/files/css/sortabletable.css"/>
                <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <script type="text/javascript">
                    var thorName='<xsl:value-of select="$thorName"/>';
                    var autoRefreshVal=<xsl:value-of select="$autoRefresh"/>;
                    var doRefresh = true;
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        var reloadTimer = null;
                        var reloadTimeout = 0;
                        function SetAutoRefresh()
                        {
                            var refreshImg = document.getElementById('refresh');
                            if (autoRefreshVal > 0)            
                            {
                                if (refreshImg)
                                {
                                    refreshImg.src = '/esp/files_/img/refreshenabled.png';
                                }
                                setReloadTimeout(autoRefreshVal); // Pass a default value
                            }
                            else
                            {
                                if (refreshImg)
                                {
                                    refreshImg.src = '/esp/files_/img/refreshdisabled.png';
                                }
                            }               
                        }

                        function TurnRefresh() 
                        {
                            if (doRefresh)
                            {
                                doRefresh = false;
                                document.getElementById('refresh').src='/esp/files_/img/refreshdisabled.png';
                                reloadTimeout = 0;
                                if (reloadTimer) 
                                {              
                                    clearTimeout(reloadTimer);
                                    reloadTimer = null;
                                }               
                            }
                            else
                            {
                                doRefresh = true;
                                document.getElementById('refresh').src='/esp/files_/img/refreshenabled.png';
                                setReloadTimeout(autoRefreshVal); // Pass a default value
                            }
                        }

                        function setReloadTimeout(mins) 
                        {
                            if (reloadTimeout == mins)
                                return; 

                            if (reloadTimer) 
                            {              
                                clearTimeout(reloadTimer);
                                reloadTimer = null;
                            }               
                            if (mins > 0)
                            {
                                reloadTimer = setTimeout('reloadPage()', Math.ceil(parseFloat(mins) * 60 * 1000));
                            }
                            reloadTimeout = mins;
                        }
        
                        function reloadPage() 
                        {
                            /*var globalframe = document.getElementById('GlobalFrame');
                            if (globalframe)
                            {
                                globalframe.src = '/WsTopology/TpThorStatus?Name=' + thorName;
                            }*/
                            document.location.href = '/WsTopology/TpThorStatus?Name=' + thorName;
                        }
                    ]]></xsl:text>
                </script>
            </head>
            <body class="yui-skin-sam" onload="nof5();SetAutoRefresh();">
                <h3>Thor Status Details</h3>
                <table style="text-align:left;" cellspacing="10">
                    <colgroup style="vertical-align:top;padding-right:10px;" span="2">
                    </colgroup>
                    <tr>
                        <th>Name:</th>
                        <td>
                            <a href="javascript:go('/WsTopology/TpLogFile/{Name}?Name={Name}&amp;Type=xml')">
                                <xsl:value-of select="Name"/>
                            </a>
                            <img id="refresh" src="/esp/files/img/refresh.png" onclick="TurnRefresh()" title="Turn on/off Auto Refresh" />
                        </td>
                    </tr>
                    <tr>
                        <th>Queue:</th>
                        <td>
                            <xsl:value-of select="Queue"/>
                        </td>
                    </tr>
                    <tr>
                        <th>Node Group:</th>
                        <td>
                            <xsl:value-of select="Group"/>
                        </td>
                    </tr>
                    <tr>
                        <th>ThorMaster Address:</th>
                        <td>
                            <xsl:value-of select="ThorMasterIPAddress"/>
                        </td>
                    </tr>
                    <tr>
                        <th>Port:</th>
                        <td>
                            <xsl:value-of select="Port"/>
                        </td>
                    </tr>
                    <tr>
                        <th>Started:</th>
                        <td>
                            <xsl:value-of select="StartTime"/>
                        </td>
                    </tr>
                    <xsl:if test="string-length(Wuid)">
                        <tr>
                            <th>Workunit:</th>
                            <td>
                                <a href="javascript:go('/WsWorkunits/WUInfo?Wuid={Wuid}')">
                                    <xsl:value-of select="Wuid"/>
                                </a>
                            </td>
                        </tr>
                        <tr>
                            <th></th>
                            <td>
                                <table class="sort-table" id="resultsTable">
                                    <colgroup>
                                        <col />
                                        <col />
                                        <col />
                                    </colgroup>
                                    <thead>
                                        <tr class="grey">
                                            <th>
                                                Graph
                                            </th>
                                            <th>
                                                SubGraph
                                            </th>
                                            <th>
                                                SubGraph Duration
                                            </th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td>
                                                <xsl:if test="string-length(Graph)">
                                                    <xsl:value-of select="Graph"/>
                                                </xsl:if>
                                            </td>
                                            <td>
                                                <xsl:if test="string-length(SubGraph)">
                                                    <xsl:value-of select="SubGraph"/>
                                                </xsl:if>
                                            </td>
                                            <td>
                                                <xsl:if test="string-length(SubGraphDuration)">
                                                    <xsl:value-of select="SubGraphDuration"/>
                                                </xsl:if>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                        </tr>
                    </xsl:if>
                    <xsl:if test="string-length(LogFile)">
                        <tr>
                            <th>Log File:</th>
                            <td>
                                <a href="javascript:go('/WsTopology/TpLogFile/{Name}?Name={Name}&amp;Type=thormaster_log')">
                                    <xsl:value-of select="LogFile"/>
                                </a>
                            </td>
                        </tr>
                    </xsl:if>
                </table>
                <xsl:text disable-output-escaping="yes"><![CDATA[
                    <iframe id="GlobalFrame" name="GlobalFrame" style="display:none; visibility:hidden;"></iframe>
                ]]></xsl:text>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="text()|comment()"/>
</xsl:stylesheet>
