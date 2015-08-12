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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
                              >
    <xsl:output method="html"/>
     <xsl:variable name="autorefreshtimer" select="/BatchJobDispatchResponse/AutoRefreshTimer"/>
    <xsl:template match="/BatchJobDispatchResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <!--meta http-equiv="Refresh" content="100"/-->
            
                <title>Dispatch Batch Job</title>
            
                <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>

        <script type="text/javascript" src="files_/scripts/tooltip.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
            <script language="JavaScript1.2" src="/esp/files_/popup.js">null</script>
            <script language="JavaScript1.2" id="menuhandlers">
                    var autoRefreshTimer = '<xsl:value-of select="$autorefreshtimer"/>'; //minute
                    var reloadTimer = null;
                    var reloadTimeout = 0;
               <xsl:text disable-output-escaping="yes"><![CDATA[

                function commandQueue(action,wuid)
                {
                    submitaction='/WsBatchWorkunits/BatchJobAction?Action='+action;
                          submitaction=submitaction+'&amp;Wuid='+wuid;
                          document.location.href=submitaction;
                }

                function wuidPopup(wuid,prev,next)
                {
                    function moveupWuid()
                    {
                        commandQueue("Up",wuid);
                    }
                    function movedownWuid()
                    {
                        commandQueue("Down",wuid);
                    }
                    function movefrontWuid()
                    {
                        commandQueue("Top",wuid);
                    }
                    function movebackWuid()
                    {
                        commandQueue("Bottom",wuid);
                    }
                    function dispatchNow()
                    {
                        //document.forms["queue"].action='/WsBatchWorkunits/BatchJobAction?Action=DispatchNow&amps;Wuid='+wuid;
                         //document.forms["queue"].submit();
                              commandQueue("DispatchNow",wuid);
                    }

                    var menu=[["Move Up",prev ? moveupWuid : null],
                              ["Move Down",next ? movedownWuid : null],
                              ["Move Top",prev ? movefrontWuid : null],
                              ["Move Bottom",next ? movebackWuid : null],
                              null,
                              ["Dispatch Now", dispatchNow]
                              ];

                    showPopup(menu,(window.event ? window.event.screenX : 0),  (window.event ? window.event.screenY : 0));
                    return false;
                }
                    
                        function setAutoRefresh(obj)
                        {
                            if (obj.checked)
                            {
                                var selection = document.getElementById("AutoRefreshTimer");
                                if (selection != NaN)
                                {
                                    setReloadTimeout(selection.options[selection.selectedIndex].value);
                                }
                            }
                            else if (reloadTimer) 
                            {              
                                clearTimeout(reloadTimer);
                                reloadTimer = null;
                            }               
                        }

                        function setReloadTimeout(mins) 
                        {
                            if (reloadTimeout != mins) 
                            {
                                if (reloadTimer) 
                                {              
                                    clearTimeout(reloadTimer);
                                    reloadTimer = null;
                                }               
                                if (mins > 0)
                                    reloadTimer = setTimeout("reloadPage()", Math.ceil(parseFloat(mins) * 60 * 1000));
                                reloadTimeout = mins;
                            }
                        }

                        function reloadPage() 
                        {
                            var url='/WsBatchWorkunits/BatchJobDispatch';

                            var checkbox = document.getElementById("AutoRefresh");
                            if (checkbox != NaN && checkbox.checked)
                            {
                                url=url+'?AutoRefreshTimer='+reloadTimeout;
                            }

                            document.location.href=url;
                        }

                     function onLoad()
                     {
                            if (autoRefreshTimer > 0)            
                            {
                                setReloadTimeout(autoRefreshTimer); // Pass a default value
                                var checkbox = document.getElementById("AutoRefresh");
                                if (checkbox != NaN)
                                checkbox.checked = true;

                                var selection = document.getElementById("AutoRefreshTimer");
                                if (selection != NaN)
                                {
                                    for (i=0; i < selection.length; i++)
                                    {
                                        if (selection.options[i].value == autoRefreshTimer)
                                        {
                                            selection.options[i].selected="selected";
                                        }
                                    }
                                }
                            }
                            else
                            {
                                dorefresh = false;
                            }
                     }       

                    ]]></xsl:text>
                </script> 
            </head>
            <body class="yui-skin-sam" onload="nof5();onLoad()">
                <form id="refreshform" method="post">
                    <table id="refreshTable">
                        <colgroup>
                            <col width="250" align="left"/>
                            <col width="120" align="left"/>
                            <col width="100" align="left"/>
                            <col width="120" align="right"/>
                        </colgroup>
                        <tr>
                            <td><b>Dispatch Batch Job</b>
                            </td>
                            <td>
                                <input type="checkbox"  id="AutoRefresh" value="Auto Refresh" onclick="setAutoRefresh(this);"/> Auto Refresh
                            </td>
                            <td>
                                <select size="1" id="AutoRefreshTimer" onchange="setReloadTimeout(options[selectedIndex].value);">
                                    <option value="1" selected="selected">1</option>
                                    <option value="2">2</option>
                                    <option value="4">4</option>
                                    <option value="16">16</option>
                                    <option value="256">256</option>
                                </select> mintes
                            </td>
                            <td>
                                <input type="button" class="sbutton" value="RefreshNow" onclick="reloadPage();"/>
                            </td>
                        </tr>
                    </table>
                </form>
                <!--div id="ToolTip"></div-->
                <xsl:choose>
                    <xsl:when test="not(DispatcherJobs/DispatcherJob[1])">
                        No job found.
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates/>
                    </xsl:otherwise>
                </xsl:choose>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="DispatcherJobs">
        <form id="listitems">
            <table class="sort-table" id="resultsTable">
                <colgroup>
                    <col width="150"/>
                    <col width="150"/>
                    <col width="150"/>
                    <col width="250"/>
                    <col width="70"/>
                    <col width="30"/>
                </colgroup>
                <thead>
                    <tr class="grey">
                        <th>WUID</th>
                        <th>ECLServer</th>
                        <th>EventName</th>
                        <th>EventText</th>
                        <th>Status</th>
                        <th>Priority</th>  
                    </tr>
                </thead>
                <tbody>
                    <xsl:apply-templates select="DispatcherJob"/>
                </tbody>
            </table>
        </form>
        <!--div id="menu" style="position:absolute;visibility:hidden;top:0;left:0"></div-->
    </xsl:template>
    
    <xsl:template match="DispatcherJob">
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

            <xsl:variable name="popup">
            <xsl:choose>
                <!--xsl:when test="State='running'">return activePopup('<xsl:value-of select="$cluster"/>','<xsl:value-of select="Wuid"/>',<xsl:value-of select="Priority='high'"/>);</xsl:when-->
                <xsl:when test="State='wait'">
                    return wuidPopup('<xsl:value-of select="Wuid"/>',<xsl:value-of select="starts-with(preceding-sibling::*[position()=1]/State,'wait')"/>,<xsl:value-of select="starts-with(following-sibling::*[position()=1]/State,'wait')"/>);
                </xsl:when>
            </xsl:choose> 
            </xsl:variable>

            <td>
                <xsl:if test="State='wait'">
                    <xsl:attribute name="oncontextmenu"><xsl:value-of select="$popup"/></xsl:attribute>
                    <img class="menu1" src="/esp/files_/img/menu1.png" onclick="{$popup}"></img>
                </xsl:if>
                <a href="javascript:go('/WsBatchWorkunits/BatchWUInfo?Wuid={Wuid}')">
                    <xsl:value-of select="Wuid"/>
                </a>
        </td>
            <td align="left">
                <xsl:value-of select="EclServer"/>
            </td>
            <td>
                <xsl:value-of select="EventName"/>
            </td>
            <td>
                <xsl:value-of select="EventText"/>
            </td>
            <td>
                <xsl:value-of select="State"/>
            </td>
            <td>
                <xsl:value-of select="Priority"/>
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="text()|comment()"/>
</xsl:stylesheet>
