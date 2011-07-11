<?xml version="1.0" encoding="utf-8"?>
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
    <xsl:template match="text()"/>
    <xsl:template match="/StartStopResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Start/Stop Response</title>
                <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script type="text/javascript">
                    var loadPreflightPageTimer = 3000;
                    function onLoad(evt)
                    {
                        startBlink();
                   reloadTimer = setTimeout("reloadPage()", loadPreflightPageTimer);
                    }
        
                  function reloadPage() 
                    {
                        document.forms['machineitems'].submit();
                    }

                    function doBlink() 
                    {
                        var obj = document.getElementById('loadingMsg');
                        obj.style.visibility = obj.style.visibility == "" ? "hidden" : "" 
                    }

                    function startBlink() {
                        if (document.all)
                            setInterval("doBlink()",500)
                    }
                </script>
            </head>
            <body onload="nof5();onLoad()">
                <h2>
                    <xsl:choose>
                        <xsl:when test="number(Stop)">Stop</xsl:when>
                        <xsl:otherwise>Start</xsl:otherwise>
                    </xsl:choose>
                    <xsl:text> Response</xsl:text>
                </h2>
                <span id="loadingMsg"><h3>Updating, please wait...</h3></span>
                <h3>
                    <xsl:value-of select="Command"/>
                </h3>
                <form id="machineitems" action="/ws_machine/GetMachineInfo?GetSoftwareInfo=1" method="post">
                    <table class="sort-table">
                        <xsl:apply-templates select="StartStopResults/StartStopResult"/>
                    </table>
                    <input type="hidden" id="AutoRefresh" name="AutoRefresh" value="1"/>
                    <input type="hidden" id="GetProcessorInfo" name="GetProcessorInfo" value="1"/>
                    <input type="hidden" id="GetStorageInfo" name="GetStorageInfo" value="1"/>
                    <input type="hidden" id="MemThreshold" name="MemThreshold" value="95"/>
                    <input type="hidden" id="DiskThreshold" name="DiskThreshold" value="95"/>
                    <input type="hidden" id="CpuThreshold" name="CpuThreshold" value="95"/>
                    <input type="hidden" id="MemThresholdType" name="MemThresholdType" value="0"/>
                    <input type="hidden" id="DiskThresholdType" name="DiskThresholdType" value="0"/>
                    <input type="hidden" id="ApplyProcessFilter" name="ApplyProcessFilter" value="1"/>
                </form>
            </body>
        </html>
    </xsl:template>
    <xsl:template match="StartStopResult">
        <xsl:variable name="original_address">
            <xsl:value-of select="AddressOrig"/>
        </xsl:variable>
        <xsl:if test="position()=1">
            <thead>
                <tr>
                    <th>Location</th>
                    <th>Component Type</th>
                    <th>Command Executed</th>
                    <th>Result Code</th>
                </tr>
            </thead>
        </xsl:if>
        <tr>
            <td rowspan="2" valign="top">
                <xsl:value-of select="Address"/>
                <br/>
                <xsl:value-of select="Path"/>
            </td>
            <td>
                <xsl:value-of select="CompType"/>
            </td>
            <td>
                <xsl:value-of select="Command"/>
            </td>
            <td>
                <xsl:value-of select="ResultCode"/>
            </td>
        </tr>
        <tr>
            <td colspan="3" style="text-align:left">
                <b>Result:</b>
                <pre>
                    <xsl:value-of select="Response"/>
                </pre>
            </td>
        </tr>
        <input type="hidden" id="Addresses_i{position()}" name="Addresses_i{position()}" value="{$original_address}"/>
    </xsl:template>
</xsl:stylesheet>
