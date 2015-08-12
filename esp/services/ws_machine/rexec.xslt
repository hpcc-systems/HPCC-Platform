<?xml version="1.0" encoding="utf-8"?>
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
    <xsl:output method="html"/>
    <xsl:template match="text()"/>
    <xsl:template match="/RemoteExecResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Remote Execution Response</title>
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
                <h2>Remote Execution Response</h2>
                <span id="loadingMsg"><h3>Updating, please wait...</h3></span>
                <h3>
                    <xsl:value-of select="Command"/>
                </h3>
                <form id="machineitems" action="/ws_machine/GetMachineInfo?GetSoftwareInfo=1" method="post">
                    <table class="sort-table">
                        <xsl:apply-templates select="RemoteExecResults/RemoteExecResult"/>
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
    <xsl:template match="RemoteExecResult">
        <xsl:variable name="original_address">
            <xsl:value-of select="AddressOrig"/>
        </xsl:variable>
        <xsl:if test="position()=1">
            <thead>
                <tr>
                    <th>Location</th>
                    <th>Result Code</th>
                    <th>Response</th>
                </tr>
            </thead>
        </xsl:if>
        <tr>
            <td valign="top">
                <xsl:value-of select="Address"/>
            </td>
            <td align="center" valign="top">
                <xsl:value-of select="ResultCode"/>
            </td>
            <td align="left">
                <pre>
                    <xsl:value-of select="Response"/>
                </pre>
            </td>
        </tr>
        <input type="hidden" id="Addresses_i{position()}" name="Addresses_i{position()}" value="{$original_address}"/>
    </xsl:template>
</xsl:stylesheet>
