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
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script type="text/javascript">
          var loadPreflightPageTimer = 3000;
          function onLoad(evt)
          {
          var sectionDiv = document.getElementById("ViewMsg");
          if (sectionDiv)
          {
          var parentSectionDiv = parent.document.getElementById("ViewMsg");
          if (parentSectionDiv)
          {
          parentSectionDiv.innerHTML = sectionDiv.innerHTML;
          }
          }
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
                <h3>
                    <xsl:value-of select="Command"/>
                </h3>
        <form name="MachineItems" action="/ws_machine/GetMachineInfo?GetSoftwareInfo=1" method="post">
          <div id="ViewMsg">
                      <table class="list">
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
            <input type="submit" value="Get Machine Information" id="submitBtn"/>
          </div>
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
        <tr class="grey">
          <th>Component</th>
          <th>Result Code</th>
          <th>Results</th>
                </tr>
            </thead>
        </xsl:if>
        <tr>
            <td>
                <xsl:value-of select="CompType"/>
            </td>
            <td>
                <xsl:value-of select="ResultCode"/>
            </td>
      <td>
        <xsl:value-of select="Response"/>
      </td>
    </tr>
        <input type="hidden" id="Addresses_i{position()}" name="Addresses_i{position()}" value="{$original_address}"/>
    </xsl:template>
</xsl:stylesheet>
