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
    <xsl:template match="/">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <xsl:if test="ProgressResponse/State != 'aborted' and ProgressResponse/State != 'finished' and ProgressResponse/State != 'failed'">
        <META HTTP-EQUIV="Refresh" CONTENT="4"/>
        </xsl:if>
        <title><xsl:value-of select="ProgressResponse/wuid"/></title>
    <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
    <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
    <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
    <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
<script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <!--style type="text/css">
        table {}
        table.workunit tr { padding:2 0; }
        table.workunit th { text-align:left; vertical-align:top; }
        table.workunit td { padding:2 2 2 12; }
            .sbutton { width: 7em; font: 10pt arial , helvetica, sans-serif; }
            .running { font-weight : bolder; }
        </style-->
    </head>
  <body class="yui-skin-sam" onload="nof5();">
        <xsl:apply-templates/>
    </body>
    </html>
</xsl:template>

<xsl:template match="ProgressResponse">
    <h3>DFU Workunit Progress</h3>
    <table class="workunit">
        <colgroup>
            <col width="19%"/>
            <col width="1%"/>
            <col width="80%"/>
        </colgroup>
        <tr>
            <th>ID</th><th>:</th>
            <td><a href="javascript:go('/FileSpray/GetDFUWorkunit?wuid={wuid}')"><xsl:value-of select="wuid"/></a></td>
        </tr>
        <tr>
            <th>State</th><th>:</th>
            <td><xsl:value-of select="State"/></td>
        </tr>
        <tr>
            <th>Percent Done</th><th>:</th>
            <td><xsl:value-of select="PercentDone"/>%
            <table>
            <tr width="100" height="20">
            <xsl:if test="PercentDone != 0">
                <td width="{PercentDone}" bgcolor="#00FF00"/>
            </xsl:if>
            <xsl:if test="PercentDone != 100">
                <td width="{100 - PercentDone}" bgcolor="#FF0000"/>
            </xsl:if>
            </tr>
            </table>
            </td>
        </tr>
        <tr>
            <th>Slaves Done</th><th>:</th>
            <td><xsl:value-of select="SlavesDone"/></td>
        </tr>
        <tr>
            <th>Time Taken</th><th>:</th>
            <td><xsl:value-of select="TimeTaken"/></td>
        </tr>
        <tr>
            <th>KB Per Second</th><th>:</th>
            <td><xsl:value-of select="KbPerSec"/></td>
        </tr>
        <tr>
            <th>KB Per Second Average</th><th>:</th>
            <td><xsl:value-of select="KbPerSecAve"/></td>
        </tr>
        <tr>
            <th>Progress Message</th><th>:</th>
            <td><xsl:value-of select="ProgressMessage"/></td>
        </tr>
        <tr>
            <th>Summary Message</th><th>:</th>
            <td><xsl:value-of select="SummaryMessage"/></td>
        </tr>
    </table>
</xsl:template>

</xsl:stylesheet>
