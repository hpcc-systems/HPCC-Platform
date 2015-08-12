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
