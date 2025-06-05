<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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
                <title>DFU Meta Field Information</title>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
                <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <style type="text/css">
                    .fieldTable {
                        border-collapse: collapse;
                        width: 100%;
                        margin-top: 20px;
                    }
                    .fieldTable th, .fieldTable td {
                        border: 1px solid #ddd;
                        padding: 8px;
                        text-align: left;
                    }
                    .fieldTable th {
                        background-color: #f2f2f2;
                        font-weight: bold;
                    }
                    .fieldTable tr:nth-child(even) {
                        background-color: #f9f9f9;
                    }
                    .refresh-form {
                        margin-bottom: 20px;
                        padding: 15px;
                        background-color: #f5f5f5;
                        border: 1px solid #ddd;
                        border-radius: 4px;
                    }
                </style>
                <script language="JavaScript1.2">
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        function refreshMetaInquiry() {
                            document.location.href = '/WsDfu/DFUGetMetaInquiry';
                        }
                    ]]></xsl:text>
                </script>
            </head>
            <body class="yui-skin-sam" onload="nof5();">
                <xsl:apply-templates/>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="DFUMetaInquiryResponse">
        <div>
            <h2>DFU Field Information</h2>

            <!-- Refresh form -->
            <div class="refresh-form">
                <p>This page displays the available fields for DFU queries for sorting and pruning.</p>
                <form method="GET" action="/WsDfu/DFUGetMetaInquiry">
                    <input type="submit" value="Refresh Meta Information" onclick="refreshMetaInquiry(); return false;"/>
                </form>
            </div>

            <!-- Fields table -->
            <xsl:choose>
                <xsl:when test="Fields/DFUMetaFieldInfo">
                    <table class="fieldTable">
                        <thead>
                            <tr>
                                <th>Field Name</th>
                                <th>Field Type</th>
                            </tr>
                        </thead>
                        <tbody>
                            <xsl:for-each select="Fields/DFUMetaFieldInfo">
                                <tr>
                                    <td><strong><xsl:value-of select="Name"/></strong></td>
                                    <td><xsl:value-of select="Type"/></td>
                                </tr>
                            </xsl:for-each>
                        </tbody>
                    </table>

                    <!-- Summary information -->
                    <div style="margin-top: 20px; padding: 10px; background-color: #e8f4f8; border-left: 4px solid #2196F3;">
                        <p><strong>Total Fields:</strong> <xsl:value-of select="count(Fields/DFUMetaFieldInfo)"/></p>
                    </div>
                </xsl:when>
                <xsl:otherwise>
                    <div style="margin-top: 20px; padding: 15px; background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 4px;">
                        <p><strong>No meta field information available.</strong></p>
                        <p>Try refreshing the page or contact your system administrator.</p>
                    </div>
                </xsl:otherwise>
            </xsl:choose>
        </div>
    </xsl:template>
</xsl:stylesheet>
