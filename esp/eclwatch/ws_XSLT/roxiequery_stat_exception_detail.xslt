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
    <xsl:include href="/esp/xslt/lib.xslt"/>

    <xsl:template match="ExceptionDetail">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Roxie Query Details</title>
        <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      </head>
      <body class="yui-skin-sam" onload="nof5();">
                <h4>Roxie Query Exception Details</h4>
                <form method="post">
                    <table class="workunit">
                        <colgroup>
                            <col width="20%"/>
                            <col width="80%"/>
                        </colgroup>
                        <tr>
                            <th>Receive Time:</th>
                            <td>
                                <xsl:value-of select="ReceiveTime"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Error Class:</th>
                            <td>
                                <xsl:value-of select="ErrorClass"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Error Code:</th>
                            <td>
                                <xsl:value-of select="ErrorCode"/>
                            </td>
                        </tr>
                        <xsl:if test="string-length(ErrorMessage)">
                            <tr>
                                <th>Error Message:</th>
                                <td>
                                    <textarea readonly="true" rows="5" STYLE="width:600"><xsl:value-of select="ErrorMessage"/></textarea>
                                </td>
                            </tr>
                        </xsl:if>
                        <tr>
                            <th>PID:</th>
                            <td>
                                <xsl:value-of select="PID"/>
                            </td>
                        </tr>
                        <tr>
                            <th>TID:</th>
                            <td>
                                <xsl:value-of select="TID"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Roxie IP:</th>
                            <td>
                                <xsl:value-of select="RoxieIP"/>
                            </td>
                        </tr>
                        <tr>
                            <th>File Path:</th>
                            <td>
                                <xsl:value-of select="FilePath"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Position:</th>
                            <td>
                                <xsl:value-of select="Position"/>
                            </td>
                        </tr>
                    </table>
                </form>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="text()|comment()"/>

</xsl:stylesheet>
