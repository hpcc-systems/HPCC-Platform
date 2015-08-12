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
    <xsl:include href="/esp/xslt/lib.xslt"/>

    <xsl:template match="QueryDetail">
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
                <h4>Roxie Query Details</h4>
                <form method="post">
                    <table class="workunit">
                        <colgroup>
                            <col width="20%"/>
                            <col width="80%"/>
                        </colgroup>
                        <tr>
                            <th>Query Name:</th>
                            <td>
                               <xsl:value-of select="QueryName"/>
                            </td>
                        </tr>
                        <xsl:if test="string-length(QueryText)">
                            <tr>
                                <th>Query:</th>
                                <td>
                                    <textarea readonly="true" rows="5" STYLE="width:600"><xsl:value-of select="QueryText"/></textarea>
                                </td>
                            </tr>
                        </xsl:if>
                        <tr>
                            <th>Result Size:</th>
                            <td>
                                <xsl:value-of select="ResultSize"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Receive Time:</th>
                            <td>
                                <xsl:value-of select="ReceiveTime"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Finish Time:</th>
                            <td>
                                <xsl:value-of select="FinishTime"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Seconds Taken:</th>
                            <td>
                                <xsl:value-of select="SecondsTaken"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Time Taken:</th>
                            <td>
                                <xsl:value-of select="TimeTaken"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Memory:</th>
                            <td>
                                <xsl:value-of select="Memory"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Priority:</th>
                            <td>
                                <xsl:value-of select="Priority"/>
                            </td>
                        </tr>
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
                            <th>Source IP:</th>
                            <td>
                                <xsl:value-of select="SourceIP"/>
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
                        <tr>
                            <th>UID:</th>
                            <td>
                                <xsl:value-of select="UID"/>
                            </td>
                        </tr>
                        <tr>
                            <th>TID:</th>
                            <td>
                                <xsl:value-of select="TID"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Slaves Reply:</th>
                            <td>
                                <xsl:value-of select="SlavesReply"/>
                            </td>
                        </tr>
                        <tr>
                            <th>Continue:</th>
                            <td>
                                <xsl:value-of select="Continue"/>
                            </td>
                        </tr>
                        <tr>
                            <th>IsBlind:</th>
                            <td>
                                <xsl:choose>
                                    <xsl:when test="IsBlind > 0">true</xsl:when>
                                    <xsl:otherwise>false</xsl:otherwise>
                                </xsl:choose>
                            </td>
                        </tr>
                        <tr>
                            <th>IsSoap:</th>
                            <td>
                                <xsl:choose>
                                    <xsl:when test="IsSoap > 0">true</xsl:when>
                                    <xsl:otherwise>false</xsl:otherwise>
                                </xsl:choose>
                            </td>
                        </tr>
                    </table>
                </form>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="text()|comment()"/>

</xsl:stylesheet>
