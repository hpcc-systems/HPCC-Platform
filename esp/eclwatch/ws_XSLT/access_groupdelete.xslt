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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
<xsl:output method="html"/>
    <xsl:output method="html"/>
    <xsl:variable name="groupnamestr" select="/GroupActionResponse/Groupnames"/>
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
                <script language="JavaScript1.2">
                         var groupnamestr = '<xsl:value-of select="$groupnamestr"/>';
                   <xsl:text disable-output-escaping="yes"><![CDATA[
                                 function btnclicked(action)
                         {
                                    if (action < 1 || groupnamestr=='')
                                        document.location.href='/ws_access/Groups';
                                    else
                                        document.location.href='/ws_access/GroupAction?ActionType=Delete&'+groupnamestr;
                         }

                   ]]></xsl:text>
                </script>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Delete Group Result</title>
        </head>
    <body class="yui-skin-sam" onload="nof5();">
            <xsl:apply-templates/>
        </body>
        </html>
    </xsl:template>

    <xsl:template match="GroupActionResponse">
        <xsl:choose>
            <xsl:when test="not(Permissions/Permission[1])">
                <table>
                    <tbody>
                        <th align="left">
                            <h2>Delete Group Result</h2>
                        </th>
                        <tr>
                            <td>
                                <xsl:choose>
                                    <xsl:when test="retcode=0">
                                        Groups have been deleted successfully.
                                    </xsl:when>
                                    <xsl:otherwise>
                                        Failed to delete groups, <xsl:value-of select="retmsg"/>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </td>
                        </tr>
                        <tr>
                            <td>
                            <br/>
                            <br/>
                            <a href="javascript:go('/ws_access/Groups')">Groups</a></td>
                        </tr>
                    </tbody>
                </table>
            </xsl:when>
            <xsl:otherwise>
                <h4><b>The permissions associated with the group(s) will also be deleted.</b></h4>
                <table>
                    <tr>
                        <td colspan="4">
                            <table class="list" width="500">
                                <colgroup style="vertical-align:top;">
                                    <col/>
                                    <col/>
                                    <col/>
                                </colgroup>
                                <tr class="grey"><th>Group</th><th>Resource Type</th><th>Resource Name</th></tr>
                                <xsl:apply-templates select="Permissions/Permission">
                                    <xsl:sort select="Group" data-type="number"/>
                                </xsl:apply-templates>
                            </table>
                        </td>
                    </tr>
                    <tr>
                        <td><br/>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <input type="submit" class="sbutton" style="width: 120px;" id="Continue" value="ContinueDelete" onclick="btnclicked(1)"/>
                        </td>
                        <td>
                            <input type="submit" class="sbutton" style="width: 100px;" id="Cancel" value="CancelDelete" onclick="btnclicked(0)"/>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <br/>
                            <br/>
                            <a href="javascript:go('/ws_access/Groups')">Groups</a>
                        </td>
                    </tr>
                </table>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="Permission">
        <tr>
            <td><xsl:value-of select="PermissionName"/></td>
            <td><xsl:value-of select="RType"/></td>
            <td><xsl:value-of select="ResourceName"/></td>
        </tr>
    </xsl:template>

</xsl:stylesheet>
