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
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>POSIX Account</title>
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
    </head>
    <body class="yui-skin-sam" onload="nof5();">
            <xsl:apply-templates/>
        </body>
        </html>
    </xsl:template>
    <xsl:template match="UserInfoEditInputResponse">
        <form method="POST" action="/ws_access/UserInfoEdit">
        <input type="hidden" id="username" name="username" value="{username}"/>
        <table name="table1">
            <tr>
                <th colspan="2">
                    <h3>User Info Edit</h3>
                </th>
            </tr>
            <tr>
                <td height="10"/>
            </tr>
            <tr>
                <td>User Name:</td>
                <td>
                    <input type="text" name="username0" value="{username}" size="35" disabled="disabled"/>
                </td>
            </tr>
            <tr>
                <td>First Name:</td>
                <td>
                    <input type="text" name="firstname" value="{firstname}" size="35"/>
                </td>
            </tr>
            <tr>
                <td>Last Name:</td>
                <td>
                    <input type="text" name="lastname" value="{lastname}" size="35"/>
                </td>
            </tr>
            <tr>
                <td/>
                <td><input type="submit" value="Submit" name="S1"/></td>
            </tr>
        </table>
        </form>
    </xsl:template>
</xsl:stylesheet>
