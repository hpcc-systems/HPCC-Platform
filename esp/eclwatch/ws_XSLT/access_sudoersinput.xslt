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
        </head>
    <body class="yui-skin-sam">
            <xsl:apply-templates/>
        </body>
        </html>
    </xsl:template>
    <xsl:template match="UserSudoersInputResponse">
        <form method="POST" action="/ws_access/UserSudoers">
        <input type="hidden" id="username" name="username" value="{username}"/>
        <table name="table1">
            <tr>
                <th colspan="2">
                    <h3>Edit Sudoers For User <xsl:value-of select="username"/></h3>
                </th>
            </tr>
            <tr>
                <td>Username:</td>
                <td>
                    <input type="text" name="username0" value="{username}" size="35" disabled="disabled"/>
                </td>
            </tr>
            <tr>
                <td>sudoHost:</td>
                <td>
                    <input type="text" name="sudoHost" value="{sudoHost}" size="35"/>
                </td>
            </tr>
            <tr>
                <td>sudoCommand:</td>
                <td>
                    <input type="text" name="sudoCommand" value="{sudoCommand}" size="35"/>
                </td>
            </tr>
            <tr>
                <td>sudoOption:</td>
                <td>
                    <input type="text" name="sudoOption" value="{sudoOption}" size="35"/>
                </td>
            </tr>
            <tr>
                <td height="10"/>
            </tr>
            <tr>
                <td/>
                <xsl:choose>
                    <xsl:when test="insudoers=0">
                    <input type="hidden" name="action" value="add"/>
                    <td><input type="submit" class="sbutton" value="  Add  " name="add"/></td>
                    </xsl:when>
                    <xsl:otherwise>
                    <td><input type="submit" class="sbutton" value="Delete" name="action"/>
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text><input type="submit" class="sbutton" value="Update" name="action"/></td>
                    </xsl:otherwise>
                </xsl:choose>
            </tr>
        </table>

        </form>
    </xsl:template>
</xsl:stylesheet>
