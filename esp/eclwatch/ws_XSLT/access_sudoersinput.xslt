<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
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
