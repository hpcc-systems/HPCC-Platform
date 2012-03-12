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
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <script language="JavaScript1.2">
            <xsl:text disable-output-escaping="yes"><![CDATA[
                function checkfields(f, i)
                {
                    if(i.checked)
                    {
                        f.uidnumber.disabled=false;
                        f.gidnumber.disabled=false;
                        f.homedirectory.disabled=false;
                        f.loginshell.disabled=false;
                    }
                    else
                    {
                        f.uidnumber.disabled=true;
                        f.gidnumber.disabled=true;
                        f.homedirectory.disabled=true;
                        f.loginshell.disabled=true;
                    }
                }
            ]]></xsl:text>
            </script>
        </head>
    <body class="yui-skin-sam" onload="nof5();">
            <xsl:apply-templates/>
        </body>
        </html>
    </xsl:template>
    <xsl:template match="UserPosixInputResponse">
        <form method="POST" action="/ws_access/UserPosix">
        <input type="hidden" id="username" name="username" value="{username}"/>
        <table name="table1">
            <tr>
                <th colspan="2">
                    <h3>POSIX Account<xsl:value-of select="rtitle"/></h3>
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
                <td>Enable Posix:</td>
                <td>
                    <xsl:choose>
                        <xsl:when test="posixenabled=1">
                            <input type="checkbox" name="posixenabled" value="1" checked="1" onclick="checkfields(this.form, this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" name="posixenabled" value="1" onclick="checkfields(this.form, this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            <tr>
                <td>UID Number:</td>
                <td>
                    <xsl:choose>
                        <xsl:when test="posixenabled=1">
                            <input type="text" name="uidnumber" value="{uidnumber}" size="35"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="text" name="uidnumber" value="{uidnumber}" size="35" disabled="true"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            <tr>
                <td>GID Number:</td>
                <td>
                    <xsl:choose>
                        <xsl:when test="posixenabled=1">
                            <input type="text" name="gidnumber" value="{gidnumber}" size="35"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="text" name="gidnumber" value="{gidnumber}" size="35" disabled="true"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            <tr>
                <td>Home Directory:</td>
                <td>
                    <xsl:choose>
                        <xsl:when test="posixenabled=1">
                            <input type="text" name="homedirectory" value="{homedirectory}" size="35"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="text" name="homedirectory" value="{homedirectory}" size="35" disabled="true"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            <tr>
                <td>Login Shell:</td>
                <td>
                    <xsl:choose>
                        <xsl:when test="posixenabled=1">
                            <input type="text" name="loginshell" value="{loginshell}" size="35"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="text" name="loginshell" value="{loginshell}" size="35" disabled="true"/>
                        </xsl:otherwise>
                    </xsl:choose>
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
