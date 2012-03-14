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
            <title>Change Password</title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <script type="text/javascript" language="javascript">
            <![CDATA[
            function handleSubmitBtn()
            {
                document.getElementById("submit").disabled = (document.getElementById("oldpass").value=='') || (document.getElementById("newpass1").value=='') || (document.getElementById("newpass2").value=='');
            }
            ]]></script>
        </head>
    <body class="yui-skin-sam" onload="nof5();">
            <p align="left" />
            <xsl:apply-templates/>
        </body>
        </html>
    </xsl:template>
    <xsl:template match="UpdateUserInputResponse">
        <form name="esp_form" method="POST" action="/ws_account/UpdateUser">
            <input type="hidden" id="username" name="username" value="{username}"/>
            <table>
                <tr><th colspan="2"><h3>Change Password</h3></th></tr>
                <tr><td><b>User Name: </b></td><td><input type="text" id="username2" name="username2" size="20" value="{username}" disabled="disabled"/></td></tr>
                <tr><td><b>Old Password: </b></td><td><input id="oldpass" type="password" name="oldpass" size="20" value=""  onchange="handleSubmitBtn()" onblur="handleSubmitBtn()" onkeypress="handleSubmitBtn()"/></td></tr>
                <tr><td><b>New password: </b></td><td><input id="newpass1" type="password" name="newpass1" size="20" value=""  onchange="handleSubmitBtn()" onblur="handleSubmitBtn()" onkeypress="handleSubmitBtn()"/></td></tr>
                <tr><td><b>Retype new password: </b></td><td><input id="newpass2" type="password" name="newpass2" size="20" value=""  onchange="handleSubmitBtn()" onblur="handleSubmitBtn()" onkeypress="handleSubmitBtn()"/></td></tr>
                <tr><td></td><td><input type="submit" value="Submit" id="submit" name="S1" disabled="true"/> <xsl:text disable-output-escaping="yes"> </xsl:text><input type="reset" value="Clear" onClick="S1.disabled=true"/> </td> </tr>
            </table>
        </form>
    </xsl:template>
</xsl:stylesheet>
