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
            <title>Reset Password</title>
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <script type="text/javascript" language="javascript">
            <![CDATA[
            function handleSubmitBtn(f)
            {
                disable = (f.username.value  == '') || (f.newPassword.value=='') || (f.newPasswordRetype.value=='')
                f.S1.disabled = disable;
            }
            ]]></script>
        </head>
    <body class="yui-skin-sam" onload="nof5();">
            <p align="left" />
            <xsl:apply-templates/>
        </body>
        </html>
    </xsl:template>
    <xsl:template match="UserResetPassInputResponse">
        <form name="esp_form" method="POST" action="/ws_access/UserResetPass">
            <input type="hidden" id="username" name="username" value="{username}"/>
            <table>
                <tr><th colspan="2"><h3>Reset Password</h3></th></tr>
                <tr><td><b>User Name: </b></td><td><input type="text" name="username0" size="20" value="{username}" disabled="disabled"/></td></tr>
                <tr><td><b>New password: </b></td><td><input type="password" name="newPassword" size="20" value=""  onchange="handleSubmitBtn(this.form)" onblur="handleSubmitBtn(this.form)" onkeypress="handleSubmitBtn(this.form)"/></td></tr>
                <tr><td><b>Retype new password: </b></td><td><input type="password" name="newPasswordRetype" size="20" value=""  onchange="handleSubmitBtn(this.form)" onblur="handleSubmitBtn(this.form)" onkeypress="handleSubmitBtn(this.form)"/></td></tr>
                <tr><td></td><td><input type="submit" value="Submit" name="S1" disabled="true"/><xsl:text> </xsl:text><input type="reset" value="Clear" onClick="S1.disabled='true'"/> </td> </tr>
            </table>
        </form>
    </xsl:template>
</xsl:stylesheet>
