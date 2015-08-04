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
