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
            <title>My Account</title>
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
      <xsl:template match="MyAccountResponse">
        <form name="esp_form" method="POST" action="/ws_account/MyAccount">
          <table>
            <tr>
              <th colspan="5">
                <h3>Your ESP Account</h3>
              </th>
            </tr>
            <tr>
              <td>
                <b>
                  <xsl:text>User Name: </xsl:text>
                </b>
              </td>
              <td>
                <xsl:value-of select="username"/>
              </td>
            </tr>
            <tr>
              <td>
                <b>First Name: </b>
              </td>
              <td>
                <xsl:value-of select="firstName"/>
              </td>
            </tr>
            <tr>
              <td>
                <b>Last Name: </b>
              </td>
              <td>
                <xsl:value-of select="lastName"/>
              </td>
            </tr>
            <tr>
              <td>
                <b>Password Expiration: </b>
              </td>
              <td>
                <xsl:value-of select="passwordExpiration"/>
              </td>
            </tr>
            <xsl:if test="passwordDaysRemaining != -2">
              <tr>
                <td>
                  <b>Days Until Expiration: </b>
                </td>
                <td>
                  <xsl:value-of select="passwordDaysRemaining"/>
                </td>
              </tr>
            </xsl:if>
          </table>
        </form>
      </xsl:template>
</xsl:stylesheet>
