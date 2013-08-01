<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
  <xsl:template match="/">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
      <head>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <title>Scope Scanning</title>
      </head>
      <body class="yui-skin-sam" onload="nof5();">
        <xsl:apply-templates/>
      </body>
    </html>
  </xsl:template>
  
  <xsl:template match="DisableScopeScansResponse/scopeScansStatus">
    <table>
      <tbody>
        <th align="left">
          <h2>Disable Scope Scans Result</h2>
        </th>
        <tr>
          <td>
            <xsl:choose>
              <xsl:when test="retcode=0">
                Scope Scans Disabled
              </xsl:when>
              <xsl:otherwise>
                Scope Scans Disable failed :
              </xsl:otherwise>
            </xsl:choose>
            <xsl:value-of select="retmsg"/>
          </td>
        </tr>
      </tbody>
    </table>
    
  </xsl:template>
</xsl:stylesheet>
