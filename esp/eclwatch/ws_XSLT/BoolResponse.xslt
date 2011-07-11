<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template match="/BoolResponse">
<html>
   <head>
      <title>Response</title>
     <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
     <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
     <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
     <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
     <style type="text/css">
         th {text-align: left }
      </style>
      <script type="text/javascript">         
         function onLoad()
         {
            var msgCtrl = document.getElementById('msg');
            msgCtrl.innerText = 'The request ' +
            <xsl:choose>
               <xsl:when test="Response=1">'was processed successfully!';</xsl:when>
               <xsl:otherwise>'failed to complete' + 
                  <xsl:choose>
                     <xsl:when test="string-length(Status)">': '+'<xsl:value-of select="Status"/>'</xsl:when>
                     <xsl:otherwise>'!'</xsl:otherwise>
                  </xsl:choose>
               </xsl:otherwise>
            </xsl:choose>
         }
         
         function onBeforeNext()
         {
            return true;
         }         
      </script>
   </head>
  <body class="yui-skin-sam" onload="nof5();onLoad()">
      <table height="100%" width="100%" align="center" valign="center">
         <tr>
            <td><h3 id="msg"/></td>
         </tr>
      </table>
   </body>
</html>
</xsl:template>

</xsl:stylesheet>
