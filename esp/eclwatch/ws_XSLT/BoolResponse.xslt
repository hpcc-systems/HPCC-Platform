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
