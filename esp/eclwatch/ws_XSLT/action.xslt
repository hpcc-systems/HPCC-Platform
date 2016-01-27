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

<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
                              xmlns:xlink="http://www.w3.org/1999/xlink"
                              xmlns:svg="http://www.w3.org/2000/svg">
<xsl:output method="html"/>
<xsl:variable name="backtopage" select="/DFUArrayActionResponse/BackToPage"/>
<xsl:variable name="redirectto" select="/DFUArrayActionResponse/RedirectTo"/>
<xsl:template match="/">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
    <head>
        <title>DFU</title>
    <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
    <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
    <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
    <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
    <script language="JavaScript1.2">
            var backToPageLink='<xsl:value-of select="$backtopage"/>';
      var redirectTo='<xsl:value-of select="$redirectto"/>';
      <xsl:text disable-output-escaping="yes"><![CDATA[
                function backToPage()
                {
                    document.location.href = backToPageLink;
                    return false;
                }

        function onLoad()
              {
          if (redirectTo != '')
          {
            var url = '/WsDFU/AddtoSuperfile?BackToPage=';
            url += backToPageLink;
            document.getElementById("Subfiles").value=redirectTo;
            document.forms["AddSubFiles"].action=url;
            document.forms['AddSubFiles'].submit();
          }
                    return false;
        }
            ]]></xsl:text>
        </script>       
    </head>
    <body class="yui-skin-sam" onload="nof5();onLoad()">
    <xsl:choose>
      <xsl:when test="string-length(/DFUArrayActionResponse/RedirectTo) > 0">
        <form id="AddSubFiles" name="AddSubFiles" method="POST" action="/WsDFU/AddtoSuperfile">
          <tr>
            <b>Please wait ...</b>
          </tr>
          <input type="hidden" id="Subfiles" name="Subfiles" value="{RedirectTo}"/>
        </form>
      </xsl:when>
      <xsl:otherwise>
            <tr>
                <b>Action status:</b>
            </tr>
            <table>
                <xsl:for-each select="//ActionResults/*">
                    <br/>
                    <xsl:value-of select="ActionResult"/>
                </xsl:for-each>
            </table>
            <xsl:choose>             
                <xsl:when test="$backtopage!=''">
            <input type="button" class="sbutton" value="Go Back" onclick="backToPage()"/>
                </xsl:when>
                <xsl:otherwise>
                    <input id="backBtn" type="button" value="Go Back" onclick="history.go(-1)"/>
                </xsl:otherwise>
            </xsl:choose>
      </xsl:otherwise>
    </xsl:choose>
    </body>
</html>

          
</xsl:template>
</xsl:stylesheet>
