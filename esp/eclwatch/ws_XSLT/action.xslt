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
                <xsl:for-each select="//Message">
                    <xsl:choose>             
                        <xsl:when test="string-length(href)">
                            <a href="{href}"><xsl:value-of select="Value"/></a>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="Value"/>
                        </xsl:otherwise>
                    </xsl:choose>
                    <br/>
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
