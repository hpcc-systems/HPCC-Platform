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
  <xsl:output method="html"/>
  <xsl:variable name="started" select="RoxieFileSearchResponse/Started"/>
  <xsl:variable name="clusterid" select="RoxieFileSearchResponse/ClusterID"/>
  <xsl:variable name="filetypeid" select="RoxieFileSearchResponse/FileTypeID"/>
  <xsl:template match="RoxieFileSearchResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
      <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <title>EclWatch</title>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script language="JavaScript1.2">
          var intervalId = 0;
          var hideLoading = 1;
          var started0 = '<xsl:value-of select="$started"/>';
          var clusterID = '<xsl:value-of select="$clusterid"/>';
          var fileTypeID = '<xsl:value-of select="$filetypeid"/>';
          <xsl:text disable-output-escaping="yes"><![CDATA[
            function goplus()
            {
              var url = "/WsRoxieQuery/RoxieFileSearch?Started=1";
              if(document.getElementById("ClusterName").selectedIndex > -1)
                url += "&ClusterID=" + document.getElementById("ClusterName").selectedIndex;
              if(document.getElementById("FileType").selectedIndex > -1)
                url += "&FileTypeID=" + document.getElementById("FileType").selectedIndex;

              document.location.href=url;
            }   
                
            function onFindClick()
            {
              var url = "/WsRoxieQuery/QueryFileList?GoBackURL=/WsRoxieQuery/RoxieFileSearch";

              var clusterC = document.getElementById("ClusterName").selectedIndex;
              if(clusterC > -1)
              {
                var cluster = document.getElementById("ClusterName").options[clusterC].text;
                url += "&Cluster=" + cluster;
              }

              var filetypeS = document.getElementById("FileType").selectedIndex;
              if(filetypeS > -1)
              {
                var filetype = document.getElementById("FileType").options[filetypeS].text;
                url += "&FileType=" + filetype;
              }

              document.location.href=url;

              return;
            }       

            function doBlink() 
            {
              var obj = document.getElementById('loadingMsg');
              obj.style.visibility = obj.style.visibility == "" ? "hidden" : "";

              if (hideLoading > 0 && intervalId && obj.style.visibility == "hidden")
              {              
                clearInterval (intervalId);
                intervalId = 0;
              }   
            }

            function startBlink() 
            {
              if (document.all)
              {
                hideLoading = 0;
                intervalId = setInterval("doBlink()",1000);
              }
            }

            function loadPageTimeOut() 
            {
              hideLoading = 1;

              var obj = document.getElementById('SearchBtn');
              obj.style.dvisibility = "";

              var obj = document.getElementById('loadingMsg');
              obj.style.display = "none";

              var obj = document.getElementById('loadingTimeOut');
              obj.style.visibility = "";
            }

            function onLoad()
            {
              if (started0 > 0)
              {
                document.getElementById("ClusterName").selectedIndex = clusterID;
                document.getElementById("FileType").selectedIndex = fileTypeID;

                var obj = document.getElementById('SearchBtn');
                obj.style.display = "none";

                startBlink();
                reloadTimer = setTimeout("loadPageTimeOut()", 300000);

                onFindClick();
              }
              else
              {
                var obj = document.getElementById('loadingMsg');
                obj.style.display = "none";

                var obj = document.getElementById('loadingTimeOut');
                obj.style.display = "none";
              }

              return;
            }   

          ]]></xsl:text>
        </script>
      </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
        <h4>Search Roxie Files:</h4>
        <table>
          <tr>
            <th colspan="2"/>
          </tr>
          <tr>
            <td>
              <b>Cluster:</b>
            </td>
            <td>
              <select id="ClusterName" name="ClusterName" size="1">
                <xsl:for-each select="ClusterNames/ClusterName">
                  <option>
                    <xsl:value-of select="."/>
                  </option>
                </xsl:for-each>
              </select>
            </td>
          </tr>
          <tr>
            <td>
              <b>File Type:</b>
            </td>
            <td>
              <select id="FileType" name="FileType" size="1">
                <xsl:for-each select="FileTypeSelections/FileTypeSelection">
                  <option>
                    <xsl:value-of select="."/>
                  </option>
                </xsl:for-each>
              </select>
            </td>
          </tr>
          <tr>
            <td/>
            <td>
              <input type="submit" id="SearchBtn" value="Search" class="sbutton" onclick="goplus()"/>
            </td>
          </tr>
        </table>
        <span id="loadingMsg"><h3>Loading, please wait...</h3></span>
        <span id="loadingTimeOut" style="visibility:hidden"><h3>Browser timed out due to a long time delay.</h3></span>
      </body>
    </html>
  </xsl:template>
</xsl:stylesheet>
