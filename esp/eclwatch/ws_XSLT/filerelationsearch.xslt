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
    <xsl:variable name="started" select="FileRelationSearchResponse/Started"/>
  <xsl:variable name="filename" select="FileRelationSearchResponse/FileName"/>
  <xsl:variable name="clusterid" select="FileRelationSearchResponse/ClusterID"/>
    <xsl:variable name="relationtypeid" select="FileRelationSearchResponse/RelationTypeID"/>
    <xsl:template match="FileRelationSearchResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <xsl:text disable-output-escaping="yes"><![CDATA[
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
                  <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/container/container_core-min.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
        ]]></xsl:text>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>EclWatch</title>
                <script language="JavaScript1.2">
                    var intervalId = 0;
                    var hideLoading = 1;
                    var started0 = '<xsl:value-of select="$started"/>';
          var fileName = '<xsl:value-of select="$filename"/>';
          var clusterID = '<xsl:value-of select="$clusterid"/>';
          var relationTypeID = '<xsl:value-of select="$relationtypeid"/>';
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                    function goplus()
                   {
                        var url = "/WsRoxieQuery/FileRelationSearch?Started=1";
                        if(document.getElementById("FileName").value != '')
                            url += "&FileName=" + document.getElementById("FileName").value;
                        
            var clusterC = document.getElementById("ClusterName").selectedIndex;
                        if(clusterC > -1)
                        {
                            var cluster = document.getElementById("ClusterName").options[clusterC].text;
                            url += "&Cluster=" + cluster;
              url += "&ClusterID=" + clusterC;
                        }
            
            var filetypeS = document.getElementById("RelationType").selectedIndex;
                        if(filetypeS > -1)
                        {
                            var filetype = document.getElementById("RelationType").options[filetypeS].text;
                            url += "&RelationType=" + filetype;
              url += "&RelationTypeID=" + filetypeS;
                        }

                       document.location.href=url;
                   }    
                    
                    function onFindClick()
                    {
                        var url = "/WsRoxieQuery/FileRelationList?GoBackURL=/WsRoxieQuery/FileRelationSearch";
                        
                        if(document.getElementById("FileName").value != '')
                            url += "&FileName=" + document.getElementById("FileName").value;

                        var clusterC = document.getElementById("ClusterName").selectedIndex;
                        if(clusterC > -1)
                        {
                            var cluster = document.getElementById("ClusterName").options[clusterC].text;
                            url += "&Cluster=" + cluster;
                            url += "&ClusterID=" + clusterC;
                        }

                        var filetypeS = document.getElementById("RelationType").selectedIndex;
                        if(filetypeS > -1)
                        {
                            var filetype = document.getElementById("RelationType").options[filetypeS].text;
                            url += "&RelationType=" + filetype;
                            url += "&RelationTypeID=" + filetypeS;
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

            function onRelationTypeChange()
            {
              if (document.getElementById("RelationType").selectedIndex == 1)
              {
                document.getElementById("ClusterName").disabled = false;
              }
              else
              {
                document.getElementById("ClusterName").disabled = true;
              }
            }

                       function onLoad()
                       {
              document.getElementById("FileName").value = fileName;
                            document.getElementById("ClusterName").selectedIndex = clusterID;
                            document.getElementById("RelationType").selectedIndex = relationTypeID;

              //onRelationTypeChange();

                            if (started0 > 0)
                            {
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
                <h4>Search File Relationships:</h4>
                <table>
          <tr>
            <td>
              <b>Relationship:</b>
            </td>
            <td>
              <!--select id="RelationType" name="RelationType" size="1" onChange="onRelationTypeChange()"-->
              <select id="RelationType" name="RelationType" size="1">
                <option value="1" selected="selected">Parent File and Index</option>
                <option value="2">Original File and Index</option>
              </select>
            </td>
          </tr>
          <tr><th colspan="2"/></tr>
          <tr>
            <td>
              <b>File:</b>
            </td>
            <td>
              <input size="20" id="FileName" name="FileName"/>
            </td>
          </tr>
          <tr>
                        <td><b>Cluster:</b></td>
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
