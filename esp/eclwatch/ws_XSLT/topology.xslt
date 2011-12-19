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
    
    <xsl:variable name="apos">'</xsl:variable>
  <xsl:variable name="enableSNMP" select="/TpClusterQueryResponse/EnableSNMP"/>

  <xsl:template match="TpClusterQueryResponse">
    <html>
      <head>
        <title>Topology</title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script type="text/javascript" src="/esp/files_/scripts/multiselect.js">
        </script>
        <script type="text/javascript">
          var enable_SNMP = '<xsl:value-of select="$enableSNMP"/>';
          <xsl:text disable-output-escaping="yes">
            <![CDATA[
              function onLoad()
              {
                initSelection('clustersTable');
                onRowCheck(true);
              }

              function onRowCheck(checked)
              {
                if (enable_SNMP > 0)
                {
                  var disable = checkedCount == 0;
                  document.getElementById('submit1').disabled = disable;
                  document.getElementById('submit2').disabled = disable;
                  document.getElementById('submit3').disabled = disable;
                  document.getElementById('submit4').disabled = disable;
                }
              }

              function onSubmit(bStop)
              {
                var rc = 0;
                if (enable_SNMP > 0)
                {
                  var username = document.getElementById("submit3").value;
                  var password = document.getElementById("submit4").value;
                  if (username == null || password == null || username == '' || password == '')
                  {
                    alert("Both UserName and Password have to be filled in.");
                    return false;
                  }
                  rc = confirm('Are you sure you want to ' + (bStop ? 'stop':'start') + ' the selected cluster(s)?');
                  if (rc)
                    document.forms[0].action='/ws_machine/StartStopBegin?Key1=' + username + '&Key2=' + password + '&Stop=' + (bStop ? '1' : '0');
                }
                else
                {
                  rc = confirm('Are you sure you want to ' + (bStop ? 'stop':'start') + ' the selected cluster(s)?');
                  if (rc)
                    document.forms[0].action='/ws_machine/StartStopBegin?Stop=' + (bStop ? '1' : '0');
                }
                return rc;
              }

              function getConfigXML(url) 
              {
                document.location.href = url;
              }                     
            ]]>
          </xsl:text>
        </script>
      </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
        <form action="" method="post">
          <xsl:choose>
            <xsl:when test="TpClusters/TpCluster">
              <table class="sort-table" id="clustersTable">
                <caption><h2 align="left">Clusters</h2></caption>
                <colgroup>
                  <col width="1%" align="center"/>
                  <col width="2%" align="center"/>
                  <col width="18%" align="left"/>
                  <col width="9%" align="left"/>
                  <col width="10%" align="left"/>
                  <col width="30%" align="left"/>
                  <col width="30%" align="left"/>
                </colgroup>
                <tr class="grey">
                  <th id="selectAll1">
                    <input type="checkbox" title="Select or deselect all clusters" onclick="selectAll(this.checked)"/>
                  </th>
                  <th colspan="2" align="left">Name</th>
                  <th>Type</th>
                  <th>Platform</th>
                  <th>Directory</th>
                  <th>Log Directory</th>
                </tr>
                <xsl:apply-templates select="TpClusters/TpCluster">
                  <xsl:sort select="Name"/>
                </xsl:apply-templates>
              </table>
              <table cellpadding="0" width="100%">
                <tr>
                  <th id="selectAll2" width="1%" style="padding-left:4px" align="left">
                    <input type="checkbox" title="Select or deselect all clusters" onclick="selectAll(this.checked)"></input>
                  </th>
                  <th colspan="5" align="left">Select All / None</th>
                </tr>
                <tr>
                  <td height="20"/>
                </tr>
                <xsl:if test="$enableSNMP != 0">
                  <tr>
                    <td><b>User Name: </b></td>
                    <td><input type="text" id="submit3" name="submit3" size="20" value=""/></td>
                  </tr>
                  <tr>
                    <td><b>Password: </b></td>
                    <td><input type="password" id="submit4" name="submit4" size="20" value=""/></td>
                  </tr>
                  <tr>
                    <td/>
                    <td>
                      <input type="submit" id="submit1" value="Start" onclick="return onSubmit(0)"/>
                      <input type="submit" id="submit2" value="Stop" onclick="return onSubmit(1)"/>
                    </td>
                  </tr>
                </xsl:if>
              </table>
            </xsl:when>
            <xsl:otherwise>
              <h3 align="center">Clusters</h3>
              <br/>No clusters defined.
            </xsl:otherwise>
          </xsl:choose>
        </form>
      </body>
    </html>
  </xsl:template>
    
  <xsl:template match="TpCluster">
    <tr onmouseenter="this.oldClass=this.className; this.className='hilite';" onmouseleave="this.className=this.oldClass">
      <xsl:attribute name="class">
        <xsl:choose>
          <xsl:when test="position() mod 2">odd</xsl:when>
          <xsl:otherwise>even</xsl:otherwise>
        </xsl:choose>
      </xsl:attribute>
      <xsl:variable name="type2">
        <xsl:choose>
          <xsl:when test="Type='RoxieCluster'">Roxie</xsl:when>
          <xsl:when test="Type='ThorCluster'">Thor</xsl:when>
          <xsl:when test="Type='HoleCluster'">Hole</xsl:when>
        </xsl:choose>
      </xsl:variable>
      <xsl:variable name="type3" select="translate($type2, 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')"/>
      <xsl:variable name="absolutePath">
        <xsl:call-template name="makeAbsolutePath">
          <xsl:with-param name="path" select="Directory"/>
          <xsl:with-param name="isLinuxInstance" select="OS!=0"/>
        </xsl:call-template>
      </xsl:variable>
      <xsl:variable name="logDir" select="string(LogDirectory)"/>
      <xsl:variable name="logPath">
        <xsl:choose>
          <xsl:when test="$logDir!='' and $logDir!='.'">
            <xsl:call-template name="makeAbsolutePath">
              <xsl:with-param name="path">
                <xsl:choose>
                  <xsl:when test="starts-with($logDir, '.')">
                    <xsl:value-of select="Directory"/>
                    <xsl:value-of select="substring($logDir, 2)"/>
                  </xsl:when>
                  <xsl:when test="starts-with($logDir, '\') and string(OS)='0'">
                    <xsl:value-of select="substring(Directory, 1, 2)"/>
                    <xsl:value-of select="$logDir"/>
                  </xsl:when>
                  <xsl:otherwise>
                    <xsl:value-of select="$logDir"/>
                  </xsl:otherwise>
                </xsl:choose>
              </xsl:with-param>
              <xsl:with-param name="isLinuxInstance" select="OS!=0"/>
            </xsl:call-template>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="$absolutePath"/>
          </xsl:otherwise>
                </xsl:choose>                                                       
            </xsl:variable>
      <td>
        <!--we encode name, type, os and path for component - the service uses the name (first token)
        to look up component's address-->
        <input type="checkbox" name="Addresses_i{count(preceding::TpCluster)}" 
        value="{Name}:{Type}:{Name}:{OS}:{translate(Directory, ':', '$')}:{$type3}MACHINES:{Path}" onclick="return clicked(this, event)">
        </input>
      </td>
      <td width="45" nowrap="true">
        <xsl:variable name="href0">
          <xsl:text disable-output-escaping="yes">/esp/iframe?esp_iframe_title=Configuration file for </xsl:text>
          <xsl:value-of select="concat($type2, ' cluster - ', Name)"/>
          <xsl:text disable-output-escaping="yes">&amp;inner=/WsTopology/TpGetComponentFile%3fFileType%3dcfg%26CompType%3d</xsl:text>
          <xsl:value-of select="concat($type2, 'Cluster%26CompName%3d', Name, '%26Directory%3d', $absolutePath, '%26OsType%3d', OS)"/>
        </xsl:variable>
        <img onclick="getConfigXML('{$href0}')" border="0" src="/esp/files_/img/config.png" alt="View configuration file..." width="14" height="14"/>
      </td>
      <td>
        <a  href="/WsTopology/TpMachineQuery?Type={$type3}MACHINES&amp;Cluster={Name}&amp;Path={Path}&amp;Directory={Directory}&amp;LogDirectory={$logPath}">
          <xsl:value-of select="Name"/>
        </a>
      </td>
      <td>
        <xsl:value-of select="$type2"/>
      </td>
      <td>
        <xsl:choose>
          <xsl:when test="OS=0">Windows</xsl:when>
          <xsl:when test="OS=2">Linux</xsl:when>
          <xsl:when test="OS=1">Solaris</xsl:when>
          <xsl:otherwise>Unknown</xsl:otherwise>
        </xsl:choose>
      </td>
      <td>
        <xsl:value-of select="$absolutePath"/>
      </td>
      <td>
        <xsl:value-of select="$logPath"/>
      </td>
    </tr>
  </xsl:template>
    
  <xsl:template name="makeAbsolutePath">
    <xsl:param name="path"/>
    <xsl:param name="isLinuxInstance"/>
    <xsl:variable name="oldPathSeparator">
      <xsl:choose>
        <xsl:when test="$isLinuxInstance">'\:'</xsl:when>
        <xsl:otherwise>'/$'</xsl:otherwise>
      </xsl:choose> 
    </xsl:variable>

    <xsl:variable name="newPathSeparator">
      <xsl:choose>
        <xsl:when test="$isLinuxInstance">'/$'</xsl:when>
        <xsl:otherwise>'\:'</xsl:otherwise>
      </xsl:choose> 
    </xsl:variable>

    <xsl:variable name="newPath" select="translate($path, $oldPathSeparator, $newPathSeparator)"/>
    <xsl:if test="$isLinuxInstance and not(starts-with($newPath, '/'))">/</xsl:if>          
    <xsl:value-of select="$newPath"/>   
  </xsl:template>
    
</xsl:stylesheet>
