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
  <xsl:param name="statusCode" select="/QueryClustersResponse/StatusCode"/>
  <xsl:param name="statusMessage" select="/QueryClustersResponse/StatusMessage" />
  <xsl:output method="html"/>
  <xsl:template match="QueryClustersResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>EclWatch</title>

        <script language="JavaScript1.2">
          var clusterIndex = 0;
          <xsl:for-each select="Clusters/Cluster[Type='roxie']">
              parent.window.ClusterNames[<xsl:value-of select="position()" />]="<xsl:value-of select="Name" />";
              if (parent.window.ClusterNames[<xsl:value-of select="position()" />] == parent.window.cluster)
              {
              clusterIndex = <xsl:value-of select="position()" />;
              }
          </xsl:for-each>

          <xsl:for-each select="Clusters/Cluster[Type='roxie']">
          parent.window.ClusterTypes[<xsl:value-of select="position()" />]="<xsl:value-of select="Type" />";
          </xsl:for-each>
          
          function onLoad()
          {
            var childCluster = document.getElementById('ClusterCell');
            if (childCluster)
            {
              var parentCluster = parent.document.getElementById('ClusterCell');
              if (parentCluster)
              {
                parentCluster.innerHTML = childCluster.innerHTML;
                var selectList = parent.document.getElementById('ClusterSelect');
                if (selectList)
                {
                  selectList.selectedIndex = clusterIndex-1;
                  if (clusterIndex != 0)
                  {
                    parent.window.onClusterSelect(clusterIndex);
                  }
                }
              }
            }
          }
        </script>
            </head>
      <body class="yui-skin-sam" onload="onLoad()">
          <table>
            <tr>
              <td id="ClusterCell">
                <xsl:choose>
                  <xsl:when test="$statusCode &gt; -1">
                    <select id="ClusterSelect" name="ClusterSelect" onchange="onClusterSelect(this.value);" style="width:100%;">
                      <xsl:apply-templates select="Clusters/Cluster[Type='roxie']" />
                    </select>
                  </xsl:when>
                  <xsl:when test="$statusCode &lt; 0">
                    Error: <xsl:value-of select="$statusMessage" />
                  </xsl:when>
                </xsl:choose>
              </td>
            </tr>
          </table>
      </body>
        </html>
    </xsl:template>

  <xsl:template match="Cluster">
      <option value="{position()}">
        <xsl:value-of select="Name"/>
      </option>
  </xsl:template>
  
</xsl:stylesheet>
