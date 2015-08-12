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
