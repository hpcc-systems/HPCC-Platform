<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
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
    <xsl:output method="html"/>
    <xsl:template match="/WUListQueriesUsingFileResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <xsl:text disable-output-escaping="yes"><![CDATA[
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
          <link type="text/css" rel="styleSheet" href="/esp/files/css/sortabletable.css"/>
          <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
        ]]></xsl:text>

            </head>
            <body onload="nof5();" class="yui-skin-sam">
                <h3>Queries using file:</h3><br/>
                <b>File: </b><xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text><xsl:value-of select="FileName"/><br/>
                <xsl:if test="Process">
                    <b>On Cluster: </b><xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text><xsl:value-of select="Process"/><br/>
                </xsl:if>
                <xsl:apply-templates select="Targets/TargetQueriesUsingFile"/>
                <xsl:if test="count(Targets/TargetQueriesUsingFile/Queries/QueryUsingFile)=0">
                    <p>The are no Queries using this file.</p>
                </xsl:if>
            </body>
        </html>
    </xsl:template>

  <xsl:template match="TargetQueriesUsingFile">
    <b>Target: </b><xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text><xsl:value-of select="Target"/><br/>
    <xsl:if test="PackageMap">
        <b>PackageMap: </b><xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text><xsl:value-of select="PackageMap"/><br/>
    </xsl:if>
    <br/>
    <xsl:apply-templates select="Queries"/><br/><br/>
  </xsl:template>

  <xsl:template match="Queries">
    <table class="sort-table" id="resultsTable">
      <thead>
        <tr class="grey">
          <th>Query ID</th>
          <th>Package</th>
        </tr>
      </thead>
      <tbody>
        <xsl:apply-templates select="QueryUsingFile"/>
      </tbody>
    </table>
  </xsl:template>

  <xsl:template match="QueryUsingFile">
    <tr>
      <td>
        <a href="/WsWorkunits/WUQueryDetails?IncludeSuperFiles=1&amp;IncludeStateOnClusters=1&amp;QueryId={Id}&amp;QuerySet={../../Target}"><xsl:value-of select="Id"/>
        </a>
      </td>
      <td>
        <xsl:value-of select="Package"/>
      </td>
    </tr>
  </xsl:template>

  <xsl:template match="*|@*|text()"/>
</xsl:stylesheet>
