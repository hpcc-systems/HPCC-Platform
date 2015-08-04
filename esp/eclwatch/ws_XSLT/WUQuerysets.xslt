<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

    <xsl:template match="/WUQuerysetsResponse">
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
                <h3>Targets</h3>
        <xsl:apply-templates select="Querysets"/>
        <xsl:if test="count(Querysets)=0">
          <p>The are no Querysets available.</p>
        </xsl:if>
      </body>
        </html>
    </xsl:template>


  <xsl:template match="Querysets">
    <table class="sort-table" id="resultsTable">
      <thead>
        <tr class="grey">
          <th>Target Name </th>
        </tr>
      </thead>
      <tbody>
        <xsl:apply-templates select="QuerySet"/>
      </tbody>
    </table>
  </xsl:template>

  <xsl:template match="QuerySet">
    <tr>
      <td>
        <a href="/WsWorkunits/WUQuerysetDetails?QuerySetName={QuerySetName}"><xsl:value-of select="QuerySetName"/>
        </a>
      </td>
    </tr>
  </xsl:template>

  <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
