<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2011 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
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
                <h3>Querysets</h3>
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
          <th>QuerySet Name </th>
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
