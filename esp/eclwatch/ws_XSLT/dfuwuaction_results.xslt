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
  <xsl:template match="DFUWorkunitsActionResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
      <head>
      <title>Workunits</title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <script type="text/javascript" src="/esp/files_/scripts/sortabletable.js">
        <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
      </script>
      <script language="JavaScript1.2">
        <xsl:text disable-output-escaping="yes"><![CDATA[
          function onLoad()
          {
            //initSelection('resultsTable');
            var table = document.getElementById('resultsTable');
            if (table)
              sortableTable = new SortableTable(table, table, ["String", "String", "String",]);
          }       
          var sortableTable = null;
        ]]></xsl:text>
      </script>
      </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
        <h1>Results:</h1>
        <xsl:apply-templates/>
      </body>
    </html>
  </xsl:template>
  
  <xsl:template match="DFUActionResults">
    <table class="sort-table" id="resultsTable">
      <colgroup>
        <col width="200"/>
        <col width="100"/>
        <col width="400"/>
      </colgroup>
      <thead>
        <tr class="grey">
          <th style="cursor:pointer">
            <xsl:choose>
              <xsl:when test="../FirstColumn=''">
                WUID
              </xsl:when>
              <xsl:otherwise>
                <xsl:value-of select="../FirstColumn"/>
              </xsl:otherwise>
            </xsl:choose>
          </th>
          <th style="cursor:pointer">Action</th>
          <th style="cursor:pointer">Result</th>
        </tr>
      </thead>
      <tbody>
        <xsl:apply-templates select="DFUActionResult">
          <xsl:sort select="ID"/>
        </xsl:apply-templates>
      </tbody>
    </table>
    <br/>
    <xsl:if test="DFUActionResults/DFUActionResult[1]/Action!='Delete'">
      <input id="backBtn" type="button" value="Go Back" onclick="history.go(-1)"> </input>
    </xsl:if>
  </xsl:template>

  <xsl:template match="DFUActionResult">
    <tr>
      <xsl:choose>
        <xsl:when test="position() mod 2">
          <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
          <xsl:attribute name="onmouseleave">this.bgColor = '#FFFFFF'</xsl:attribute>
        </xsl:when>
        <xsl:otherwise>
          <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
          <xsl:attribute name="onmouseleave">this.bgColor = '#F0F0F0'</xsl:attribute>
        </xsl:otherwise>
      </xsl:choose>  
      <td>
        <xsl:choose>
          <xsl:when test="../../FirstColumn='File'">
            <xsl:value-of select="ID"/>
          </xsl:when>
          <xsl:otherwise>
            <a href="javascript:go('/FileSpray/GetDFUWorkunit?wuid={ID}')">
              <xsl:choose>
                <xsl:when test="State=2 or State=3">
                  <b>
                    <xsl:value-of select="ID"/>
                  </b>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:value-of select="ID"/>
                </xsl:otherwise>
              </xsl:choose>
            </a>
          </xsl:otherwise>
        </xsl:choose>
      </td>
      <td>
        <xsl:value-of select="Action"/>
      </td>
      <td>
        <xsl:value-of select="substring(concat(substring(Result,1,100),'...'),1,string-length(Result))"/>
      </td>
    </tr>
  </xsl:template>

  <xsl:template match="text()|comment()"/>
</xsl:stylesheet>
