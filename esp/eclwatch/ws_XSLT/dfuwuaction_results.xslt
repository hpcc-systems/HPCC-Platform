<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
      </head>
      <body class="yui-skin-sam" onload="nof5()">
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
