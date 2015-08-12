<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
-->

<!DOCTYPE xsl:stylesheet [
    <!--define the HTML non-breaking space:-->
    <!ENTITY nbsp "<xsl:text disable-output-escaping='yes'>&amp;nbsp;</xsl:text>">
]>
<xsl:stylesheet version="1.0" 
 xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
 xmlns:xs="http://www.w3.org/2001/XMLSchema">

<xsl:variable name="debug" select="0"/>
<xsl:variable name="filesPath">
    <xsl:choose>
        <xsl:when test="$debug">c:/development/bin/debug/files</xsl:when>
        <xsl:otherwise>/esp/files_</xsl:otherwise>
    </xsl:choose>
</xsl:variable>


<xsl:template match="/MultiStatusResponse">
    <html>
        <head>
            <title><xsl:value-of select="Caption"/></title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link type="text/css" rel="StyleSheet" href="{$filesPath}/css/sortabletable.css"/>
            <link type="text/css" rel="StyleSheet" href="{$filesPath}/css/tabs.css"/>
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <script type="text/javascript">
                function submitForm(action)
                {
                    form = document.forms[0];
                    form.action = action;
                    form.submit();
                }
            </script>
        </head>
    <body class="yui-skin-sam" onload="nof5();">
            <xsl:if test="string(SelectedTabName)!= ''">
                <xsl:apply-templates select="TabContainer">
                    <xsl:with-param name="SelectedTabName" select="SelectedTabName"/>
                </xsl:apply-templates>
            </xsl:if>
            <h1><xsl:value-of select="Caption"/></h1>
            <table border="0">
                <tbody>
                    <tr>
                        <th>
                            <table class="sort-table" noWrap="true">
                                <tbody>
                                    <tr>
                                        <xsl:for-each select="ColumnHeadings/Item">
                                            <th><xsl:value-of select="."/></th>                         
                                        </xsl:for-each>
                                    </tr>
                                    <xsl:for-each select="StatusRows/StatusRow">
                                        <tr>
                                            <xsl:for-each select="Columns/Item">
                                                <td align="left">
                                                    <xsl:value-of select="."/>
                                                </td>
                                            </xsl:for-each>
                                        </tr>
                                    </xsl:for-each>
                                </tbody>
                            </table>
                        </th>
                    </tr>
                    <!--insert vertical spacing-->
                    <tr>
                        <td height="20"></td>
                    </tr>
                    <tr>
                        <td colspan="{count(ColumnHeadings/Item)}">
                            <xsl:if test="string(ShowBackButton)='1'">
                            <input type="button" value="Go Back" onclick="history.back()"> </input>
                            </xsl:if>
                            <form action="">
                                <xsl:for-each select="Buttons/ButtonInfo">
                                    &nbsp;&nbsp;
                                    <input type="{Type}" name="{Name}" value="{Value}" onclick="submitForm('{OnClick}')"> </input>
                                </xsl:for-each>
                            </form>
                        </td>
                    </tr>
                </tbody>
            </table>
        </body>
    </html>
</xsl:template>


<xsl:template match="TabContainer">
    <xsl:param name="SelectedTabName"/>
  <div id="pageHeader">
    <div id="tabContainer" width="100%">
      <ul id="tabNavigator">
        <xsl:for-each select="Tab">
          <li>
            <a href="{Url}">
              <xsl:if test="string(Name)=$SelectedTabName">
                <xsl:attribute name="class">active</xsl:attribute>
              </xsl:if>
              <xsl:value-of select="Name"/>
            </a>
          </li>
        </xsl:for-each>
      </ul>
    </div>
  </div>
</xsl:template>


</xsl:stylesheet>
