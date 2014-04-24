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
    <xsl:template match="Found">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
          <head>
            <title>XRef - Found Files</title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
               <script language="javascript" src="/esp/files_/scripts/multiselect.js">
                  <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
               </script>
            <script language="JavaScript1.2"><xsl:text disable-output-escaping="yes"><![CDATA[
               function onRowCheck(checked)
               {
                  document.forms[0].deleteBtn.disabled = checkedCount == 0;
                  document.forms[0].attachBtn.disabled = checkedCount == 0;
               }              
               
                function onLoad()
                {
                  initSelection('resultsTable');
                  onRowCheck(false);
                }   
              ]]></xsl:text>
            </script>
          </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
          <h3>Found files on '<xsl:value-of select="Cluster"/>' cluster:</h3>
            <form id="listitems" action="/WsDFUXRef/DFUXRefArrayAction?Cluster={Cluster}&amp;Type=Found" method="post">
                <table class="sort-table" id="resultsTable">
                    <colgroup>
                       <col width="5"/>
                       <col width="200"/>
                       <col width="200"/>
                       <col width="50"/>
                       <col width="100"/>
                    </colgroup>
                    <thead>
                        <tr class="grey">
                                <th>
                                       <!--xsl:if test="File[2]">
                                          <xsl:attribute name="id">selectAll1</xsl:attribute>
                                          <input type="checkbox" title="Select or deselect all files" onclick="selectAll(this.checked)"/>
                                       </xsl:if-->
                                </th>
                                <th>Name</th> 
                                <th>Modified</th>
                                <th>Parts</th>
                                <th>Size</th>
                        </tr>
                         </thead>
                         <tbody>
                        <xsl:apply-templates select="File">
                            <xsl:sort select="Name"/>
                        </xsl:apply-templates>
                        </tbody>
                </table>
                     <!--xsl:if test="File[2]">
                        <table  class="select-all">
                           <tr>
                              <th id="selectAll2">
                                 <input type="checkbox" title="Select or deselect all files" onclick="selectAll(this.checked)"/>
                              </th>
                              <th align="left" colspan="6">Select All / None</th>
                           </tr>
                        </table>
                     </xsl:if-->
                     <br/><br/>
                <input id="deleteBtn" type="submit" class="sbutton" name="Action" value="Delete" disabled="true"
                             onclick="return confirm('Delete selected Files?')"/>
                <xsl:text disable-output-escaping="yes">&amp;nbsp;&amp;nbsp;</xsl:text>
                    <input id="attachBtn" type="submit" class="sbutton" name="Action" value="Attach" disabled="true" 
                                 onclick="return confirm('Add these Files to Dali?')"/>
            </form>
          </body> 
        </html>
    </xsl:template>

    <xsl:template match="File">
      <tr onmouseenter="this.bgColor = '#F0F0FF'">
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
        <td><input type="checkbox" name="XRefFiles_i{position()}" value="{Partmask}" onclick="clicked(this, event)"/></td>
        <td align="left"><xsl:value-of select="Partmask"/> </td>
        <td><xsl:value-of select="Modified"/></td>
        <td><xsl:value-of select="Numparts"/></td>
        <td><xsl:value-of select="Size"/></td>
        </tr>
    </xsl:template>
    <xsl:template match="Part">
        <tr>
        <td></td>
        <td></td>
        <td></td>
        <td></td>
        <td></td>
        <td><xsl:value-of select="Node"/> </td>
        <td><xsl:value-of select="Num"/></td>
        </tr>
    </xsl:template>
</xsl:stylesheet>
