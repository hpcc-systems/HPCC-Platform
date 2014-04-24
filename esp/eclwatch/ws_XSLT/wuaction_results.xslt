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
   <xsl:template match="WUActionResponse">
      <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
         <head>
            <title>Workunits</title>
           <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
           <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
           <link type="text/css" rel="StyleSheet" href="/esp/files/css/sortabletable.css"/>
           <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
           <script type="text/javascript" src="/esp/files/scripts/multiselect.js">
            <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
           </script>
            <script language="JavaScript1.2">
               <xsl:text disable-output-escaping="yes"><![CDATA[
                     function onLoad()
                     {
                        initSelection('resultsTable');
                     }       
               ]]></xsl:text>
            </script>
         </head>
     <body class="yui-skin-sam" onload="nof5();onLoad()">
            <h4>Results:</h4>
            <xsl:apply-templates/>
     </body>
      </html>
   </xsl:template>

    <xsl:template match="ActionResults">
        <table class="sort-table" id="resultsTable">
      <xsl:text disable-output-escaping="yes"><![CDATA[
            <colgroup>
                <col width="200"/>
                <col width="100"/>
                <col width="400"/>
            </colgroup>
      <thead>
        <tr class="grey">
          <th>WUID</th>
          <th>Action</th>
          <th>Status</th>
        </tr>
      </thead>
      ]]></xsl:text>
            <tbody>
                <xsl:apply-templates select="WUActionResult">
                <xsl:sort select="Wuid"/>
                </xsl:apply-templates>
       </tbody>
        </table>
        <br/>
    <input id="backBtn" type="button" value="Go Back" onclick="window.location.href=document.referrer">&#160;</input>
    </xsl:template>

    <xsl:template match="WUActionResult">
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
                    <xsl:when test="Action!='Delete'">
                        <a href="javascript:go('/WsWorkunits/WUInfo?Wuid={Wuid}')">
                            <xsl:choose>
                                <xsl:when test="State=2 or State=3"><b><xsl:value-of select="Wuid"/></b></xsl:when>
                                <xsl:otherwise><xsl:value-of select="Wuid"/></xsl:otherwise>
                            </xsl:choose>
                        </a>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="Wuid"/>
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
