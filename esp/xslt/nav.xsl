<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
<!ENTITY nbsp "&#160;">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
    <xsl:output method="html"/>
    
    <xsl:template match="EspNavigationData">
        <html>
            <head>
                <title>ESP Navigation Window</title>
          <script language="JavaScript1.2">
             <xsl:text disable-output-escaping="yes"><![CDATA[
               function gomain(url)
               {
                  var f=top.frames['main'];
                  f.focus();
                  if (f) 
                      f.location=url;
                    return false;
               }                     
                    ]]></xsl:text>
          </script>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/resize/assets/skins/sam/resize.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/layout/assets/skins/sam/layout.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script language="JavaScript">
          var app_name = '<xsl:value-of select="@appName"/>';
        </script>
        <script language="JavaScript" src="files_/scripts/rightSideBar.js">
                </script>
                <style type="text/css" media="screen">
                    @import url( files_/css/rightSideBar.css );
                    table {
                        width: 100%;
                    }
                </style>
            </head>
            <body onload="nof5();onLoadNav()" style="background:#D5E4F2; overflow:hidden" onresize="leftFrameResized()" class="yui-skin-sam yui-layout-doc">
                <div id="pageBody" style="padding:0">
          <table cellspacing="0" cellpadding="0" id="table1">
                        <tr>
                            <td>
                                <table border="0" cellspacing="0" cellpadding="0" id="table2">
                                    <xsl:apply-templates select="*"/>
                                </table>
                            </td>
                        </tr>
                    </table>
        </div>
        <div id="espnavcollapse" class="espnavcollapse" title="Click to collapse this pane." onclick="onToggleTreeView(this)">
        </div>
      </body>
        </html>
    </xsl:template>
    
    <xsl:template match="Folder">
        <tr height="18px">
            <td width="160" class="espnavfolder">
                <img border="0" src="files/img/menudown.png">
                    <xsl:attribute name="onclick">
                        <xsl:text disable-output-escaping="yes">submenu=document.getElementById('</xsl:text>
                        <xsl:value-of select="@name"/>')
                        <xsl:text disable-output-escaping="yes">;if (submenu.style.display=='none') {submenu.style.display=''; src='files/img/menudown.png'} else {submenu.style.display='none'; src='files/img/menuup.png';}</xsl:text>
                    </xsl:attribute>
                </img>
                    <b>
                        <a>
                            <xsl:if test="@tooltip">
                                <xsl:attribute name="title">
                                    <xsl:value-of select="@tooltip"/>
                                </xsl:attribute>
                            </xsl:if>
                            <xsl:value-of select="@name"/>
                        </a>
                    </b>
                    <xsl:if test="@showSchemaLinks">
                      <xsl:text disable-output-escaping="yes">&nbsp;</xsl:text>
                      <a target="main">
                       <xsl:attribute name="href">
                          <xsl:value-of select="concat('../', @name, '?wsdl', @urlParams)"/>
                       </xsl:attribute>
                       <img src="files_/img/wsdl.gif" alt="WSDL" border="0"></img>
                      </a>
                      <xsl:text disable-output-escaping="yes">&nbsp;</xsl:text>
                       <a target="main">
                       <xsl:attribute name="href">
                          <xsl:value-of select="concat('../', @name, '?xsd', @urlParams)"/>
                       </xsl:attribute>
                       <img src="files_/img/xsd.gif" alt="Schema" border="0"></img>
                      </a>                  
                 </xsl:if>      
            </td>
        </tr>
        <xsl:if test="*">
            <tr>
                <td>
                    <table border="0" cellspacing="0" cellpadding="0">
                        <xsl:attribute name="id"><xsl:value-of select="@name"/></xsl:attribute>
                            <xsl:choose>
                                <xsl:when test="@sort=1">
                                    <xsl:apply-templates select="*">
                                        <xsl:sort select="translate(@name,'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"/>
                                    </xsl:apply-templates>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:apply-templates select="*"/>
                                </xsl:otherwise>
                            </xsl:choose>
                    </table>
                </td>
            </tr>
        </xsl:if>
    </xsl:template>
        
    <xsl:template match="Link">
        <tr onmouseover="this.className='espnavlinkhover'" onmouseout="this.className=''">
            <td colspan="2">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;&amp;nbsp;</xsl:text>
                <a class="espnavlink">
                    <xsl:if test="@path">
                        <xsl:attribute name="href"><xsl:value-of select="@path"/></xsl:attribute>
                        <xsl:choose>
                            <xsl:when test="@target">
                                <xsl:attribute name="target"><xsl:value-of select="@target"/></xsl:attribute>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:attribute name="onclick">return gomain('<xsl:value-of select="@path"/>')</xsl:attribute>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:if>
                    <xsl:if test="@tooltip">
                        <xsl:attribute name="alt"><xsl:value-of select="@tooltip"/></xsl:attribute>
                    </xsl:if>
                    <xsl:value-of select="@name"/>
                </a>
            </td>
        </tr>
    </xsl:template>
    
</xsl:stylesheet>
