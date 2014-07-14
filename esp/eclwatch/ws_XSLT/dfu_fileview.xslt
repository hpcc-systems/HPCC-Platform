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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
    <xsl:output method="html"/>
    
    <xsl:variable name="scope" select="/DFUFileViewResponse/Scope"/>
    <xsl:variable name="baseUrl">
        <xsl:choose>
            <xsl:when test="$scope != ''">
                <xsl:value-of select="concat('/WsDfu/DFUFileView?Scope=', $scope, '::' )"/>
            </xsl:when>             
            <xsl:otherwise>/WsDfu/DFUFileView?Scope=</xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Choose File!</title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/sortabletable.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <style type="text/css">
                    .selectedRow { background-color: #0af;}
                </style>
        <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js">&#160;</script>
        <script type="text/javascript" src="/esp/files/yui/build/container/container_core-min.js">&#160;</script>
        <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js">&#160;</script>
        <script type="text/javascript" id="menuhandlers">
          var oMenu;
          <xsl:text disable-output-escaping="yes"><![CDATA[
          function DFUFilePopup(query, filename, cluster, replicate, roxiecluster, browsedata, PosId) {
            function detailsDFUFile() {
              document.location.href='/WsDfu/DFUInfo?Name='+ escape(filename) + '&Cluster=' + cluster;
                      }
                        function browseDFUData() {
                          document.location.href='/WsDfu/DFUGetDataColumns?OpenLogicalName='+filename;
                        }
                        function searchDFUData() {
                          if (roxiecluster != "0") {
                                  document.location.href='/WsDfu/DFUSearchData?OpenLogicalName='+filename+ '&ClusterType=roxie&Cluster=' + cluster;
                            } else {
                                  document.location.href='/WsDfu/DFUSearchData?OpenLogicalName='+filename+ '&ClusterType=thor&Cluster=' + cluster + '&RoxieSelections=0';
              }
            }
            function replicateDFUFile() {
              document.location.href='/FileSpray/Replicate?sourceLogicalName='+filename;
            }
            function copyDFUFile() {
              document.location.href='/FileSpray/CopyInput?sourceLogicalName='+filename;
            }
            function desprayDFUFile()
            {
              document.location.href='/FileSpray/DesprayInput?sourceLogicalName='+filename;
            }
            function renameDFUFile() {
              document.location.href='/FileSpray/RenameInput?sourceLogicalName='+filename;
            }
            var xypos = YAHOO.util.Dom.getXY('mn' + PosId);
            if (oMenu) {
              oMenu.destroy();
            }              
            oMenu = new YAHOO.widget.Menu("logicalfilecontextmenu", {position: "dynamic", xy: xypos} );
            oMenu.clearContent();

            oMenu.addItems([
                { text: "Details", onclick: { fn: detailsDFUFile } },
                { text: "Copy", onclick: { fn: copyDFUFile } },
                { text: "Rename", onclick: { fn: renameDFUFile } }
            ]);
            if (browsedata != 0) {
                oMenu.addItems([
                    { text: "View Data File", onclick: { fn: searchDFUData } }
                ]);
            }
            if (replicate != 0) {
                oMenu.addItems([
                    { text: "Replicate", onclick: { fn: replicateDFUFile } }
                ]);
            }
            if (roxiecluster == 0) {
                oMenu.addItems([
                    { text: "Despray", onclick: { fn: desprayDFUFile } }
                ]);
            }

            //showPopup(menu,(window.event ? window.event.screenX : 0),  (window.event ? window.event.screenY : 0));
            oMenu.render("dfulogicalfilemenu");
            oMenu.show();
            return false;

            }
          ]]></xsl:text>

        </script>
                <script type="text/javascript">
                    var originalPath = '<xsl:value-of select="DFUFileViewResponse/Scope"/>';
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                    var nSelected = -1;
                    var nPrevClass = null;

                    function onLoad()
                    {
                        initSelection('resultsTable');
                    } 
                    
                    function onRowCheck(checked)
                    {
                        document.getElementById("deleteBtn").disabled = checkedCount == 0;
                    }                            
                    
                    function getSelected(o)
                    {
                        if (o.tagName=='INPUT')
                            return o.checked ? '\n'+o.value : '';
                    
                        var s='';
                        var ch=o.childNodes;
                        if (ch)
                            for (var i in ch)
                            s=s+getSelected(ch[i]);
                         return s;
                    }

                    function clearAll()
                    {
                        selectAll(false);
                        document.getElementById("CheckAll").checked = false;
                    }

                    function back()
                    {
                        var url = "/WsDfu/DFUFileView";
                        var sp = originalPath.lastIndexOf('::');
                        if (sp != -1)
                        {
                            url += "?Scope=" + originalPath.substring(0, sp);
                        }
                        document.location = url;
                    }
                ]]></xsl:text>
                </script>
        <script language="JavaScript1.2" src="/esp/files/scripts/multiselect.js">&#160;</script>
      </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <xsl:apply-templates/>
        <div id="dfulogicalfilemenu" />
      </body>
        </html>
    </xsl:template>
    
    
    <xsl:template match="DFUFileViewResponse">
        <b>
            <xsl:if test="$scope != ''">
                <table>
                    <tr>
                        <td>
                            <xsl:text>Files under:  </xsl:text>
                            <font face="Verdana" color="#ff0000">
                                <b><xsl:value-of select="$scope"/></b>
                            </font>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <a href="javascript:void(0)" onclick="back(); return false;">Parent scope</a>
                        </td>
                    </tr>
                </table>
            </xsl:if>
        </b>
        <p/>
        <form id="listitems" action="/WsDFU/DFUArrayAction" method="POST">
            <input type="hidden" id="BackToPage" name="BackToPage" value="/WsDfu/DFUFileView?Scope={$scope}"/>
            <table class="sort-table" id="resultsTable" align="center" valign="center" width="100%">
                <colgroup>
                    <col/>
                    <col/>
                    <col style="text-align:left"/>
                    <col/>
                    <col class="number" style="text-align:center"/>
                    <col class="number" style="text-align:center"/>
                    <col/>
                    <col/>
                    <col/>
                    <col class="number" style="text-align:center"/>
                </colgroup>
                <thead>
                    <tr>
                        <th>
                            <xsl:if test="number(NumFiles) &gt; 0">
                                <input type="checkbox" id="CheckAll" title="Select or deselect all logical files" onclick="selectAll(this.checked)"/>
                            </xsl:if>
                        </th>
                        <th title="Compressed"><img src="/esp/files_/img/zip.gif"></img></th>
                        <th>Name</th>
                        <th>Description</th>
                        <th>Size</th>
                        <th>Records</th>
                        <th>Modified (UTC/GMT)</th>
                        <th>Owner</th>
                        <th>Cluster</th>
                        <th>Parts</th>
                    </tr>
                </thead>                
                <xsl:choose>
                    <xsl:when test="DFULogicalFiles/DFULogicalFile[1]">
                        <xsl:apply-templates select="DFULogicalFiles"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <tr>
                            <td colspan="3">No items.</td>
                        </tr>
                    </xsl:otherwise>
                </xsl:choose>
            </table>
            <xsl:if test="number(NumFiles) &gt; 0">
                <br/>
                <table id="btnTable" style="margin:20 0 0 0">
                    <colgroup>
                        <col span="8" width="100"/>
                    </colgroup>
                    <tr>
                        <td>
                            <input type="button" class="sbutton" value="Clear" onclick="clearAll()"/>
                        </td>
                        <td>
                            <input type="submit" class="sbutton" id="deleteBtn" name="Type" value="Delete" disabled="true" onclick="return confirm('Are you sure you want to delete the following file(s) ?\n\n'+getSelected(document.forms['listitems']).substring(1,1000))"/>
                        </td>
                        <td>
                            <input type="submit" class="sbutton" name="Type" value="Add To Superfile"/>
                        </td>
                    </tr>
                </table>
            </xsl:if>
        </form>
    </xsl:template>
    
    
    <xsl:template match="DFULogicalFiles">
        <xsl:variable name="directories" select="DFULogicalFile[isDirectory=1]"/>
        <xsl:apply-templates select="$directories">
            <xsl:sort select="Name"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="DFULogicalFile[isDirectory=0]">
            <xsl:sort select="Name"/>
            <xsl:with-param name="dirs" select="count($directories)"/>
        </xsl:apply-templates>
    </xsl:template>
    
    
    <xsl:template match="DFULogicalFile">
        <xsl:param name="dirs" select="1"/>
        <xsl:variable name="href">
            <xsl:value-of select="concat($baseUrl, Directory)"/>
        </xsl:variable>
        <xsl:variable name="info_query">
            <xsl:value-of select="Name"/>
            <xsl:choose>
                <xsl:when test="string-length(ClusterName)">&amp;Cluster=<xsl:value-of select="ClusterName"/></xsl:when>
            </xsl:choose>
        </xsl:variable>
    <xsl:variable name="href2">
      <xsl:value-of select="concat('/WsDfu/DFUInfo?Name=', $info_query)"/>
    </xsl:variable>
        <tr onmouseenter="this.oldClass=this.className; this.className='hilite'" onmouseleave="this.className=this.oldClass">
            <xsl:attribute name="class">
                <xsl:choose>
                    <xsl:when test="($dirs + position()) mod 2">odd</xsl:when>
                    <xsl:otherwise>even</xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
            <td>
                <xsl:if test="isDirectory=0">
                    <xsl:variable name="popup">return DFUFilePopup('<xsl:value-of select="$info_query"/>', '<xsl:value-of select="Name"/>', '<xsl:value-of select="ClusterName"/>', '<xsl:value-of select="Replicate"/>', '<xsl:value-of select="FromRoxieCluster"/>', '<xsl:value-of select="BrowseData"/>', '<xsl:value-of select="position()"/>')</xsl:variable>
                    <input type="checkbox" name="LogicalFiles_i{position()}" value="{Name}@{ClusterName}" onclick="return clicked(this, event)">
                        <xsl:attribute name="oncontextmenu"><xsl:value-of select="$popup"/></xsl:attribute>
                    </input>
          <img id="mn{position()}" class="menu1" src="/esp/files/img/menu1.png" onclick="{$popup}">&#160;</img>
        </xsl:if>
            </td>
            <td>
                <xsl:if test="isZipfile=1">
                    <img title="Compressed" border="0" src="/esp/files_/img/zip.gif" width="16" height="16"/>
                </xsl:if>
            </td>
            <td align="left">
                <xsl:choose>
                    <xsl:when test="isDirectory=1">
                        <a title="Open folder..." href="{$href}">
              <img src="/esp/files_/img/folder.gif" width="19" height="16" border="0" alt="Open folder..." style="vertical-align:bottom">&#160;</img>
                            <xsl:value-of select="Directory"/>
                        </a>
                    </xsl:when>
                    <xsl:otherwise>
            <img src="/esp/files_/img/page.gif" width="19" height="16" style="vertical-align:bottom"/>
            <a title="Open file..." href="{$href2}">
                          <xsl:choose>
                            <xsl:when test="isSuperfile=1">
                              <I><b><xsl:value-of select="Name"/></b></I>
                            </xsl:when>
                            <xsl:otherwise>
                              <xsl:value-of select="Name"/>
                            </xsl:otherwise>
                          </xsl:choose>
            </a>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
            <xsl:choose>
                <xsl:when test="isDirectory=0">
                    <td>
                            <xsl:value-of select="Description"/>
                    </td>
                    <td>
                            <xsl:value-of select="IntSize"/>
                    </td>
                    <td>
                            <xsl:value-of select="IntRecordCount"/>
                    </td>
                    <td nowrap="nowrap" align="center">
                            <xsl:value-of select="Modified"/>
                    </td>
                    <td>
                            <xsl:value-of select="Owner"/>
                    </td>
                    <td>
                            <xsl:value-of select="ClusterName"/>
                    </td>
                    <td>
                            <xsl:value-of select="Parts"/>
                    </td>
                </xsl:when>
                <xsl:otherwise>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td></td>
                </xsl:otherwise>
            </xsl:choose>
        </tr>
    </xsl:template>
    
</xsl:stylesheet>
