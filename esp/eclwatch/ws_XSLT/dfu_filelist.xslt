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
    
    <xsl:variable name="acceptLanguage" select="/FileListResponse/AcceptLanguage"/>
    <xsl:variable name="localiseFile"><xsl:value-of select="concat('nls/', $acceptLanguage, '/hpcc.xml')"/></xsl:variable>
    <xsl:variable name="hpccStrings" select="document($localiseFile)/hpcc/strings"/>

    <!--note that /FileListResponse/Path is guaranteed to contain no backslashes and end with / -->
    <xsl:variable name="baseUrl">
        <xsl:for-each select="/FileListResponse">
            <xsl:choose>
                <xsl:when test="/FileListResponse/DirectoryOnly=1">
                    <xsl:value-of select="concat('/FileSpray/FileList?DirectoryOnly=1&amp;Mask=', Mask, '&amp;Netaddr=', Netaddr, '&amp;OS=', OS, '&amp;Path=', Path )"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="concat('/FileSpray/FileList?Mask=', Mask, '&amp;Netaddr=', Netaddr, '&amp;OS=', OS, '&amp;Path=', Path )"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:for-each>
    </xsl:variable>
    
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Choose File</title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <style type="text/css">
                    .selectedRow { background-color: #0af;}
                </style>
                <script language="JavaScript1.2">
                    var originalPath = '<xsl:value-of select="FileListResponse/Path"/>';
                    var os = <xsl:value-of select="/FileListResponse/OS"/>;
                    var dironly = <xsl:value-of select="/FileListResponse/DirectoryOnly"/>;
                    var lostReferenceToParentWindowAlert = '<xsl:value-of select="$hpccStrings/st[@id='LostReferenceToParentWindow']"/>';
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                    var nSelected = -1;
                    var nPrevClass = null;
                    
                    function onLoad()
                    {
                    } 
                    
                    function onOK(name)
                    {
                        var opener = window.opener;
                        if (opener)
                        {                           
                            if (!opener.closed)
                            {
                                var path = originalPath;
                                if (dironly)
                                {
                                    path += name;
                                    if (os == 0)//windows
                                        path = path.replace(new RegExp('/'), '\\');
                                }
                                else
                                {
                                    path += document.getElementById('selected').value;
                                    if (os == 0)//windows
                                        path = path.replace(new RegExp('/'), '\\');
                                }
                                opener.setSelectedPath( path );
                            }
                            window.close();
                        }
                        else
                        {
                            alert(lostReferenceToParentWindowAlert);
                            unselect();
                        }
                    }
                                    
                    function unselect()
                    {
                        if (nSelected != -1)
                        {
                            var table = document.getElementById('resultsTable');
                            var prevRow = table.rows[nSelected];
                            prevRow.className = prevRow.oldClass = nPrevClass;
                            nSelected = -1;
                        }
                    }
                    
                    function setValue(row, val, dir)
                    {
                        unselect();
                        nSelected = row.rowIndex;
                        nPrevClass = row.oldClass;
                        row.className = row.oldClass = 'selectedRow';
                        document.getElementById('selected').value=val
                        document.getElementById('selectBtn').disabled = false;
                    }
                ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <xsl:apply-templates/>
            </body>
        </html>
    </xsl:template>
    
    
    <xsl:template match="FileListResponse">
        <xsl:if test="string(Mask)!=''">
            <b><xsl:value-of select= "$hpccStrings/st[@id='Files']"/>: </b><xsl:value-of select="Mask"/>
            <br/>
        </xsl:if>
        <b><xsl:value-of select= "$hpccStrings/st[@id='Folder']"/>: </b>
            <xsl:choose>
                <xsl:when test="substring(Path, 2, 1)=':'">
                    <xsl:value-of select="translate(Path, '/', '\')"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="Path"/>
                </xsl:otherwise>
            </xsl:choose>
        <br/>
        <b><xsl:value-of select= "$hpccStrings/st[@id='NetworkAddress']"/>: </b><xsl:value-of select="Netaddr"/>
        <p/>
        <form method="POST">
            <table class="sort-table" id="resultsTable" align="center" width="100%">
                <thead>
                    <tr>
                        <xsl:if test="/FileListResponse/DirectoryOnly=1">
                            <th></th>
                        </xsl:if>
                        <th><xsl:value-of select= "$hpccStrings/st[@id='Name']"/></th>
                        <th><xsl:value-of select= "$hpccStrings/st[@id='Size']"/></th>
                        <th><xsl:value-of select= "$hpccStrings/st[@id='Date']"/></th>
                    </tr>
                </thead>                
                <colgroup>
                    <xsl:if test="/FileListResponse/DirectoryOnly=1">
                    <col style="text-align:left"/>
                    </xsl:if>
                    <col style="text-align:left"/>
                    <col style="text-align:right"/>
                    <col style="text-align:right"/>
                </colgroup>
                <xsl:choose>
                    <xsl:when test="files/PhysicalFileStruct[1]">
                        <!--tr onmouseenter="this.oldClass=this.className; this.className='hilite'"
                              onmouseleave="this.className=this.oldClass" class="odd" ondblclick="goBack()">
                            <td colspan="3">
                                <a title="Open parent folder..." href="" onclick="return goBack()">
                                    <img src="/esp/files_/img/folder.gif" width="19" height="16" border="0" alt="Open parent folder..." style="vertical-align:bottom"/>
                                    <xsl:text>..</xsl:text>
                                </a>
                            </td>
                        </tr-->
                        <xsl:apply-templates select="files"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <tr>
                            <td colspan="3"><xsl:value-of select= "$hpccStrings/st[@id='NoItems']"/></td>
                        </tr>
                    </xsl:otherwise>
                </xsl:choose>
            </table>
            <input type="hidden" id="selected"/>
            <p align="center">
                <input type="button" value="{$hpccStrings/st[@id='Select']}" id="selectBtn" onclick="onOK('')" disabled="true"/>
                <xsl:text disable-output-escaping='yes'>&amp;nbsp;&amp;nbsp;</xsl:text>
                <input type="button" value="{$hpccStrings/st[@id='Cancel']}" onclick="window.close()"/>
            </p>
        </form>
    </xsl:template>
    
    
    <xsl:template match="files">
        <xsl:variable name="directories" select="PhysicalFileStruct[isDir=1]"/>
        <xsl:apply-templates select="$directories">
            <xsl:sort select="name"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="PhysicalFileStruct[isDir=0]">
            <xsl:sort select="name"/>
            <!--xsl:with-param name="dirs" select="count($directories)+1"/--><!--use this when parent folder link is reinstated-->
            <xsl:with-param name="dirs" select="count($directories)"/>
        </xsl:apply-templates>
    </xsl:template>
    
    
    <xsl:template match="PhysicalFileStruct">
        <xsl:param name="dirs" select="1"/>
        <xsl:variable name="href">
            <xsl:value-of select="concat($baseUrl, name)"/>
            <!--xsl:if test="$dirs">
                <xsl:value-of select="$pathSep"/>
            </xsl:if-->
        </xsl:variable>
        <tr onmouseenter="this.oldClass=this.className; this.className='hilite'" onmouseleave="this.className=this.oldClass">
            <xsl:attribute name="ondblclick">
                <xsl:choose>
                    <xsl:when test="isDir=1">unselect();document.location="<xsl:value-of select='$href'/>"</xsl:when>
                    <xsl:otherwise>setValue(this, '<xsl:value-of select="name"/>', 0); onOK('');</xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
            <xsl:if test="isDir=0">
                <xsl:attribute name="onclick">setValue(this, '<xsl:value-of select="name"/>', 0);</xsl:attribute>
            </xsl:if>
            
            <xsl:attribute name="class">
                <xsl:choose>
                    <xsl:when test="($dirs + position()) mod 2">odd</xsl:when>
                    <xsl:otherwise>even</xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
            <xsl:if test="/FileListResponse/DirectoryOnly=1">
                <td>
                    <input type="button" value="{$hpccStrings/st[@id='Select']}" id="selectBtn_i{position()}" onclick="onOK('{name}')"/>
                </td>
            </xsl:if>
            <td align="left">
                <xsl:choose>
                    <xsl:when test="isDir=1">
                        <a title="Open folder..." href="{$href}">
                            <img src="/esp/files_/img/folder.gif" width="19" height="16" border="0" alt="Open folder..." style="vertical-align:bottom"/>
                            <xsl:value-of select="name"/>
                        </a>
                    </xsl:when>
                    <xsl:otherwise>
                        <img src="/esp/files_/img/page.gif" width="19" height="16" style="vertical-align:bottom"/>
                        <xsl:value-of select="name"/>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
            <td align="right">
                <xsl:if test="isDir=0">
                    <xsl:value-of select="filesize"/>
                </xsl:if>
            </td>
            <td>
                <xsl:value-of select="modifiedtime"/>
            </td>
        </tr>
    </xsl:template>
    
</xsl:stylesheet>
