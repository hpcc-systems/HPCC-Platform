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
    <xsl:param name="actualSize" select="File/ActualSize"/>
    <xsl:param name="issuperfile" select="File/isSuperfile"/>
   <xsl:variable name="wuid" select="DFUInfoResponse/FileDetail/Wuid"/>

   <xsl:template match="FileDetail">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
          <head>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
            <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
            <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <script type="text/javascript" src="files_/scripts/sortabletable.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="JavaScript1.2">
                   <xsl:text disable-output-escaping="yes"><![CDATA[
                         function selectAll0(select)
                         {
                           document.getElementById('TopSelectAll').checked = select;
                           document.getElementById('BottomSelectAll').checked = select;
                           document.getElementById('removeSuperfile').checked = select;
                           selectAll(select);
                         }
                         function onRowCheck(checked)
                         {
                            document.getElementById('deleteBtn').disabled = checkedCount == 0;
                         }
                         function getSelected(o)
                         {
                            if (o.tagName=='INPUT' && o.type == 'checkbox' && o.value != 'on')
                                return o.checked ? '\n'+o.value : '';

                            var s='';
                            var ch=o.childNodes;
                            if (ch)
                                for (var i in ch)
                                    s=s+getSelected(ch[i]);
                            return s;
                         }
                         function onLoad()
                         {
                            initSelection('resultsTable');
                            var table = document.getElementById('resultsTable');
                            if (table)
                                sortableTable = new SortableTable(table, table, ["None", "String"]);

                            document.getElementById("Save Description").disabled = true;
                         }       
                         function onSubmit(o, theaction)
                         {
                            document.forms[0].action = ""+theaction;
                            return true;
                         }
                         function keyupOnDescription()
                         {
                            //if (document.getElementById("Save Description").disabled == true)
                            //  document.getElementById("Save Description").disabled = false;

                            var desc = document.getElementById("FileDesc").value;
                            if (desc.length > 0)
                                document.getElementById("Save Description").disabled = false;
                            else
                                document.getElementById("Save Description").disabled = true;
                         }
                         function onSaveDescription(name, description)
                         {
                            //document.location.href='/WsDfu/DFUInfo?Name='+name+'&UpdateDescription=true&NewDescription='+description;
                            document.forms["saveDescription"].action = ""+theaction;
                         }
                         function submitaction(button, filename)
                         {
                            if (button == "Delete")
                            {
                                var val = filename;
                                var pt = val.indexOf("@");
                                if (pt > 0)
                                    val = val.substring(0, pt);
                                if (confirm("Are you sure you want to delete "+ val+ "?"))
                                {
                                    document.location.href="/WsDFU/DFUArrayAction?Type=Delete&LogicalFiles_i0=" + filename;
                                    return true;
                                }
                            }
                            else if (button == "Copy")
                            {
                                document.location.href="/FileSpray/CopyInput?sourceLogicalName=" + filename;
                                return true;
                            }
                            else if (button == "Rename")
                            {
                                document.location.href="/FileSpray/RenameInput?sourceLogicalName=" + filename;
                                return true;
                            }
                            else if (button == "Despray")
                            {
                                document.location.href="/FileSpray/DesprayInput?sourceLogicalName=" + filename;
                                return true;
                            }
                            return false;
                         }
                         function getfile(name)
                         {
                            document.location.href="/WsDfu/DFUInfo?Name=" + escape(name);
                         }                     
                         var sortableTable = null;
                   ]]></xsl:text>
                </script>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title><xsl:value-of select="Name"/></title>
          </head>
          <xsl:choose>
          <xsl:when test="number(isSuperfile)">
            <body onload="nof5();onLoad()" class="yui-skin-sam">
                <h3>Superfile <xsl:value-of select="Name"/> has the following subfiles:</h3>
                <p/>
                <tr><th>Description:</th><td>               
                    <form id="saveDescription" action="/WsDfu/DFUInfo" method="post">
                    <input type="hidden" name="FileName" value='{Name}'/>
                    <input type="hidden" name="UpdateDescription" value='true'/>
                    <textarea rows="4" STYLE="width:350px" name="FileDesc" id="FileDesc" onkeyup="keyupOnDescription()">
            <xsl:value-of select="Description"/>&#160;
          </textarea>
                    <input type="submit" name="Save Description" value="Save Description"/>
                    </form>
                </td></tr>
                <p/>
                <form id="listitems" action="/WsDfu/SuperfileAction">
                <xsl:apply-templates select="subfiles" mode="list"/>
                <input type="hidden" name="superfile" value="{Name}"/>
                <p/>
                <table xmlns="" name="table1">
                <xsl:if test="string-length(Wuid)">
                    <tr>
                    <th>Workunit:</th>
                    <td>
                        <xsl:choose>
                        <xsl:when test="starts-with(Wuid,'D')">
                            <a href="javascript:go('/FileSpray/GetDFUWorkunit?wuid={Wuid}')"><xsl:value-of select="Wuid"/></a>
                        </xsl:when>
                        <xsl:when test="starts-with(Wuid,'W')">
                            <a href="javascript:go('/WsWorkunits/WUInfo?Wuid={Wuid}')"><xsl:value-of select="Wuid"/></a>
                        </xsl:when>
                        </xsl:choose>
                    </td>
                    </tr>
                </xsl:if>
                <tr><th>Size:</th><td><xsl:value-of select="Filesize"/>
                </td></tr>
                <tr>
                <th>Add subfiles:</th><td><input type="text" name="subfiles" size="50"/></td>
                </tr>
                <tr>
                <th>Before:</th><td><select name="before"><option value=""></option><xsl:apply-templates select="subfiles" mode="add"/></select></td>
                </tr>
                <tr>
                <td></td>
                <td><input type="submit" class="sbutton" id="action" name="action" value="add"/></td>
                </tr>
                </table>
                </form>
            </body>
          </xsl:when>
          <xsl:otherwise>
          <body class="yui-skin-sam" onload="nof5();">
            <h3>Logical File Details</h3>
            <table style="text-align:left;" cellspacing="10">
            <colgroup style="vertical-align:top;padding-right:10px;" span="2">
            </colgroup>
                <tr><th>Logical Name:</th><td><xsl:value-of select="Name"/></td></tr>
                <tr><th>Description:</th><td>               
                    <form id="saveDescription" action="/WsDfu/DFUInfo" method="post">
                    <input type="hidden" name="FileName" value='{Name}'/>
                    <input type="hidden" name="UpdateDescription" value='true'/>
                    <textarea rows="4" STYLE="width:350" name="FileDesc" id="FileDesc" onkeyup="keyupOnDescription()"><xsl:value-of select="Description"/>&#160;</textarea>
                    <input type="submit" name="Save Description" id="Save Description" value="Save Description" disabled="true"/>
                    </form>
                </td></tr>
                <tr><th>Modification Time:</th><td><xsl:value-of select="Modified"/> (UTC/GMT)</td></tr>
            <xsl:if test="string-length(UserPermission)">
                    <tr><th>User Permission:</th><td><xsl:value-of select="UserPermission"/></td></tr>
            </xsl:if>
                <tr><th>Directory:</th><td><xsl:value-of select="Dir"/></td></tr>
                <tr><th>Pathmask:</th><td><xsl:value-of select="PathMask"/></td></tr>
                <xsl:if test="string-length(Wuid)">
                    <tr>
                    <th>Workunit:</th>
                    <td>
                        <xsl:choose>
                        <xsl:when test="starts-with(Wuid,'D')">
                            <a href="javascript:go('/FileSpray/GetDFUWorkunit?wuid={Wuid}')"><xsl:value-of select="Wuid"/></a>
                        </xsl:when>
                        <xsl:when test="starts-with(Wuid,'W')">
                            <a href="javascript:go('/WsWorkunits/WUInfo?Wuid={Wuid}')"><xsl:value-of select="Wuid"/></a>
                        </xsl:when>
                        </xsl:choose>
                    </td>
                    </tr>
                </xsl:if>
                <xsl:if test="string-length(Owner)">
                    <tr><th>Owner:</th><td><a><xsl:value-of select="Owner"/></a></td></tr>
                </xsl:if>
                <xsl:if test="string-length(JobName)">
                    <tr><th>Job Name:</th><td><xsl:value-of select="JobName"/></td></tr>
                </xsl:if>
                <tr><th>Size:</th><td><xsl:value-of select="Filesize"/>
                    <xsl:if test="IsCompressed=1">
                        (This is a compressed file.)
                    </xsl:if>
                </td></tr>
                <xsl:if test="string-length(ActualSize)">
                    <tr><th>Actual Size:</th><td><xsl:value-of select="ActualSize"/></td></tr>
                </xsl:if>
                <xsl:if test="count(Stats)">
                    <tr><th>Min Skew:</th><td><xsl:value-of select="Stats/MinSkew"/></td></tr>
                    <tr><th>Max Skew:</th><td><xsl:value-of select="Stats/MaxSkew"/></td></tr>
                </xsl:if>
                <xsl:if test="string-length(Ecl)">
                    <tr><th>Ecl  (<a href="javascript:go('/WsDfu/DFUDefFile/{Filename}?Name={Name}&amp;FileName={Filename}&amp;Format=def')">.def</a>/<a href="javascript:go('/WsDfu/DFUDefFile/{Filename}?Name={Name}&amp;FileName={Filename}&amp;Format=xml')">.xml</a>):</th><td><textarea readonly="true" rows="10" STYLE="width:500"><xsl:value-of select="Ecl"/></textarea></td></tr>
                </xsl:if>
                <xsl:if test="number(RecordSize)>0">
                    <tr><th>Record Size:</th><td><xsl:value-of select="RecordSize"/></td></tr>
                </xsl:if>
                <xsl:if test="string-length(RecordCount)">
                    <tr>
                    <th>Record Count:</th>
                    <td>
                    <a>
                    <xsl:if test="number(ShowFileContent) and string-length(Ecl)">
                        <xsl:choose>
                        <xsl:when test="string-length(Cluster)">
                            <xsl:attribute name="href">javascript:go('/WsWorkunits/WUResult?LogicalName=<xsl:value-of select="Name"/>&amp;Cluster=<xsl:value-of select="Cluster"/>')</xsl:attribute>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:attribute name="href">javascript:go('/WsWorkunits/WUResult?LogicalName=<xsl:value-of select="Name"/>')</xsl:attribute>
                        </xsl:otherwise>
                        </xsl:choose>
                    </xsl:if>
                    <xsl:value-of select="RecordCount"/>
                    </a>
                    </td>
                    </tr>
                </xsl:if>
                <xsl:if test="string-length(Persistent)">
                    <tr><th>Persistent:</th><td><xsl:value-of select="Persistent"/></td></tr>
                </xsl:if>
                <xsl:if test="string-length(Format)">
                    <tr><th>Format:</th>
                    <td>
                        <xsl:choose>
                            <xsl:when test="Format='csv'">Delimited</xsl:when>
                            <xsl:otherwise><xsl:value-of select="Format"/></xsl:otherwise>
                        </xsl:choose>
                    </td>
                    </tr>
                </xsl:if>
                <xsl:if test="string-length(MaxRecordSize)">
                    <tr><th>MaxRecordSize:</th><td><xsl:value-of select="MaxRecordSize"/></td></tr>
                </xsl:if>
                <xsl:if test="string-length(CsvSeparate)">
                    <tr><th>Separators:</th><td><xsl:value-of select="CsvSeparate"/></td></tr>
                </xsl:if>
                <xsl:if test="string-length(CsvQuote)">
                    <tr><th>Quote:</th><td><xsl:value-of select="CsvQuote"/></td></tr>
                </xsl:if>
                <xsl:if test="string-length(CsvTerminate)">
                    <tr><th>Terminators:</th><td><xsl:value-of select="CsvTerminate"/></td></tr>
                </xsl:if>
                <xsl:if test="string-length(CsvEscape)">
                    <tr><th>Escape:</th><td><xsl:value-of select="CsvEscape"/></td></tr>
                </xsl:if>
                    <xsl:if test="count(Graphs/ECLGraph)">
                        <th>Graphs:</th>
                        <td>            
                            <table>
                                <colgroup style="vertical-align:top;">
                                    <col width="150"/>
                                    <col/>
                                </colgroup>
                                <xsl:apply-templates select="Graphs"/>
                            </table>
                        </td>
                    </xsl:if>
            </table>
            <br/><br/>
            <h4>File Parts:</h4>            
            <table class="list" width="500px">
                <colgroup style="vertical-align:top;">
                    <col span="2"/>
                    <col span="3" class="number"/>
                </colgroup>
                <tr class="grey"><th>Number</th><th>IP</th><th>Size</th><xsl:if test="string-length($actualSize)"><th>Actual Size</th></xsl:if></tr>
                <xsl:apply-templates select="DFUFilePartsOnClusters/DFUFilePartsOnCluster/DFUFileParts/DFUPart">
                    <xsl:sort select="Id" data-type="number"/>
                    <xsl:sort select="Copy" data-type="number"/>
                </xsl:apply-templates>
            </table>
            <xsl:if test="count(Superfiles/DFULogicalFile)">
                <br/><br/>
                <h3>This file belongs to following superfile(s):</h3>               
                <table class="list">
                    <colgroup>
                        <col width="150"/>
                        <col width="300"/>
                    </colgroup>
                    <tr>
                        <td/>
                        <td>
                            <table>
                                <xsl:apply-templates select="Superfiles"/>
                            </table>
                        </td>
                    </tr>
                </table>
            </xsl:if>
            <!--form id="delete" action="/WsDFU/DFUArrayAction" method="post">
                <input type="hidden" name="Type" value="Delete"/>
                <input type="hidden" name="LogicalFiles_i0" value="{Name}"/>
                <input type="submit" class="sbutton" id="deleteBtn" value="Delete" onclick="return confirm('Are you sure you want to delete '+'{Name}'+'?')"/>
            </form-->
            <br/><br/>
            <input type="button" class="sbutton" id="deleteBtn" value="Delete" onclick="submitaction('Delete','{Name}@{Cluster}')"/>
            <input type="button" class="sbutton" id="copyBtn" value="Copy" onclick="submitaction('Copy','{Name}')"/>
            <input type="button" class="sbutton" id="renameBtn" value="Rename" onclick="submitaction('Rename','{Name}')"/>
            <xsl:if test="FromRoxieCluster=0">
                <input type="button" class="sbutton" id="desprayBtn" value="Despray" onclick="submitaction('Despray','{Name}')"/>
            </xsl:if>
        </body> 
        </xsl:otherwise>
        </xsl:choose>
        </html>
    </xsl:template>

    <xsl:template match="DFUPart">
        <xsl:if test="Copy=1"> <!-- Copy > 1: replicate copy -->
            <tr>
                <td><xsl:value-of select="Id"/></td>
                <td><xsl:value-of select="Ip"/></td>
                <td class="number"><xsl:value-of select="Partsize"/></td>
                <xsl:if test="string-length($actualSize)"><td class="number"><xsl:value-of select="ActualSize"/></td></xsl:if>
            </tr>
        </xsl:if>
    </xsl:template>

    <xsl:template match="subfiles" mode="list">
        <table class="sort-table" id="resultsTable">
        <colgroup>
            <col width="5"/>
            <col />
        </colgroup>
        <thead>
        <tr class="grey">
        <th>
        <xsl:if test="Item[2]">
            <xsl:attribute name="id">selectAll1</xsl:attribute>
            <input type="checkbox" id="TopSelectAll" title="Select or deselect all subfiles" onclick="selectAll0(this.checked)"/>
        </xsl:if>
        </th>
        <th>
            Subfile
        </th>
        </tr>
        </thead>
        <tbody>
        <xsl:apply-templates select="Item" mode="list">
        </xsl:apply-templates>
        </tbody>
        </table>
        <xsl:if test="Item[2]">
            <table class="select-all">
            <tr>
            <th id="selectAll2">
            <input type="checkbox" id="BottomSelectAll" title="Select or deselect all subfiles" onclick="selectAll0(this.checked)"/>
            </th>
            <th align="left" colspan="7">Select All / None</th>
            </tr>
            </table>
        </xsl:if>
        <table id="btnTable" style="margin:0 0 20 20">
            <tr>
                <td>
                    <xsl:if test="Item[1]">
                        <input type="checkbox" id="removeSuperfile" name="removeSuperfile" title="Remove Superfile when the file has no subfile" disabled="true">Remove Superfile</input>
                    </xsl:if>
                </td>
            </tr>
            <tr>
                <td>
                    <xsl:choose>
                        <xsl:when test="Item[1]">
                            <input type="submit" class="sbutton" id="deleteBtn" name="action" value="remove" disabled="true" onclick="return confirm('Are you sure you want to delete the following subfiles ?\n\n'+getSelected(document.forms['listitems']).substring(1,1000))"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="button" class="sbutton" id="deleteBtn" value="Delete" onclick="submitaction('Delete','{../Name}@{../Cluster}')"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
        </table>
    </xsl:template>

    <xsl:template match="Item" mode="list">
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
        <td>
        <input type="checkbox" name="subfiles_i{position()}" value="{.}" onclick="return clicked(this, event)"/>
        </td>
        <xsl:variable name="inf_query"><xsl:value-of select="."/>
        </xsl:variable>
        <td align="left"><a href="javascript:getfile('{$inf_query}')"><xsl:value-of select="."/></a>
        </td>
        </tr>
    </xsl:template>

    <xsl:template match="subfiles" mode="add">
        <xsl:apply-templates select="Item" mode="add"/>
    </xsl:template>

    <xsl:template match="Item" mode="add">
        <option value="{.}"><xsl:value-of select="."/></option>
    </xsl:template>

    <xsl:template match="DFULogicalFile">
        <tr onmouseenter="this.bgColor = '#F0F0FF'">
            <xsl:variable name="inf_query1"><xsl:value-of select="Name"/>
            </xsl:variable>
            <td align="left"><a href="javascript:getfile('{$inf_query1}')">
                <xsl:value-of select="Name"/></a>
            </td>
        </tr>
    </xsl:template>
    <xsl:template match="ECLGraph">
        <tr>
         <xsl:attribute name="class">grey</xsl:attribute>
            <xsl:variable name="item"><xsl:value-of select="."/></xsl:variable>
            <td>
        <a href="/WsWorkunits/GVCAjaxGraph?Name={$wuid}&amp;GraphName={$item}" >
                    <xsl:value-of select="."/>
                </a>
            </td>
        <td>
            </td>
        </tr> 
    </xsl:template>
    <xsl:template match="text()|comment()"/>
</xsl:stylesheet>
