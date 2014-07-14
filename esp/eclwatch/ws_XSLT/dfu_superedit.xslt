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
    <xsl:template match="/SuperfileListResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Editing User</title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
            <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="JavaScript1.2">
            <xsl:text disable-output-escaping="yes"><![CDATA[
                function onRowCheck(checked)
                {
                    document.forms[0].deleteBtn.disabled = checkedCount == 0;
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
                }       

                function onSubmit(o, theaction)
                {
                    document.forms[0].action = ""+theaction;
                    return true;
                }
            ]]></xsl:text>
            </script>
        </head>
    <body class="yui-skin-sam" onload="nof5();onLoad()">
            <h3>Superfile <xsl:value-of select="superfile"/> has the following subfiles:</h3>
            <p/>
            <form id="listitems" action="/WsDfu/SuperfileAction">
            <xsl:apply-templates select="subfiles" mode="list"/>
            <input type="hidden" name="superfile" value="{superfile}"/>
            <p/>
            <table xmlns="" name="table1">
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
        </html>
    </xsl:template>

    <xsl:template match="subfiles" mode="list">
        <table class="sort-table" id="resultsTable">
        <colgroup>
            <col width="5"/>
            <col width="150"/>
        </colgroup>
        <thead>
        <tr class="grey">
        <th>
        <xsl:if test="Item[2]">
            <xsl:attribute name="name">selectAll1</xsl:attribute>
            <input type="checkbox" title="Select or deselect all subfiles" onclick="selectAll(this.checked)"/>
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
            <input type="checkbox" title="Select or deselect all subfiles" onclick="selectAll(this.checked)"/>
            </th>
            <th align="left" colspan="7">Select All / None</th>
            </tr>
            </table>
        </xsl:if>
        <table id="btnTable" style="margin:20 0 0 0">
        <colgroup>
            <col span="8" width="100"/>
        </colgroup>
        </table>
        <input type="submit" class="sbutton" id="deleteBtn" name="action" value="remove" disabled="true" onclick="return confirm('Are you sure you want to delete the following subfiles ?\n\n'+getSelected(document.forms['listitems']).substring(1,1000))"/>
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
        <td align="left">
        <xsl:value-of select="."/>
        </td>
        </tr>
    </xsl:template>

    <xsl:template match="subfiles" mode="add">
        <xsl:apply-templates select="Item" mode="add"/>
    </xsl:template>

    <xsl:template match="Item" mode="add">
        <option value="."><xsl:value-of select="."/></option>
    </xsl:template>

    <xsl:template match="*|@*|text()"/>

</xsl:stylesheet>
