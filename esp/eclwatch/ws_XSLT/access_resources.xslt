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
    <xsl:template match="/ResourcesResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title><xsl:value-of select="rtitle"/>s</title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
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
                function onRowCheck(checked)
                {
                    document.getElementById('deleteBtn').disabled = checkedCount == 0;
                    if (document.getElementById('updateBtn'))
                        document.getElementById('updateBtn').disabled = checkedCount == 0;
                    document.getElementById('addBtn').disabled = checkedCount != 0;
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
                        sortableTable = new SortableTable(table, table, ["None", "String", "String", "None"]);
                }

                function onSubmit(o, theaction)
                {
                    document.forms[0].action = ""+theaction;
                    return true;
                }

                function doUpdateNow(value)
                {
                    document.getElementById('DoUpdate').value=value;
                }

                var sortableTable = null;
            ]]></xsl:text>
            </script>
        </head>
    <body class="yui-skin-sam" onload="onLoad()">
            <h3><xsl:value-of select="rtitle"/>s</h3>
      <xsl:choose>
        <xsl:when test="toomany=1">
          <form xmlns="" method="POST" action="/ws_access/Resources">
            <input type="hidden" name="basedn" value="{basedn}"/>
            <input type="hidden" name="rtype" value="{rtype}"/>
            <input type="hidden" name="rtitle" value="{rtitle}"/>
            <input type="hidden" name="prefix" value="{prefix}"/>
            <table>
              <tr>
                <td colspan="4">
                  <b>LDAP services cannot return the resource list because of "Too Many" resources.</b>
                </td>
              </tr>
              <tr>
                <td>
                  <b>Please use a Search filter: </b>
                  <input type="text" name="searchinput" size="20" value="" />
                  <input type="submit" value="search" name="S1" />
                </td>
              </tr>
              <tr/>
              <tr/>
              <tr>
                <td>
                </td>
              </tr>
              <tr>
                <td>
                  <input type="button" class="sbutton" id="addBtn" name="action" value="  Add  " onClick="document.forms['addform'].submit()"/>
                </td>
              </tr>
            </table>
          </form>
        </xsl:when>
        <xsl:otherwise>
          <form id="listitems" action="/ws_access/ResourceDelete" method="post">
                    <input type="hidden" name="basedn" value="{basedn}"/>
                    <input type="hidden" name="rtype" value="{rtype}"/>
                    <input type="hidden" name="rtitle" value="{rtitle}"/>
                    <input type="hidden" name="prefix" value="{prefix}"/>
                    <input type="hidden" id="DoUpdate" name="DoUpdate" value="0"/>

                    <xsl:apply-templates/>

                    <table id="btnTable" style="margin:20 0 0 0">
                        <colgroup>
                            <col span="2" width="100"/>
                        </colgroup>
                        <tr>
                            <td>
                                <input type="submit" class="sbutton" id="deleteBtn" name="action" value="Delete" disabled="true" onclick="return confirm('Are you sure you want to delete the following {../rtitle}(s) ?\n\n'+getSelected(document.forms['listitems']).substring(1,1000))"/>
                            </td>
                            <td>
                                <input type="button" class="sbutton" id="updateBtn" name="action" value="Update" disabled="true" onclick="doUpdateNow(1); document.forms['listitems'].submit()"/>
                            </td>
                            <td>
                                <input type="button" class="sbutton" id="addBtn" name="action" value="  Add  " onClick="document.forms['addform'].submit()"/>
                            </td>
                        </tr>
                    </table>

                </form>
        </xsl:otherwise>
      </xsl:choose>
            <table id="btnTable2" style="margin:20 0 0 0">
                <colgroup>
                    <col span="8" width="50"/>
                </colgroup>
                <tr>
                    <xsl:if test="rtype='module'">
                        <td>
                            <form action="/ws_access/Resources">
                            <input type="hidden" name="basedn" value="{basedn}"/>
                            <input type="hidden" name="rtype" value="service"/>
                            <input type="hidden" name="rtitle" value="CodeGenerator Permission"/>
                            <input type="hidden" name="prefix" value="codegenerator."/>
                            <input type="submit" class="sbutton" id="sumbitCodeGen" name="action" value="Code Generator"/>
                            </form>
                        </td>
                    </xsl:if>
                    <xsl:if test="default_name/text()">
                        <td>
                            <form action="/ws_access/ResourcePermissions">
                            <input type="hidden" name="basedn" value="{default_basedn}"/>
                            <input type="hidden" name="rtype" value="{rtype}"/>
                            <input type="hidden" name="name" value="{default_name}"/>
                            <input class="sbutton" type="submit" id="sumbitBtn" name="action" value="Default Permissions"/>
                            </form>
                        </td>
                    </xsl:if>
                    <xsl:if test="rtype='file'">
                        <td>
                            <form action="/ws_access/ResourcePermissions">
                            <input type="hidden" name="basedn" value="{basedn}"/>
                            <input type="hidden" name="rtype" value="file"/>
                            <input type="hidden" name="rtitle" value="FileScope"/>
                            <input type="hidden" name="name" value="file"/>
                            <input type="submit" class="sbutton" id="sumbitFiles" name="action" value="Physical Files"/>
                            </form>
                        </td>
            <td>
              <form action="/ws_access/FilePermission">
                <input type="submit" class="sbutton" id="sumbitFile" name="action" value="Check File Permission"/>
              </form>
            </td>
          </xsl:if>
            <td>
                <form action="/ws_access/ClearPermissionsCache">
                  <input id="clearPermissionsCacheBtn" class="sbutton" type="submit" name="action" value="Clear Permissions Cache" onclick="return confirm('Are you sure you want to clear the DALI and ESP permissions caches? Running workunit performance might degrade significantly until the caches have been refreshed.')"/>
                </form>
            </td>

            <xsl:if test="scopeScansStatus/retcode=0">
              <xsl:if test="scopeScansStatus/isEnabled=0">
                <td>
                  <form action="/ws_access/EnableScopeScans">
                    <input id="EnableScopeScansBtn" class="sbutton" type="submit"  name="action" value="Enable Scope Scans" onclick="return confirm('Are you sure you want to enable Scope Scans?')"/>
                  </form>
                </td>
              </xsl:if>
              <xsl:if test="scopeScansStatus/isEnabled=1">
                <td>
                  <form action="/ws_access/DisableScopeScans">
                    <input id="DisableScopeScansBtn" class="sbutton" type="submit"  name="action" value="Disable Scope Scans" onclick="return confirm('Are you sure you want to disable Scope Scans?')"/>
                  </form>
                </td>
              </xsl:if>
            </xsl:if>

                </tr>
            </table>
      <form action="/ws_access/ResourceAddInput" id="addform">
        <input type="hidden" name="basedn" value="{basedn}"/>
        <input type="hidden" name="rtype" value="{rtype}"/>
        <input type="hidden" name="rtitle" value="{rtitle}"/>
        <input type="hidden" name="prefix" value="{prefix}"/>
      </form>
    </body>
        </html>
    </xsl:template>

    <xsl:template match="Resources">
        <table class="sort-table" id="resultsTable">
            <colgroup>
                <col width="5"/>
                <col width="200"/>
                <col width="200"/>
                <col width="100"/>
            </colgroup>
            <thead>
                <tr class="grey">
                    <th id="selectAll1">
                        <xsl:if test="Resource[2]">
                            <!--xsl:attribute name="Resource">selectAll1</xsl:attribute-->
                            <input type="checkbox" title="Select or deselect all" onclick="selectAll(this.checked)"/>
                        </xsl:if>
                    </th>
                    <th align="left">Name</th>
                    <th>Description</th>
                    <th>Operation</th>
                </tr>
            </thead>
            <tbody>
                <xsl:apply-templates select="Resource">
                    <xsl:sort select="name"/>
                </xsl:apply-templates>
            </tbody>
        </table>
        <xsl:if test="Resource[2]">
            <table class="select-all">
                <tr>
                    <th id="selectAll2">
                        <input type="checkbox" title="Select or deselect all" onclick="selectAll(this.checked)"/>
                    </th>
                    <th align="left" colspan="7">Select All / None</th>
                </tr>
            </table>
        </xsl:if>
    </xsl:template>

    <xsl:template match="Resource">
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
        <input type="checkbox" name="names_i{position()}" value="{name}" onclick="return clicked(this)"/>
        </td>
        <td align="left">
        <xsl:choose>
            <xsl:when test="isSpecial=1">
                <b>
                <xsl:value-of select="name"/>
                </b>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="name"/>
            </xsl:otherwise>
        </xsl:choose>
        </td>
        <td align="left">
        <xsl:value-of select="description"/>
        </td>
        <td align="center">
            <a href="javascript:go('/ws_access/ResourcePermissions?basedn={../../basedn}&amp;rtype={../../rtype}&amp;rtitle={../../rtitle}&amp;name={name}&amp;prefix={../../prefix}')">Permissions</a>
        </td>
        </tr>
    </xsl:template>

    <xsl:template match="*|@*|text()"/>

</xsl:stylesheet>
