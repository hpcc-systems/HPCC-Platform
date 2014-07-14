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
    <xsl:template match="/ResourcePermissionsResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Permissions</title>
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
                function updatePermRow(rowId)
                {
                    var formId = 'row_action_'+rowId;
                    var thisForm = document.forms[formId];
                    thisForm.allow_access.value = document.getElementById('allow_access_'+rowId).checked;
                    thisForm.allow_read.value = document.getElementById('allow_read_'+rowId).checked;
                    thisForm.allow_write.value = document.getElementById('allow_write_'+rowId).checked;
                    thisForm.allow_full.value = document.getElementById('allow_full_'+rowId).checked;
                    thisForm.deny_access.value = document.getElementById('deny_access_'+rowId).checked;
                    thisForm.deny_read.value = document.getElementById('deny_read_'+rowId).checked;
                    thisForm.deny_write.value = document.getElementById('deny_write_'+rowId).checked;
                    thisForm.deny_full.value = document.getElementById('deny_full_'+rowId).checked;
                }

                function permChange(rowId, i)
                {
                    if(i.name == "allow_access")
                    {
                        if(i.checked)
                        {
                            document.getElementById('deny_access_'+rowId).checked=false;
                            document.getElementById('deny_full_'+rowId).checked=false;
                        }
                        else
                        {
                            document.getElementById('allow_full_'+rowId).checked=false;
                        }
                    }
                    else if(i.name == "allow_read")
                    {
                        if(i.checked)
                        {
                            document.getElementById('deny_read_'+rowId).checked=false;
                            document.getElementById('deny_full_'+rowId).checked=false;
                        }
                        else
                            document.getElementById('allow_full_'+rowId).checked = false;
                    }
                    else if(i.name == "allow_write")
                    {
                        if(i.checked)
                        {
                            document.getElementById('deny_write_'+rowId).checked=false;
                            document.getElementById('deny_full_'+rowId).checked=false;
                        }
                        else
                            document.getElementById('allow_full_'+rowId).checked = false;
                    }
                    else if(i.name == "allow_full")
                    {
                        if(i.checked)
                        {
                            document.getElementById('deny_access_'+rowId).checked=false;
                            document.getElementById('deny_read_'+rowId).checked=false;
                            document.getElementById('deny_write_'+rowId).checked=false;
                            document.getElementById('deny_full_'+rowId).checked=false;
                            document.getElementById('allow_access_'+rowId).checked = true;
                            document.getElementById('allow_read_'+rowId).checked = true;
                            document.getElementById('allow_write_'+rowId).checked = true;
                        }
                    }
                    else if(i.name == "deny_access")
                    {
                        if(i.checked)
                        {
                            document.getElementById('allow_access_'+rowId).checked=false;
                            document.getElementById('allow_full_'+rowId).checked=false;
                        }
                        else
                            document.getElementById('deny_full_'+rowId).checked = false;
                    }
                    else if(i.name == "deny_read")
                    {
                        if(i.checked)
                        {
                            document.getElementById('allow_read_'+rowId).checked=false;
                            document.getElementById('allow_full_'+rowId).checked=false;
                        }
                        else
                            document.getElementById('deny_full_'+rowId).checked = false;
                    }
                    else if(i.name == "deny_write")
                    {
                        if(i.checked)
                        {
                            document.getElementById('allow_write_'+rowId).checked=false;
                            document.getElementById('allow_full_'+rowId).checked=false;
                        }
                        else
                            document.getElementById('deny_full_'+rowId).checked = false;
                    }
                    else if(i.name == "deny_full")
                    {
                        if(i.checked)
                        {
                            document.getElementById('allow_access_'+rowId).checked=false;
                            document.getElementById('allow_read_'+rowId).checked=false;
                            document.getElementById('allow_write_'+rowId).checked=false;
                            document.getElementById('allow_full_'+rowId).checked=false;
                            document.getElementById('deny_access_'+rowId).checked = true;
                            document.getElementById('deny_read_'+rowId).checked = true;
                            document.getElementById('deny_write_'+rowId).checked = true;
                        }
                    }
                    updatePermRow(rowId);
                }
            ]]></xsl:text>
            </script>
        </head>
    <body class="yui-skin-sam" onload="nof5()">
            <h3>Permissions of <xsl:value-of select="name"/></h3>
            <p/>
            <xsl:choose>
                <xsl:when test="not(Permissions/Permission[1])">
                </xsl:when>
                <xsl:otherwise>
                    <xsl:apply-templates/>
                </xsl:otherwise>
            </xsl:choose>
        <form method="POST" action="/ws_access/PermissionAddInput">
            <input type="hidden" name="basedn" value="{basedn}"/>
            <input type="hidden" name="rtype" value="{rtype}"/>
            <input type="hidden" name="rtitle" value="{rtitle}"/>
            <input type="hidden" name="rname" value="{name}"/>
            <input type="hidden" name="prefix" value="{prefix}"/>
            <input type="submit" class="sbutton" name="action" value="  Add  "/>
        </form>
        </body>
        </html>
    </xsl:template>

    <xsl:template match="Permissions">
        <table class="sort-table">
        <colgroup>
            <col width="150"/>
            <col width="200"/>
            <col width="200"/>
            <col width="150"/>
        </colgroup>
        <thead>
        <tr class="grey">
        <th align="left">Account</th>
        <th><table><tr><th/><th>allow</th><th/></tr><tr><td width="50">access</td><td width="50">read</td><td width="50">write</td><td width="50">full</td></tr></table></th>
        <th><table><tr><th/><th>deny</th><th/></tr><tr><td width="50">access</td><td width="50">read</td><td width="50">write</td><td width="50">full</td></tr></table></th>
        <th>Operation</th>
        </tr>
        </thead>
        <tbody>
        <xsl:apply-templates select="Permission">
            <xsl:sort select="account_name"/>
        </xsl:apply-templates>
        </tbody>
        </table>
    </xsl:template>

    <xsl:template match="Permission">
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
        <xsl:variable name="pid" select="position()"/>
        <td align="left">
        <xsl:value-of select="account_name"/>
        </td>
        <xsl:variable name="ar" select="checked"/>
        <td>
            <table>
            <tr>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="allow_access=1">
                            <input type="checkbox" id="allow_access_{$pid}" name="allow_access" value="1" checked="1" onClick="permChange({$pid},this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="allow_access_{$pid}" name="allow_access" value="1"  onClick="permChange({$pid},this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="allow_read=1">
                            <input type="checkbox" id="allow_read_{$pid}" name="allow_read" value="1" checked="1" onClick="permChange({$pid},this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="allow_read_{$pid}" name="allow_read" value="1"  onClick="permChange({$pid},this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="allow_write=1">
                            <input type="checkbox" id="allow_write_{$pid}" name="allow_write" value="1" checked="1" onClick="permChange({$pid},this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="allow_write_{$pid}" name="allow_write" value="1" onClick="permChange({$pid},this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="allow_full=1">
                            <input type="checkbox" id="allow_full_{$pid}" name="allow_full" value="1" checked="1" onClick="permChange({$pid},this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="allow_full_{$pid}" name="allow_full" value="1" onClick="permChange({$pid},this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            </table>
        </td>
        <td>
            <table>
            <tr>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="deny_access=1">
                            <input type="checkbox" id="deny_access_{$pid}" name="deny_access" value="1" checked="1" onClick="permChange({$pid},this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="deny_access_{$pid}" name="deny_access" value="1" onClick="permChange({$pid},this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="deny_read=1">
                            <input type="checkbox" id="deny_read_{$pid}" name="deny_read" value="1" checked="1" onClick="permChange({$pid},this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="deny_read_{$pid}" name="deny_read" value="1" onClick="permChange({$pid},this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="deny_write=1">
                            <input type="checkbox" id="deny_write_{$pid}" name="deny_write" value="1" checked="1" onClick="permChange({$pid},this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="deny_write_{$pid}" name="deny_write" value="1" onClick="permChange({$pid},this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="deny_full=1">
                            <input type="checkbox" id="deny_full_{$pid}" name="deny_full" value="1" checked="1" onClick="permChange({$pid},this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="deny_full_{$pid}" name="deny_full" value="1" onClick="permChange({$pid},this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            </table>
        </td>
        <td>
            <form id="row_action_{$pid}" method="post" action="/ws_access/PermissionAction">
                <input type="hidden" name="basedn" value="{../../basedn}"/>
                <input type="hidden" name="rtype" value="{../../rtype}"/>
                <input type="hidden" name="rname" value="{../../name}"/>
                <input type="hidden" name="rtitle" value="{../../rtitle}"/>
                <input type="hidden" name="prefix" value="{../../prefix}"/>
                <input type="hidden" name="account_name" value="{account_name}"/>
                <input type="hidden" name="account_type" value="{account_type}"/>
                <input type="hidden" name="allow_access" value="{allow_access}"/>
                <input type="hidden" name="allow_read" value="{allow_read}"/>
                <input type="hidden" name="allow_write" value="{allow_write}"/>
                <input type="hidden" name="allow_full" value="{allow_full}"/>
                <input type="hidden" name="deny_access" value="{deny_access}"/>
                <input type="hidden" name="deny_read" value="{deny_read}"/>
                <input type="hidden" name="deny_write" value="{deny_write}"/>
                <input type="hidden" name="deny_full" value="{deny_full}"/>
                <input type="submit" name="action" value="delete" onclick="return confirm('Are you sure you want to delete permissions for {escaped_account_name}?')"/>
                <input type="submit" name="action" value="update" onclick="return confirm('Are you sure you want to update permissions for {escaped_account_name}?')"/>
            </form>
        </td>
    </tr>
    </xsl:template>

    <xsl:template match="*|@*|text()"/>

</xsl:stylesheet>
