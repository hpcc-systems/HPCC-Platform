<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
      <script type="text/javascript" src="files_/scripts/sortabletable.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="JavaScript1.2">
            <xsl:text disable-output-escaping="yes"><![CDATA[
                function getSelected(o)
                {
                    if (o.tagName=='INPUT' && o.type == 'checkbox' && o.value != 'on')
                        return o.checked ? '\n'+o.value : '';

                    var s='';
                    var ch=o.children;
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
                        sortableTable = new SortableTable(table, table, ["String", "None", "None", "None"]);
                }

                function onSubmit(o, theaction)
                {
                    document.forms[0].action = ""+theaction;
                    return true;
                }

                function translate(boolval)
                {
                    if(boolval == 1)
                    {
                        return "checked";
                    }
                    else
                    {
                        return "";
                    }
                }
                function permChange(f, i)
                {
                    if(i.name == "allow_access")
                    {
                        if(i.checked)
                        {
                            f.deny_access.checked=false;
                            f.deny_full.checked=false;
                        }
                        else
                            f.allow_full.checked = false;
                    }
                    else if(i.name == "allow_read")
                    {
                        if(i.checked)
                        {
                            f.deny_read.checked=false;
                            f.deny_full.checked=false;
                        }
                        else
                            f.allow_full.checked = false;
                    }
                    else if(i.name == "allow_write")
                    {
                        if(i.checked)
                        {
                            f.deny_write.checked=false;
                            f.deny_full.checked=false;
                        }
                        else
                            f.allow_full.checked = false;
                    }
                    else if(i.name == "allow_full")
                    {
                        if(i.checked)
                        {
                            f.deny_access.checked=false;
                            f.deny_read.checked=false;
                            f.deny_write.checked=false;
                            f.deny_full.checked=false;
                            f.allow_access.checked = true;
                            f.allow_read.checked = true;
                            f.allow_write.checked = true;
                        }
                    }
                    else if(i.name == "deny_access")
                    {
                        if(i.checked)
                        {
                            f.allow_access.checked=false;
                            f.allow_full.checked=false;
                        }
                        else
                            f.deny_full.checked = false;
                    }
                    else if(i.name == "deny_read")
                    {
                        if(i.checked)
                        {
                            f.allow_read.checked=false;
                            f.allow_full.checked=false;
                        }
                        else
                            f.deny_full.checked = false;
                    }
                    else if(i.name == "deny_write")
                    {
                        if(i.checked)
                        {
                            f.allow_write.checked=false;
                            f.allow_full.checked=false;
                        }
                        else
                            f.deny_full.checked = false;
                    }
                    else if(i.name == "deny_full")
                    {
                        if(i.checked)
                        {
                            f.allow_access.checked=false;
                            f.allow_read.checked=false;
                            f.allow_write.checked=false;
                            f.allow_full.checked=false;
                            f.deny_access.checked = true;
                            f.deny_read.checked = true;
                            f.deny_write.checked = true;
                        }
                    }

                }

                var sortableTable = null;
            ]]></xsl:text>
            </script>
        </head>
    <body class="yui-skin-sam" onload="nof5();onLoad()">
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
        <table class="sort-table" id="resultsTable">
        <colgroup>
            <col width="150"/>
            <col width="200"/>
            <col width="200"/>
            <col width="150"/>
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
        <form  method="post" action="/ws_access/PermissionAction">
        <input type="hidden" name="basedn" value="{../../basedn}"/>
        <input type="hidden" name="rtype" value="{../../rtype}"/>
        <input type="hidden" name="rname" value="{../../name}"/>
        <input type="hidden" name="rtitle" value="{../../rtitle}"/>
        <input type="hidden" name="prefix" value="{../../prefix}"/>
        <input type="hidden" name="account_name" value="{account_name}"/>
        <input type="hidden" name="account_type" value="{account_type}"/>
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
                            <input type="checkbox" name="allow_access" value="1" checked="1" onClick="permChange(this.form,this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" name="allow_access" value="1"  onClick="permChange(this.form,this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="allow_read=1">
                            <input type="checkbox" name="allow_read" value="1" checked="1" onClick="permChange(this.form,this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" name="allow_read" value="1"  onClick="permChange(this.form,this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="allow_write=1">
                            <input type="checkbox" name="allow_write" value="1" checked="1" onClick="permChange(this.form,this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" name="allow_write" value="1" onClick="permChange(this.form,this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="allow_full=1">
                            <input type="checkbox" name="allow_full" value="1" checked="1" onClick="permChange(this.form,this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" name="allow_full" value="1" onClick="permChange(this.form,this)"/>
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
                            <input type="checkbox" name="deny_access" value="1" checked="1" onClick="permChange(this.form,this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" name="deny_access" value="1" onClick="permChange(this.form,this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="deny_read=1">
                            <input type="checkbox" name="deny_read" value="1" checked="1" onClick="permChange(this.form,this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" name="deny_read" value="1" onClick="permChange(this.form,this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="deny_write=1">
                            <input type="checkbox" name="deny_write" value="1" checked="1" onClick="permChange(this.form,this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" name="deny_write" value="1" onClick="permChange(this.form,this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50" align="right">
                    <xsl:choose>
                        <xsl:when test="deny_full=1">
                            <input type="checkbox" name="deny_full" value="1" checked="1" onClick="permChange(this.form,this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" name="deny_full" value="1" onClick="permChange(this.form,this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            </table>
        </td>
        <td><input type="submit" name="action" value="delete" onclick="return confirm('Are you sure you want to delete permissions for {escaped_account_name}?')"/><input type="submit" name="action" value="update" onclick="return confirm('Are you sure you want to update permissions for {escaped_account_name}?')"/></td>
        </form>
        </tr>
    </xsl:template>

    <xsl:template match="*|@*|text()"/>

</xsl:stylesheet>
