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
    <xsl:template match="/AccountPermissionsResponse">
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
                function permChange(i)
                {
          iname = i.id.substr(0, 12);
          iposition = i.id.substr(12);
                    if(iname == "allow_access")
                    {
                        if(i.checked)
                        {
              document.getElementById('deny__access' + iposition).checked=false;
              document.getElementById('deny__full__' + iposition).checked=false;
                        }
                        else
              document.getElementById('allow_full__' + iposition).checked=false;
                    }
                    else if(iname == "allow_read__")
                    {
                        if(i.checked)
                        {
                            document.getElementById('deny__read__' + iposition).checked=false;
                            document.getElementById('deny__full__' + iposition).checked=false;
                        }
                        else
                            document.getElementById('allow_full__' + iposition).checked = false;
                    }
                    else if(iname == "allow_write_")
                    {
                        if(i.checked)
                        {
                            document.getElementById('deny__write_' + iposition).checked=false;
                            document.getElementById('deny__full__' + iposition).checked=false;
                        }
                        else
                            document.getElementById('allow_full__' + iposition).checked = false;
                    }
                    else if(iname == "allow_full__")
                    {
                        if(i.checked)
                        {
                            document.getElementById('deny__access' + iposition).checked=false;
                            document.getElementById('deny__read__' + iposition).checked=false;
                            document.getElementById('deny__write_' + iposition).checked=false;
                            document.getElementById('deny__full__' + iposition).checked=false;
                            document.getElementById('allow_access' + iposition).checked = true;
                            document.getElementById('allow_read__' + iposition).checked = true;
                            document.getElementById('allow_write_' + iposition).checked = true;
                        }
                    }
                    else if(iname == "deny__access")
                    {
                        if(i.checked)
                        {
                            document.getElementById('allow_access' + iposition).checked=false;
                            document.getElementById('allow_full__' + iposition).checked=false;
                        }
                        else
                            document.getElementById('deny__full__' + iposition).checked = false;
                    }
                    else if(iname == "deny__read__")
                    {
                        if(i.checked)
                        {
                            document.getElementById('allow_read__' + iposition).checked=false;
                            document.getElementById('allow_full__' + iposition).checked=false;
                        }
                        else
                            document.getElementById('deny__full__' + iposition).checked = false;
                    }
                    else if(iname == "deny__write_")
                    {
                        if(i.checked)
                        {
                            document.getElementById('allow_write_' + iposition).checked=false;
                            document.getElementById('allow_full__' + iposition).checked=false;
                        }
                        else
                            document.getElementById('deny__full__' + iposition).checked = false;
                    }
                    else if(iname == "deny__full__")
                    {
                        if(i.checked)
                        {
                            document.getElementById('allow_access' + iposition).checked=false;
                            document.getElementById('allow_read__' + iposition).checked=false;
                            document.getElementById('allow_write_' + iposition).checked=false;
                            document.getElementById('allow_full__' + iposition).checked=false;
                            document.getElementById('deny__access' + iposition).checked = true;
                            document.getElementById('deny__read__' + iposition).checked = true;
                            document.getElementById('deny__write_' + iposition).checked = true;
                        }
                    }
                }
        function onUpdate(f, position, message)
                {
          ret = confirm(message);
          if (ret)
          {
            f.allow_access.value = (document.getElementById('allow_access_' + position).checked ? 1: 0);
            f.allow_read.value   = (document.getElementById('allow_read___' + position).checked ? 1: 0);
            f.allow_write.value  = (document.getElementById('allow_write__' + position).checked ? 1: 0);
            f.allow_full.value   = (document.getElementById('allow_full___' + position).checked ? 1: 0);
            f.deny_access.value  = (document.getElementById('deny__access_' + position).checked ? 1: 0);
            f.deny_read.value    = (document.getElementById('deny__read___' + position).checked ? 1: 0);
            f.deny_write.value   = (document.getElementById('deny__write__' + position).checked ? 1: 0);
            f.deny_full.value    = (document.getElementById('deny__full___' + position).checked ? 1: 0);
          }

          return ret;
                }

                function toggleElement(ElementId)
                {
                    var obj = document.getElementById('group_div_' + ElementId);
                    if (obj == null)
                        return;
                    explink = document.getElementById('explink_' + ElementId);
                    if (obj.style.visibility == 'visible')
                    {
                        obj.style.display = 'none';
                        obj.style.visibility = 'hidden';
                        if (explink)
                            explink.className = 'wusectionexpand';
                    }
                    else
                    {
                        obj.style.display = 'inline';
                        obj.style.visibility = 'visible';
                        if (explink)
                            explink.className = 'wusectioncontract';
                    }
                }
            ]]></xsl:text>
            </script>
        </head>
    <body class="yui-skin-sam" onload="nof5();onLoad()">
            <h3>Permissions of <xsl:value-of select="AccountName"/></h3>
            <p/>
            <xsl:if test="Permissions/Permission[1]">
                <!--xsl:apply-templates select="Permissions"/-->
                <xsl:apply-templates select="Permissions">
                    <xsl:with-param name="AccountN" select="AccountName"/>
                    <xsl:with-param name="AccountT" select="IsGroup"/>
                </xsl:apply-templates>
            </xsl:if>
            <xsl:if test="count(BasednNames/Item)">
                <form method="POST" action="/ws_access/PermissionAddInput">
                    <input type="hidden" name="AccountName" value="{AccountName}"/>
                    <input type="hidden" name="AccountType" value="{IsGroup}"/>
                    <input type="hidden" name="prefix" value="{prefix}"/>
                    <select size="1" name="BasednName">
                        <xsl:apply-templates select="BasednNames"/>
                    </select>
                    <input type="submit" class="sbutton" name="action" value="  Add  "/>
                </form>
            </xsl:if>
            <p/>
            <xsl:if test="GroupPermissions/GroupPermission[1]">
                <xsl:apply-templates select="GroupPermissions"/>
            </xsl:if>
        </body>
        </html>
    </xsl:template>

    <xsl:template match="GroupPermissions">
        <xsl:apply-templates select="GroupPermission"/>
            <!--xsl:sort select="GroupName"/>
        </xsl:apply-templates-->
    </xsl:template>

    <xsl:template match="GroupPermission">
        <xsl:variable name="groupDivId">
            <xsl:value-of select="concat('group_div_', GroupName)"/>
        </xsl:variable>
        <xsl:variable name="explinkId">
            <xsl:value-of select="concat('explink_', GroupName)"/>
        </xsl:variable>
        <br/>
        <div>
            <a href="javascript:void(0)" onclick="toggleElement('{GroupName}');" id="{$explinkId}" class="wusectionexpand">
                Inherited Permissions from Group: <xsl:value-of select="GroupName"/> (Changes inside this section will be applied to the whole group.)
                <br/><br/>
            </a>
        </div>
        <span id ="{$groupDivId}" style="display:none; visibility:hidden;">
            <xsl:if test="Permissions/Permission[1]">
                <!--xsl:apply-templates/-->
                <xsl:apply-templates select="Permissions">
                    <xsl:with-param name="AccountN" select="GroupName"/>
                    <xsl:with-param name="AccountT" select="1"/>
                </xsl:apply-templates>
            </xsl:if>
            <xsl:if test="count(BasednNames/Item)">
                <form method="POST" action="/ws_access/PermissionAddInput">
                    <input type="hidden" name="AccountName" value="{GroupName}"/>
                    <input type="hidden" name="AccountType" value="1"/>
                    <input type="hidden" name="prefix" value="{prefix}"/>
                    <select size="1" name="BasednName">
                        <xsl:apply-templates select="BasednNames"/>
                    </select>
                    <input type="submit" class="sbutton" name="action" value="Add To This Group"/>
                </form>
            </xsl:if>
            <br/>
        </span>
    </xsl:template>

    <xsl:template match="Permissions">
        <xsl:param name="AccountN"/>
        <xsl:param name="AccountT"/>
        <table class="sort-table" id="resultsTable">
        <colgroup>
            <col width="150"/>
            <col width="150"/>
            <col width="150"/>
            <col width="150"/>
            <col width="150"/>
        </colgroup>
        <thead>
        <tr class="grey">
        <th align="left">Resource</th>
        <th align="left">Permission</th>
        <th><table><tr><th/><th>allow</th><th/></tr><tr><td width="50">access</td><td width="50">read</td><td width="50">write</td><td width="50">full</td></tr></table></th>
        <th><table><tr><th/><th>deny</th><th/></tr><tr><td width="50">access</td><td width="50">read</td><td width="50">write</td><td width="50">full</td></tr></table></th>
        <th>Operation</th>
        </tr>
        </thead>
        <tbody>
        <xsl:apply-templates select="Permission">
            <xsl:with-param name="AccountN" select="$AccountN"/>
            <xsl:with-param name="AccountT" select="$AccountT"/>
            <xsl:sort select="ResourceName"/>
        </xsl:apply-templates>
        </tbody>
        </table>
    </xsl:template>

    <xsl:template match="Permission">
        <xsl:param name="AccountN"/>
        <xsl:param name="AccountT"/>
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
        <td align="left">
        <xsl:value-of select="ResourceName"/>
        </td>
        <td align="left">
        <xsl:value-of select="PermissionName"/>
        </td>
        <xsl:variable name="ar" select="checked"/>
        <td>
            <table>
            <tr>
                <td width="50">
                    <xsl:choose>
                        <xsl:when test="allow_access=1">
                            <input type="checkbox" id="allow_access_{$AccountN}_{position()}" name="allow_access" value="1" checked="1" onClick="permChange(this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="allow_access_{$AccountN}_{position()}" name="allow_access" value="1"  onClick="permChange(this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50">
                    <xsl:choose>
                        <xsl:when test="allow_read=1">
                            <input type="checkbox" id="allow_read___{$AccountN}_{position()}" name="allow_read" value="1" checked="1" onClick="permChange(this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="allow_read___{$AccountN}_{position()}" name="allow_read" value="1"  onClick="permChange(this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50">
                    <xsl:choose>
                        <xsl:when test="allow_write=1">
                            <input type="checkbox" id="allow_write__{$AccountN}_{position()}" name="allow_write" value="1" checked="1" onClick="permChange(this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="allow_write__{$AccountN}_{position()}" name="allow_write" value="1" onClick="permChange(this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50">
                    <xsl:choose>
                        <xsl:when test="allow_full=1">
                            <input type="checkbox" id="allow_full___{$AccountN}_{position()}" name="allow_full" value="1" checked="1" onClick="permChange(this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="allow_full___{$AccountN}_{position()}" name="allow_full" value="1" onClick="permChange(this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            </table>
        </td>
        <td>
            <table>
            <tr>
                <td width="50">
                    <xsl:choose>
                        <xsl:when test="deny_access=1">
                            <input type="checkbox" id="deny__access_{$AccountN}_{position()}" name="deny_access" value="1" checked="1" onClick="permChange(this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="deny__access_{$AccountN}_{position()}" name="deny_access" value="1" onClick="permChange(this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50">
                    <xsl:choose>
                        <xsl:when test="deny_read=1">
                            <input type="checkbox" id="deny__read___{$AccountN}_{position()}" name="deny_read" value="1" checked="1" onClick="permChange(this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="deny__read___{$AccountN}_{position()}" name="deny_read" value="1" onClick="permChange(this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50">
                    <xsl:choose>
                        <xsl:when test="deny_write=1">
                            <input type="checkbox" id="deny__write__{$AccountN}_{position()}" name="deny_write" value="1" checked="1" onClick="permChange(this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="deny__write__{$AccountN}_{position()}" name="deny_write" value="1" onClick="permChange(this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
                <td width="50">
                    <xsl:choose>
                        <xsl:when test="deny_full=1">
                            <input type="checkbox" id="deny__full___{$AccountN}_{position()}" name="deny_full" value="1" checked="1" onClick="permChange(this)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" id="deny__full___{$AccountN}_{position()}" name="deny_full" value="1" onClick="permChange(this)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            </table>
        </td>
        <td>
      <form  method="post" action="/ws_access/PermissionAction">
        <input type="hidden" name="AccountPermission" value="1"/>
        <input type="hidden" name="basedn" value="{basedn}"/>
        <input type="hidden" name="rtype" value="{RType}"/>
        <input type="hidden" name="rname" value="{PermissionName}"/>
        <input type="hidden" name="rtitle" value="{ResourceName}"/>
        <!--input type="hidden" name="prefix" value="{../../prefix}"/-->
        <input type="hidden" name="account_name" value="{$AccountN}"/>
        <input type="hidden" name="account_type" value="{$AccountT}"/>
        <input type="hidden" name="allow_access" value=""/>
        <input type="hidden" name="allow_read" value=""/>
        <input type="hidden" name="allow_write" value=""/>
        <input type="hidden" name="allow_full" value=""/>
        <input type="hidden" name="deny_access" value=""/>
        <input type="hidden" name="deny_read" value=""/>
        <input type="hidden" name="deny_write" value=""/>
        <input type="hidden" name="deny_full" value=""/>

        <input type="submit" name="action" value="delete" onclick="return confirm('Are you sure you want to delete permissions for {$AccountN}?')"/><input type="submit" name="action" value="update" onclick="return onUpdate(this.form, '{$AccountN}_{position()}', 'Are you sure you want to update permissions for {$AccountN}?')"/>
      </form>
    </td>
        <!--td><input type="submit" name="action" value="delete" onclick="return confirm('Are you sure?')"/><input type="submit" name="action" value="update" onclick="return confirm('Are you sure?')"/></td-->
      </tr>
  </xsl:template>

    <xsl:template match="BasednNames">
        <xsl:apply-templates select="Item" />
    </xsl:template>

    <xsl:template match="Item">
        <option value="{.}"><xsl:value-of select="."/></option>
    </xsl:template>
    <xsl:template match="*|@*|text()"/>

</xsl:stylesheet>
