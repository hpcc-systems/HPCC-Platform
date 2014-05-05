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
    <xsl:output method="html"/>
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Add Permission</title>
         <link type="text/css" rel="stylesheet" href="/esp/files_/css/espdefault.css"/>
            <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
      <script type="text/javascript" src="files_/scripts/espdefault.js">&#160;</script>
            <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="JavaScript1.2">
            <xsl:text disable-output-escaping="yes"><![CDATA[
                function onRowCheck(checked)
                {
                    validateInput();
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

                function getSelected1(o)
                {
                    var loc = -1;
                    if (o.tagName =='INPUT' && o.type == 'checkbox')
                    {
                        loc = o.name.indexOf("name");
                    }

                    if (o.tagName=='INPUT' && o.type == 'checkbox' && (o.value != 'on' || loc != 0))
                        return o.checked ? '\n'+o.name : '';

                    var s='';
                    var ch=o.childNodes;
                    if (ch)
                        for (var i in ch)
                            s=s+getSelected1(ch[i]);
                    return s;
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

                function validateInput()
                {
                    var resources = 1;
                    var sel = getSelected1(document.getElementById('resultsTable'));
                    if (sel == '')
                        resources = 0;
                    //alert(resources);

                    fm = document.forms[0];
                    if (fm.usernames && fm.groupnames)
                    {
                        if((fm.usernames.value != '' || fm.groupnames.value != '') &&                               (fm.allow_read.checked || fm.allow_write.checked || fm.allow_full.checked
                         || fm.deny_read.checked || fm.deny_write.checked || fm.deny_full.checked || fm.allow_access.checked || fm.deny_access.checked))
                        {
                            if (resources > 0)
                                fm.submit.disabled = false;
                            else
                                fm.submit.disabled = true;
                        }
                        else
                        {
                            fm.submit.disabled = true;
                        }
                    }

                    if (fm.ResourceName)
                    {
                        if((fm.ResourceName.value != '') && (fm.allow_read.checked || fm.allow_write.checked || fm.allow_full.checked
                         || fm.deny_read.checked || fm.deny_write.checked || fm.deny_full.checked || fm.allow_access.checked || fm.deny_access.checked))
                        {
                            if (resources > 0)
                                fm.submit.disabled = false;
                            else
                                fm.submit.disabled = true;
                        }
                        else
                        {
                            fm.submit.disabled = true;
                        }
                    }
                }

                function PreSubmit()
                {
                   confirmed = confirm('Are you sure you want to change the permission(s) for the following resource(s)?\n\n'+getSelected(document.getElementById('resultsTable')).substring(1,1000));
                    if (!confirmed)
                        return false;

                    var selObj = document.getElementById("usernames");
                    if (selObj.tagName =='INPUT')
                    {
                        document.getElementById("userarray").value = selObj.value;
                    }
                    else
                    {
                        var usernames0 = '';
                        for (i=0; i<selObj.options.length; i++)
                        {
                            if (selObj.options[i].selected)
                            {
                                usernames0 = usernames0 + selObj.options[i].text + ',';
                            }
                        }
                        document.getElementById("userarray").value = usernames0;
                    }

                    selObj = document.getElementById("groupnames");
                    if (selObj.tagName =='INPUT')
                    {
                        document.getElementById("grouparray").value = selObj.value;
                    }
                    else
                    {
                        var groupnames0 = '';
                        for (i=0; i<selObj.options.length; i++)
                        {
                            if (selObj.options[i].selected)
                            {
                                groupnames0 = groupnames0 + selObj.options[i].text + ',';
                            }
                        }
                        document.getElementById("grouparray").value = groupnames0;
                    }
                }

                function toggleElement()
                {
                     obj = document.getElementById('NameArea');
                     obj1 = document.getElementById('NameAreaHdr');
                     if (obj)
                     {
                        if (obj.style.visibility == 'visible')
                        {
                          obj.style.display = 'none';
                          obj.style.visibility = 'hidden';
                          if (obj1)
                                obj1.innerText = 'Click here to view resources.';
                        }
                        else
                        {
                          obj.style.display = 'inline';
                          obj.style.visibility = 'visible';
                          if (obj1)
                                obj1.innerText = 'Click here to hide this section.';
                        }
                     }
                }

                function onLoad()
                {
                    initSelection('resultsTable');

                    selectAll(true);

                    obj = document.getElementById('NameArea');
                    if (obj)
                    {
                        obj.style.display = 'none';
                        obj.style.visibility = 'hidden';
                        obj1 = document.getElementById('NameAreaHdr');
                        if (obj1)
                            obj1.innerText = 'Click here to view resources.';
                    }
                }

            ]]></xsl:text>
            </script>

        </head>
    <body class="yui-skin-sam" onload="nof5();onLoad()">
            <xsl:apply-templates/>
        </body>
        </html>
    </xsl:template>

    <xsl:template match="PermissionsResetInputResponse">
        <form id="permissionform" method="POST" action="/ws_access/PermissionsReset">
        <input type="hidden" name="basedn" value="{basedn}"/>
        <input type="hidden" name="rname" value="{rname}"/>
        <input type="hidden" name="rtype" value="{rtype}"/>
        <input type="hidden" name="rtitle" value="{rtitle}"/>
        <input type="hidden" name="prefix" value="{prefix}"/>
        <input type="hidden" name="BasednName" value="{BasednName}"/>
        <input type="hidden" id="userarray" name="userarray" value=""/>
        <input type="hidden" id="grouparray" name="grouparray" value=""/>

        <h3>Permission Reset</h3>
        <div>
            <div>
                <A href="javascript:void(0)" onclick="toggleElement();" class="wusectionexpand">
                    <div id="NameAreaHdr">
                        Click here to view resources.
                    </div>
                </A>
            </div>
            <div id="NameArea">
                <xsl:attribute name="style">display:none</xsl:attribute>
                <table class="sort-table" id="resultsTable">
                    <colgroup>
                      <col width="5"/>
                      <col/>
                    </colgroup>
                    <thead>
                      <tr class="grey">
                         <th id="selectAll1">
                            <xsl:if test="Resources/Resource[2]">
                                <input type="checkbox" id="SelectAllBtn" title="Select or deselect all" onclick="selectAll(this.checked)"/>
                            </xsl:if>
                         </th>
                         <th align="left">Resource</th>
                      </tr>
                    </thead>
                    <tbody>
                        <xsl:apply-templates select="Resources/Resource">
                            <xsl:sort select="Resource"/>
                        </xsl:apply-templates>
                    </tbody>
                </table>
            </div>
        </div>
        <table style="text-align:left;" cellspacing="10" name="table1">
            <tr>
                <td height="10"></td>
            </tr>

            <tr>
                <th>allow:</th>
                <td>
                    <table>
                        <tr>
                        </tr>
                        <tr>
                            <td width="50">access</td>
                            <td width="50">read</td>
                            <td width="50">write</td>
                            <td width="50">full</td>
                        </tr>
                        <tr>
                            <td width="50"><input type="checkbox" name="allow_access" value="1" onClick="permChange(this.form,this);validateInput()"/></td>
                            <td width="50"><input type="checkbox" name="allow_read" value="1" onClick="permChange(this.form,this);validateInput()"/></td>
                            <td width="50"><input type="checkbox" name="allow_write" value="1" onClick="permChange(this.form,this);validateInput()"/></td>
                            <td width="50"><input type="checkbox" name="allow_full" value="1" onClick="permChange(this.form,this);validateInput()"/></td>
                        </tr>
                    </table>
                </td>
            </tr>

            <tr>
                <th>deny:</th>
                <td>
                    <table>
                        <tr>
                        </tr>
                        <tr>
                            <td width="50">access</td>
                            <td width="50">read</td>
                            <td width="50">write</td>
                            <td width="50">full</td>
                        </tr>
                        <tr>
                            <td width="50"><input type="checkbox" name="deny_access" value="1" onClick="permChange(this.form,this);validateInput()"/></td>
                            <td width="50"><input type="checkbox" name="deny_read" value="1" onClick="permChange(this.form,this);validateInput()"/></td>
                            <td width="50"><input type="checkbox" name="deny_write" value="1" onClick="permChange(this.form,this);validateInput()"/></td>
                            <td width="50"><input type="checkbox" name="deny_full" value="1" onClick="permChange(this.form,this);validateInput()"/></td>
                        </tr>
                    </table>
                </td>
            </tr>

            <tr>
                <td height="10"></td>
            </tr>

            <tr>
                <xsl:choose>
                    <xsl:when test="toomany=1">
                        <th>User:</th>
                        <td>
                            <input size="20" name="usernames"/>
                        </td>
                    </xsl:when>
                    <xsl:when test="not(Users/User[4])">
                        <th>Select User:</th>
                        <td>
                        <select size="3" id="usernames" name="usernames" multiple="multiple" onChange="validateInput()">
                            <xsl:apply-templates select="Users"/>
                        </select>
                        </td>
                    </xsl:when>
                    <xsl:otherwise>
                        <th>Select user:</th>
                        <td>
                        <select size="5" id="usernames" name="usernames" multiple="multiple" onChange="validateInput()">
                            <xsl:apply-templates select="Users"/>
                        </select>
                        </td>
                    </xsl:otherwise>
                </xsl:choose>
            </tr>

            <tr>
                <td height="10"></td>
            </tr>

            <tr>
                <th>Select Group:</th>
                <td>
                    <xsl:choose>
                        <xsl:when test="not(Groups/Group[4])">
                            <select size="3" id="groupnames" name="groupnames" multiple="multiple" onChange="validateInput()">
                                <xsl:apply-templates select="Groups"/>
                            </select>
                        </xsl:when>
                        <xsl:otherwise>
                            <select size="5" id="groupnames" name="groupnames" multiple="multiple" onChange="validateInput()">
                                <xsl:apply-templates select="Groups"/>
                            </select>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>

            <tr>
                <td></td>
                <td><input type="submit" class="sbutton" value="  Submit  " name="submit" disabled="true" onclick="return PreSubmit()"/></td>
            </tr>
            </table>
        </form>
    </xsl:template>

    <xsl:template match="Users">
        <option value="">(none)</option>
        <xsl:apply-templates select="User">
            <xsl:sort select="username"/>
        </xsl:apply-templates>
    </xsl:template>

    <xsl:template match="User">
        <option value="{username}"><xsl:value-of select="username"/></option>
    </xsl:template>

    <xsl:template match="Groups">
        <option value="">(none)</option>
        <xsl:apply-templates select="Group">
            <xsl:sort select="name"/>
        </xsl:apply-templates>
    </xsl:template>

    <xsl:template match="Group">
        <option value="{name}"><xsl:value-of select="name"/></option>
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
        <input type="checkbox" name="names_i{position()}" value="{.}" onclick="return clicked(this)"/>
        </td>
        <td align="left">
                <xsl:value-of select="."/>
        </td>
        </tr>
  </xsl:template>

    <xsl:template match="*|@*|text()"/>
</xsl:stylesheet>
