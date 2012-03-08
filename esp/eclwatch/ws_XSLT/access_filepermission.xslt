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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
<xsl:output method="html"/>
    <xsl:output method="html"/>
    <xsl:variable name="usernameselected" select="FilePermissionResponse/UserName"/>
  <xsl:variable name="groupnameselected" select="FilePermissionResponse/GroupName"/>
  <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Check File Permission</title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <script language="JavaScript1.2">
            <xsl:text disable-output-escaping="yes"><![CDATA[
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

                function validateInput(id)
                {
                    fm = document.forms[0];
                    //if((fm.user.value != '' || fm.group.value != '') &&                               (fm.allow_read.checked || fm.allow_write.checked || fm.allow_full.checked
                    //   || fm.deny_read.checked || fm.deny_write.checked || fm.deny_full.checked || fm.allow_access.checked || fm.deny_access.checked))
                    if (id == 1 && fm.UserName.value != '')
                        fm.GroupName.value = '';
                    if (id == 2 && fm.GroupName.value != '')
                        fm.UserName.value = '';

                    if((fm.UserName.value != '' || fm.GroupName.value != '') && (fm.FileName.value != ''))
                    {
                        fm.submit.disabled = false;
                    }
                    else
                    {
                        fm.submit.disabled = true;
                    }
                }

            ]]></xsl:text>
            </script>

        </head>
    <body class="yui-skin-sam" onload="nof5();">
            <xsl:apply-templates/>
        </body>
        </html>
    </xsl:template>

    <xsl:template match="FilePermissionResponse">
        <form id="permissionform" method="POST" action="/ws_access/FilePermission">
        <table style="text-align:left;" cellspacing="10" name="table1">
            <tr>
                <th colspan="2">
                    <h3>Check File Permission</h3>
                </th>
            </tr>
            <tr>
                <td height="5"></td>
            </tr>
            <tr>
                <th>File Name:</th>
                <td>
                <input type="text" size="50" name="FileName" value="{FileName}" onKeyUp="validateInput(0)"/>
                </td>
            </tr>
            <tr>
                <xsl:choose>
                <xsl:when test="toomany=1">
                    <th>User:</th>
                    <td>
                    <input size="20" name="UserName" onKeyUp="validateInput(1)"/>
                    </td>
                </xsl:when>
                <xsl:otherwise>
                    <th>Select User:</th>
                    <td>
                    <select size="1" name="UserName" onChange="validateInput(1)">
                        <option value="">none</option>
                        <xsl:apply-templates select="Users"/>
                    </select>
                    </td>
                </xsl:otherwise>
                </xsl:choose>
            </tr>
            <tr>
                <th>Or group:</th>
                <td>
                <select size="1" name="GroupName" onChange="validateInput(2)">
                    <option value="">none</option>
                    <xsl:apply-templates select="Groups"/>
                </select>
                </td>
            </tr>

            <!--tr>
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
            </tr-->

            <xsl:if test="string-length(UserPermission)">
                <tr>
                    <td height="5"></td>
                </tr>
                <tr><th>Permission:</th><td><xsl:value-of select="UserPermission"/></td></tr>
         </xsl:if>

            <tr>
                <td height="5"></td>
            </tr>

            <tr>
                <td></td>
                <td>
                <input type="submit" class="sbutton" value="  Check  " name="submit" disabled="true"/>
                </td>
            </tr>
            </table>
        </form>
    </xsl:template>

    <xsl:template match="Users">
        <xsl:apply-templates select="User">
            <xsl:sort select="username"/>
        </xsl:apply-templates>
    </xsl:template>

    <xsl:template match="User">
        <xsl:choose>
            <xsl:when test="$usernameselected=username">
                        <option value="{username}" selected="selected"><xsl:value-of select="username"/></option>
            </xsl:when>
            <xsl:otherwise>
                        <option value="{username}"><xsl:value-of select="username"/></option>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="Groups">
        <xsl:apply-templates select="Group">
            <xsl:sort select="name"/>
        </xsl:apply-templates>
    </xsl:template>

    <xsl:template match="Group">
    <!--option value="{name}"><xsl:value-of select="name"/></option-->
    <xsl:choose>
      <xsl:when test="$groupnameselected=name">
        <option value="{name}" selected="selected">
          <xsl:value-of select="name"/>
        </option>
      </xsl:when>
      <xsl:otherwise>
        <option value="{name}">
          <xsl:value-of select="name"/>
        </option>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>
