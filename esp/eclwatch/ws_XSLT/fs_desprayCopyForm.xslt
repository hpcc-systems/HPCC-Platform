<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
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
    <xsl:param name="fullHtml" select="1"/>
    <xsl:param name="includeFormTag" select="1"/>
    <xsl:param name="method" select="'Despray'"/>
    <xsl:param name="sourceLogicalName" select="''"/>
    <xsl:param name="compressflag" select="''"/>
    <xsl:param name="supercopyflag" select="''"/>
    <xsl:template match="/Environment">
    <xsl:choose>
      <xsl:when test="ErrorMessage">
        <h4>
          <xsl:value-of select="$method"/> File: 
          <xsl:value-of select="ErrorMessage"/>
        </h4>
      </xsl:when>
      <xsl:otherwise>
        <xsl:choose>
            <xsl:when test="$fullHtml">
                <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
                    <head>
                        <title>Spray / Despray result</title>
            <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
            <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
            <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
            <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
          </head>
          <body class="yui-skin-sam" onload="nof5();onChangeMachine(true)">
                        <form method="POST" action="/FileSpray/{$method}">
                            <xsl:call-template name="generateForm"/>
                        </form>
                    </body>
                </html>
            </xsl:when>
            <xsl:otherwise>
                <xsl:choose>
                    <xsl:when test="$includeFormTag">
                        <form method="POST" action="/FileSpray/{$method}">
                            <xsl:call-template name="generateForm"/>
                        </form>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:call-template name="generateForm"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:otherwise>
        </xsl:choose>
      </xsl:otherwise>
    </xsl:choose>
    </xsl:template>
    <xsl:template name="generateForm">
        <script type="text/javascript" language="javascript">
            method = '<xsl:value-of select="$method"/>';
            <![CDATA[
                var firsttime = true;
                var labelChanged = false;
                var sourceOS = -1;
                function onChangeMachine(resetPath)
                {
                    if (method == 'Despray')
                    {
                        machineDropDown = document.getElementById('machine');
                        if (machineDropDown.selectedIndex >=0)
                        {
                            selected=machineDropDown.options[machineDropDown.selectedIndex]
              pos = selected.title.indexOf(';');
              path = directory = selected.title.substring(0, pos);
              linux = selected.title.substring(pos+1);

              document.getElementById('destIP').value=selected.value;
                          sourceOS = linux == 'true' ? 1: 0;
                            if (resetPath)
                            {
                                //if (firsttime == true && document.getElementById('destPath').value.length > 1)
                                if (firsttime == true && document.getElementById('sourceLogicalName').value.length > 1)
                                {
                                    //path = document.getElementById('destPath').value;
                                    path = document.getElementById('sourceLogicalName').value;
                                    pt = path.indexOf("::");
                                    while (pt != -1)
                                    {
                                        path = path.substring(pt + 2, path.length);
                                        pt = path.indexOf("::");
                                    }
                                
                                    //directory = selected.directory;
                                    pathSep = linux == '' ? '\\' : '/';

                                    //prefix = directory.length > 0 && (directory.substring(0,1) == pathSep) ? '' : pathSep;
                                    prefix = pathSep;
                                    if (directory.length > 0)
                                    {
                                        pt = directory.indexOf(":");
                                        if (directory.substring(0,1) == pathSep || pt != -1)
                                        {
                                            prefix = '';
                                        }
                                    }
                                    directory = prefix + directory;

                                    if ((directory.substring(directory.length -1) == pathSep) && (path.substring(0,1) == pathSep))
                                        document.getElementById('destPath').value = directory + path.substring(1);
                                    else if ((directory.substring(directory.length -1) == pathSep) || (path.substring(0,1) == pathSep))
                                        document.getElementById('destPath').value = directory + path;
                                    else
                                        document.getElementById('destPath').value = directory + pathSep + path;
                                }
                                else if (document.getElementById('sourceLogicalName').value.length > 1)
                                {
                                    path = document.getElementById('sourceLogicalName').value;
                                    pt = path.indexOf("::");
                                    while (pt != -1)
                                    {
                                        path = path.substring(pt + 2, path.length);
                                        pt = path.indexOf("::");
                                    }

                                    //directory = selected.directory;
                                    if (directory.charAt(0) == '\\' && directory.indexOf(':') != -1)
                                        directory = path.substring(1, directory.length);
                                    directory = linux == '' ? directory.replace(/\$/, ':') : directory;
                                    pathSep = linux == '' ? '\\' : '/';
                                    if ((directory.substring(directory.length -1) == pathSep) && (path.substring(0,1) == pathSep))
                                        document.getElementById('destPath').value = directory + path.substring(1);
                                    else if ((directory.substring(directory.length -1) == pathSep) || (path.substring(0,1) == pathSep))
                                        document.getElementById('destPath').value = directory + path;
                                    else
                                        document.getElementById('destPath').value = directory + pathSep + path;
                                }
                                else
                                {
                                    //path = selected.directory;              
                                    if (path.charAt(0) == '\\' && path.indexOf(':') != -1)
                                        path = path.substring(1, path.length);
                                    document.getElementById('destPath').value = linux == '' ? path.replace(/\$/, ':') : path;
                                }
                                firsttime = false;
                            }
                            else
                                path = document.getElementById('destPath').value;
                            path = path.replace(/:/, '$'); //to be appended to network path
                            var sourceObj = document.getElementById('NetworkPath');
                            if (linux == '')//windows box
                            {
                                if (path.length > 0 && (path.substring(0,1) == '\\' || path.substring(0,1) == '/'))
                                    sourceObj.innerHTML='\\\\' + selected.value + path;
                                else
                                    sourceObj.innerHTML='\\\\' + selected.value + '\\' + path;
                            }
                            else
                            {
                                if(path.length > 0 && (path.substring(0,1) == '\\' || path.substring(0,1) == '/'))
                                    sourceObj.innerHTML='//' + selected.value + path;
                                else
                                    sourceObj.innerHTML='//' + selected.value + '/' + path;
                            }
                        }
                    }
                    else
                    {
                        onChangeGroup();
                        if (document.forms[0].label.value.length > 0)
                            onChangeLabel(document.forms[0].label)
                    }
                    handleSubmitBtn();
                }
                
                /*function onChangeReplicate()
                {
                    var enabled = document.getElementById("replicate").checked && document.getElementById("destGroupRoxie").value == 'Yes';
                    document.getElementById("ReplicateOffset").disabled = !enabled;
                }*/

                function onChangeLabel(o)
                {
                    document.getElementById('destLogicalName').value = o.value;
                    document.getElementById('mask').innerHTML = o.value+'._$P$_of_$N$';
                    handleSubmitBtn();
                }
                
                function onLabelKeyUp(o)
                {
                    labelChanged = true;
                }               

                function onChangeGroup()
                {
                    var groups = document.getElementById("destGroup");
                    var group = groups.options[groups.selectedIndex];
                    if (group.label0.substring(0,1) == 'T')
                        setThorGroup();
                    else
                        setRoxieGroup();
                }
                
                function setThorGroup()             
                {
                    document.getElementById('destGroupRoxie').value='No';
                    document.getElementById('roxie_span').style.display='none';
                    //document.getElementById('Servers').checked=false;
                    //document.getElementById('Wrap').checked=false;
                    document.getElementById('Multicopy').checked=false;
                    //document.getElementById('ReplicateOffset').value='';
                    document.getElementById('SourceDiffKeyName').value='';
                    document.getElementById('DestDiffKeyName').value='';
                    document.getElementById('Advanced').value='Advanced >>';
                    document.getElementById('option_span').style.display='none';
    
                    //Disable Roxie replicate for now
                    document.getElementById('replicate').disabled = false;
                    //document.getElementById('ReplicateOffset').disabled = false;
                    document.getElementById('wrap').checked = false;
                    document.getElementById('wrap').disabled = false;
                }
                
                function setRoxieGroup()
                {
                    document.getElementById('destGroupRoxie').value='Yes';
                    document.getElementById('roxie_span').style.display='block';
    
                    //Disable Roxie replicate for now
                    document.getElementById('replicate').disabled = true;
                    //document.getElementById('ReplicateOffset').disabled = true;
                    document.getElementById('wrap').checked = true;
                    document.getElementById('wrap').disabled = true;
                }

                function show_hide(obj_name)
                {
                    if (obj_name.style.display == 'block')
                    {
                        obj_name.style.display='none';
                        return ">>";
                    }
                    else
                    {
                        obj_name.style.display='block';
                        return "<<";
                    }
                }
                
                function make_null(check_null, obj_name)
                {
                    if (obj_name.disabled)
                    {
                        obj_name.disabled = false;
                        obj_name.value = "";
                        check_null.value = false;
                    }
                    else
                    {
                        obj_name.disabled = true;
                        obj_name.value = "null";
                        check_null.value = true;
                    }
                }

                function updateLabel()
                {   
                    if (!labelChanged)
                    {
                        document.getElementById('label').value = document.getElementById('sourceLogicalName').value;
                    }           
                    onChangeLabel(document.forms[0].label); 
                }
            
                function handleSubmitBtn()
                {                   
                    disable = document.getElementById('sourceLogicalName').value == '';
                    if (!disable)
                        if (method == 'Despray')
                            disable =  document.getElementById('destIP').value == '' || document.getElementById('destPath').value == '';
                        else
                            disable =  document.getElementById('label').value == '' || document.getElementById('destLogicalName').value == '';

                    document.getElementById('submitBtn').readonly = disable;
                }

                var pathonly;
                var filename;
                
                function convertWindowsPathString(oldpath)
                {
                    if (oldpath.length < 3)
                        return oldpath;

                    var s0, s1, s2, path0 = '';
                    for (i=0; i < oldpath.length - 2; i++)
                    {
                        s0 = oldpath.charAt(i);
                        s1 = oldpath.charAt(i+1);
                        s2 = oldpath.charAt(i+2);
                        if (s0 == '%' && s1 == '3' && s2 == 'A')
                        {
                            path0 += ':';
                            i += 2;
                        }
                        else if (s0 == '%' && s1 == '5' && s2 == 'C')
                        {
                            path0 += '\\';
                            i += 2;
                        }
                        else
                        {
                            path0 += s0;
                            if (i == oldpath.length - 3)
                            {
                                path0 += s1;
                                path0 += s2;
                            }
                        }
                    }

                    return path0;
                }

                function popup(ip, path)
                {
                    selected=machineDropDown.options[machineDropDown.selectedIndex];               
                  var pathSep = '/';
          pos = selected.title.indexOf(';');
          linux = selected.title.substring(pos+1);
                    if (linux == '')
                    {
                        pathSep = '\\';
                        path = convertWindowsPathString(path);
                    } 

                    if (path.charAt(path.length -1) == pathSep)
                    {
                        pathonly = path;
                        filename = '';
                    }
                    else
                    {
                       var sep_loc=-1;
                       for (i=path.length-1; i > -1; i--)
                       {
                            if (path.charAt(i) == pathSep)
                            {
                                sep_loc = i;
                                break;
                            }
                        }

                        if (sep_loc < 0)
                        {
                            pathonly = path;
                            filename = '';
                        }
                        else
                        {
                            pathonly = '';
                            filename = '';
                           for (i=0; i < sep_loc+1; i++)
                           {
                                pathonly += path.charAt(i);
                            }
                            
                           for (i=sep_loc+1; i < path.length; i++)
                           {
                                filename += path.charAt(i);
                            }
                        }
                    }

                   if (sourceOS > -1 && ip != '')
                   {
                        mywindow = window.open ("/FileSpray/FileList?DirectoryOnly=true&Netaddr="+ip+"&OS="+sourceOS+"&Path="+pathonly, "mywindow", 
                                                        "location=0,status=1,scrollbars=1,resizable=1,width=500,height=600");
                        if (mywindow.opener == null)
                            mywindow.opener = window;
                        mywindow.focus();
                    }
                } 

                //note that the following function gets invoked from the file selection window
                //
                function setSelectedPath(path)
                {
                   machineDropDown = document.forms[0].machine;
                   if (machineDropDown.selectedIndex >=0)
                   {
                       selected=machineDropDown.options[machineDropDown.selectedIndex];   
             pos = selected.title.indexOf(';');
             linux = selected.title.substring(pos+1);
                       if (linux != '')
                           document.forms[0].destPath.value = path + '/' + filename;
                       else
                       {
                           var s='';
                           for (i=0; i<path.length; i++)
                           {
                                if (path.charAt(i) == '/')
                                    s += '\\';
                                else
                                    s += path.charAt(i);
                           }
                           document.forms[0].destPath.value = s;
                       }

                       var pathSep = linux == '' ? '\\' : '/';
                       var prefix = path.length > 0 && (path.substring(0,1) == pathSep) ? '' : pathSep;
                       document.getElementById('NetworkPath').innerHTML = pathSep + pathSep + selected.value + prefix + path + '/' + filename;
                       handleSubmitBtn();
                   }
                }
            ]]>
        </script>
        <table name="table1">
            <tr>
                <th colspan="2">
                    <h3>
                        <xsl:value-of select="$method"/> File</h3>
                </th>
            </tr>
            <tr>
                <td height="10"/>
            </tr>
            <tr>
                <td colspan="2">
                    <b>Source</b>
                </td>
            </tr>
            <xsl:variable name="srcname">
                <xsl:choose>
                    <xsl:when test="$sourceLogicalName = ''">
                        <xsl:value-of select="Software/DfuWorkunit/Source/OrigName"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="$sourceLogicalName"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <tr>
                <td>Logical File:</td>
                <td>
                    <xsl:choose>
                        <xsl:when test="$srcname=''">
                            <input type="text" id="sourceLogicalName" name="sourceLogicalName" size="70" value="{$srcname}" onKeyUp="updateLabel()" onblur="handleSubmitBtn()" onchange="handleSubmitBtn()"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="$srcname"/>
                            <input type="hidden" id="sourceLogicalName" name="sourceLogicalName" value="{$srcname}"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            <xsl:if test="$method='Copy'">
                <xsl:if test="$sourceLogicalName=''">
                    <tr>
                        <td>Source Dali:</td>
                        <td>
                            <input type="text" id="sourceDali" name="sourceDali" value="{Software/DfuWorkunit/Source/Attr/@foreignDali}" size="35" 
                                onblur="handleSubmitBtn()" onchange="handleSubmitBtn()"/>
                        </td>
                    </tr>
                    <tr>
                        <td>Source Username:</td>
                        <td>
                            <input type="text" id="srcusername" name="srcusername" size="35" value="{Software/DfuWorkunit/Source/Attr/@foreignUser}" 
                                onblur="handleSubmitBtn()" onchange="handleSubmitBtn()"/>
                        </td>
                    </tr>
                    <tr>
                        <td>Source Password:</td>
                        <td>
                            <input type="password" id="srcpassword" name="srcpassword" size="37" onblur="handleSubmitBtn()" onchange="handleSubmitBtn()"/>
                        </td>
                    </tr>
                </xsl:if>
            </xsl:if>
            <tr>
                <td height="10"/>
            </tr>
            <tr>
                <td colspan="2">
                    <b>Destination</b>
                </td>
            </tr>
            <xsl:choose>
                <xsl:when test="$method='Despray'">
                    <tr>
                        <td>Machine/dropzone:</td>
                        <td>
                            <xsl:variable name="dstip" select="Software/DfuWorkunit/Destination/Part/@node"/>
                            <select name="machine" id="machine" onchange="onChangeMachine(true)">
                                <xsl:for-each select="Software/DropZone">
                                    <option>
                                        <xsl:variable name="curip" select="@netAddress"/>
                                        <xsl:attribute name="value"><xsl:value-of select="@netAddress"/></xsl:attribute>
                                        <xsl:attribute name="title">
                      <xsl:value-of select="@directory"/>;<xsl:value-of select="@linux"/>
                    </xsl:attribute>
                                        <xsl:if test="$dstip = $curip">
                                            <xsl:attribute name="selected"/>
                                        </xsl:if>
                                        <xsl:value-of select="@computer"/>/<xsl:value-of select="@name"/>
                                    </option>
                                </xsl:for-each>
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td>IP Address:</td>
                        <td>
                            <input type="text" id="destIP" name="destIP" size="35"/>
                        </td>
                    </tr>
                    <tr>
                        <td>Local Path:</td>
                        <td>
                            <xsl:variable name="partmask0" select="Software/DfuWorkunit/Destination/@partmask"/>
                            <!--xsl:variable name="directory0" select="Software/DfuWorkunit/Destination/@directory"/-->
                            <xsl:choose>
                                <xsl:when test="string-length($partmask0)">
                                    <input type="text" name="destPath" id="destPath" size="70" 
                                        value="{$partmask0}" 
                                        onblur="onChangeMachine(false)" onchange="onChangeMachine(false)"/>
                                </xsl:when>
                                <xsl:when test="string-length($srcname)">
                                    <input type="text" name="destPath" id="destPath" size="70" 
                                        value="{$srcname}" 
                                        onblur="onChangeMachine(false)" onchange="onChangeMachine(false)"/>
                                </xsl:when>
                                <xsl:otherwise>
                                    <input type="text" name="destPath" id="destPath" size="70" 
                                        value="default_despray_file" 
                                        onblur="onChangeMachine(false)" onchange="onChangeMachine(false)"/>
                                </xsl:otherwise>
                            </xsl:choose>
                           <input type="button" name="Choose Path" value="Choose Path" onclick="popup(destIP.value, escape(destPath.value))"/>
                        </td>
                    </tr>
                    <tr>
                        <td>Network Path:</td>
                    <td id="NetworkPath" name="NetworkPath"/>
                    </tr>
                    <tr>
                      <td>Split Prefix:</td>
                      <td>
                        <input type="text" name="splitprefix" size="30" value=""/>
                      </td>
                    </tr>
                    <tr>
                        <td>Use Single Connection:</td>
                        <td>
                            <input type="checkbox" name="SingleConnection"/>
                        </td>
                    </tr>
                </xsl:when>
                <xsl:otherwise>
                    <tr>
                        <td>Group:</td>
                        <td>
                            <input type="hidden" id="destGroupRoxie" name="destGroupRoxie"/>
                            <select name="destGroup" id="destGroup" onkeyup="onChangeGroup()" onchange="onChangeGroup()">
                                <xsl:variable name="grp" select="Software/DfuWorkunit/Destination/@group"/>
                                <optgroup label="Thor">
                                    <xsl:for-each select="Software/ThorCluster">
                                        <option>
                                            <xsl:attribute name="label0"><xsl:text>T</xsl:text><xsl:value-of select="@name"/></xsl:attribute>
                                            <xsl:attribute name="value"><xsl:value-of select="@name"/></xsl:attribute>
                                            <xsl:variable name="curgrp" select="@name"/>
                                            <xsl:if test="$grp = $curgrp">
                                                <xsl:attribute name="selected"/>
                                            </xsl:if>
                                            <xsl:value-of select="@name"/>
                                        </option>
                                    </xsl:for-each>
                                    <xsl:for-each select="Software/EclAgentProcess/Instance">
                                        <option>
                                            <xsl:attribute name="label0"><xsl:text>T</xsl:text><xsl:value-of select="../@name"/></xsl:attribute>
                                            <xsl:attribute name="value"><xsl:value-of select="@netAddress"/></xsl:attribute>
                                            <xsl:value-of select="../@name"/>
                                            <xsl:text> </xsl:text>
                                            <xsl:value-of select="@netAddress"/>
                                        </option>
                                    </xsl:for-each>
                                </optgroup>
                                <xsl:if test="Software/RoxieCluster[1]">
                                    <optgroup label="Roxie">
                                        <xsl:for-each select="Software/RoxieCluster">
                                            <option>
                                                <xsl:attribute name="label0"><xsl:text>R</xsl:text><xsl:value-of select="@name"/></xsl:attribute>
                                                <xsl:attribute name="value"><xsl:value-of select="@name"/></xsl:attribute>
                                                <xsl:variable name="curgrp1" select="@name"/>
                                                <xsl:if test="$grp = $curgrp1">
                                                    <xsl:attribute name="selected"/>
                                                </xsl:if>
                                                <xsl:value-of select="@name"/>
                                            </option>
                                        </xsl:for-each>
                                    </optgroup>
                                </xsl:if>
                            </select>
                        </td>
                    </tr>
                    <xsl:variable name="origname" select="Software/DfuWorkunit/Destination/OrigName"/>
                    <tr>
                        <td>Logical Name:</td>
                        <td>
                            <xsl:choose>
                                <xsl:when test="$srcname=''">
                                    <input type="text" id="label" name="label" size="70" value="{$origname}" onchange="onChangeLabel(this)" onblur="onChangeLabel(this)" onKeyUp="onLabelKeyUp(this)"/>
                                </xsl:when>
                                <xsl:otherwise>
                                    <input type="text" id="label" name="label" size="70" value="{$srcname}" onchange="onChangeLabel(this)" onblur="onChangeLabel(this)" onKeyUp="onLabelKeyUp(this)"/>
                                </xsl:otherwise>
                            </xsl:choose>
                            <input type="hidden" name="destLogicalName" id="destLogicalName" size="70" value="{$origname}"/>
                        </td>
                    </tr>
                    <tr>
                        <td>Mask:</td>
                        <td id="mask">
                            <xsl:value-of select="$origname"/>._$P$_of_$N$</td>
                    </tr>
                    <tr>
                        <td height="10"/>
                    </tr>
                </xsl:otherwise>
            </xsl:choose>
            <xsl:if test="$method='Copy'">
                <tr>
                    <td height="10"/>
                </tr>
                <tr>
                    <td colspan="2">
                        <b>Options</b>
                    </td>
                </tr>
                <tr>
                    <td>Replicate:</td>
                    <td>
                        <input type="checkbox" name="replicate" id="replicate" checked="true"/>
                    </td>
                </tr>
                <tr>
                    <td>Wrap:</td>
                    <td>
                        <input type="checkbox" name="Wrap" id="Wrap" checked="true"/>
                    </td>
                </tr>
                <tr>
                  <td>No Split:</td>
                  <td>
                    <input type="checkbox" name="nosplit"/>
                  </td>
                </tr>
            </xsl:if>
            <tr>
                <td>Overwrite:</td>
                <td>
                    <input type="checkbox" name="overwrite"/>
                </td>
            </tr>
            <xsl:if test="$method='Copy'">
                <tr>
                    <td>Compress:</td>
                    <td>
                        <xsl:choose>
                            <xsl:when test="$compressflag &gt; 1">
                                <input type="checkbox" name="compress" value="1" checked="true"/>
                            </xsl:when>
                            <xsl:when test="$compressflag &gt; 0">
                                <input type="checkbox" name="compress" value="1"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <input type="checkbox" name="compress" value="1" disabled="true"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </td>
                </tr>
            </xsl:if>
        </table>
        <xsl:if test="$method='Copy'">
            <tr>
                <td>Retain Superfile Structure:</td>
                <td>
                    <xsl:choose>
                        <xsl:when test="$supercopyflag &gt; 1">
                            <input type="checkbox" name="superCopy" value="1" checked="true"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" name="superCopy" value="0" disabled="false"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            <span id="roxie_span" style="display:none">
                <br/>
                <table>
                    <tr>
                        <td>
                            <input type="button" id="Advanced" name="Advanced" value="Advanced >>" onclick="value = 'Advanced ' + show_hide(document.getElementById('option_span'));"/>
                            <span id="option_span" style="display:none">
                                <table>
                                    <!--tr>
                                        <td>Use Roxie Servers:</td>
                                        <td>
                                            <input type="checkbox" name="Servers" id="Servers"/>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td>Wrapped:</td>
                                        <td>
                                            <input type="checkbox" name="Wrap" id="Wrap"/>
                                        </td>
                                    </tr-->
                                    <tr>
                                        <td>Multicopy:</td>
                                        <td>
                                            <input type="checkbox" id="Multicopy" name="Multicopy"/>
                                        </td>
                                    </tr>
                                    <!--tr>
                                        <td>Replicate Offset:</td>
                                        <td>
                                            <input type="text" id="ReplicateOffset" name="ReplicateOffset" size="5" value=""/>
                                        </td>
                                    </tr-->
                                    <tr>
                                        <td>Source Diff/patch Key:</td>
                                        <td>
                                            <input type="text" id="SourceDiffKeyName" name="SourceDiffKeyName" size="70" value=""/>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td>Destination Diff/patch Key:</td>
                                        <td>
                                            <input type="text" id="DestDiffKeyName" name="DestDiffKeyName" size="70" value=""/>
                                        </td>
                                    </tr>
                                </table>
                            </span>
                        </td>
                    </tr>
                </table>
                <br/>
            </span>
        </xsl:if>
        <xsl:if test="$fullHtml='1'">
            <table>
                <tr>
                    <td/>
                    <td>
                        <input type="submit" id="submitBtn" name="submitBtn" value="Submit" readonly="true"/>
                    </td>
                </tr>
            </table>
        </xsl:if>
    </xsl:template>
</xsl:stylesheet>
