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
    <xsl:template match="WUQuery">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>EclWatch</title>
                <!--style type="text/css">
                body { background-color: #white;}
                table { padding:3px; }
                .number { text-align:right; }
                .sbutton { width: 5em; font: 10pt arial , helvetica, sans-serif; }
                </style-->
                <script language="JavaScript1.2">
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                    var rbid = 1;
                    function onRBChanged(id)
                    {
                        rbid = id;
                        if (id > 0)
                        {
                            document.getElementById("From").disabled = true;
                            document.getElementById("From").value = '';
                            document.getElementById("To").disabled = true;
                            document.getElementById("To").value = '';
                            document.getElementById("Range").disabled = false;
                        }
                        else
                        {
                            document.getElementById("From").disabled = false;
                            document.getElementById("To").disabled = false;
                            document.getElementById("Range").disabled = true;
                            document.getElementById("Range").value = '';
                        }
                    }
                    function onChangeType()
                    {
            var o = document.getElementById("Type");
            if (!o)
              return;

                        var type = o.selectedIndex;
                        if (type > 0)
                        {
                            //document.getElementById("SashaServer").disabled = false;
                            document.getElementById("Cluster").disabled = true;
                            document.getElementById("ECL").disabled = true;
                            document.getElementById("LogicalFile").disabled = true;
                            document.getElementById("LogicalFileSearchType").disabled = true;
                        }
                        else
                        {
                            //document.getElementById("SashaServer").disabled = true;
                            document.getElementById("Cluster").disabled = false;
                            document.getElementById("ECL").disabled = false;
                            document.getElementById("LogicalFile").disabled = false;
                            document.getElementById("LogicalFileSearchType").disabled = false;
                        }

                        onRBChanged(rbid);
                        document.ECLWUSearchForm.DateRB[rbid].checked = true;
                        var obj = document.getElementById('warningDiv');
                        if (obj)
                        {
                            if (type > 0)
                            {
                                obj.style.display = 'inline';
                                obj.style.visibility = 'visible';
                            }
                            else
                            {
                                obj.style.visibility = "hidden";
                                obj.style.display = "none";
                            }
                        }
                    }
            
                    function format2(n)
                    {
                        return new String(Math.floor(n/10))+ new String(n%10);
                    }

                    function formatDate(d)
                    {
                        var dt=new Date(d);
                        if(isNaN(dt)) return null;

                        return (dt.getFullYear()>1950 ?  dt.getFullYear() : dt.getFullYear()+100)+'-'+
                               format2(dt.getMonth()+1)+'-'+
                               format2(dt.getDate())+'T'+
                               format2(dt.getHours())+':'+
                               format2(dt.getMinutes())+':'+
                               format2(dt.getSeconds())+'Z';
                    }
                    function submit_search()
                    {                       
                        document.getElementById("StartDate").value='';
                        document.getElementById("EndDate").value='';
                                
                                if (rbid > 0)
                                {
                                    if(document.getElementById('Range').value)
                                    {
                                        var d=parseInt(document.getElementById("Range").value);
                                        if(isNaN(d))
                                        {
                                            alert("Invalid data in a 'Date' field");
                                            return false;
                                        }

                                        var cur=new Date();
                                        var startDate=formatDate(new Date(cur.getFullYear(),cur.getMonth(),cur.getDate()-d+1));
                                        if(!startDate) 
                                        {
                                            alert("Invalid data in a 'Date' field");
                                            return false;
                                        }

                                        document.getElementById("EndDate").value=formatDate(cur);
                                        document.getElementById("StartDate").value = startDate;
                                        document.getElementById("LastNDays").value = d;
                                        //document.getElementById("From").value='';
                                        //document.getElementById("To").value='';
                                    }
                                }
                                else
                                {
                                    if(!document.getElementById('From').value && !document.getElementById('To').value)
                                    {
                                        return true;
                                    }

                                    if(document.getElementById('From').value)
                                    {
                                        var d=Date.parse(document.getElementById("From").value);
                                        if(isNaN(d))
                                        {
                                            alert("Invalid data in a 'Date' field");
                                            return false;
                                        }

                                        var df=formatDate(d);
                                        if(!df) 
                                        {
                                            alert("Invalid data in a 'Date' field");
                                            return false;
                                        }

                                        document.getElementById("StartDate").value=df;
                                    }

                                    if(document.getElementById('To').value)
                                    {
                                        var d=Date.parse(document.getElementById("To").value);
                                        if(isNaN(d))
                                        {
                                            alert("Invalid data in a 'Date' field");
                                            return false;
                                        }

                                        if(String(document.getElementById("To").value).indexOf(":")<0)
                                          d+=1000*60*60*24-1;

                                        var dt=formatDate(d);
                                        if(!dt) 
                                        {
                                            alert("Invalid data in a 'Date' field");
                                            return false;
                                        }
                                        document.getElementById("EndDate").value=dt;
                                    }
                                }
                                return true;
                            }
                    function submit_wuid()
                    {
                        var r=String(document.getElementById("Wuid").value).match(/[Ww](\d{8}-\d{6}(-\d+)?)/);
                        if(r && r[1])
                        {
                            document.getElementById("Wuid").value='W'+r[1];
                            return true;
                        }
                        alert('Wrong WUID');
                        return false;
                    }
                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onChangeType()">
        <xsl:choose>
          <xsl:when test="ErrorMessage">
            <h4>Search Workunits:</h4>
            <br/><br/>
            <xsl:value-of select="ErrorMessage"/>
          </xsl:when>
          <xsl:otherwise>
            <h4>Open Workunit:</h4>
                <form action="/WsWorkunits/WUInfo" method="get" onsubmit="return submit_wuid()">
                    <table>
                        <tr>
                            <td>Wuid:</td>
                            <td>
                                <input name="Wuid" size="25" type="text"/>
                            </td>
                            <td>
                                <input type="submit" value="Open" class="sbutton"/>
                            </td>
                        </tr>
                    </table>
                </form>
                <br/><br/>
                <h4>Search Workunits:</h4>
                <form name="ECLWUSearchForm" action="/WsWorkunits/WUQuery"  method="get" onsubmit="return submit_search()">
                    <table>
                        <tr>
                            <td>Type:</td>
                            <td>
                                <select name="Type" id="Type" size="1" onchange="onChangeType()">
                                    <option>non-archived workunits</option>
                                    <option>archived workunits</option>
                                </select>
                            </td>
                        </tr>
                        <!--tr>
                            <td>Sasha Server:</td>
                            <td>
                                <select id="SashaServer" name="SashaServer" size="1">
                                    <option></option>
                                    <xsl:for-each select="SashaServer">
                                        <option>
                                            <xsl:attribute name="sashaaddress"><xsl:value-of select="@Address"/></xsl:attribute>
                                            <xsl:value-of select="."/>
                                        </option>
                                    </xsl:for-each>
                                </select>
                            </td>
                            <input type="hidden" id="SashaNetAddress" name="SashaNetAddress" value=""/>
                        </tr-->
                        <tr>
                            <td>Username:</td>
                            <td>
                                <input name="Owner" size="12" type="text"/>
                            </td>
                        </tr>
                        <tr>
                            <td>Cluster:</td>
                            <td>
                                <!--input name="Cluster" size="12" type="text"/-->
                                <select id="Cluster" name="Cluster" size="1">
                                    <option></option>
                                    <xsl:for-each select="Cluster">
                                        <option>
                                            <xsl:value-of select="."/>
                                        </option>
                                    </xsl:for-each>
                                </select>
                            </td>
                        </tr>
                        <tr>
                            <td>State:</td>
                            <td>
                                <select name="State" size="1">
                                    <option/>
                                    <option>unknown</option>
                  <option>compiled</option>
                                    <option>running</option>
                                    <option>completed</option>
                  <option>aborting</option>
                  <option>aborted</option>
                  <option>blocked</option>
                  <option>submitted</option>
                  <option>scheduled</option>
                  <option>wait</option>
                                    <option>failed</option>
                  <option>compiling</option>
                  <option>uploading_files</option>
                  <option>debugging</option>
                  <option>debug_running</option>
                  <option>paused</option>
                </select>
                            </td>
                        </tr>
                        <tr>
                            <td>ECL text:</td>
                            <td>
                                <input name="ECL" id="ECL" size="12" type="text"/>
                            </td>
                        </tr>
                        <tr>
                            <td>Job Name:</td>
                            <td>
                                <input name="Jobname" size="12" type="text"/>
                            </td>
                        </tr>
                        <tr>
                            <td>Logical File:</td>
                            <td>
                                <input name="LogicalFile" id="LogicalFile" size="24" type="text"/> 
                                <select name="LogicalFileSearchType" id="LogicalFileSearchType" size="1">
                                    <option>Used</option>
                                    <option>Created</option>
                                </select> by the workunit(s)
                            </td>
                        </tr>
                        <tr><td><br/></td></tr>
                        <tr>
                            <td rowspan="3">
                                <table>
                                    <tr>
                                        <td>Date:</td>
                                        <td><input type="radio" name="DateRB" value="0" onclick="onRBChanged(0)"/></td>
                                        <td>From</td>               
                                    </tr>
                                    <tr>
                                        <td></td>
                                        <td></td>
                                        <td>To</td>
                                    </tr>
                                    <tr>
                                        <td></td>
                                        <td><input type="radio" name="DateRB" value="1" checked="checked" onclick="onRBChanged(1)"/></td>
                                        <td>or in the last</td>
                                    </tr>
                                </table>
                            </td>
                            <td><input id="From" size="12" type="text"/> (mm/dd/yyyy hh:mm am/pm)</td>
                        </tr>
                        <tr>
                            <td>
                                <input id="To" size="12" type="text"/> (mm/dd/yyyy hh:mm am/pm)
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <input id="Range" size="5" type="text" style="text-align:right;"/> days
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <input type="submit" value="Find" class="sbutton"/>
                                <input id="StartDate" name="StartDate" type="hidden"/>
                                <input id="EndDate" name="EndDate" type="hidden"/>
                                <input id="LastNDays" name="LastNDays" type="hidden"/>
                            </td>
                        </tr>
                    </table>
                        <div id="warningDiv">
                            <h3>
                                Warning: please specify a small date range. If not, it may take some time to retrieve <br/>
                                <xsl:text disable-output-escaping="yes">&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;</xsl:text> 
                                the workunits and the browser may be timed out. 
                            </h3>
                        </div>
                </form>
            </xsl:otherwise>
          </xsl:choose>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
