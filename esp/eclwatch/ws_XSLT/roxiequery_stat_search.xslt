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
    <xsl:template match="StatisticsSearchResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>EclWatch</title>
                <script language="JavaScript1.2">
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                    function onChangeType()
                    {
                      if (document.getElementById("Type").selectedIndex > 1)
                      {
                        document.getElementById("QueryName").disabled = true;
                        document.getElementById("TimeBy").disabled = true;
                        document.getElementById("ErrorClass").disabled = false;
                      }
                      else if (document.getElementById("Type").selectedIndex > 0)
                      {
                        document.getElementById("QueryName").disabled = false;
                        document.getElementById("TimeBy").disabled = true;
                        document.getElementById("ErrorClass").disabled = true;
                      }
                      else
                      {
                        document.getElementById("QueryName").disabled = false;
                        document.getElementById("TimeBy").disabled = false;
                        document.getElementById("ErrorClass").disabled = true;
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
                        if(document.getElementById("Range").value)
                        {
                            var d=parseInt(document.getElementById("Range").value);
                            if(isNaN(d))
                                return false;
                            var cur=new Date();
                            document.getElementById("StartDate").value=formatDate(new Date(cur.getYear(),cur.getMonth(),cur.getDate()-d+1));
                            document.getElementById("EndDate").value=formatDate(cur);

                            document.getElementById("From").value='';
                            document.getElementById("To").value='';
                        }
                        else
                        {
                            if(document.getElementById("From").value)
                            { 
                                var df=formatDate(document.getElementById("From").value);
                                if(!df) return false;

                                document.getElementById("StartDate").value=df;
                            }
                            if(document.getElementById("To").value)
                            {
                                var d=Date.parse(document.getElementById("To").value);
                                if(isNaN(d))
                                    return false;
                                if(String(document.getElementById("To").value).indexOf(":")<0)
                                    d+=1000*60*60*24-1;

                                var dt=formatDate(d);
                                if(!dt) return false;
                                document.getElementById("EndDate").value=dt;
                            }
                        }
                        return true;
                    }

                    function onSubmitClick()
                    {
                        if(submit_search())
                        { 
                            var url = "/WsRoxieQuery/StatisticsReport";
                            var first = true;
                            
                            var typeS = document.getElementById("Type").selectedIndex;
                            if(typeS > -1)
                            {
                                var type = document.getElementById("Type").options[typeS].text;
                                url += "?Type=" + type;
                                first = false;
                            }
                        
                            var name = document.getElementById("QueryName").value;
                            if(name)
                            {
                                if (first)
                                {
                                    url += "?QueryName=" + name;
                                    first = false;
                                }
                                else
                                {
                                    url += "&QueryName=" + name;
                                }
                            }
                        
                            var eClassS = document.getElementById("ErrorClass").selectedIndex;
                            if(eClassS > 0)
                            {
                                var eClass = document.getElementById("ErrorClass").options[eClassS].text;
                                if (first)
                                {
                                    url += "?ErrorClass=" + eClass;
                                    first = false;
                                }
                                else
                                {
                                    url += "&ErrorClass=" + eClass;
                                }
                            }

                            var startDate = document.getElementById("StartDate").value;
                            if(startDate)
                            {
                                if (first)
                                {
                                    url += "?StartDate=" + startDate;
                                    first = false;
                                }
                                else
                                {
                                    url += "&StartDate=" + startDate;
                                }
                            }
                        
                            var endDate = document.getElementById("EndDate").value;
                            if(endDate)
                            {
                                if (first)
                                {
                                    url += "?EndDate=" + endDate;
                                    first = false;
                                }
                                else
                                {
                                    url += "&EndDate=" + endDate;
                                }
                            }
                        
                            var timeByS = document.getElementById("TimeBy").selectedIndex;
                            if(timeByS > -1)
                            {
                                var timeBy = document.getElementById("TimeBy").options[timeByS].text;
                                if (first)
                                {
                                    url += "?TimeBy=" + timeBy;
                                    first = false;
                                }
                                else
                                {
                                    url += "&TimeBy=" + timeBy;
                                }
                            }
                            document.location.href=url;
                        }       
                        return;
                    }       

                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onChangeType()">
                <h4>Roxie Query Statistics:</h4>
                <table>
                    <tr>
                        <td>Type:</td>
                        <td>
                            <select name="Type" id="Type" size="1" onchange="onChangeType()">
                                <option>Query Summary</option>
                                <option>Query Logs</option>
                                <option>Exceptions</option>
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td>Query Name:</td>
                        <td>
                            <input id="QueryName" name="QueryName" size="12" type="text"/>
                        </td>
                    </tr>
                    <tr>
                        <td>Date/time:</td><td> </td>
                    </tr>
                    <tr>
                        <td align="right">in the last</td>
                        <td>
                            <input id="Range" size="5" type="text"/> days
                        </td>
                    </tr>
                    <tr>
                        <td  align="center">or</td><td></td>
                    </tr>
                    <tr>
                        <td align="right">from</td>
                        <td>
                            <input id="From" size="12" type="text"/> (mm/dd/yyyy hh:mm am/pm)
                        </td>
                    </tr>
                    <tr>
                        <td align="right">to</td>
                        <td>
                            <input id="To" size="12" type="text"/> (mm/dd/yyyy hh:mm am/pm)
                        </td>
                    </tr>
                    <tr>
                        <td align="right">by</td>
                        <td>
                            <select name="TimeBy" id="TimeBy" size="1">
                                <option>Days</option>
                                <option>Hours</option>
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td>Error Class:</td>
                        <td>
                            <select id="ErrorClass" name="ErrorClass" size="1">
                                <option/>
                                <option>SLOW</option>
                                <option>ERROR</option>
                                <option>EXCEPTION</option>
                                <option>FAILED</option>
                                <option>Roxie restarting</option>
                            </select>
                        </td>
                    </tr>
                    <tr><td><br/></td></tr>
                    <tr>
                        <td>
                            <input type="submit" value="Submit" class="sbutton" onclick="onSubmitClick()"/>
                            <input id="StartDate" name="StartDate" type="hidden"/>
                            <input id="EndDate" name="EndDate" type="hidden"/>
                        </td>
                    </tr>
                </table>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
