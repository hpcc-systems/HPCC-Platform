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
    <xsl:template match="DFUSearchResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
<script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
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
/*
                    function cleanDate(i)
                    {
                        if ((document.getElementById("From").value == "") || (document.getElementById("To").value == ""))
                        {
                            document.getElementById("Range").disabled = false;
                        }
                        else
                        {
                            document.getElementById("Range").disabled = true;
                        }

                        if (document.getElementById("Range").value == "")
                        {
                            document.getElementById("From").disabled = false;
                            document.getElementById("To").disabled = false;
                        }
                        else
                        {
                            document.getElementById("From").disabled = true;
                            document.getElementById("To").disabled = true;
                        }
                    }
*/
                    function cleanNewest(i)
                    {
                        if (i != 1)
                            document.getElementById("FileNewest").value="";
                        if (i != 2)
                            document.getElementById("FileOldest").value="";
                        if (i != 3)
                            document.getElementById("FileLargest").value="";
                        if (i != 4)
                            document.getElementById("FileSmallest").value="";
                    }
                    function submit_search()
                    {
                        //document.getElementById("StartDate").value='';
                        //document.getElementById("EndDate").value='';

                        if (rbid > 0)
                        {
                            if(document.getElementById('Range').value)
                            {
                                var d=parseInt(document.getElementById("Range").value);
                                if(isNaN(d))
                                {
                                    alert("Invalid data in a 'Modified Date' field");
                                    return false;
                                }
                                var cur=new Date();
                                document.getElementById("StartDate").value=formatDate(new Date(cur.getFullYear(),cur.getMonth(),cur.getDate()-d+1));
                                document.getElementById("EndDate").value=formatDate(cur);

                                //document.getElementById("From").value='';
                                //document.getElementById("To").value='';
                            }
                        }
                        else
                        {
                            if(!document.getElementById('From').value || !document.getElementById('To').value)
                            {
                                alert("Both the 'From' or 'To' field should be defined.");
                                return false;
                            }

                            var df=formatDate(document.getElementById("From").value);
                            if(!df) 
                            {
                                alert("Invalid data in a 'Modified Date' field");
                                return false;
                            }
                            document.getElementById("StartDate").value=df;

                            var d=Date.parse(document.getElementById("To").value);
                            if(isNaN(d))
                            {
                                alert("Invalid data in a 'Modified Date' field");
                                return false;
                            }
                            if(String(document.getElementById("To").value).indexOf(":")<0)
                                d+=1000*60*60*24-1;

                            var dt=formatDate(d);
                            if(!df) 
                            {
                                alert("Invalid data in a 'Modified Date' field");
                                return false;
                            }
                            document.getElementById("EndDate").value=dt;
                        }
                        return true;
                    }

                    function onFindClick()
                    {
                        if(submit_search())
                        { 
                            var url = "/WsDfu/DFUQuery";
                            var first = true;
                            
                            var owner = document.getElementById("Owner").value;
                            if(owner)
                            {
                                url += "?Owner=" + owner;
                                first = false;
                            }
                        
                            //var clusterS = document.getElementById("ClusterName").selectedIndex;
                            //if(clusterS > 0)
                            {
                                //var cluster = document.getElementById("ClusterName").options[clusterS].text;
                                var cluster = '';
                                var selObj = document.getElementById("ClusterName");
                                for (i=0; i<selObj.options.length; i++) 
                                {
                                    if (selObj.options[i].selected) 
                                    {
                                        cluster = cluster + selObj.options[i].text + ',';
                                    }
                                }

                                if (first)
                                {
                                    url += "?ClusterName=" + cluster;
                                    first = false;
                                }
                                else
                                {
                                    url += "&ClusterName=" + cluster;
                                }
                            }

                            var filetypeS = document.getElementById("FileType").selectedIndex;
                            if(filetypeS > 0)
                            {
                                var filetype = document.getElementById("FileType").options[filetypeS].text;
                                if (first)
                                {
                                    url += "?FileType=" + filetype;
                                    first = false;
                                }
                                else
                                {
                                    url += "&FileType=" + filetype;
                                }
                            }
                        
                            var logicalname = document.getElementById("LogicalName").value;
                            if(logicalname)
                            {
                                if (first)
                                {
                                    url += "?LogicalName=" + logicalname;
                                    first = false;
                                }
                                else
                                {
                                    url += "&LogicalName=" + logicalname;
                                }
                            }
                        
                            var description = document.getElementById("Description").value;
                            if(description)
                            {
                                if (first)
                                {
                                    url += "?Description=" + description;
                                    first = false;
                                }
                                else
                                {
                                    url += "&Description=" + description;
                                }
                            }
                        
                            var firstN = document.getElementById("FirstN").value;
                            if(isNaN(firstN))
                            {
                                alert("Invalid data in a 'First N' field");
                                return false;
                            }
                            else
                            {
                                var firstNTypeS = document.getElementById("FirstNType").selectedIndex;
                                var sortBy = "Modified";
                                var descending = true;
                                if (firstNTypeS > 2)//largest
                                {
                                    sortBy = "FileSize";
                                    descending = "false";
                                }
                                else if (firstNTypeS > 1)//smallest
                                    sortBy = "FileSize";
                                else if (firstNTypeS > 0)//oldest
                                    descending = "false";
                                if (first)
                                {
                                    url += "?FirstN=" + firstN + "&Sortby=" + sortBy + "&Descending=" + descending;
                                    first = false;
                                }
                                else
                                {
                                    url += "&FirstN=" + firstN + "&Sortby=" + sortBy + "&Descending=" + descending;
                                }
                            }
                        
                            var sizeFrom = document.getElementById("FileSizeFrom").value;
                            if(isNaN(sizeFrom))
                            {
                                alert("Invalid data in a 'File Size' field");
                                return false;
                            }
                            else
                            {
                                if (first)
                                {
                                    url += "?FileSizeFrom=" + sizeFrom;
                                    first = false;
                                }
                                else
                                {
                                    url += "&FileSizeFrom=" + sizeFrom;
                                }
                            }
                        
                            var sizeTo = document.getElementById("FileSizeTo").value;
                            if(isNaN(sizeTo))
                            {
                                alert("Invalid data in a 'FileSize' field");
                                return false;
                            }
                            else
                            {
                                if (first)
                                {
                                    url += "?FileSizeTo=" + sizeTo;
                                    first = false;
                                }
                                else
                                {
                                    url += "&FileSizeTo=" + sizeTo;
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
                        
                            document.location.href=url;
                        }       
                        return;
                    }       

          function checkEnter(e) {
            if (!e) 
              { e = window.event; }
            if (e && e.keyCode == 13)
            {
              onOpenClick();
            }
          }

                    function onOpenClick()
                    {
                        var name = document.getElementById("Name").value;
                        if(name)
                        { 
                            document.location.href="/WsDfu/DFUInfo?Name=" + name;
                        }   
                        else
                        {
                            onFindClick();
                        }   
                        return;
                    }       

                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();">
                <h3>Open Logical File:</h3>
                <table>
                    <tr>
                        <td>File Name:</td>
                        <td>
                            <input id="Name" size="25" type="text" onkeyup="checkEnter(event)" />
                        </td>
                        <td>
                            <input type="button" value="Open" class="sbutton" onclick="onOpenClick()"/>
                        </td>
                    </tr>
                </table>
                <br/><br/>
                <h3>Search Logical Files:</h3>
                <!--form action="/WsDfu/DFUQuery"  method="get" onsubmit="return submit_search()"-->
                    <table>
                        <tr>
                            <td>File Type:</td>
                            <td>
                                <select id="FileType" name="FileType" size="1">
                                    <xsl:for-each select="FileTypes/FileType">
                                        <option>
                                            <xsl:value-of select="."/>
                                        </option>
                                    </xsl:for-each>
                                </select>
                            </td>
                        </tr>
                        <tr>
                            <td>File Name Pattern:</td>
                            <td>
                                <input id="LogicalName" name="LogicalName" size="100" type="text" onkeyup="checkEnter(event)"/>
                                <xsl:text disable-output-escaping="yes">&amp;nbsp;&amp;nbsp;</xsl:text>
                                <a href="javascript:go('/WsDfu/DFUSearch?ShowExample=yes')">Examples</a>                                
                            </td>
                        </tr>
                        <tr>
                            <td>File Description Pattern:</td>
                            <td>
                                <input id="Description" name="Description" size="100" type="text" onkeyup="checkEnter(event)"/>
                            </td>
                        </tr>
                        <tr>
                        <xsl:if test="string-length(ShowExample)">
                            <td>Example Patterns:</td>
                            <td>
                            <ul>
                            <li>All files - leave it blank or *</li>
                            <li>Files beginning with "thor::key" - thor::key*</li>
                            <li>Files containing "key::" - *key::*</li>
                            <li> Files ending with "header" -  *header </li>
                            <li>Files matching "thor::key::header" exactly - thor::key::header</li>
                            </ul>
                            </td>
                        </xsl:if>
                        </tr>
                        <tr><td><br/></td></tr>
                        <tr>
                            <td>Owner:</td>
                            <td>
                                <input id="Owner" name="Owner" size="20" type="text" onkeyup="checkEnter(event)"/>
                            </td>
                        </tr>
                        <tr>
                            <td>Cluster:</td>
                            <td>
                                <select id="ClusterName" name="ClusterName" size="3" multiple="multiple">
                                    <xsl:for-each select="ClusterNames/ClusterName">
                                        <option>
                                            <xsl:value-of select="."/>
                                        </option>
                                    </xsl:for-each>
                                </select>
                            </td>
                        </tr>
                        <tr>
                            <td>First N:</td>
                            <td><input id="FirstN" name="FirstN" size="3" type="text" onkeyup="checkEnter(event)"/>
                                <select id="FirstNType" name="FirstNType" size="1">
                                    <option>newest</option>
                                    <option>oldest</option>
                                    <option>largest</option>
                                    <option>smallest</option>
                                </select>
                            </td>
                        </tr>
                        <!--tr>
                            <td  align="center">select all files</td><td>or <input id="FileNewest" name="FileNewest" size="3" type="text" style="text-align:right;" onkeydown="cleanNewest(1);"/> newest files</td>
                        </tr>
                        <tr>
                            <td></td><td>or <input id="FileOldest" name="FileOldest" size="3" type="text" style="text-align:right;" onkeydown="cleanNewest(2);"/> oldest files</td>
                        </tr>
                        <tr>
                            <td></td><td>or <input id="FileLargest" name="FileLargest" size="3" type="text" style="text-align:right;" onkeydown="cleanNewest(3);"/> largest files</td>
                        </tr>
                        <tr>
                            <td></td><td>or <input id="FileSmallest" name="FileSmallest" size="3" type="text" style="text-align:right;" onkeydown="cleanNewest(4);"/> smallest files</td>
                        </tr>
                        <tr><td><br/></td></tr>
                        <tr>
                            <td>    Under the following requirements:</td>
                        </tr-->
                        <tr><td><br/></td></tr>
                        <tr>
                            <td rowspan="2">
                                <table>
                                    <tr>
                                        <td>File Size:</td>
                                        <td></td>
                                        <td>From</td>               
                                    </tr>
                                    <tr>
                                        <td></td>
                                        <td></td>
                                        <td>To</td>
                                    </tr>
                                </table>
                            </td>
                            <td>
                                <input id="FileSizeFrom" name="FileSizeFrom" size="20" type="text" style="text-align:right;" onkeyup="checkEnter(event)"/>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <input id="FileSizeTo" name="FileSizeTo" size="20" type="text" style="text-align:right;" onkeyup="checkEnter(event)"/>
                            </td>
                        </tr>
                        <tr><td><br/></td></tr>
                        <tr>
                            <td rowspan="3">
                                <table>
                                    <tr>
                                        <td>Modified Date:</td>
                                        <td><input type="radio" id="DateRB" name="DateRB" value="" onclick="onRBChanged(0)" onkeyup="checkEnter(event)"/></td>
                                        <td>From</td>               
                                    </tr>
                                    <tr>
                                        <td></td>
                                        <td></td>
                                        <td>To</td>
                                    </tr>
                                    <tr>
                                        <td></td>
                                        <td><input type="radio" id="DateRB" name="DateRB" value="" checked="checked" onclick="onRBChanged(1)" onkeyup="checkEnter(event)"/></td>
                                        <td>or in the last</td>
                                    </tr>
                                </table>
                            </td>
                            <td><input id="From" size="20" type="text" onkeyup="checkEnter(event)"/> (mm/dd/yyyy hh:mm am/pm)</td>
                        </tr>
                        <tr>
                            <td>
                                <input id="To" size="20" type="text" onkeyup="checkEnter(event)" /> (mm/dd/yyyy hh:mm am/pm)
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <input id="Range" size="5" type="text" style="text-align:right;" onkeyup="checkEnter(event)"/> days
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <input type="button" value="Find" class="sbutton" onclick="onFindClick()"/>
                                <input id="StartDate" name="StartDate" type="hidden"/>
                                <input id="EndDate" name="EndDate" type="hidden"/>
                            </td>
                        </tr>
                    </table>
                <!--/form-->
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
