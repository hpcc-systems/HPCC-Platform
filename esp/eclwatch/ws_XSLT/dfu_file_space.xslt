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
    <xsl:variable name="countby" select="/DFUSpaceResponse/CountBy"/>
    <xsl:variable name="scopeunder" select="/DFUSpaceResponse/ScopeUnder"/>
    <xsl:variable name="ownerunder" select="/DFUSpaceResponse/OwnerUnder"/>
    <xsl:variable name="startdate" select="/DFUSpaceResponse/StartDate"/>
    <xsl:variable name="enddate" select="/DFUSpaceResponse/EndDate"/>
    <xsl:variable name="interval" select="/DFUSpaceResponse/Interval"/>
    <xsl:template match="/DFUSpaceResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script language="JavaScript1.2" src="/esp/files_/popup.js">null</script>
                    <script language="JavaScript1.2">
                        var counttype='<xsl:value-of select="$countby"/>';
                        var underscope='<xsl:value-of select="$scopeunder"/>';
                        var underowner='<xsl:value-of select="$ownerunder"/>';
                        var fromdate='<xsl:value-of select="$startdate"/>';
                        var todate='<xsl:value-of select="$enddate"/>';
                        var subtype='<xsl:value-of select="$interval"/>';
                        function ChangeHeader(o1, headerid)
                        {
                            if (headerid%2)
                            {
                                o1.bgColor = '#CCCCCC';
                            }
                            else
                            {
                                o1.bgColor = '#F0F0FF';
                            }
                        }
                    </script>
                    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                    <title>DFU</title>
                    <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
                    <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
                    <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                        <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                    </script>
                    <script language="JavaScript1.2">
                        <xsl:text disable-output-escaping="yes"><![CDATA[
                            var countbySelected = 1;
                            var interval = "Year";
                            function onChangeCountType()
                            {
                                if (document.getElementById("CountType").selectedIndex > 1)
                                {
                                    countbySelected = 3;
                                    if (document.getElementById("CountType").selectedIndex > 5)
                                    {
                                        interval = "Day";
                                    }
                                    else if (document.getElementById("CountType").selectedIndex > 4)
                                    {
                                        interval = "Month";
                                    }
                                    else if (document.getElementById("CountType").selectedIndex > 3)
                                    {
                                        interval = "Quarter";
                                    }
                                    else
                                    {
                                        interval = "Year";
                                    }
                                }
                            }

                            function onCountByChanged(countby)
                            {       
                                countbySelected = countby;  
                                if (countbySelected == 1)
                                {
                                    document.getElementById("UnderScope").disabled = false;
                                    document.getElementById("FromDate").disabled = true;
                                    document.getElementById("ToDate").disabled = true;
                                    document.getElementById("Interval").disabled = true;
                                }
                                else if (countbySelected == 2)
                                {
                                    document.getElementById("UnderScope").disabled = true;
                                    document.getElementById("FromDate").disabled = true;
                                    document.getElementById("ToDate").disabled = true;
                                    document.getElementById("Interval").disabled = true;
                                }
                                else
                                {
                                    document.getElementById("UnderScope").disabled = true;
                                    document.getElementById("FromDate").disabled = false;
                                    document.getElementById("ToDate").disabled = false;
                                    document.getElementById("Interval").disabled = false;
                                }

                                return;
                            }

                            function onLoad()
                            {
                                if (counttype == '')
                                {
                                    document.getElementById("FromDate").value = fromdate;
                                    document.getElementById("ToDate").value = todate;
                                    document.getElementById("UnderScope").value = underscope;
                                    document.getElementById("UnderOwner").value = underowner;
                                    
                                    if (counttype == "Scope")
                                    {
                                        document.getElementById("CountType").selectedIndex = 0;
                                    }
                                    else if (counttype == "Owner")
                                    {
                                        document.getElementById("CountType").selectedIndex = 1;
                                    }
                                    else if (subtype == "Year")
                                    {
                                        document.getElementById("CountType").selectedIndex = 2;
                                    }
                                    else if (subtype == "Quarter")
                                    {
                                        document.getElementById("CountType").selectedIndex = 3;
                                    }
                                    else if (subtype == "Month")
                                    {
                                        document.getElementById("CountType").selectedIndex = 4;
                                    }
                                    else if (subtype == "Day")
                                    {
                                        document.getElementById("CountType").selectedIndex = 5;
                                    }
                                    else
                                    {
                                        document.getElementById("CountType").selectedIndex = 0;
                                    }
                                }
                                initSelection('resultsTable');
                            }        
                          
                            function format2(n)
                            {
                                return new String(Math.floor(n/10))+ new String(n%10);
                            }

                            function formatDate(d)
                            {
                                var dt=new Date(d);
                                if(isNaN(dt)) 
                                {
                                    alert("Incorrect date format!");
                                    return null;
                                }

                                return (dt.getFullYear()>1950 ?  dt.getFullYear() : dt.getFullYear()+100)+'-'+
                                       format2(dt.getMonth()+1)+'-'+
                                       format2(dt.getDate())+'T'+
                                       format2(dt.getHours())+':'+
                                       format2(dt.getMinutes())+':'+
                                       format2(dt.getSeconds())+'Z';
                            }
                    
                            function onGetInfo()
                            {
                                var url;
                                var fromDate;
                                var endDate;
                                if(document.getElementById("FromDate").value)
                                { 
                                    fromDate=formatDate(document.getElementById("FromDate").value);
                                    if (fromDate == null)
                                        return false;
                                }
                                if(document.getElementById("ToDate").value)
                                {
                                    var d=Date.parse(document.getElementById("ToDate").value);
                                    if(isNaN(d))
                                    {
                                        alert("Incorrect date format!");
                                        return false;
                                    }
                                    if(String(document.getElementById("ToDate").value).indexOf(":")<0)
                                        d+=1000*60*60*24-1;

                                    endDate=formatDate(d);
                                }

                                if (document.getElementById("CountType").selectedIndex > 1)
                                {
                                    if (document.getElementById("CountType").selectedIndex > 4)
                                    {
                                        interval = "Day";
                                    }
                                    else if (document.getElementById("CountType").selectedIndex > 3)
                                    {
                                        interval = "Month";
                                    }
                                    else if (document.getElementById("CountType").selectedIndex > 2)
                                    {
                                        interval = "Quarter";
                                    }
                                    else
                                    {
                                        interval = "Year";
                                    }
                                    url='/WsDfu/DFUSpace?CountBy=Date&Interval='+interval;
                                }
                                else if (document.getElementById("CountType").selectedIndex > 0)
                                {
                                    url='/WsDfu/DFUSpace?CountBy=Owner';
                                }
                                else
                                {
                                    url='/WsDfu/DFUSpace?CountBy=Scope';
                                }

                                if(document.getElementById("UnderScope").value) 
                                {
                                    url=url+'&ScopeUnder='+document.getElementById("UnderScope").value;
                                }

                                if(document.getElementById("UnderOwner").value) 
                                {
                                    url=url+'&OwnerUnder='+document.getElementById("UnderOwner").value;
                                }

                                if(fromDate)
                                {
                                    url=url+'&StartDate='+fromDate;
                                } 
                                
                                if(endDate) 
                                {
                                    url=url+'&EndDate='+endDate;
                                }

                                document.location.href=url;
                                return;
                            }
                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <h3>Disk Space Usage:</h3>
                <xsl:choose>
                    <xsl:when test="string-length($countby)">
                        <!--table>
                            <colgroup>
                                <col width="280"/>
                                <col width="500"/>
                            </colgroup>
                            <tr>
                                <td>
                                    Scope: <input type="text" name="UnderScope" id="UnderScope"/>
                                </td>
                                <td>
                                    Owner: <input type="text" name="UnderOwner" id="UnderOwner"/>
                                </td>
                                <td>
                                    Date/time from: <input type="text" name="FromDate" id="FromDate"/>
                                </td>
                                <td>
                                    to: <input type="text" name="ToDate" id="ToDate"/> (mm/dd/yyyy hh:mm am/pm)
                                </td>
                            </tr>
                            <tr>
                                <td>List by 
                                    <select id="CountType" name="CountType" size="1" onchange="onChangeCountType()">
                                        <xsl:choose>
                                            <xsl:when test="$countby='Owner'">
                                                <option value="Scope">Scope</option>
                                                <option value="Owner" selected="selected">Owner</option>
                                                <option value="Year">Year</option>
                                                <option value="Quarter">Quarter</option>
                                                <option value="Month">Month</option>
                                                <option value="Day">Day</option>
                                            </xsl:when>
                                            <xsl:when test="$countby='Year'">
                                                <option value="Scope">Scope</option>
                                                <option value="Owner">Owner</option>
                                                <option value="Year" selected="selected">Year</option>
                                                <option value="Quarter">Quarter</option>
                                                <option value="Month">Month</option>
                                                <option value="Day">Day</option>
                                            </xsl:when>
                                            <xsl:when test="$countby='Quarter'">
                                                <option value="Scope">Scope</option>
                                                <option value="Owner">Owner</option>
                                                <option value="Year">Year</option>
                                                <option value="Quarter" selected="selected">Quarter</option>
                                                <option value="Month">Month</option>
                                                <option value="Day">Day</option>
                                            </xsl:when>
                                            <xsl:when test="$countby='Month'">
                                                <option value="Scope">Scope</option>
                                                <option value="Owner">Owner</option>
                                                <option value="Year">Year</option>
                                                <option value="Quarter">Quarter</option>
                                                <option value="Month" selected="selected">Month</option>
                                                <option value="Day">Day</option>
                                            </xsl:when>
                                            <xsl:when test="$countby='Day'">
                                                <option value="Scope">Scope</option>
                                                <option value="Owner">Owner</option>
                                                <option value="Year">Year</option>
                                                <option value="Quarter">Quarter</option>
                                                <option value="Month">Month</option>
                                                <option value="Day" selected="selected">Day</option>
                                            </xsl:when>
                                            <xsl:otherwise>
                                                <option value="Scope" selected="selected">Scope</option>
                                                <option value="Owner">Owner</option>
                                                <option value="Year">Year</option>
                                                <option value="Quarter">Quarter</option>
                                                <option value="Month">Month</option>
                                                <option value="Day">Day</option>
                                            </xsl:otherwise>
                                        </xsl:choose>
                                    </select>
                                </td>
                                <td>
                                    <input type="submit" class="sbutton" name="GetInfo" id="GetInfo" value="Get" onclick="onGetInfo()"/>
                                </td>
                            </tr>
                        </table>
                        <br/-->
                        <xsl:choose>
                            <xsl:when test="not(DFUSpaceItems/DFUSpaceItem[1])">
                                No files found.
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:apply-templates/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>
                        <table>
                            <colgroup>
                                <col width="80"/>
                                <col width="500"/>
                            </colgroup>
                            <tr>
                                <td>
                                    Scope:
                                </td>
                                <td>
                                    <input type="text" name="UnderScope" id="UnderScope"/>
                                </td>
                            </tr>
                            <tr>
                                <td>
                                    Owner:
                                </td>
                                <td>
                                    <input type="text" name="UnderOwner" id="UnderOwner"/>
                                </td>
                            </tr>
                            <tr>
                                <td>
                                    Date/time from:
                                </td>
                                <td>
                                    <input type="text" name="FromDate" id="FromDate"/> (mm/dd/yyyy hh:mm am/pm)
                                </td>
                            </tr>
                            <tr>
                                <td>
                                    Date/time to:
                                </td>
                                <td>
                                    <input type="text" name="ToDate" id="ToDate"/> (mm/dd/yyyy hh:mm am/pm)
                                </td>
                            </tr>
                            <tr>
                                <td>
                                    List by 
                                </td>
                                <td>
                                    <select id="CountType" name="CountType" size="1" onchange="onChangeCountType()">
                                        <xsl:choose>
                                            <xsl:when test="$countby='Owner'">
                                                <option value="Scope">Scope</option>
                                                <option value="Owner" selected="selected">Owner</option>
                                                <option value="Year">Year</option>
                                                <option value="Quarter">Quarter</option>
                                                <option value="Month">Month</option>
                                                <option value="Day">Day</option>
                                            </xsl:when>
                                            <xsl:when test="$countby='Year'">
                                                <option value="Scope">Scope</option>
                                                <option value="Owner">Owner</option>
                                                <option value="Year" selected="selected">Year</option>
                                                <option value="Quarter">Quarter</option>
                                                <option value="Month">Month</option>
                                                <option value="Day">Day</option>
                                            </xsl:when>
                                            <xsl:when test="$countby='Quarter'">
                                                <option value="Scope">Scope</option>
                                                <option value="Owner">Owner</option>
                                                <option value="Year">Year</option>
                                                <option value="Quarter" selected="selected">Quarter</option>
                                                <option value="Month">Month</option>
                                                <option value="Day">Day</option>
                                            </xsl:when>
                                            <xsl:when test="$countby='Month'">
                                                <option value="Scope">Scope</option>
                                                <option value="Owner">Owner</option>
                                                <option value="Year">Year</option>
                                                <option value="Quarter">Quarter</option>
                                                <option value="Month" selected="selected">Month</option>
                                                <option value="Day">Day</option>
                                            </xsl:when>
                                            <xsl:when test="$countby='Day'">
                                                <option value="Scope">Scope</option>
                                                <option value="Owner">Owner</option>
                                                <option value="Year">Year</option>
                                                <option value="Quarter">Quarter</option>
                                                <option value="Month">Month</option>
                                                <option value="Day" selected="selected">Day</option>
                                            </xsl:when>
                                            <xsl:otherwise>
                                                <option value="Scope" selected="selected">Scope</option>
                                                <option value="Owner">Owner</option>
                                                <option value="Year">Year</option>
                                                <option value="Quarter">Quarter</option>
                                                <option value="Month">Month</option>
                                                <option value="Day">Day</option>
                                            </xsl:otherwise>
                                        </xsl:choose>
                                    </select>
                                </td>
                            </tr>
                        <table>
                        <br/>
                        </table>
                            <tr>
                                <td>
                                    <input type="submit" class="sbutton" name="GetInfo" id="GetInfo" value="Get" onclick="onGetInfo()"/>
                                </td>
                            </tr>
                        </table>
                    </xsl:otherwise>
                </xsl:choose>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="DFUSpaceItems">
        <table class="sort-table" id="resultsTable">
            <colgroup>
                <col width="100"/>
                <col width="100" class="number"/>
                <col width="100"/>
                <col width="100" class="number"/>
                <col width="100"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
            </colgroup>
            <thead>
            <tr class="grey">
                <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 0)" onmouseout="ChangeHeader(this, 1)">
                    <xsl:choose>
                        <xsl:when test="string-length($countby)">
                            <xsl:value-of select="$countby"/>
                        </xsl:when>
                        <xsl:otherwise>Scope</xsl:otherwise>
                    </xsl:choose>
                </th>
                <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 4)" onmouseout="ChangeHeader(this, 5)">Total Size</th>
                <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 6)" onmouseout="ChangeHeader(this, 7)">Largest File</th>
                <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 6)" onmouseout="ChangeHeader(this, 7)">Largest Size</th>
                <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 8)" onmouseout="ChangeHeader(this, 9)">Smallest File</th>
                <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 8)" onmouseout="ChangeHeader(this, 9)">Smallest Size</th>
                <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 2)" onmouseout="ChangeHeader(this, 3)">File Counts</th>
                <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 2)" onmouseout="ChangeHeader(this, 3)">Files with Unknown Size</th>
            </tr>
            </thead>
            <tbody>
            <xsl:apply-templates select="DFUSpaceItem">
                <xsl:sort select="Name"/>
            </xsl:apply-templates>
         </tbody>
        </table>
    </xsl:template>
    
    <xsl:template match="DFUSpaceItem">
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
            <!--td>
                <input type="checkbox" name="LogicalFiles_i{position()}" value="{Name}" onclick="return clicked(this)"/>
                <xsl:variable name="popup">return DFUFilePopup('<xsl:value-of select="$info_query"/>', '<xsl:value-of select="Name"/>')</xsl:variable>
                <xsl:attribute name="oncontextmenu"><xsl:value-of select="$popup"/></xsl:attribute>
                <img class="menu1" src="/esp/files_/img/menu1.png" onclick="{$popup}"></img>
            </td>
            <td>
              <xsl:if test="isZipfile=1">
                <img border="0" src="/esp/files_/img/zip.gif" width="16" height="16"/>
              </xsl:if>
            </td-->
            <td align="left">
                <xsl:choose>
                    <xsl:when test="$countby!='Owner' and $countby!='Date'">
                        <a href="/WsDfu/DFUSpace?CountBy=Scope&amp;ScopeUnder={Name}">
                            <xsl:value-of select="Name"/>
                        </a>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="Name"/>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
            <td>
                <xsl:value-of select="TotalSize"/>
            </td>
            <td align="left">
                <xsl:choose>
                    <xsl:when test="string-length(LargestFile)">
                        <a href="/WsDfu/DFUInfo?Name={LargestFile}">
                            <xsl:value-of select="LargestFile"/>
                        </a>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="LargestFile"/>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
            <td>
                <xsl:value-of select="LargestSize"/>
            </td>
            <td align="left">
                <xsl:choose>
                    <xsl:when test="string-length(SmallestFile)">
                        <a href="/WsDfu/DFUInfo?Name={SmallestFile}">
                            <xsl:value-of select="SmallestFile"/>
                        </a>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="SmallestFile"/>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
            <td>
                <xsl:value-of select="SmallestSize"/>
            </td>
            <td>
                <xsl:value-of select="NumOfFiles"/>
            </td>
            <td>
                <xsl:value-of select="NumOfFilesUnknown"/>
            </td>
        </tr>
    </xsl:template>
    
    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
