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
    <xsl:template match="WUJobList">
    <html>
    <head>
      <style type="text/css">
        #labelPanel.yui-panel .bd {
        background-color:#FF0;
        }
      </style>

      <link rel="stylesheet" type="text/css" href="/esp/files/default.css"/>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/container/assets/skins/sam/container.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <script type="text/javascript" src="/esp/files_/joblist.js">0</script>
        <script type="text/javascript" src="/esp/files_/jobqueue.js">0</script>
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <script type="text/javascript" src="/esp/files/yui/build/yuiloader/yuiloader-min.js" ></script>
      <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/element/element-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/container/container-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/event/event-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/dom/dom-min.js"></script>
      <script type="text/javascript">
        <xsl:text disable-output-escaping="yes"><![CDATA[ 
            var getGraph = 1;
            var rbid = 1;
            function onRBChanged(id)
            {
                rbid = id;
                document.getElementById('Range').disabled = false;
                document.getElementById('Hours').disabled = false;
                document.getElementById('From').disabled = false;
                document.getElementById('To').disabled = false;
                if (id < 1)
                {
                    document.getElementById('Range').value = '';
                    document.getElementById('Range').disabled = true;
                    document.getElementById('Hours').value = '';
                    document.getElementById('Hours').disabled = true;
                }
                else if (id < 2)
                {
                    document.getElementById('Hours').value = '';
                    document.getElementById('From').value = '';
                    document.getElementById('To').value = '';
                    document.getElementById('Hours').disabled = true;
                    document.getElementById('From').disabled = true;
                    document.getElementById('To').disabled = true;
                }
                else
                {
                    document.getElementById('Range').value = '';
                    document.getElementById('From').value = '';
                    document.getElementById('To').value = '';
                    document.getElementById('Range').disabled = true;
                    document.getElementById('From').disabled = true;
                    document.getElementById('To').disabled = true;
                }
            }

            function get_graph(x)
            {
                getGraph = x;
            }

            function submit_list()
            {
                var startDate='', endDate='';

                if (rbid > 1)
                {
                    if(!document.getElementById('Hours').value)
                    {
                        alert("Hours field should be defined.");
                        return false;
                    }

                    var d=parseInt(document.getElementById('Hours').value);
                    if(isNaN(d))
                    {
                        alert("Invalid data in a 'Hours' field");
                        return false;
                    }

                    var cur=new Date();
          var yr = cur.getYear();
          if (yr < 1900) { yr+=1900; } 
                    startDate=formatUTC(new Date(yr,cur.getMonth(),cur.getDate(),cur.getHours()-d,cur.getMinutes(),cur.getSeconds()));
                    if(!startDate) 
                    {
                        alert("Invalid data in a 'Hours' field");
                        return false;
                    }

                    endDate=formatUTC(cur);
                    if(!endDate) 
                    {
                        alert("Invalid data in a 'Hours' field");
                        return false;
                    }
                }
                else if (rbid > 0)
                {
                    if(!document.getElementById('Range').value)
                    {
                        alert("Date field should be defined.");
                        return false;
                    }

                    var d=parseInt(document.getElementById('Range').value);
                    if(isNaN(d))
                    {
                        alert("Invalid data in a 'Date' field");
                        return false;
                    }
                    var cur=new Date();

          var yr = cur.getYear();
          if (yr < 1900) { yr+=1900; } 
                    startDate=formatUTC(new Date(yr,cur.getMonth(),cur.getDate()-d+1));
                    if(!startDate) 
                    {
                        alert("Invalid data in a 'Date' field");
                        return false;
                    }
                    endDate=formatUTC(cur);
                    if(!endDate) 
                    {
                        alert("Invalid data in a 'Date' field");
                        return false;
                    }
                }
                else
                {
                    if(!document.getElementById('From').value || !document.getElementById('To').value)
                    {
                        alert("Both the 'From' or 'To' field should be defined.");
                        return false;
                    }

                    var df=formatUTC(document.getElementById('From').value);
                    if(!df) 
                    {
                        alert("Invalid data in a 'Date' field");
                        return false;
                    }
                    var d=Date.parse(document.getElementById('To').value);
                    if(isNaN(d))
                    {
                        alert("Invalid data in a 'Date' field");
                        return false;
                    }

                    if(String(document.getElementById('To').value).indexOf(":")<0)
                        d+=1000*60*60*24-1;

                    var dt=formatUTC(d);
                    if(!dt) 
                    {
                        alert("Invalid data in a 'Date' field");
                        return false;
                    }

                    startDate=df;
                    endDate=dt;
                }
                var src='';
                if (getGraph > 1)
                {
                    src='/WsWorkunits/JobQueue?Cluster=';
                    var select=document.getElementById('TargetCluster');
                    src+=select.value;
                }
                else
                {
                    if (getGraph > 0)
                        src='/WsWorkunits/JobList?Cluster=';
                    else
                        src='/WsWorkunits/WUClusterJobSummaryXLS?Cluster=';
                    var select=document.getElementById('Cluster');
                    if(select.selectedIndex>=0) src+=select.options[select.selectedIndex].text;
                }
                if(startDate) src+='&StartDate='+startDate;
                if(endDate) src+='&EndDate='+endDate;
                src+='&ShowAll=1';
                src+='&BusinessStartTime='+document.getElementById('BusinessStartTime').value;
                src+='&BusinessEndTime='+document.getElementById('BusinessEndTime').value;

                if (getGraph > 1)
                    reload_queue_graph(src);
                else if (getGraph > 0)
                    reload_graph(src);
                else
                    document.location.href=src;
              return false;
         }

            function onLoad()
            {
                rbid = 0;
                if (document.getElementById('Range').value != '')
                    rbid = 1;
                if (document.getElementById('Hours').value != '')
                    rbid = 2;

                onRBChanged(rbid)
            }    

       ]]></xsl:text>
        </script>
    </head>
    <body class="yui-skin-sam" onload="nof5();onLoad()">
         Display workunits:<br/><br/>
         <form onsubmit="return submit_list()">
         <input type="hidden" id="TargetCluster" name="TargetCluster" value="{TargetCluster}"/>
         <table>
            <tr>
                <td>Cluster</td>
                    <td>
                        <select id="Cluster" name="Cluster" size="1">
                        <xsl:for-each select="Cluster">
                            <option>
                            <xsl:if test="@selected">
                                <xsl:attribute name="selected">selected</xsl:attribute>
                            </xsl:if> 
                            <xsl:value-of select="."/>
                            </option>
                        </xsl:for-each>
                        </select>
                    </td>
            </tr>
            <tr>
                <td rowspan="4">
                    <table>
                        <tr>
                            <td>Date</td>
                            <td><input type="radio" name="DateRB" value="" onclick="onRBChanged(0)"/></td>
                            <td>From</td>               
                        </tr>
                        <tr>
                            <td></td>
                            <td></td>
                            <td>To</td>
                        </tr>
                        <tr>
                            <td></td>
                            <td><input type="radio" name="DateRB" value="" checked="checked" onclick="onRBChanged(1)"/></td>
                            <td>or in the last</td>
                        </tr>
                        <tr>
                            <td></td>
                            <td><input type="radio" name="DateRB" value="" onclick="onRBChanged(2)"/></td>
                            <td>or in the last</td>
                        </tr>
                    </table>
                </td>
                <td colspan='2'><input id="From" size="12" type="text"/> (mm/dd/yyyy hh:mm am/pm)</td>
            </tr>
            <tr>
                    <td colspan='2'><input id="To" size="12" type="text"/> (mm/dd/yyyy hh:mm am/pm)</td>
            </tr>
            <tr>
                    <td><input id="Range" size="5" type="text" value="{Range}"  style="text-align:right;"/> days</td>
            </tr>
                <tr>
                    <td><input id="Hours" size="5" type="text" value="{Hours}"  style="text-align:right;"/> hours</td>
            </tr>
            <tr>
                <td>Business hour start time</td>
                <td><input id="BusinessStartTime" size="5" type="text" value="08:00"/>(00:00~24:00)</td>
            </tr>
            <tr>
                <td>Business hour end time</td>
                <td><input id="BusinessEndTime" size="5" type="text" value="17:00"/>(00:00~24:00)</td>
            </tr>
            <tr>
                <!--td><input type="submit" class="sbutton" name="ActionType" value="Get Usage Graph" onclick="get_graph(3)"/></td>
                <td><input type="submit" class="sbutton" name="ActionType" value="Get MultiCluster Usage Graph" onclick="get_graph(1)"/></td>
                <td><input type="submit" class="sbutton" name="ActionType" value="Get Usage Summary" onclick="get_graph(2)"/></td-->
                <td><input type="submit" class="sbutton" name="ActionType" id="UsageGraphBtn" value="Get Usage Graph" style="height: 25px; width: 150px" onclick="get_graph(1)"/></td>
                <td><input type="submit" class="sbutton" name="ActionType" id="UsageSummaryBtn" value="Get Usage Summary" style="height: 25px; width: 150px" onclick="get_graph(0)"/></td>
                <td><input type="submit" class="sbutton" name="ActionType" id="ThorQueueBtn" value="Get Thor Queue" style="height: 25px; width: 150px" onclick="get_graph(2)"/></td>
            </tr>
        </table>
        </form>
      <table>
        <tr>
          <td>
            <div id="progress"></div>
          </td>
          <td>
            <embed id="SVG0" width="1" height="1" src="/esp/files_/empty.svg" type="image/svg+xml" pluginspage="http://www.adobe.com/svg/viewer/install/"/>
          </td>
        </tr>
        <tr>
          <td colspan="2">
            <embed id="SVG" width="1" height="1" src="/esp/files_/empty.svg" type="image/svg+xml" pluginspage="http://www.adobe.com/svg/viewer/install/"/>
          </td>
        </tr>
      </table>
      <iframe id="loader" width="0" height="0" style="display:none; visibility:hidden;"/>
    </body>
    </html>
    </xsl:template>

    <xsl:template match="text()|comment()"/>

</xsl:stylesheet>
