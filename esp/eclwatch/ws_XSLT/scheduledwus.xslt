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
   <xsl:variable name="eventname" select="/WUShowScheduledResponse/EventName"/>
   <xsl:variable name="pusheventname" select="/WUShowScheduledResponse/PushEventName"/>
   <xsl:variable name="pusheventtext" select="/WUShowScheduledResponse/PushEventText"/>
   <xsl:variable name="clustersel" select="/WUShowScheduledResponse/ClusterSelected"/>
   <xsl:template match="WUShowScheduledResponse">
      <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
         <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Workunits</title>
            <link type="text/css" rel="stylesheet" href="/esp/files_/default.css"/>
           <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
           <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
           <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
           <script language="JavaScript1.2" src="/esp/files_/scripts/multiselect.js">&#160;</script>
           <script language="JavaScript1.2">
               var eventnamesel = '<xsl:value-of select="$eventname"/>';
               var pusheventnamesel = '<xsl:value-of select="$pusheventname"/>';
               var pusheventtextsel = '<xsl:value-of select="$pusheventtext"/>';
               var serversel = '<xsl:value-of select="$clustersel"/>';
               <xsl:text disable-output-escaping="yes"><![CDATA[
                     var checkedcount = 0;
                     function checkSelected(o)
                     {
                        if ((o.tagName=='INPUT') && (o.id!='All1') && o.checked)
                        {
                            checkedcount++;
                            return;
                        }

                        var ch=o.childNodes;
                        if (ch)
                            for (var i in ch)
                                checkSelected(ch[i]);
                         return;
                     }
                     function onRowCheck(checked)
                     {
                        checkedcount = 0;
                        document.getElementById("deleteBtn").disabled = true;
                    
                        checkSelected(document.forms['listitems']);

                        if (checkedcount > 0)
                        {
                            document.getElementById("deleteBtn").disabled = false;
                        }
                     }     
                     function onLoad()
                     {
                        if (serversel > 0)
                            document.getElementById("Clusters").selectedIndex = serversel;
                        if (eventnamesel && (eventnamesel != ""))
                            document.getElementById("EventName").value = eventnamesel;
                        if (pusheventnamesel && (pusheventnamesel != ""))
                            document.getElementById("PushEventName").value = pusheventnamesel;
                        if (pusheventtextsel && (pusheventtextsel != ""))
                            document.getElementById("PushEventText").value = pusheventtextsel;

                        initSelection('resultsTable');
                     }       
                     function push()
                     {
                        var pushevent = document.getElementById("PushEventName");
                        var pusheventtext = document.getElementById("PushEventText");
                        if (pushevent.value != "" && pusheventtext.value != "")
                        {
                            document.location.href = "/WsWorkunits/WUPushEvent?EventName=" + pushevent.value + "&EventText=" + pusheventtext.value;
                        }
                     }
                     function search(type, str)
                     {
                        var url = "/WsWorkunits/WUShowScheduled";
                        if (type < 1)
                        {
                            var event = document.getElementById("EventName");
                            var ecl = document.getElementById("Clusters");
                            if (ecl.selectedIndex > 0)
                            {
                                var selected=ecl.options[ecl.selectedIndex];
                var value = selected.name;
                if (!value)
                  value = selected.value;
                                url += ("?Cluster=" + value);
                                if (event.value != "")
                                {
                                    url += ("&EventName=" + event.value);
                                }
                            }
                            else if (event.value != "")
                            {
                                url += ("?EventName=" + event.value);
                            }
                        }
                        else if (type < 2 && (str != ""))
                        {
                            url += ("?Cluster=" + str);
                            if (eventnamesel != "")
                            {
                                url += ("&EventName=" + eventnamesel);
                            }
                        }
                        else if (type < 3 && (str != ""))
                        {
                            url += ("?EventName=" + str);
                            if (serversel > 0)
                            {
                                var ecl = document.getElementById("Clusters");
                                var selected=ecl.options[serversel];
                var value = selected.name;
                if (!value)
                  value = selected.value;
                                url += ("&Cluster=" + value);
                            }                           
                        }
                        document.location.href=url;
                     }
               ]]></xsl:text>
            </script>
         </head>
        <body class="yui-skin-sam" onload="nof5();onLoad()">
           <h3>Event Scheduled Workunits</h3>
           <tr>
               <td>EventName:
                  <input type="text" name="PushEventName" id="PushEventName"/>
               </td>
               <td>EventText:
                  <input type="text" name="PushEventText" id="PushEventText"/>
               </td>
               <td>
                  <input type="submit" class="sbutton" name="Type" id="pushBtn" value="PushEvent" onclick="return push()"/>
               </td>
           </tr>
           <br/><br/>
           <tr>
               <td>Cluster:
           <select id="Clusters" name="Clusters" size="1">
                    <option/>
                    <xsl:for-each select="Clusters/ServerInfo/Name">
                        <option>
                            <xsl:attribute name="name"><xsl:value-of select="."/></xsl:attribute>
                            <xsl:value-of select="."/>
                        </option>
                    </xsl:for-each>
                  </select>
               </td>
               <td>EventName:
                  <input type="text" name="EventName" id="EventName"/>
               </td>
               <td>
                  <input type="submit" class="sbutton" name="Type" id="searchBtn" value="Search" onclick="return search(0, '')"/>
               </td>
           </tr>
           <br/><br/>
           <xsl:choose>
              <xsl:when test="Workunits/ScheduledWU[1]">
                <form id="listitems" action="/WsWorkunits/WUAction?{/WUShowScheduledResponse/Query}" method="post">
                    <table class="sort-table" id="resultsTable">
                        <colgroup>
                            <col width="5"/>
                            <col width="300" align="left"/>
                            <col width="100"/>
                            <col width="150"/>
                            <col width="150"/>
                            <col width="150"/>
                        </colgroup>
                        <thead>
                            <tr>
                                <th></th><th>WUID</th><th>Cluster</th><th>Job Name</th><th>Event Name</th><th>Event Text</th>
                            </tr>
                        </thead>
                        <tbody>
                            <xsl:apply-templates/>
                        </tbody>
                    </table>
                    <!--xsl:if test="Workunits/ScheduledWU[2]">
                        <table class="select-all">
                            <tr>
                                <th id="selectAll">
                                    <input type="checkbox"  id="All1" title="Select or deselect all workunits" onclick="selectAll(this.checked)"/>
                                </th>
                                <th>Select All / None</th>
                            </tr>
                        </table>
                    </xsl:if-->

                    <table style="margin:10 0 0 0" id="btnTable" name="btnTable">
                        <tr>
                            <td>
                                <input type="submit" class="sbutton" name="ActionType" id="deleteBtn" value="Deschedule" disabled="true" onclick="return confirm('Delete selected workunits from schedule list?')"/>
                            </td>
                        </tr>
                    </table>
                  </form>
              </xsl:when>
              <xsl:otherwise>
                 No workunits found.<br/>
              </xsl:otherwise>
           </xsl:choose>
         </body>
      </html>
   </xsl:template>
   
   
   <xsl:template match="ScheduledWU">
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
            <input type="checkbox" name="Wuids_i{position()}" value="{Wuid}" onclick="return clicked(this, event)"/>
         </td>
         <td>
            <a href="javascript:go('/WsWorkunits/WUInfo?Wuid={Wuid}')">
               <xsl:value-of select="Wuid"/>
            </a>
         </td>
         <td>
             <xsl:choose>
                <xsl:when test="$clustersel='0'">
                    <a href="javascript:search(1, '{Cluster}')">
                    <xsl:value-of select="Cluster"/></a>
                </xsl:when>
                <xsl:otherwise>
                      <xsl:value-of select="Cluster"/>
                </xsl:otherwise>
             </xsl:choose>
         </td>
         <td>
                <xsl:value-of select="JobName"/>
            </td>
         <td>
             <xsl:choose>
                <xsl:when test="string-length(EventName) and not(string-length($eventname))">
                    <a href="javascript:search(2, '{EventName}')">
                    <xsl:value-of select="EventName"/></a>
                </xsl:when>
                <xsl:otherwise>
                      <xsl:value-of select="EventName"/>
                </xsl:otherwise>
             </xsl:choose>
         </td>
         <td>
            <xsl:value-of select="EventText"/>
         </td>
      </tr>
   </xsl:template>
   <xsl:template match="text()|comment()"/>
</xsl:stylesheet>
