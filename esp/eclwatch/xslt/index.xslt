<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
          <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>EclWatch</title>
            <style type="text/css">
                body { background-color: #white;}
                table { padding:3px; }
                .number { text-align:right; }
                .cluster { text-align:left; }
                .sbutton { width: 5em; font: 10pt arial , helvetica, sans-serif; }
            </style>
            <script language="JavaScript1.2">
            <xsl:text disable-output-escaping="yes">
            function goWorkunit()
            {
                var wuid=document.all.wuid.value;
                document.location='/wuid/'+wuid;
                return false;
            }
            function searchWorkunits()
            {
                var user=document.all.userid.value;
                var index=document.all.state.selectedIndex;
                var state;
                if(index>0 &amp;&amp; document.all.state.options(index))
                    state=document.all.state.options(index).text;

                var url='';
                if(user)
                {
                    url=url+'?owner='+user;
                }
                if(state)
                {
                    url=url+(url ? '&amp;' : '?')+'state='+state;
                }

                document.location='/wuid'+url;
                return false;
            }
            </xsl:text>
            </script>
          </head>
          <body>
                <xsl:apply-templates/>
                <h4><br/>Workunits</h4>

                    <form onsubmit="return goWorkunit()">
                    <table>
                        <tr>
                        <td>Wuid:</td>
                        <td><input id="wuid" size="25" type="text"></input></td>
                        <td><input type="submit" value="Open"  class="sbutton"></input></td>
                        </tr>
                    </table>
                    </form>
                    <form onsubmit="return searchWorkunits()">
                    <table>
                        <tr>
                        <td>Username:</td><td><input id="userid" size="12" type="text"></input></td>
                        <td><input type="submit" value="Find" class="sbutton"></input></td>
                        </tr>
                        <tr>
                        <td>State:</td>
                        <td>
                            <select id="state" size="1">
                            <option></option>
                            <option>unknown</option>
                            <option>compiled</option>
                            <option>running</option>
                            <option>completed</option>
                            <option>failed</option>
                            <option>archived</option>
                            <option>aborting</option>
                            <option>aborted</option>
                            <option>blocked</option>
                            </select>
                        </td>
                        </tr>
                    </table>
                    </form>
                    <a href="/wuid">All Workunits</a><span style="padding:20">My Workunits</span><br/><br/>
             
                <h4><br/>
                    <a href="/dfu">DFU</a><br/>
                </h4>
          </body> 
        </html>
    </xsl:template>

    <xsl:template match="Clusters">
        <h4>Thor Clusters:</h4>
        <table class='clusters'>
            <colgroup>
               <col width="150" class="cluster"/>
               <col width="300" class="cluster"/>
               <col width="100" class="cluster"/>
               <col width="100" class="cluster"/>
            </colgroup>
            <tr><th>Cluster</th><th>Active workunit</th><th>Owner</th><th>Job name</th></tr>
            <xsl:apply-templates/>
        </table>
    </xsl:template>

    <xsl:template match="Cluster">
        <tr>
        <td><a href="/thor/{Name}"><xsl:value-of select="Name"/></a></td>
        <td>
            <xsl:if test="Wuid"><a href="/wuid/{Wuid}"><xsl:value-of select="Wuid"/></a></xsl:if>
        </td>
        <td><xsl:value-of select="Owner"/></td>
        <td><xsl:value-of select="JobName"/></td>
        </tr>
    </xsl:template>
</xsl:stylesheet> 
