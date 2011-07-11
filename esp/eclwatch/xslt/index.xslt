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
