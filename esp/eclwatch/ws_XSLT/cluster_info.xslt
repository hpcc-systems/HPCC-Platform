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
    <xsl:template match="TpClusterInfoResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
          <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title><xsl:value-of select="Name"/></title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      </head>
      <body class="yui-skin-sam" onload="nof5();">
            <h2>Thor Status</h2>
        
      <xsl:choose>
        <xsl:when test="TpQueues/TpQueue">
                <table class="sort-table" id="resultsTable">
                    <colgroup>
                        <col width="15%" align="left"/>
                        <col width="35%" align="left"/>
                        <col width="50%" align="left"/>
                    </colgroup>
                    <thead>
                        <tr>
                            <th colspan="2" align="left">Name</th>
                            <th>Workunit</th>
                        </tr>
                    </thead>
                    <tbody>
              <!--tr>
                            <td>
                                <a href="/WsTopology/TpLogFile/{Name}?Name={Name}&amp;Type=thormaster_log">
                                    <img border="0" src="/esp/files_/img/base.gif" alt="View latest log file" width="19" height="16"/>
                                </a>
                            </td>
                            <td>
                                <a href="/WsTopology/TpLogFile/{Name}?Name={Name}&amp;Type=xml">
                                    <xsl:value-of select="Name"/>
                                </a>
                            </td>
                            <td>
                                <xsl:choose>
                                    <xsl:when test="string-length(WorkUnit)"> 
                                        <a><xsl:value-of select="WorkUnit"/></a>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        none
                                    </xsl:otherwise>
                                </xsl:choose>
                            </td>
                        </tr>
                        <xsl:if test="string-length(Name2)">
                            <tr>
                                <td>
                                    <a href="/WsTopology/TpLogFile/{Name2}?Name={Name2}&amp;Type=thormaster_log">
                                        <img border="0" src="/esp/files_/img/base.gif" alt="View latest log file" width="19" height="16"/>
                                    </a>
                                </td>
                                <td>
                                    <a href="/WsTopology/TpLogFile/{Name2}?Name={Name2}&amp;Type=xml">
                                        <xsl:value-of select="Name2"/>
                                    </a>
                                </td>
                                <td>
                                    <xsl:choose>
                                        <xsl:when test="string-length(WorkUnit2)"> 
                                            <a><xsl:value-of select="WorkUnit2"/></a>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            none
                                        </xsl:otherwise>
                                    </xsl:choose>
                                </td>
                            </tr>
                        </xsl:if-->
              <xsl:apply-templates select="TpQueues/TpQueue">
                <xsl:sort select="Name"/>
              </xsl:apply-templates>
            </tbody>
          </table>
        </xsl:when>
        <xsl:otherwise>
          <br/>No queue defined.
        </xsl:otherwise>
      </xsl:choose>
        
            <!--table>
                <colgroup>
                   <col width="200" class="cluster"/>
                   <col width="300" class="cluster"/>
                </colgroup>
                <tr><th>Thor:</th><td><a href="/WsTopology/TpLogFile/{Name}?Name={Name}&amp;Type=xml"><xsl:value-of select="Name"/></a></td></tr>
                <tr><th>WorkUnit:</th>
                    <td>
                    <xsl:choose>
                        <xsl:when test="string-length(WorkUnit)"> 
                            <a><xsl:value-of select="WorkUnit"/></a>
                        </xsl:when>
                        <xsl:otherwise>
                            none
                        </xsl:otherwise>
                    </xsl:choose>
                    </td>
                </tr>
                <tr><th>Master log file:</th>
                    <td>
                    <a href="/WsTopology/TpLogFile/{Name}?Name={Name}&amp;Type=thormaster_log">thormaster.log</a>
                    </td>
                </tr>
            </table>
            <xsl:if test="string-length(Name2)">
                <br/>
                <table>
                    <colgroup>
                       <col width="200" class="cluster"/>
                       <col width="300" class="cluster"/>
                    </colgroup>
                    <tr><th>Thor 2:</th><td><a href="/WsTopology/TpLogFile/{Name2}?Name={Name2}&amp;Type=xml"><xsl:value-of select="Name2"/></a></td></tr>
                    <tr><th>WorkUnit:</th>
                        <td>
                        <xsl:choose>
                            <xsl:when test="string-length(WorkUnit2)"> 
                                <a><xsl:value-of select="WorkUnit2"/></a>
                            </xsl:when>
                            <xsl:otherwise>
                                none
                            </xsl:otherwise>
                        </xsl:choose>
                        </td>
                    </tr>
                    <tr><th>Master log file:</th>
                        <td>
                        <a href="/WsTopology/TpLogFile/{Name2}?Name={Name2}&amp;Type=thormaster_log">thormaster.log</a>
                        </td>
                    </tr>
                </table>
            </xsl:if-->
          </body> 
        </html>
    </xsl:template>

  <xsl:template match="TpQueue">
    <tr>
      <td>
        <a href="/WsTopology/TpLogFile/{Name}?Name={Name}&amp;Type=thormaster_log">
          <img border="0" src="/esp/files_/img/base.gif" alt="View latest log file" width="19" height="16"/>
        </a>
      </td>
      <td>
        <a href="/WsTopology/TpThorStatus?Name={Name}">
          <xsl:value-of select="Name"/>
        </a>
      </td>
      <td>
        <xsl:choose>
          <xsl:when test="string-length(WorkUnit)">
            <a>
              <xsl:value-of select="WorkUnit"/>
            </a>
          </xsl:when>
          <xsl:otherwise>
            none
          </xsl:otherwise>
        </xsl:choose>
      </td>
    </tr>
  </xsl:template>

</xsl:stylesheet>
