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
    <xsl:template match="Thor">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
          <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title><xsl:value-of select="Name"/></title>
            <style type="text/css">
                body { background-color: #white;}
                .cluster { text-align:left; }
            </style>
          </head>
          <body>
            <table>
                <colgroup>
                   <col width="200" class="cluster"/>
                   <col width="300" class="cluster"/>
                </colgroup>
                <tr><th>Thor:</th><td><a href="/thor/{Name}/xml"><xsl:value-of select="Name"/></a></td></tr>
                <tr><th>WorkUnit:</th>
                    <td>
                    <xsl:choose>
                        <xsl:when test="string-length(Wuid)"> 
                            <a href="/wuid/{Wuid}"><xsl:value-of select="Wuid"/></a>
                        </xsl:when>
                        <xsl:otherwise>
                            none
                        </xsl:otherwise>
                    </xsl:choose>
                    </td>
                </tr>
                <tr><th>Master log file:</th>
                    <td>
                    <a href="/thor/{Name}/thormaster.log">thormaster.log</a>
                    </td>
                </tr>
            </table>
          </body> 
        </html>
    </xsl:template>
</xsl:stylesheet>
