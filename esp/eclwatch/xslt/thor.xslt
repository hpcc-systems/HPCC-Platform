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
