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
    <xsl:param name="actualSize" select="File/ActualSize"/>
    <xsl:template match="File">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
          <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title><xsl:value-of select="Name"/></title>
            <style type="text/css">
                body { background-color: #white;}
                table.list { border-collapse: collapse; border: double solid #777; font: 10pt arial, helvetica, sans-serif; }
                table.list th, table.list td { border: 1 solid #777; padding:2px; }
                .grey { background-color: #ddd;}
                .number { text-align:right; }
            </style>
          </head>
          <body>
            <table style="text-align:left;" cellspacing="10">
            <colgroup style="vertical-align:top;padding-right:10px;" span="2">
            </colgroup>
                <tr><th>Logical Name:</th><td><xsl:value-of select="Name"/></td></tr>
                <tr><th>Modification Time:</th><td><xsl:value-of select="Modified"/></td></tr>
                <tr><th>Directory:</th><td><xsl:value-of select="Dir"/></td></tr>
                <tr><th>Pathmask:</th><td><xsl:value-of select="PathMask"/></td></tr>
                <xsl:if test="string-length(Wuid)">
                    <tr><th>Workunit:</th><td><a href="/wuid/{Wuid}"><xsl:value-of select="Wuid"/></a></td></tr>
                </xsl:if>
                <xsl:if test="string-length(Owner)">
                    <tr><th>Owner:</th><td><a href="/wuid?owner={Owner}"><xsl:value-of select="Owner"/></a></td></tr>
                </xsl:if>
                <xsl:if test="string-length(JobName)">
                    <tr><th>Job Name:</th><td><xsl:value-of select="JobName"/></td></tr>
                </xsl:if>
                <tr><th>Size:</th><td><xsl:value-of select="Size"/></td></tr>
                <xsl:if test="string-length(ActualSize)">
                    <tr><th>Actual Size:</th><td><xsl:value-of select="ActualSize"/></td></tr>
                </xsl:if>
                <xsl:if test="count(Stats)">
                    <tr><th>Min Skew:</th><td><xsl:value-of select="Stats/MinSkew"/></td></tr>
                    <tr><th>Max Skew:</th><td><xsl:value-of select="Stats/MaxSkew"/></td></tr>
                </xsl:if>
                <xsl:if test="string-length(Ecl)">
                    <tr><th>Ecl  (<a href="/dfu/{Name}/{Filename}.def">.def</a>)(<a href="/dfu/{Name}/{Filename}.xml">.xml</a>):</th><td><textarea readonly="true" rows="10" STYLE="width:500"><xsl:value-of select="Ecl"/></textarea></td></tr>
                </xsl:if>
                <xsl:if test="number(RecordSize)>0">
                    <tr><th>Record Size:</th><td><xsl:value-of select="RecordSize"/></td></tr>
                    <tr><th>Record Count:</th><td><xsl:value-of select="RecordCount"/></td></tr>
                </xsl:if>
                <xsl:if test="Persistent">
                    <tr><th>Persistent:</th><td><xsl:value-of select="Persistent"/></td></tr>
                </xsl:if>
                <xsl:if test="Format">
                    <tr><th>Format:</th><td><xsl:value-of select="Format"/></td></tr>
                </xsl:if>
                <xsl:if test="MaxRecordSize">
                    <tr><th>MaxRecordSize:</th><td><xsl:value-of select="MaxRecordSize"/></td></tr>
                </xsl:if>
                <xsl:if test="CsvSeparate">
                    <tr><th>CsvSeparate:</th><td><xsl:value-of select="CsvSeparate"/></td></tr>
                </xsl:if>
                <xsl:if test="CsvQuote">
                    <tr><th>CsvQuote:</th><td><xsl:value-of select="CsvQuote"/></td></tr>
                </xsl:if>
                <xsl:if test="CsvTerminate">
                    <tr><th>CsvTerminate:</th><td><xsl:value-of select="CsvTerminate"/></td></tr>   
                </xsl:if>
            </table>
            <br/><br/>
            <h4>File Parts:</h4>
            <table class="list" width="500">
            <colgroup style="vertical-align:top;">
            <col span="2"/>
            <col span="3" class="number"/>
            </colgroup>
            <tr class="grey"><th>Number</th><th>Copy</th><th>IPs</th><th>Size</th><xsl:if test="string-length($actualSize)"><th>Actual Size</th></xsl:if></tr>
            <xsl:apply-templates>
                <xsl:sort select="Id" data-type="number"/>
                <xsl:sort select="Copy" data-type="number"/>
            </xsl:apply-templates>
            </table>
          </body> 
        </html>
    </xsl:template>

    <xsl:template match="Part">
        <tr>
        <td><xsl:value-of select="Id"/></td>
        <td><xsl:value-of select="Copy"/></td>
        <td><xsl:value-of select="Ip"/></td>
        <td class="number"><xsl:value-of select="Size"/></td>
        <xsl:if test="string-length($actualSize)"><td class="number"><xsl:value-of select="ActualSize"/></td></xsl:if>
        </tr>
    </xsl:template>

    <xsl:template match="text()|comment()"/>
</xsl:stylesheet>
