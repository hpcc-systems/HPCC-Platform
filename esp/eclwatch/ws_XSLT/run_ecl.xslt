<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2012 HPCC Systems.

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
    <xsl:template match="RunEclEx">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Run Ecl form</title>
            </head>
            <body>
                <xsl:call-template name="form"/>
            </body>
        </html>
    </xsl:template>
    <xsl:template name="form">
        <h4>Submit ECL text for execution:</h4>
        <form method="POST">
            <xsl:attribute name="action">
                <xsl:choose>
                    <xsl:when test="Redirect='Yes'">/EclDirect/RunEclEx?redirect</xsl:when>
                    <xsl:otherwise>/EclDirect/RunEclEx/DisplayResult</xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
            <table cellSpacing="0" cellPadding="0" width="90%" border="0">
                <tbody>
                    <br/>
                    <br/>
                    <tr>
                        <td valign="top">
                            <b>ECL Text: </b>
                        </td>
                        <td>
                            <textarea name="eclText" rows="20" cols="80" >
                            </textarea>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <b>Cluster: </b>
                        </td>
                        <td>
                            <select name="cluster">
                                <xsl:for-each select="Cluster">
                                    <option>
                                        <xsl:value-of select="."/>
                                    </option>
                                </xsl:for-each>
                            </select>
                        </td>
                    </tr>
                    <xsl:if test="UseEclRepository='Yes'">
                        <tr>
                            <td>
                                <b>Repository Label </b>
                            </td>
                            <td valign="top" rowspan="2">
                                <input type="text" name="snapshot"/>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <b>(Legacy): </b>
                            </td>
                        </tr>
                    </xsl:if>
                    <xsl:if test="IncludeResults='Yes'">
                        <tr>
                            <input type="hidden" value="1" name="includeResults"/>
                            <td>
                                <b>Output: </b>
                            </td>
                            <td>
                                <select name="format" >
                                    <option value="Table" selected="1">Table</option>
                                    <option value="Xml" >XML</option>
                                    <option value="ExtendedXml" >Extended XML</option>
                                </select>
                            </td>
                        </tr>
                     </xsl:if>
                     <tr>
                        <td/>
                        <td>
                            <input type="checkbox" name="resultLimit" checked="1" value="100"/> Limit Result Count to 100.
                        </td>
                    </tr>
                    <tr>
                        <td/>
                        <td>
                            <input type="submit" value="Submit" name="S1"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </form>
    </xsl:template>
    <xsl:template match="*|@*|text()"/>
</xsl:stylesheet>
