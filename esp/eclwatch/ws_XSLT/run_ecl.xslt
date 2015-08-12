<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2012 HPCC Systems®.

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
