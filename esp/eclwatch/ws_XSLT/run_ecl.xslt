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
    <xsl:template match="RunEclEx">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
                <title>Run Ecl form</title>
                <script language="JavaScript1.2">
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                    function hideOption(obj) {
                        var option_span = document.getElementById("option_span");
                        if (option_span.style.display=='block'){
                            option_span.style.display='none';
                            obj.value='options >>';
                        } else {
                            option_span.style.display='block';
                            obj.value='<< options';
                        }
                    }
                    ]]></xsl:text>
                </script>
            </head>
            <body>
                <xsl:apply-templates/>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="EclWatchRunEcl">
        <h4>Submit ECL text for execution:</h4>
        <form method="POST" action="/EclDirect/RunEclEx">
            <table cellSpacing="0" cellPadding="0" width="90%" border="0">
                <tbody>
                    <tr>
                        <td valign="top"><b>ECL Text: </b></td>
                        <td>
                            <textarea name="eclText" rows="20" cols="80" ></textarea>
                        </td>
                    </tr>
                    <tr>
                        <td><b>Cluster: </b></td>
                        <td>
                            <select name="cluster" >
                                <option value="default" selected="1" >default</option>
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
                    <tr>
                        <td>
                            <b>Output: </b>
                        </td>
                        <td>
                            <select name="outputFormat" >
                                <option value="TABLE" selected="1">Table</option>
                                <option value="XML" >XML</option>
                                <option value="EXTENDEDXML" >Extended XML</option>
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td/>
                        <td>
                            <input type="checkbox" name="limitResults" checked="1" value="1"/> Limit Result Count to 100.
                        </td>
                    </tr>
                    <tr>
                        <td/>
                        <td>
                            <input type="submit" value="Submit" name="S1" />
                        </td>
                    </tr>
                </tbody>
            </table>
        </form>
    </xsl:template>

    <xsl:template match="RunEcl">
        <p align="center" />
        <table cellSpacing="0" cellPadding="1" width="90%" bgColor="#4775FF" border="0">
            <tbody>
                <tr align="middle" bgColor="#4775FF">
                    <td height="23">
                        <p align="left">
                            <font color="#efefef">
                                <b><xsl:value-of select="ServiceQName"/> [Version <xsl:value-of select="ClientVersion"/>]</b>
                            </font>
                        </p>
                    </td>
                </tr>
                <tr bgColor="#CBE5FF">
                    <td height="3">
                        <p align="left">
                            <b>&gt; <xsl:value-of select="MethodQName"/></b>
                        </p>
                    </td>
                </tr>
                <tr bgColor="#666666">
                    <table cellSpacing="0" width="90%" bgColor="#efefef" border="0">
                        <tbody>
                            <tr>
                                <td vAlign="center" align="left">
                                    <p align="left">
                                        <br/>Submit ECL text for execution.
                                    </p>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </tr>
                <tr bgColor="#666666">
                    <table cellSpacing="0" width="90%" bgColor="#efefef" border="0">
                        <tbody>
                            <tr>
                                <td vAlign="center" align="left">
                                    <form method="POST" action="/EclDirect/RunEcl">
                                        <p align="left">
                                            <b>ECL Text: </b>
                                            <br/>
                                            <textarea name="eclText" rows="20" cols="80" ></textarea>
                                            <br/>
                                            <input type='button' value='options >>' onclick="hideOption(this);"/>
                                            <span id="option_span" style="display:none">
                                                <br/>
                                                <b>Cluster: </b><br/>
                                                <select name="cluster" >
                                                    <option value="default" selected="1" >default</option>
                                                    <xsl:for-each select="Cluster">
                                                        <option>
                                                            <xsl:value-of select="."/>
                                                        </option>
                                                    </xsl:for-each>
                                                </select>
                                                <br/>
                                                <br/>
                                                <b>Repository Label (Legacy): </b>
                                                <br/>
                                                <input type="text" name="snapshot"/>
                                                <br/>
                                                <br/>
                                                <input type="checkbox" name="limitResults" checked="1" value="1"> Limit Result Count to 100.</input>
                                                <br/>
                                                <input type="checkbox" name="rawxml_" > Output Xml?</input>
                                            </span>
                                            <br/><input type="submit" value="Submit" name="S1" />
                                        </p>
                                    </form>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </tr>
            </tbody>
        </table>
    </xsl:template>

    <xsl:template match="*|@*|text()"/>
</xsl:stylesheet>
