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
    <xsl:variable name="backtopage" select="/DFUArrayActionResponse/BackToPage"/>

    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script type="text/javascript">
                    var backToPageLink='<xsl:value-of select="$backtopage"/>';
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        // This function gets called when the window has completely loaded.

                       function onLoad()
                       {
                            var sectionDiv = document.getElementById("StatusDiv");
                            if (sectionDiv)
                            {
                                var parentSectionDiv = parent.document.getElementById("StatusDiv");
                                if (parentSectionDiv)
                                {
                                    parentSectionDiv.innerHTML = sectionDiv.innerHTML; 
                                }
                            }

                            return;
                        }              

                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                    <div id="StatusDiv">
                        <h3>Action status:</h3>
                        <table>
                            <xsl:for-each select="//Message">
                                <xsl:choose>             
                                    <xsl:when test="string-length(href)">
                                        <a href="{href}"><xsl:value-of select="Value"/></a>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:value-of select="Value"/>
                                    </xsl:otherwise>
                                </xsl:choose>
                                <br/>
                            </xsl:for-each>
                        </table>
                        <br/>
                        <xsl:choose>             
                            <xsl:when test="$backtopage!=''">
                                <form onsubmit="return backToPage('{$backtopage}')" method="post">
                                    <input type="submit" class="sbutton" value="Back"/>
                                </form>
                            </xsl:when>
                            <xsl:otherwise>
                                <input id="backBtn" type="button" value="Go Back" onclick="history.go(-1)"> </input>
                            </xsl:otherwise>
                        </xsl:choose>
                    </div>
            </body>
        </html>
    </xsl:template>

</xsl:stylesheet>
