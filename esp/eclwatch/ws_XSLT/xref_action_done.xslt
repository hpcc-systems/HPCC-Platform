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
