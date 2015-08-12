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
    <xsl:variable name="prevpage" select="TpLogFileResponse/PrevPage"/>
    <xsl:variable name="nextpage" select="TpLogFileResponse/NextPage"/>
    <xsl:variable name="reversely" select="TpLogFileResponse/Reversely"/>

    <xsl:template match="TpLogFileResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <link type="text/css" rel="stylesheet" href="/esp/files_/default.css"/>
                <link type="text/css" rel="stylesheet" href="/esp/files_/css/espdefault.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <script type="text/javascript" src="/esp/files_/joblist.js">0</script>
                <script type="text/javascript">
                    var prevPage = <xsl:value-of select="$prevpage"/>;
                    var nextPage = <xsl:value-of select="$nextpage"/>;
                    var reversely0 = <xsl:value-of select="$reversely"/>;
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        // This function gets called when the window has completely loaded.

                       function onLoad()
                       {
                            var obj = document.getElementById('LogText');
                            if (obj)
                            {
                                obj.style.width = parent.document.body.clientWidth-20;
                                obj.style.height = parent.document.body.clientHeight-180;
                            }

                            var sectionDiv = document.getElementById("ViewLogData");
                            if (sectionDiv)
                            {
                                var parentSectionDiv = parent.document.getElementById("ViewLogData");
                                if (parentSectionDiv)
                                {
                                    parentSectionDiv.innerHTML = sectionDiv.innerHTML; 
                                }
                            }

                            if (prevPage > -1)
                                parent.document.getElementById('PrevPage').disabled = false;
                            if (nextPage > -1)
                                parent.document.getElementById('NextPage').disabled = false;

                            parent.document.getElementById('GetLog').disabled = false;
                            if (reversely0)
                                parent.document.getElementById("GetLogR").focus();
                            else
                                parent.document.getElementById("GetLog").focus();

                            return;
                        }              

                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <form name="ViewLogForm" >
                    <div id="ViewLogData">
                        <xsl:choose>
                            <xsl:when test="string-length(LogData)">
                                <div>
                                  <textarea id="LogText" readonly="true" wrap="off" rows="30" style="width:800px">
                                     <xsl:value-of select="LogData"/>
                                  </textarea>
                                </div>
                                <table>
                                    <tr>
                                        <td>Total file size: <xsl:value-of select="FileSize"/> bytes; </td>
                                        <xsl:if test="number(PageTo) > 0">
                                            <td>this page from byte <xsl:value-of select="number(PageFrom) + 1"/> to byte <xsl:value-of select="PageTo"/></td>
                                        </xsl:if>
                                    </tr>
                                    <xsl:if test="number(FileSize) > 4000000">
                                        <tr>
                                            <td colspan="2"><b>Warning: Using time based search may take a long time for a large file. Browser may be timed out.</b></td>
                                        </tr>
                                    </xsl:if>
                                </table>
                            </xsl:when>
                            <xsl:otherwise>
                                <table>
                                    <tr>
                                        <td><br/>No data found</td>
                                    </tr>
                                </table>
                            </xsl:otherwise>
                        </xsl:choose>
                    </div>
                </form>
            </body>
        </html>
    </xsl:template>

</xsl:stylesheet>
