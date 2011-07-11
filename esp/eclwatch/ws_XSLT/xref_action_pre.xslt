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
    <xsl:variable name="parameters" select="/DFUXRefArrayActionResponse/DFUXRefArrayActionResult"/>
    <xsl:template match="/">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <script language="JavaScript1.2">
                var parametersLink='<xsl:value-of select="$parameters"/>';
                <xsl:text disable-output-escaping="yes"><![CDATA[
                    function backToPage()
                    {
                        document.location.href = 'backToPageLink';
                        return false;
                    }
                ]]></xsl:text>
            </script>       
            <script type="text/javascript">
                <xsl:text disable-output-escaping="yes"><![CDATA[
                        // This function gets called when the window has completely loaded.
                        var intervalId = 0;
                        var hideLoading = 1;

                        function startBlink() 
                        {
                            if (document.all)
                            {
                                hideLoading = 0;
                                intervalId = setInterval("doBlink()",500);
                            }
                        }

                        function doBlink() 
                        {
                            var obj = document.getElementById('loadingMsg');
                            if (obj)
                            {
                                obj.style.visibility = obj.style.visibility == "" ? "hidden" : "";
                             
                                if (hideLoading > 0 && intervalId && obj.style.visibility == "hidden")
                                {              
                                    clearInterval (intervalId);
                                    intervalId = 0;
                                }   
                            }
                        }

                      function loadPageTimeOut() 
                        {
                            hideLoading = 1;

                            var obj = document.getElementById('loadingMsg');
                            if (obj)
                                obj.style.display = "none";

                            var obj1 = document.getElementById('loadingTimeOut');
                            if (obj1)
                                obj1.style.visibility = "";

                            ///document.getElementById('GetLog').disabled = false;
                            ///document.getElementById("GetLog").focus();
                        }

                       function onActionWork()
                        {
                            var ref = '/WsDfuXRef/DFUXRefArrayActionWork?'+parametersLink.replace(/&amp;/g, "&");
                        //alert(ref);
                            var dataFrame = document.getElementById('DataFrame');
                            if (dataFrame)
                            {
                                dataFrame.src = ref;
                            }
                          
                            return true;
                        }

                       function onLoad()
                       {
                            //document.getElementById('loadingResult').disabled = true;

                            startBlink();
                            reloadTimer = setTimeout("loadPageTimeOut()", 300000);

                            onActionWork();

                            return;
                        }              

                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <div id="StatusDiv">
                    <span id="loadingMsg"><h3>Busy, please wait...</h3></span>
                    <span id="loadingTimeOut" style="visibility:hidden"><h3>Browser timed out due to a long time delay.</h3></span>
                </div>
                <iframe id="DataFrame" name="DataFrame" style="display:none; visibility:hidden;"></iframe>
            </body>
        </html>
    </xsl:template>

</xsl:stylesheet>
