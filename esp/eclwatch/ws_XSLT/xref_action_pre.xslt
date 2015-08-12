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
