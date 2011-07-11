<?xml version="1.0" encoding="utf-8"?>
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
    <xsl:variable name="addresses" select="StartStopBeginResponse/Addresses"/>
  <xsl:variable name="key1" select="StartStopBeginResponse/Key1"/>
  <xsl:variable name="key2" select="StartStopBeginResponse/Key2"/>
  <xsl:variable name="stop" select="StartStopBeginResponse/Stop"/>

  <xsl:template match="StartStopBeginResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <script type="text/javascript">
          var addresses0 = '<xsl:value-of select="$addresses"/>';
          var key10 = '<xsl:value-of select="$key1"/>';
          var key20 = '<xsl:value-of select="$key2"/>';
          var stop0 = '<xsl:value-of select="$stop"/>';

          <xsl:text disable-output-escaping="yes"><![CDATA[
                        // This function gets called when the window has completely loaded.
                        var intervalId = 0;
                        var hideLoading = 1;

          function onStartStop()
          {
            var dataFrame = document.getElementById('MsgFrame');
            if (dataFrame)
            {
              var para = addresses0.replace(/&amp;/g, "&");
              var ref = '/ws_machine/StartStopDone?Addresses='+para+'&Key1='+key10;
              ref += '&Key2='+key20+'&Stop='+stop0;
              dataFrame.src = ref;
            }

            return true;
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

          function startBlink() 
          {
            if (document.all)
            {
              hideLoading = 0;
              intervalId = setInterval("doBlink()",500);
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
          }

          function onLoad()
          {
            startBlink();
            reloadTimer = setTimeout("loadPageTimeOut()", 300000);

            onStartStop();

            return;
          }              

                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="onLoad()">
        <form name="MachineItems" action="/ws_machine/GetMachineInfo?GetSoftwareInfo=1" method="post">
          <div id="ViewMsg">
                      <span id="loadingMsg"><h3>Running, please wait...</h3></span>
                      <span id="loadingTimeOut" style="visibility:hidden"><h3>Browser timed out due to a long time delay.</h3></span>
                  </div>
        </form>
                <iframe id="MsgFrame" name="MsgFrame" style="display:none; visibility:hidden;"></iframe>
            </body>
        </html>
    </xsl:template>

</xsl:stylesheet>
