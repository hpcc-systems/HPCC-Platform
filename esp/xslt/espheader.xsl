<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:template match="EspHeader">
        <html>
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script language="JavaScript1.2">
        <xsl:text disable-output-escaping="yes"><![CDATA[
          function go(url)
          {
                if (isChrome)
                   url = url + "&PlainText=yes"
                var f=top.frames['main'];
                if (f)
                   f.location=url;
                return false;
          }

          function logout()
          {
            parent.location = 'logout';
          }

          function copy_url(inner)
          {
            var ploc = top.location;
            var url = ploc.protocol + "//" + ploc.host + ploc.pathname + "?legacy";
            if ((inner.pathname != "" && inner.pathname != '/') || inner.search!="")
            {
              var b = false;
              var s1 = inner.search;
              if (s1.split("?").length > 1)
              {
                s2 = s1.split("?")[1];
                if (s2.split("&").length > 1)
                {
                  s3 = s2.split("&")[0];
                  s4 = s2.split("&")[1];
                  if ((s3.indexOf('inner=') == 0) && (s4.indexOf('esp_iframe_title=') == 0))
                  {
                    b = true;
                    url += '&inner=' + escape('..' + inner.pathname + '?' + s4 + '&' + s3);
                  }
                }
              }
              if (!b)
                url += '&inner=' + escape('..' + inner.pathname + inner.search);
            }

            var html = '<html><head><title>View URL</title>';
            html += '</head><body ><form>';
                    html += "<table border=\"0\" cellpadding=\"2\" cellspacing=\"2\">";
                    var usequote = "\"";
                    html += "<tr><td><a href=" +  usequote + url + usequote + "><b>full:</b></a>&nbsp;</td><td>" + url + "</td></tr>";
                    html += "<tr><td><a href=" +  usequote + ploc + usequote + "><b>top:</b></a>&nbsp;</td><td>" + ploc + "</td></tr>";                 
            html += '</table></form>';
            html += '</body></html>';

            var windowWidth = 800;
            var windowHeight = 500;
            var centerWidth = (window.screen.width - windowWidth) / 2;
            var centerHeight = (window.screen.height - windowHeight) / 2;

                    var args='width='+ windowWidth+ ',height='+ windowHeight+ ',left='+ centerWidth+ ',top='+ centerHeight+ ',toolbar=0,';
            args+='location=0,status=0,menubar=0,scrollbars=0,resizable=0';  
            var wnd = window.open("about:blank","COPY_URLs",args); 
            if (wnd)
            {
              wnd.document.open(); 
              wnd.document.write(html); 
              wnd.document.close(); 
              wnd.focus(); 
            }
                }                    
        ]]></xsl:text></script>
                <title>top_</title>
            </head>
      <body TOPMARGIN="0" LEFTMARGIN="0" RIGHTMARGIN="0" MARGINWIDTH="0" MARGINHEIGHT="0" class="yui-skin-sam" onload="nof5();">
        <table border="0" height="57" width="100%">
          <tr>
            <td height="51" width="160" rowspan="2">
              <a href="/" onclick="top.location.href='/'">
                <img border="0" src="files_/img/hpccsystemsECLWatch.png" title="ESP Home"/>
              </a>
            </td>
            <td height="25" width="100%">
              <table border="0" height="100%" width="100%">
                <tr>
                  <td id="AppName" width="70%"/>
                  <td width="30%">
                    <p align="center">
                      <b>
                        <font size="3">Enterprise Services Platform</font>
                      </b>
                    </p>
                  </td>
                </tr>
                <tr>
                  <td height="23" style="text-align:left;">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                    <a href="../?main" onclick="return go('../?main')">
                      <img border="0" src="files_/img/home.png" title="Home" width="14" height="14"/>
                    </a>
                    <xsl:if test="ConfigAccess='1'">
                      <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                      <img onclick="go('iframe?inner=../main/%3Fconfig_&amp;esp_iframe_title=ESP%20Configuration%20File')"
                      border="0" src="files_/img/config.png" title="View Configuration" width="14" height="14"/>
                    </xsl:if>
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                    <img src="files_/img/refresh.png" style="cursor:pointer" onclick="parent.frames['main'].location.reload();" title="Refresh" width="13" height="15"/>
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                    <img src="files_/img/copyurl.png" style="cursor:pointer" onclick="copy_url(parent.frames['main'].location)" title="View URL" width="13" height="15"/>
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                    <img border="0" src="files_/img/topurl.png" title="No Frames" width="13" height="15" style="cursor:pointer"
                    onclick="top.location.href=top.frames['main'].location.href"/>
                    <xsl:if test="LogOut = '1'">
                        <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                        <a onclick="return logout();">
                            Logout
                        </a>
                    </xsl:if>
                    <noscript>
                      <span style="color:red;">
                        <small>JavaScript needs to be enabled for the Enterprise Services Platform to work correctly.</small>
                      </span>
                    </noscript>
                  </td>
                  <td>
                    <p align="center">
                      <strong>
                        <xsl:value-of select="BuildVersion"/>
                      </strong>
                    </p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>
      </body>
    </html>
  </xsl:template>
</xsl:stylesheet>
