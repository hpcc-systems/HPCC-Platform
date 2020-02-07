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

<!DOCTYPE xsl:stylesheet [
    <!ENTITY nbsp "&#160;">
    <!ENTITY apos "&#39;">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format"
    exclude-result-prefixes="fo">
    <xsl:output method="html" indent="yes" omit-xml-declaration="yes"
      doctype-public="-//W3C//DTD HTML 4.01 Transitional//EN"/>
    <!-- ===============================================================================
  parameters
  ================================================================================ -->
    <xsl:param name="pageName" select="'JSON Test'"/>
    <xsl:param name="serviceName" select="'FormTest'"/>
    <xsl:param name="methodName" select="'BasicTest'"/>
    <xsl:param name="destination" select="'zz'"/>
    <xsl:param name="header" select="'xx'"/>
    <!-- ===============================================================================-->
    <xsl:template match="/">
    <html>
      <head>
        <title>JSON Test Page</title>
        <link rel="shortcut icon" href="files_/img/affinity_favicon_1.ico" />
        <link rel="stylesheet" type="text/css" href="files_/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="files_/css/espdefault.css" />

        <script>dojoConfig = {async:true, parseOnLoad:false}</script>
        <script src="files_/dist/dojoLib.eclwatch.js"></script>
    </head>
    <body class="yui-skin-sam" id="body">
      <h3>
        <table cellSpacing="0" cellPadding="1" width="100%" bgColor="#4775FF" border="0" >
          <tr align="left">
            <td height="23" bgcolor="000099" align="center"><font color="#ffffff"><b><xsl:value-of select="concat('  ', $pageName, '  ')"/></b></font></td>
            <td height="23" align="center"><font color="#ffffff"><b><xsl:value-of select="concat($serviceName, ' / ', $methodName)"/></b></font></td>
          </tr>
        </table>
      </h3>
      <b>&nbsp;&nbsp;Destination:  </b> <xsl:value-of select="$destination"/>
      <p/>
      <hr/>
      <table width="100%">
      <tr><th align="left" id="req_label">Request: </th> <th align="left" id="resp_label">Response: </th></tr>
      <tr>
      <td width="50%">
        <table width="100%" border="0" cellspacing="0" cellpadding="1">
        <tr>
          <td> <b>Headers:</b> </td>
        </tr>
        <tr>
          <td>
            <textarea id="req_header" style="width:100%" rows="4"><xsl:value-of select="$header"/></textarea>
          </td>
        </tr>
        <tr>
          <td>
            <table width="100%">
              <tr><td align="right"></td></tr>
            </table>
          </td>
        </tr>
        <tr>
          <td>
            <textarea id="req_body" name="req_body" style="width:100%" rows="25" wrap="on"/>
          </td>
        </tr>
        </table>
      </td>
      <td width="50%">
        <table width="100%" border="0" cellspacing="0" cellpadding="1">
          <tr>
            <td> <b>Headers:</b> </td>
          </tr>
          <tr>
            <td>
              <textarea id="resp_header" cols="10" style="width:100%" rows="4" readonly="true"></textarea>
            </td>
          </tr>
          <tr>
            <td>
              <table width="100%">
                <tr><td align="right"/></tr>
              </table>
            </td>
          </tr>
          <tr>
            <td>
              <textarea id="resp_body" name="response" style="width:100%" rows="25" wrap="on" readonly="true"></textarea>
            </td>
          </tr>
        </table>
      </td>
    </tr>
    <tr>
      <td align="center" colspan="2">
        <input type="button" id="sendButton" value="Send Request"/>
        <input type="checkbox" checked="true" id="check_req"/><label for='check_req'>Check well-formness before send</label>
      </td>
    </tr>
    </table>
        <script type="text/javascript">
<xsl:text disable-output-escaping="yes">
<![CDATA[
          function jsonPrettyIndent(indent)
          {
              var s = '\n';
              if (indent>0)
              {
                 var spaces = indent * 2;
                 s += Array(spaces).join(' ');
              }
              return s;
          }
          function jsonPretty(jsonstring)
          {
              var pretty = '';
              var indent = 0;
              var instring = false;
              for (var i=0; i<jsonstring.length; i++)
              {
                  var ch = jsonstring.charAt(i);
                  switch (ch)
                  {
                      case '{':
                      case '[':
                          pretty += ch + jsonPrettyIndent(++indent);
                          break;

                      case ',':
                          pretty += ch + jsonPrettyIndent(indent);
                          break;

                      case ']':
                      case '}':
                          pretty += jsonPrettyIndent(--indent) + ch;
                          break;
                      case '\"':
                          instring = true;
                          pretty += ch;
                          while (instring && i<jsonstring.length)
                          {
                            var strCh = jsonstring.charAt(++i);
                            pretty += strCh;
                            switch(strCh)
                            {
                                case '\\':
                                    pretty += jsonstring.charAt(++i);
                                    break;
                                case '\"':
                                    instring = false;
                                    break;
                            }
                          }
                          break;
                       case '\n':
                          break;
                       case ' ':
                          if (i > 0 && jsonstring.charAt(i-1) == ':')
                              pretty += ch;
                          break;
                       default:
                          pretty += ch;
                  }
              }
              return pretty;
          }
          require(["dojo/ready", "dojo/request/handlers", "dojo/request/xhr", "dojo/json", "dojo/dom", "dojo/on"],
          function(ready, handlers, xhr, JSON, dom, on){
            ready(function(){
              var jsonreq = ']]></xsl:text><xsl:value-of select="/srcxml/jsonreq"/><xsl:text disable-output-escaping="yes"><![CDATA[';
              try {
                var obj = JSON.parse(jsonreq); //validate
                var output = jsonPretty(jsonreq); //obj.stringify can't handle 64bit integers
                dom.byId("req_body").value = output;
              } catch (err){
                dom.byId("req_body").value = err.toString() + ": \n\n" + jsonreq;
              };
            });
            handlers.register("json_show_headers", function(response){
              dom.byId("resp_header").value = "Content-Type: " + response.getHeader("Content-Type");
              JSON.parse(response.text); //validate
              return response.text; //JSON.parse can't handle 64 bit integers, so just format the original response string
            });
            on(dom.byId("sendButton"), "click", function(){
              dom.byId("resp_body").value = "";
              var jsonreq = dom.byId("req_body").value;
              if (dom.byId("check_req").checked)
              {
                try {
                  JSON.parse(jsonreq);
                } catch (err){
                  alert(err.toString() + ": \n\n" + jsonreq);
                  return;
                }
              }
              xhr("]]></xsl:text><xsl:value-of disable-output-escaping="yes" select="$destination"/><xsl:text disable-output-escaping="yes"><![CDATA[", {
                handleAs: "json_show_headers",
                method: "POST",
                data: jsonreq,
                headers: { 'Content-Type': 'application/json' }
              }).then(function(data){
                dom.byId("resp_body").value = jsonPretty(data);
              }, function(err){
                dom.byId("resp_body").value = jsonPretty(err.response.text);
                alert(err.toString());
              });
            });
          });
]]>
</xsl:text>

          var gServiceName = "<xsl:value-of select="$serviceName"/>";
          var gMethodName = "<xsl:value-of select="$methodName"/>";;
        </script>
  </body>
</html>
   </xsl:template>
</xsl:stylesheet>
