<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    <xsl:param name="pageName" select="'SOAP Test'"/>
    <xsl:param name="serviceName" select="'FormTest'"/>
    <xsl:param name="methodName" select="'BasicTest'"/>
    <xsl:param name="wuid"/>
    <xsl:param name="destination" select="'zz'"/>
    <xsl:param name="header" select="'xx'"/>
    <xsl:param name="inhouseUser" select="false()"/>
    <xsl:param name="showhttp" select="false()"/>
    <!-- ===============================================================================-->
    <xsl:template match="/">
    <html>
        <head>
            <title>Soap Test Page</title>
                <link rel="shortcut icon" href="/esp/files/img/affinity_favicon_1.ico" />
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />

                <script type="text/javascript" src="/esp/files/get_input.js"/>
                <script type="text/javascript" src="/esp/files/stack.js"/>
                <script type="text/javascript" src="/esp/files/stringbuffer.js"/>

<script type="text/javascript">
<xsl:text disable-output-escaping="yes">
<![CDATA[ 
  var xmlhttp = null;

function isSpace(ch) {
    if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n') 
        return true;
    return false;
}
 
// return true if succeeded
function loadJsonDoc(url, user, passwd)
{
  // code for Mozilla, etc.
   if (window.XMLHttpRequest)  {
     xmlhttp=new XMLHttpRequest();
  }
  // code for IE
  else if (window.ActiveXObject)  {
     try {
        xmlhttp=new ActiveXObject("Msxml2.XMLHTTP.4.0");
    } catch (e) {
       try {
           xmlhttp=new ActiveXObject("Msxml2.XMLHTTP");
      } catch (e) {
         xmlhttp=new ActiveXObject("Microsoft.XMLHTTP");
     }
   }
    if (xmlhttp == null) {
      alert("Can not create XMLHTTP in IE");
      return false;
    }
  }
  
   if (xmlhttp)  {
     xmlhttp.onreadystatechange = xmlhttpChange;
//   alert("url: "+url);
     xmlhttp.open("POST",url,true, user, passwd);

     //Set headers
     try {
        var header = document.getElementById("req_header").value;
        var lines = header.split('\n');
        for (var i = 0; i<lines.length;i++) {
            var line = lines[i];
            var idx = line.indexOf(':');
            if (idx <= 0) {
                alert("Invalid header line: "+line);
                return false;
           }
           //alert("Set header: " +   line.substring(0, idx) + "=" +line.substring(idx+1,line.length));
           xmlhttp.setRequestHeader(line.substring(0, idx), line.substring(idx+1,line.length));
        }
     } catch (e) {
         alert("Exception when setRequestHeader(): "+e);
     }
    
 //    alert("Request: "+document.getElementById("req_body").value);
     var jsonmsg = document.getElementById("req_body").value;
//    alert("Trying: url="+url+"\nuser="+user+"\npasswd="+passwd+"\nxml="+xml);          
      var button = document.getElementById("sendButton");
      if (button)
      {
          button.value = "Please wait ...";
          button.disabled = true;
     }

      document.getElementById("body").style.cursor = "wait";
      xmlhttp.send(jsonmsg);
  }
  
   return true;
}

function xmlEncode(val) 
{
    return val.toString().replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");    
}

function getAttrs(node)
{
}

function allSpaces(s)
{
   if (s==null) return true;
   for (var i=0; i<s.length; i++)
      if ( !isSpace(s.charAt(i)) )
        return false;
   return true;
}

function toXML2(buf, tree)
{
      //alert("toXML2: name="+tree.tagName + "; type="+tree.nodeType);
      switch (tree.nodeType)
      { 
      case 1: // document.ELEMENT_NODE:
           var count = tree.childNodes.length;
           if (count==0)  {
                 buf.push("<" + tree.tagName + getAttrs(tree) + "/>");
           } else  {
               var val = "";
               var started = false;
               for (var i=0; i<count; i++) 
               {
                   var node = tree.childNodes[i];
                   
                    switch (node.nodeType)
                    {
                    case 3: // document.TEXT_NODE:
                      if (!allSpaces(node.nodeValue))
                        val += xmlEncode(node.nodeValue);
                     //else  alert("Ignore text node: ["+node.nodeValue+"]");
                     break;
                    case 8: // document.COMMENT_NODE:
                       val += "<!" + "--"+node.nodeValue+"--" + ">";
                       break;
                  case 4: // document.CDATA_SECTION_NODE:
                        val += "<![" + "CDATA[" + node.nodeValue + "]]" + ">";                  
                        break;
               case 7: // document.PROCESSING_INSTRUCTION_NODE:
                     val += "<?"+node.target + " " + node.data + "?>";
                     break;
               case 1: // document.ELEMENT_NODE:
                    if (!started) {
                             buf.push( "<"+tree.tagName+getAttrs(tree)+">");                        
                             started = true;
                        } 
                        if (val.length>0) {
                           buf.push(val); 
                           val = '';
                        }
                       toXML2(buf, node);
                       break;
                       
                    default:
                        alert("Unhandled node [1]: <" + node.tagName + ">; type: " + node.nodeType );
                  }
               }

               if (!started)
                     buf.push( "<"+tree.tagName+getAttrs(tree) + ">");
                  buf.push(val + "</" + tree.tagName + ">");               
         }
         break;
         
      case 7: // document.PROCESSING_INSTRUCTION_NODE:
         buf.push( "<?"+tree.target + " " + tree.data + "?>");                 
         break;
         
      case 8: // document.COMMENT_NODE:
         buf.push("<!" + "--"+node.nodeValue+"--" + ">");
         break;
          
      defailt: 
           alert("Unhandled node [2]: <" + tree.tagName + ">; type: " + tree.nodeType );
      }     
      
      //alert("buf = " + buf.join('|'));
}

function toXML(tree)
{
     var buf = new Array();
     if (tree)
         toXML2(buf,tree);
 
     return buf.join('');
}

function toXMLIndented2(buf, tree, indent)
{
      //alert("toXML2: name="+tree.tagName + "; type="+tree.nodeType);
      switch (tree.nodeType)
      { 
      case 1: // document.ELEMENT_NODE:
           var count = tree.childNodes.length;
           if (count==0)  {
                 buf.push(indent + "<" + tree.tagName + getAttrs(tree) + "/>");
           } else  {
               var val = "";
               var started = false;
               for (var i=0; i<count; i++) 
               {
                   var node = tree.childNodes[i];
                   
                  switch (node.nodeType)
                  {
                  case 3: // document.TEXT_NODE:
                    if (!allSpaces(node.nodeValue))
                       val += xmlEncode(node.nodeValue);
                    //alert("Text node: ["+node.nodeValue+"]");
                    break;
                   case 8: // document.COMMENT_NODE:
                       val += "<!" + "--"+node.nodeValue+"--" + ">";
                       break;
                case 4: // document.CDATA_SECTION_NODE:
                        val += "<![" + "CDATA[" + node.nodeValue + "]]" + ">";                  
                        break;
               case 7: // document.PROCESSING_INSTRUCTION_NODE:
                     if (val.length==0) 
                         val += indent + " <?"+node.target + " " + node.data + "?>\n";
                     else // in mixed content environment
                         val += "<?"+node.target + " " + node.data + "?>";
                     break;
               case 1: // document.ELEMENT_NODE:
                    if (!started) {
                       buf.push( indent + "<"+tree.tagName+getAttrs(tree) + ">");
                       started = true;
                    }
                     
                     if (val.length>0) {
                        buf.push(val);
                        val = '';
                     }      
                       toXMLIndented2(buf, node,indent+' ');
                       break;
                       
                 default:
                        alert("Unhandled node [1]: <" + node.tagName + ">; type: " + node.nodeType );
                }
               }
                
                if (!started)
                     buf.push( indent + "<"+tree.tagName+getAttrs(tree) + ">" + val + "</" + tree.tagName + ">");
                  else
                     buf.push(indent + val + "</" + tree.tagName + ">");               
         }
         break;
         
      case 7: // document.PROCESSING_INSTRUCTION_NODE:
         buf.push(indent+ "<?"+tree.target + " " + tree.data + "?>");                 
         break;
         
      case 8: // document.COMMENT_NODE:
         buf.push("<!" + "--"+node.nodeValue+"--" + ">");
         break;
          
      defailt: 
           alert("Unhandled node [2]: <" + tree.tagName + ">; type: " + tree.nodeType );
      }     
      
      //alert("buf = " + buf.join('|'));
}

function toXMLIndented(tree)
{
     var buf = new Array();
     if (tree)
         toXMLIndented2(buf,tree,"");
 
     return buf.join('\n');
}

function setResponseBodyHeader()
{
      document.getElementById("resp_body").value = xmlhttp.responseText;
      document.getElementById("resp_header").value = xmlhttp.getAllResponseHeaders();
      if (xmlhttp.responseXML && xmlhttp.responseXML.parseError && xmlhttp.responseXML.parseError.errorCode!=0)
      {
          var parseError = xmlhttp.responseXML.parseError;
          alert("\nError in line " + parseError.line + "\nposition " + parseError.linePos + "\nError Code: " + parseError.errorCode + "\nError Reason: " + parseError.reason + "\nError Line: " + parseError.srcText);
    }

}

function xmlhttpChange()
{
   // if xmlhttp shows "loaded"
  if (xmlhttp.readyState==4)
  {
     var button = document.getElementById("sendButton");
      if (button)
      {
          button.value = "Send Request";
          button.disabled = false;
     }

     document.getElementById("body").style.cursor = "default";

    // if "OK"
    if (xmlhttp.status==200)
        setResponseBodyHeader();
    else
    {
         setResponseBodyHeader();

         var msg = "Problem occurred in response:\n ";
         msg += "Status Code: " + xmlhttp.status+"\n";
         msg += "Messsage: "+xmlhttp.statusText + "\n";
         msg += "Response: see Response Body";
         alert(msg);         
    }
  }
}

function onSendRequest()
{
    // clear
    document.getElementById("resp_body").value = "";
    document.getElementById("resp_header").value = "";
    
    var url = "]]></xsl:text><xsl:value-of disable-output-escaping="yes" select="$destination"/><xsl:text disable-output-escaping="yes"><![CDATA[";
    var user = document.getElementById("username").value;
    var passwd = document.getElementById("password").value;
    loadJsonDoc(url,user,passwd);
    return true;
}

//-------------------------------------------------------------------

// mozilla only
function checkForParseError (xmlDocument) 
{
    var errorNamespace = 'http://www.mozilla.org/newlayout/xml/parsererror.xml';
    var documentElement = xmlDocument.documentElement;
    var parseError = { errorCode : 0 };
    if (documentElement.nodeName == 'parsererror' &&
        documentElement.namespaceURI == errorNamespace) {
          parseError.errorCode = 1;
         var sourceText = documentElement.getElementsByTagNameNS(errorNamespace, 'sourcetext')[0];
         if (sourceText != null) {
           parseError.srcText = sourceText.firstChild.data
        }
        parseError.reason = documentElement.firstChild.data;
    }
    return parseError;
}

function parseXmlString(xml)
{
   var xmlDoc = null;
   
   try {
      var dom = new DOMParser();      
      xmlDoc = dom.parseFromString(xml, 'text/xml'); 
      var error = checkForParseError(xmlDoc);
      if (error.errorCode!=0)
      {
         alert(error.reason + "\n" + error.srcText);
         return null;
      }
   } catch (e) {
     try {
        xmlDoc = new ActiveXObject("Microsoft.XMLDOM");
        xmlDoc.async="false";
        xmlDoc.loadXML(xml);
        if (xmlDoc.parseError != 0)
        {
          alert("XML Parse Error: " + xmlDoc.parseError.reason);
          return null;
        }
     } catch(e) {
        alert("Error: " + e.name + "\n" + e.message);
        return null;
     }
  }
     
   if (!xmlDoc)
     alert("Create xmlDoc failed! You browser is not supported yet.");

   return xmlDoc;
}

function getRootElement(xmlDom)
{
   var root = null;
   if (xmlDom)
   {
     root = xmlDom.firstChild;
     if (root && root.nodeType!=1) // IE treat <?xml ?> as first child
     {
         //alert("firstChild type = " + root.nodeType);
      root = xmlDom.childNodes[1]; 
     }
   }
   
   return root;
}

function isBlank(s)
{
   var len = s ? s.length : 0;
   for (var i=0; i<len; i++)
   {
       var ch = s.charAt(i);
       if (ch != ' ' && ch != '\t' && ch!='\n')
          return false;
   }
   return true;
}

//-------------------------------------------------------------------

function hasAttr(tree)
{
    // some browser (such as IE) does not support hasAttributes()
    if (tree.hasAttributes)
        return tree.hasAttributes();
    else
        return tree.attributes!=null && tree.attributes.length>0;
}

function removeEmptyNodes(tree)
{
//     alert("Node type: " + tree.nodeType + "\nNode value:"+tree.nodeValue+"\nTag name:"+tree.tagName);

      if (tree.nodeType==1) // ELEMENT_NODE
      {
          var count = tree.childNodes.length;
          if (count==0 && !hasAttr(tree))  
             return null;
          for (var i = count-1; i>=0; i--)  // do backward so the remove would not invalid the list
          {
              var node = tree.childNodes[i];
              var newnode = removeEmptyNodes(node);
              if (!newnode)
                  tree.removeChild(node);
              else
                  tree.replaceChild(newnode,node);
         }
         return (tree.hasChildNodes()  || hasAttr(tree)) ? tree : null;
    } else if (tree.nodeType==3) {
        if ( (isBlank(tree.nodeValue) || tree.nodeValue=='0') && !hasAttr(tree) ) 
          return null;
   }

    return tree;
}

function getFirstElementNode(tree)
{
      if (tree)
      {
          var nodes = tree.childNodes;
        for (var i=0; i<nodes.length; i++)
        {
             var node = nodes[i];
             if (node.nodeType == 1)
                return node;
          }
     }
      
     return null;           
}

function clearEmptySoapFields(doc, root)
{
}

function clearEmptyFields(xml)
{
}

function onClearEmptyFields()
{
}

function prettifyXML(inXML)
{
}

function prettifyXMLDom(doc)
{
}
function onPrettifyXML(txtCtrl, chkCtrl)
{
}
 
function constructXmlFromConciseForm(txt)
{
}
 
function inputReturnMethod()
{
    var txt = gWndObj.value;
    gWndObjRemoveWatch();
    
    if (txt!=null && txt!="")
    {  
       var xml = constructXmlFromConciseForm(txt)
       
       var tagLen = gMethodName.length+2;
       if ( xml.substr(0, tagLen) != ('<'+gMethodName+'>') ) {
           alert("The request must starts with <"+gMethodName+">");
           return;
      }
      
      var head = '<?xml version="1.0" encoding="UTF-8"?>'
             + '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"'
             + ' xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"'
             + ' xmlns="urn:LNHPCC:' + gServiceName + '">'
             + ' <soap:Body><'
             + gMethodName + 'Request>';
        var end = '</' + gMethodName + 'Request></soap:Body></soap:Envelope>';        
        xml = head + xml.substring(tagLen, xml.length-tagLen-1) + end;
        document.getElementById('req_body').value =  xml;
        
        /*TODO: request prettify_req. Why this does not work??
        var checked = document.getElementById('prettify_req').checked;
        if (checked) { xml = prettifyXml(xml); }
        alert(document.getElementById('req_body'));
        document.getElementById('req_body').value =  xml;
        alert(document.getElementById('req_body').value);
        */
     }
     document.getElementById('import').disabled = false;
}

function onImportConciseRequest()
{
      document.getElementById('import').disabled = true;
      showGetInputWnd('The Concise Request Text (from esp log):', 'inputReturnMethod()');     
}

var jsonreq = ']]></xsl:text><xsl:value-of select="/srcxml/jsonreq"/><xsl:text disable-output-escaping="yes"><![CDATA[';

function setJsonReq()
{
     var ctrl = document.getElementById("req_body");
     if (!ctrl) return;
     ctrl.value = jsonreq;
     return true;
}

]]></xsl:text>

var gServiceName = "<xsl:value-of select="$serviceName"/>";
var gMethodName = "<xsl:value-of select="$methodName"/>";;

</script>
        </head>
    <body class="yui-skin-sam" id="body" onload="setJsonReq()">
            <h3>
                
                    <table cellSpacing="0" cellPadding="1" width="100%" bgColor="#4775FF" border="0" >
                        <tr align="left">
                            <td height="23" bgcolor="000099" align="center"><font color="#ffffff"><b><xsl:value-of select="concat('  ', $pageName, '  ')"/></b></font></td>
                            <td height="23" align="center"><font color="#ffffff"><b><xsl:value-of select="concat($serviceName, ' / ', $methodName)"/></b></font></td>
                        </tr>
                    </table>
                
            </h3>
                    <b>&nbsp;&nbsp;Destination:  </b> <xsl:value-of select="$destination"/>
                     <span id="auth_" style="display:none"> Username: <input type="text" name="username" id="username" value="" size="10"/>
                            Password:   <input type="password" name="password" id="password" size="10"/>
                    </span>

            <p/> 
            <xsl:if test="$showhttp">
            </xsl:if>

                      <hr/>                      
             <table width="100%">
                <tr><th align="left">Request: </th> <th align="left">Response: </th></tr>
                <tr>
                  <td width="50%">
                 <table width="100%" border="0" cellspacing="0" cellpadding="1">
                    <xsl:if test="$showhttp">
                        <tr>
                            <td> <b>Headers:</b> </td>
                        </tr>
                        <tr>
                            <td>
                                <textarea id="req_header" style="width:100%" rows="4"><xsl:value-of select="$header"/></textarea>
                            </td>
                        </tr>
                    </xsl:if>
                    <tr>
                        <td> 
                           <table width="100%">
                            <tr> <!--td align="left"><b>Request Body:</b></td--> 
                             <td align="right">
                                 </td></tr>
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
                    <xsl:if test="$showhttp">
                    <tr>
                        <td> <b>Headers:</b> </td>
                    </tr>
                    <tr>
                        <td>
                            <textarea id="resp_header" cols="10" style="width:100%" rows="4" readonly="true"></textarea>
                        </td>
                    </tr>
                    </xsl:if>                           
                    <tr>
                      <td>
                       <table width="100%">
                          <tr>
                        <!--td> <b>Response Body:</b>  </td-->
                        <td align="right"/>
                        </tr>
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
                            <input type="button" id="sendButton" value="Send Request" onclick="onSendRequest()"/>  <input type="checkbox" checked="true" id="check_req"> Check well-formness before send</input>
                        </td>
                    </tr>
                
                </table>

             <!-- </form>  -->
        </body>
    </html>
   </xsl:template>
</xsl:stylesheet>
