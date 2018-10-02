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
    <xsl:param name="serviceName" select="'FormTest'"/>
    <xsl:param name="methodName" select="'BasicTest'"/>
    <xsl:param name="destination" select="'zz'"/>
    <xsl:param name="header" select="'xx'"/>
    <xsl:param name="roxiebody" select="'yy'"/>
    <xsl:param name="roxieUrl" select="'aa'"/>
    <!-- ===============================================================================-->
    <xsl:template match="/">
    <html>
        <head>
            <title>Roxie Test Page</title>

<script type="text/javascript" src="files_/get_input.js"/>
<script type="text/javascript" src="files_/stack.js"/>
<script type="text/javascript" src="files_/stringbuffer.js"/>

<script type="text/javascript">
<![CDATA[
  var xmlhttp = null;

function isSpace(ch) {
    if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n')
        return true;
    return false;
}

function show_hide_advanced_button(obj, ctrl)
{
 if (obj.style.display == 'block') {
 obj.style.display='none';
 ctrl.value ="Advanced >>";
 return ">>";
 } else {
 obj.style.display='block';
 ctrl.value ="Advanced <<";
 return "<<";
 }
}

// return true if succeeded
function loadXMLDoc(url, user, passwd)
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
     try
     {
        xmlhttp.setRequestHeader('Content-type', document.getElementById("req_content").value);

     } catch (e) {
         alert("Exception when setRequestHeader(): "+e);
     }

     var xml = document.getElementById("req_body").value;
      var button = document.getElementById("sendButton");
      if (button)
      {
          button.value = "Please wait ...";
          button.disabled = true;
     }

      document.getElementById("body").style.cursor = "wait";
      xmlhttp.send(xml);
  }

   return true;
}

function xmlEncode(val)
{
    return val.toString().replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

function getAttrs(node)
{
     var attrs = node.attributes;
     if (attrs==null)
        return "";
     var s = "";
     for (var i = 0; i<attrs.length; i++)  {
         var node = attrs.item(i);
         s += ' ' + node.nodeName + '=' + '"' + xmlEncode(node.nodeValue) + '"';
     }
     //alert("Node: "+node.tagName + "; attrs=" + node.attributes + "; s="+s);
     return s;
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
    if (xmlhttp.responseText == "")
    {
        var msg = "Empty response detected:\n ";
        msg += "Status Code: " + xmlhttp.status+"\n";
        alert(msg);
    }
    // if "OK"
    else if (xmlhttp.status==200)
    {
        try
        {
            //IE
            xmlDoc=new ActiveXObject("Microsoft.XMLDOM");
            xmlDoc.async="false";
            xmlDoc.loadXML(xmlhttp.responseText);
        }
        catch(e)
        {
            try
            {
                //the others
                parser=new DOMParser();
                xmlDoc=parser.parseFromString(xmlhttp.responseText,"text/xml");
            }
            catch(e)
            {
                document.getElementById("resp_body").value = xmlhttp.responseText;
                alert('Problem parsing response' + e.message);
                return;
            }
        }

        var action = xmlDoc.getElementsByTagName('*')[0].nodeName;
        // roxie-request and error actions expected as <action><content>XML</content></action>
        if (action == "roxie-request")
        {
            x = xmlDoc.getElementsByTagName('content')[0].childNodes;

            content = '';
            for (var i = 0; i<x.length;i++)
                content = content + x[i].nodeValue;

            document.getElementById("resp_body").setAttribute('wrap', 'hard');
            document.getElementById("resp_body").value = "";

            if (document.getElementById("prettify_resp").checked)
                document.getElementById("resp_body").value = prettifyXML(content);
            else
                document.getElementById("resp_body").value = content;
        }
        else if (action == "error")
        {
            x = xmlDoc.getElementsByTagName('content')[0].childNodes;

            content = '';
            for (var i = 0; i<x.length;i++)
                content = content + x[i].nodeValue;

            document.getElementById("resp_body").value = content;
        }
        else
        {
            if (document.getElementById("prettify_resp").checked)
                document.getElementById("resp_body").value = prettifyXML(xmlhttp.responseText);
            else
                document.getElementById("resp_body").value = xmlhttp.responseText;
        }
    }
    else
    {
        document.getElementById("resp_body").value = xmlhttp.responseText;
        var msg = "Problem occurred in response:\n ";
        msg += "Status Code: " + xmlhttp.status+"\n";
        msg += "Messsage: "+xmlhttp.statusText + "\n";
        msg += "Response: see Response Body";
        alert(msg);
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
        setResponseBodyHeader();
    }
}

function onSendRequest()
{
    // check well-formness
    if (document.getElementById("check_req").checked)
    {
        var doc = parseXmlString(document.getElementById("req_body").value);
        if (!doc) return;
    }

    if (document.getElementById("req_url").value == "")
    {
        alert("Please speficy the target ROXIE url protocol://host:port in the 'Advanced' section");
        return true;
    }

    // clear
    document.getElementById("resp_body").value = "";
   // document.getElementById("resp_header").value = "";

    var url = "]]><xsl:value-of select="$destination"/><![CDATA[";
    url = url + "&_RoxieRequest";
    url = url + "&roxie-url=" + document.getElementById("req_url").value;
    var user = document.getElementById("username").value;
    var passwd = document.getElementById("password").value;
    loadXMLDoc(url,user,passwd);
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

function clearEmptyFields(xml)
{
    var doc = parseXmlString(xml);
    if (!doc) return null;
    var root = doc.documentElement;
    var body = getFirstElementNode(root);
    if (!body) return null;
    var req = getFirstElementNode(body);
    if (!req) return null;
    var reqTag = req.tagName;
    var newreq = removeEmptyNodes(req);
    if (!newreq)
         newreq = doc.createElement(reqTag);
    body.replaceChild(newreq,req);
    return root;
}

function onClearEmptyFields()
{
       var ctrl = document.getElementById("req_body");
       var xml = ctrl.value;
       var tree = clearEmptyFields(xml);

       if (tree)
       {
          var newxml;
          var prettify = document.getElementById("prettify_req").checked;
          if (prettify)
               newxml = toXMLIndented(tree);
          else
               newxml = toXML(tree);

          ctrl.value = '<?xml version="1.0" encoding="UTF-8"?>' + (prettify?'\n':'') + newxml;
       }
}

function prettifyXML(inXML)
{
       var doc = parseXmlString(inXML);
       return prettifyXMLDom(doc);
}

function prettifyXMLDom(doc)
{
      if (doc)
       {
            // IE, Opera do not support xmlVersion, xmlEncoding
            var version = (doc.xmlVersion==undefined)?"1.0":doc.xmlVersion;
            var encoding = (doc.xmlEncoding==undefined) ? "utf-8" : doc.xmlEncoding;
            var xmlDeclare =  '<?xml version="' + version + '" encoding="' + encoding + '"?>';
            return xmlDeclare + "\n" + toXMLIndented(doc.documentElement);
       }
       return "";
}
function onPrettifyXML(txtCtrl, chkCtrl)
{
       var ctrl = document.getElementById(txtCtrl);
       if (isBlank(ctrl.value))
        return;
       var doc = parseXmlString(ctrl.value);
       var chkbox = document.getElementById(chkCtrl);
       var checked =  chkbox.checked;
       var xmlDeclare = null;

       if (doc) {
            var version = (doc.xmlVersion==undefined)?"1.0":doc.xmlVersion;
            var encoding = (doc.xmlEncoding==undefined) ? "utf-8" : doc.xmlEncoding;
            xmlDeclare =  '<?xml version="' + version + '" encoding="' + encoding + '"?>';
          doc = doc.documentElement;
      }
      else
          chkbox.checked = !checked;

       var value = ctrl.value;
       if (doc)
       {
           if (checked)
               value = toXMLIndented(doc);
          else
               value = toXML(doc);
       }

       ctrl.value = xmlDeclare + (checked?'\n':'') + value;
}

function constructXmlFromConciseForm(txt)
{
     var ret = new StringBuffer();
     var stack = new Stack();
     var curIdx = 0;
     var len = txt.length;
     while (curIdx<len)
     {
           while ( isSpace(txt.charAt(curIdx)) ) { curIdx++; }
           if (txt.charAt(curIdx) == ']')
           {
                var tag = stack.pop();
                if (!tag) {
                      var msg = "Invalid data: no tag for ]:\n";
                      msg += txt.substr(0,curIdx+1)+ "<<ERROR<<" + txt.substring(curIdx+1);
                      alert(msg);
                      return null;
                }
                ret.append("</" + tag + ">");
                curIdx++;
                continue;
           }

           // parse tag
           var idx =curIdx;
           while (idx<len) {
                var ch = txt.charAt(idx);
                if (ch=='(' || ch =='[')
                    break;
                idx++;
           }
           // trim spaces
           var end = idx-1;
           while ( isSpace(txt.charAt(end)) ) { end--; }
           var tag = txt.substring(curIdx,end+1);
           //alert("tag: ["+tag+']');
           if (ch=='(')
           {
                curIdx = idx+1;
                idx = txt.indexOf(')',curIdx);
                if (idx<0) { alert("Invalid input: not ending '(' for <"+tag+">"); return null; }
                ret.append('<'+tag+'>' + txt.substring(curIdx, idx) + '</' + tag + '>');
                curIdx = idx+1;
           } else if (ch=='[') {
                 stack.push(tag);
                 //alert("stack.push: [" + tag+']');
                 ret.append('<' + tag + '>');
                 curIdx = idx+1;
           } else {
               alert("Invalid input: ["+ ch + "]; only '(' or '[' is valid after tag <"+tag+">.");
               return null;
           }
     }
     return ret.toString();
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
             + '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope"'
             + ' xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding"'
             + ' xmlns="http://webservices.seisint.com/' + gServiceName + '">'
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

var roxieBody = ']]><xsl:variable name="temp">
 <xsl:call-template name="string-replace">
  <xsl:with-param name="src" select="$roxiebody"/>
  <xsl:with-param name="replace" select="'&#xa;'"/>
  <xsl:with-param name="by" select="'\r'"/>
 </xsl:call-template>
</xsl:variable>
<xsl:call-template name="string-replace">
  <xsl:with-param name="src" select="$temp"/>
  <xsl:with-param name="replace" select="'&#xd;'"/>
  <xsl:with-param name="by" select="'\n'"/>
</xsl:call-template><![CDATA[';

function setRoxieBody()
{
     var ctrl = document.getElementById("req_body");
     if (!ctrl) return;
     ctrl.value = prettifyXML(roxieBody);
     return true;
}

]]>

var gServiceName = "<xsl:value-of select="$serviceName"/>";
var gMethodName = "<xsl:value-of select="$methodName"/>";;

</script>
        </head>
        <body id="body" onload="setRoxieBody()">
            <h3>
                <xsl:value-of select="concat($serviceName, '::', $methodName)"/>
                - This transaction will not be logged!
            </h3>
            <span id="auth_" style="display:none"> Username: <input type="text" name="username" id="username" value="" size="10"/>
                            Password:   <input type="password" name="password" id="password" size="10"/>
            </span>
                      <hr/>
             <table width="100%">
               <th>Request </th> <th>Response</th>
                <tr>
                  <td width="50%">
                 <table width="100%" border="0" cellspacing="0" cellpadding="1">

                    <tr>
                        <td>
                           <table width="100%">
                            <tr> <td align="left"><b>Request Body:</b></td>
                             <td align="right">
                                   <input type="button" value="Remove Empty Node" onclick="onClearEmptyFields()"/>
                                    <input type="checkbox" id="prettify_req" checked="true" onclick="onPrettifyXML('req_body','prettify_req')"/><label for='prettify_req'>Prettify XML</label>
                                    </td>
                                 </tr>
                           </table>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <textarea id="req_body" name="req_body" style="width:100%" rows="30" wrap="on"/>
                        </td>
                    </tr>
                           </table>
                          </td>
                           <td width="50%">
                  <table width="100%" border="0" cellspacing="0" cellpadding="1">
                    <tr>
                      <td>
                       <table width="100%">
                          <tr>
                        <td> <b>Response Body:</b>  </td>
                        <td align="right"><input type="checkbox" id="prettify_resp" checked="true" onclick="onPrettifyXML('resp_body','prettify_resp')"/><label for='prettify_resp'>Prettify XML</label>  </td>
                        </tr>
                       </table>
                       </td>
                    </tr>
                    <tr>
                        <td>
                            <textarea id="resp_body" name="response" style="width:100%" rows="30" wrap="on" readonly="true"></textarea>
                        </td>
                    </tr>
                 </table>
                </td>
                </tr>
                    <tr>
                        <td align="left">
                            <input type="button" id="sendButton" value="Send Request" onclick="onSendRequest()"/>
                        </td>
                    </tr>
                    <tr>
                        <td align="left" class='options'>
                            <input type='button' id='advanced_button' value='Advanced &gt;&gt;'
                                onclick='show_hide_advanced_button(document.getElementById("option_span"), document.getElementById("advanced_button"));' />
                            <span id='option_span' style='display:none'>
                                <table style="background-color:#DCDCDC">
                                    <tr>
                                        <td colspan="2">
                                        <input type="checkbox" checked="true" id="check_req"/><label for='check_req'>Check well-formness before send</label>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td>
                                            <b>Target Roxie URL:</b>
                                        </td>
                                        <td>
                                            <!--input type="text" id="req_host"/-->
                                            <xsl:element name="input">
                                                <xsl:attribute name="id">req_url</xsl:attribute>
                                                <xsl:attribute name="size">25</xsl:attribute>
                                                <xsl:attribute name="value"><xsl:value-of select="$roxieUrl" /></xsl:attribute>
                                            </xsl:element>
                                        </td>
                                    </tr>
                                    <textarea id="req_content" style="display:none" rows="1">
                                        <xsl:value-of select="$header" />
                                    </textarea>
                                </table>
                            </span>
                        </td>
                    </tr>
                </table>
        </body>
    </html>
   </xsl:template>
   <xsl:template name="string-replace">
      <xsl:param name="src"/>
      <xsl:param name="replace"/>
      <xsl:param name="by"/>
      <xsl:choose>
        <xsl:when test="contains($src,$replace)">
          <xsl:value-of select="substring-before($src,$replace)"/>
          <xsl:value-of select="$by"/>
          <xsl:call-template name="string-replace">
            <xsl:with-param name="src" select="substring-after($src,$replace)"/>
            <xsl:with-param name="replace" select="$replace"/>
            <xsl:with-param name="by" select="$by"/>
          </xsl:call-template>
        </xsl:when>
        <xsl:otherwise>
         <xsl:value-of select="$src"/>
        </xsl:otherwise>
      </xsl:choose>
   </xsl:template>
</xsl:stylesheet>
