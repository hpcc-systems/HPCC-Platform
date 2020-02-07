<?xml version="1.0" encoding="UTF-8"?>
<!--

## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
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
    <xsl:param name="destination" select="'zz'"/>
    <xsl:param name="header" select="'xx'"/>
    <xsl:param name="soapbody" select="'yy'"/>
    <xsl:param name="inhouseUser" select="false()"/>
    <xsl:param name="showhttp" select="false()"/>
    <xsl:param name="showLogout" select="showLogout"/>

    <!-- ===============================================================================-->
    <xsl:template match="/">
    <html>
        <head>
            <title>Soap Test Page</title>
      <link rel="stylesheet" type="text/css" href="files_/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="files_/css/espdefault.css" />

      <script type="text/javascript" src="files_/get_input.js"/>
      <script type="text/javascript" src="files_/stack.js"/>
      <script type="text/javascript" src="files_/stringbuffer.js"/>
      <script type="text/javascript" src="files_/logout.js"/>

<script type="text/javascript">
var showhttp = '<xsl:value-of select="$showhttp"/>';

<![CDATA[ 
  var xmlhttp = null;

function isSpace(ch) {
    if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n') 
        return true;
    return false;
}
 
// return true if succeeded
function loadXMLDoc(url)
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
     xmlhttp.open("POST",url);

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
     var xml = document.getElementById("req_body").value;
//    alert("Trying: url="+url+"\nuser="+user+"\npasswd="+passwd+"\nxml="+xml);          
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
      if (document.getElementById("prettify_resp").checked && xmlhttp.responseXML && xmlhttp.responseXML.documentElement)
           document.getElementById("resp_body").value = prettifyXMLDom(xmlhttp.responseXML);
      else
           document.getElementById("resp_body").value = xmlhttp.responseText;
      if (showhttp == 'true')
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
   // check well-formness
   if (document.getElementById("check_req").checked)
   {
       var doc = parseXmlString(document.getElementById("req_body").value);
       if (!doc) return;
   }
   
    // clear
    document.getElementById("resp_body").value = "";
    if (showhttp == 'true')
       document.getElementById("resp_header").value = "";
    
    var url = "]]><xsl:value-of select="$destination"/><![CDATA[";
    loadXMLDoc(url);
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

function removeEmptyNodes(tree, remove_empty, remove_zero)
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
              var newnode = removeEmptyNodes(node, remove_empty, remove_zero);
              if (!newnode)
                  tree.removeChild(node);
              else
                  tree.replaceChild(newnode,node);
         }
         return (tree.hasChildNodes()  || hasAttr(tree)) ? tree : null;
    } else if (tree.nodeType==3) {
        if (!hasAttr(tree) && ((remove_empty && isBlank(tree.nodeValue)) || (remove_zero && tree.nodeValue=='0')) ) 
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

function clearEmptySoapFields(doc, root, remove_empty, remove_zero)
{
    var body = getFirstElementNode(root);
    if (!body) return null;
    var req = getFirstElementNode(body);
    if (!req) return null;
    var reqTag = req.tagName;
    var newreq = removeEmptyNodes(req, remove_empty, remove_zero);
    if (!newreq)
         newreq = doc.createElement(reqTag);
    body.replaceChild(newreq,req);
    return root;
}

function clearEmptyFields(xml, remove_empty, remove_zero)
{
    var doc = parseXmlString(xml);
    if (!doc) return null;
    var root = doc.documentElement;
    if (!root) return null;
    if (root.getLocalName=='Envelope')
        return clearEmptySoapFields(doc, root);
    else
    {
        var reqTag = root.tagName;
        var newreq = removeEmptyNodes(root, remove_empty, remove_zero);
        if (!newreq)
             newreq = doc.createElement(reqTag);
        doc.replaceChild(newreq,root);
    }
    return root;
}

function onClearEmptyFields(remove_empty, remove_zero)
{
       var ctrl = document.getElementById("req_body");
       var xml = ctrl.value;    
       var tree = clearEmptyFields(xml, remove_empty, remove_zero);
       
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
             + '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"'
             + ' xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"'
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

function onImportConciseRequest()
{
      document.getElementById('import').disabled = true;
      showGetInputWnd('The Concise Request Text (from esp log):', 'inputReturnMethod()');     
}

var soapBody = ']]><xsl:value-of select="$soapbody"/><![CDATA[';

function setSoapBody()
{
     var ctrl = document.getElementById("req_body");
     if (!ctrl) return;
     ctrl.value = prettifyXML(soapBody);
     return true;
}

]]>

var gServiceName = "<xsl:value-of select="$serviceName"/>";
var gMethodName = "<xsl:value-of select="$methodName"/>";;

</script>
        </head>
    <body class="yui-skin-sam" id="body" onload="setSoapBody()">
            <h3>
                
                    <table cellSpacing="0" cellPadding="1" width="100%" bgColor="#4775FF" border="0" >
                        <tr align="left">
                            <td height="23" bgcolor="000099" align="center"><font color="#ffffff"><b><xsl:value-of select="concat('  ', $pageName, '  ')"/></b></font></td>
                            <td height="23" align="center"><font color="#ffffff"><b><xsl:value-of select="concat($serviceName, ' / ', $methodName)"/></b></font></td>
                            <xsl:if test="$showLogout">
                                <td><a href="javascript:void(0)" onclick="logout();">Log Out</a></td>
                            </xsl:if>
                        </tr>
                    </table>
                
            </h3>
                    <b>&nbsp;&nbsp;Destination:  </b> <xsl:value-of select="$destination"/>
            <p/> 
             <table width="100%">
                <tr><th align="left">Request: </th> <th align="left">Response: </th></tr>
                <tr>
                  <td width="50%">
                 <table width="100%" border="0" cellspacing="0" cellpadding="1">
                    <xsl:choose>
                         <xsl:when test="$showhttp">
                             <tr>
                                 <td>
                                     <b>Headers:</b>
                                 </td>
                             </tr>
                             <tr>
                                 <td>
                                     <textarea id="req_header" style="width:100%" rows="4">
                                         <xsl:value-of select="$header"/>
                                     </textarea>
                                 </td>
                             </tr>
                         </xsl:when>
                         <xsl:otherwise>
                             <input type="hidden" id="req_header" name="req_header" value="{$header}"/>
                         </xsl:otherwise>
                    </xsl:choose>
                    <tr>
                        <td> 
                           <table width="100%">
                            <tr> <!--td align="left"><b>Request Body:</b></td--> 
                             <td align="right">
                                   <input type="button" value="Remove Empty" onclick="onClearEmptyFields(true, false)"/>
                                   <input type="button" value="Remove Zeros" onclick="onClearEmptyFields(false, true)"/>
                                   <xsl:if test="$inhouseUser">&nbsp;<input id="import" type="button" value="Import..." onclick="onImportConciseRequest()"/></xsl:if>
                                   <input type="checkbox" id="prettify_req" checked="true" onclick="onPrettifyXML('req_body','prettify_req')"/><label for='prettify_req'>Prettify XML</label>  
                                 </td></tr>
                           </table>   
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <textarea id="req_body" name="req_body" style="width:100%" rows="35" wrap="on"/>
                        </td>
                    </tr>
                           </table>

                          </td>
                            
                           <td width="50%">
                  <table width="100%" border="0" cellspacing="0" cellpadding="1">
                    <xsl:if test="$showhttp">
                      <tr>
                        <td>
                          <b>Headers:</b>
                        </td>
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
                        <td align="right"><input type="checkbox" id="prettify_resp" checked="true" onclick="onPrettifyXML('resp_body','prettify_resp')"/><label for='prettify_resp'>Prettify XML</label>  </td>
                        </tr>
                       </table>
                       </td>
                    </tr>
                    <tr>
                        <td>
                            <textarea id="resp_body" name="response" style="width:100%" rows="35" wrap="on" readonly="true"></textarea>
                        </td>
                    </tr>
                 </table>
                </td>
                </tr>
                    <tr>
                        <td align="center" colspan="2"> 
                            <input type="button" id="sendButton" value="Send Request" onclick="onSendRequest()"/>  <input type="checkbox" checked="true" id="check_req"/><label for='check_req'>Check well-formness before send</label>
                        </td>
                    </tr>
                
                </table>

             <!-- </form>  -->
        </body>
    </html>
   </xsl:template>
</xsl:stylesheet>
