/*##############################################################################
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
############################################################################## */

var Xml2JSON=Class.create();   
Xml2JSON.prototype={   
    initialize:function(response){   
    this.xmlDoc=this.getDom(response);   
    },   
    getDom:function(response){   
        var dom = null;   
        if(response.responseText){   
            if (window.DOMParser){ //support firefox   
          try {dom = (new DOMParser()).parseFromString(response.responseText, "text/xml");}    
          catch(e){dom = null;}   
           }   
           else if(window.ActiveXObject){   
          try{   
         dom = new ActiveXObject('Microsoft.XMLDOM');   
         dom.async = false;   
         if(!dom.loadXML(response.responseText)) // parse error ..   
            window.alert(dom.parseError.reason + dom.parseError.srcText);   
              }catch(e){ dom = null; }   
            }   
           else  
          alert("cannot parse xml string!");   
        }   
        if (dom.nodeType == 9) // document node   
      dom = dom.documentElement;   
         
      return dom;   
    },   
    toObj: function(xml){
          
        var o = {};   
         if (xml.nodeType==1) {   // element node ..   
            if (xml.attributes.length)   // element with attributes  ..   
               for (var i=0; i<xml.attributes.length; i++)   
                  o["@"+xml.attributes[i].nodeName] = (xml.attributes[i].nodeValue||"").toString();   
            if (xml.firstChild) { // element has child nodes ..   
               var textChild=0, cdataChild=0, hasElementChild=false;   
               for (var n=xml.firstChild; n; n=n.nextSibling) {   
                  if (n.nodeType==1) hasElementChild = true;   
                  else if (n.nodeType==3 && n.nodeValue.match(/[^ \f\n\r\t\v]/)) textChild++; // non-whitespace text   
                  else if (n.nodeType==4) cdataChild++; // cdata section node   
               }   
               if (hasElementChild) {   
                  if (textChild < 2 && cdataChild < 2) { // structured element with evtl. a single text or/and cdata node ..   
                     this.removeWhite(xml);   
                     for (var n=xml.firstChild; n; n=n.nextSibling) {   
                        if (n.nodeType == 3)  // text node   
                           o["#text"] = this.escape(n.nodeValue);   
                        else if (n.nodeType == 4)  // cdata node   
                           o["#cdata"] = this.escape(n.nodeValue);   
                        else if (o[n.nodeName]) {  // multiple occurence of element ..   
                           if (o[n.nodeName] instanceof Array)   
                              o[n.nodeName][o[n.nodeName].length] = this.toObj(n);   
                           else  
                              o[n.nodeName] = [o[n.nodeName], this.toObj(n)];   
                        }   
                        else  // first occurence of element..   
                           o[n.nodeName] = this.toObj(n);   
                     }   
                  }   
                  else { // mixed content   
                     if (!xml.attributes.length)   
                        o = this.escape(this.innerXml(xml));   
                     else  
                        o["#text"] = this.escape(this.innerXml(xml));   
                  }   
               }   
               else if (textChild) { // pure text   
                  if (!xml.attributes.length)   
                     o = this.escape(this.innerXml(xml));   
                  else  
                     o["#text"] = this.escape(this.innerXml(xml));   
               }   
               else if (cdataChild) { // cdata   
                  if (cdataChild > 1)   
                     o = this.escape(this.innerXml(xml));   
                  else  
                     for (var n=xml.firstChild; n; n=n.nextSibling)   
                        o["#cdata"] = this.escape(n.nodeValue);   
               }   
            }   
            if (!xml.attributes.length && !xml.firstChild) o = null;   
         }   
         else if (xml.nodeType==9) { // document.node   
            o = this.toObj(xml.documentElement);   
         }   
         else  
            alert("unhandled node type: " + xml.nodeType);   
         return o;   
    },   
    toJson: function(o, name, ind) {   
           
        var json = name ? ("\""+name+"\"") : "";   
         if (o instanceof Array) {   
            for (var i=0,n=o.length; i<n; i++)   
               o[i] = this.toJson(o[i], "", ind+"\t");   
            json += (name?":[":"[") + (o.length > 1 ? ("\n"+ind+"\t"+o.join(",\n"+ind+"\t")+"\n"+ind) : o.join("")) + "]";   
         }   
         else if (o == null)   
            json += (name&&":") + "null";   
         else if (typeof(o) == "object") {   
            var arr = [];   
            for (var m in o)   
               arr[arr.length] = this.toJson(o[m], m, ind+"\t");   
            json += (name?":{":"{") + (arr.length > 1 ? ("\n"+ind+"\t"+arr.join(",\n"+ind+"\t")+"\n"+ind) : arr.join("")) + "}";   
         }   
         else if (typeof(o) == "string")   
            json += (name&&":") + "\"" + o.toString() + "\"";   
         else  
            json += (name&&":") + o.toString();   
         return json;   
    },   
    innerXml: function(node) {   
        var s = ""  
         if ("innerHTML" in node)   
            s = node.innerHTML;   
         else {   
            var asXml = function(n) {   
               var s = "";   
               if (n.nodeType == 1) {   
                  s += "<" + n.nodeName;   
                  for (var i=0; i<n.attributes.length;i++)   
                     s += " " + n.attributes[i].nodeName + "=\"" + (n.attributes[i].nodeValue||"").toString() + "\"";   
                  if (n.firstChild) {   
                     s += ">";   
                     for (var c=n.firstChild; c; c=c.nextSibling)   
                        s += asXml(c);   
                     s += "</"+n.nodeName+">";   
                  }   
                  else  
                     s += "/>";   
               }   
               else if (n.nodeType == 3)   
                  s += n.nodeValue;   
               else if (n.nodeType == 4)   
                  s += "<![CDATA[" + n.nodeValue + "]]>";   
               return s;   
            };   
            for (var c=node.firstChild; c; c=c.nextSibling)   
               s += asXml(c);   
         }   
         return s;   
    },   
    escape: function(txt){   
        return txt.replace(/[\\]/g, "\\\\")   
                   .replace(/[\"]/g, '\\"')   
                   .replace(/[\n]/g, '\\n')   
                   .replace(/[\r]/g, '\\r');   
    },   
    removeWhite: function(e) {   
        e.normalize();   
         for (var n = e.firstChild; n; ) {   
            if (n.nodeType == 3) {  // text node   
               if (!n.nodeValue.match(/[^ \f\n\r\t\v]/)) { // pure whitespace text node   
                  var nxt = n.nextSibling;   
                  e.removeChild(n);   
                  n = nxt;   
               }   
               else  
                  n = n.nextSibling;   
            }   
            else if (n.nodeType == 1) {  // element node   
               this.removeWhite(n);   
               n = n.nextSibling;   
            }   
            else                      // any other node   
               n = n.nextSibling;   
         }   
         return e;   
    },   
    Convert:function(){   
        var o=this;   
        var tab="";   
        var json = o.toJson(o.toObj(o.removeWhite(o.xmlDoc)), o.xmlDoc.nodeName, "\t");   
       var jsonStr= "{\n" + tab + (tab ? json.replace(/\t/g, tab) : json.replace(/\t|\n/g, "")) + "\n}";  
       alert(jsonStr); 
       return jsonStr.evalJSON();   
    }   
}  
