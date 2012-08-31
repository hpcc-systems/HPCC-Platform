/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
############################################################################## */

// Get input using window
var gInputWnd;
var gWndObjInterval;
var gWndObj = new Object;

gWndObj.value = '';
gWndObj.eventhandler = '';
 
function gWndObjMaintainFocus()
{
  try  {
    if (gInputWnd.closed) {
        window.clearInterval(gWndObjInterval);
        eval(gWndObj.eventhandler);       
        return;
    }
    gInputWnd.focus(); 
  }
  catch (everything) {   }
}
        
function gWndObjRemoveWatch()
{
    gWndObj.value = '';
    gWndObj.eventhandler = '';
}
        
function gWndObjShow(Title,BodyText,EventHandler)
{
   gWndObjRemoveWatch();
   gWndObj.eventhandler = EventHandler;

   var args='width=600,height=405,left=200,top=200,toolbar=0,';
   args+='location=0,status=0,menubar=0,scrollbars=0,resizable=0';  

   gInputWnd=window.open("","",args); 
   gInputWnd.document.open(); 
   var html = '<html><head><title>' + Title + '</title>'
+ '<script language="JavaScript">\n'
+ 'function CloseForm(Response)  {\n'
+'   window.opener.gWndObj.value = Response; \n'
+   'window.close(); \n'
+'} \n'
+ 'function onClose(ok) {  var val = ""; if (ok) { var ctrl = document.getElementById("textarea1"); val = ctrl.value; } CloseForm(val); return true;  }\n'
+ 'function onFocus() { document.getElementById("textarea1").focus(); }\n'
+ 'function onLoad() {  onFocus(); document.onfocusin=onFocus; }\n'
+ '</script' + '>\n'
+ '</head>\n'
// this cause IE textarea readonly
//+ '<body onblur="window.focus();">\n'  
+ '<body onload="onLoad()" onunload="onClose(0)">\n'  
+ '<form> <table border="0" width="95%" align="center" cellspacing="0" cellpadding="2">\n'
+ '<tr><td align="left">' + BodyText + '</td></tr>'
+ '<tr><td align="left"><br/></td></tr>'
+'<tr><td align="center"> <textarea id="textarea1" rows="18" cols="65"></textarea></td></tr>'
//+'<tr><td> <textarea id="textarea1" style="width:100%; overflow:visible" rows="18"></textarea></td></tr>'
+ '<tr><td align="center"> <input type="submit" value="Submit" onclick="onClose(1)"/> <input type="submit" value="Cancel" onclick="onClose(0)"/></td></tr>'
+ '</table></form>'
+ '</body></html>';

   gInputWnd.document.write(html); 
   gInputWnd.document.close(); 
   gInputWnd.focus(); 
   gWndObjInterval = window.setInterval("gWndObjMaintainFocus()",5);
 }

function showGetInputWnd(BodyText,EventHandler)
{
      gWndObjShow("Dialog",BodyText,EventHandler);
}
