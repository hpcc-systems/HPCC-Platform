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

// for bpsreport: Options
function  doSelectOptions(toSelect, parentId) {
//   alert("toSelect="+toSelect+", parentId=" + parentId);
   // TODO: we can do better start with children of this elements
   var ctrls = document.forms['esp_form'].elements;
   for (var idx=0; idx<ctrls.length; idx++) {
       var c = ctrls[idx];
       if (c.id!='' && c.type == 'checkbox'){
         if  ( (c.id.substr(0,parentId.length+1)==parentId+'.') && (c.id.indexOf("Include")>0) ) {
                 if ((c.checked && !toSelect) || (!c.checked && toSelect))
                    c.click(); // this ensure calls the onclick()
         }
       }
   }        
}

var optionsSelected = false;
function selectAllBpsReportOptions(img, idCtrl)
{
      optionsSelected = !optionsSelected;
      img.src =  'files_/img/' + (optionsSelected ? 'unselectall.gif':'selectall.gif' );  
      img.title = optionsSelected ? "Unselect All" : "Select All";
      doSelectOptions(optionsSelected,idCtrl); 
}