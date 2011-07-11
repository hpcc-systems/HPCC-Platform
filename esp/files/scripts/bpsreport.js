/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
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