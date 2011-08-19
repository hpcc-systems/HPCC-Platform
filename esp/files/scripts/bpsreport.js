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