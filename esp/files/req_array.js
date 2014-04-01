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

//--  $I.: Img, $L: Label, $C: Control, $M: more control, $V: enable checkbox,
//    $$: parentId placeholder - to be replaced by the real one

function show_hide(obj)
{
   if (obj.style.display == 'block') {
     obj.style.display='none';
     return ">>";
   } else {
     obj.style.display='block';
     return "<<";
   }
}

function show_hide_inline(obj)
{
   if (obj.style.display == 'inline') {
     obj.style.display='none';
   } else {
     obj.style.display='inline';
   }
}

function onClickDest(chked, ctrlObj)
{
   if (!chked) {
     ctrlObj.style.display='none';
   } else {
     ctrlObj.style.display='inline';
     ctrlObj.value = document.forms[0].action;
   }
}

function enableButton(tableId)
{
     var table = document.getElementById(tableId);
     var b = document.getElementById(tableId+"_RvBtn");
     b.disabled = (table.rows.length==0);
     
     //alert("btn id="+tableId+"_Remove" + " disabled "+ (table.rows.length==0));
}

function removeRow(tableId)
{
    var table = document.getElementById(tableId);
    table.deleteRow(-1);
    enableButton(tableId);
    document.getElementById(tableId+'_ItemCt').value = table.rows.length;
}

function appendRow(tableId, itemName, htmlContentFunc)
{
     var table = document.getElementById(tableId);
     var x = table.insertRow(-1);
     var idx = x.rowIndex;
     var oldContent = htmlContentFunc(tableId,itemName);

     // replace $$  with tableId
     var regex1 = new RegExp('\\$\\$', 'g'); 
     var newId = tableId+'.'+idx;
     var ctrlId = newId;
     if (newId.indexOf('$$.')==0) 
          newId = newId.substring(4);
     newContent = oldContent.replace(regex1,  newId);
   
     if (idx>0)
     {
         var regex = new RegExp(tableId+'\\.0\\.', 'g'); 
         newContent = newContent.replace(regex,  ctrlId+'.');
     }
   
    {
        var r1=x.insertCell(0);
        r1.innerHTML = "<img src='/esp/files/img/form_minus.gif' alt='-' id='$I." + ctrlId + "' onclick='hideIt(\""+ ctrlId + "\")'/>";
        //           + "<input type='checkbox' checked='1' onClick='enableInput(this,\"" + ctrlId + "\")' id='$V." + ctrlId + "'/>";

        var r2=x.insertCell(1);
        r2.innerHTML = "<span id='$M." + ctrlId + "' style='display:none'><img src='/esp/files/img/form_more.gif' alt='more' onclick='onMore(\"" + ctrlId + "\")'/></span>"
                     + "<span id='$L." + ctrlId + "'><b>" + itemName + "[" + (idx + 1) + "]:</b></span>";


        var r3=x.insertCell(2);
        r3.innerHTML = newContent; 
    }
    
    enableButton(tableId);
    document.getElementById(tableId+'_ItemCt').value = table.rows.length;
}

function hideIt(idCtrl) {
     var ctrl = document.getElementById('$C.'+idCtrl);
     if (!ctrl) { 
        return;
     }
     var moreCtrl = document.getElementById('$M.'+idCtrl);

     if (ctrl.style.display != "none")   {
         document.getElementById('$I.'+idCtrl).src = "/esp/files/img/form_plus.gif";
         ctrl.style.display = "none";
         if (moreCtrl) moreCtrl.style.display = "block";
     } else {
         document.getElementById('$I.'+idCtrl).src = "/esp/files/img/form_minus.gif";
         ctrl.style.display = "block";
         if (moreCtrl) moreCtrl.style.display = "none";
     }
}

function onMore(idCtrl) {
     var ctrl = document.getElementById('$C.'+idCtrl);
     if (!ctrl) return;

     document.getElementById('$I.'+idCtrl).src = "/esp/files/img/form_minus.gif";
     ctrl.style.display = "block";

     ctrl = document.getElementById('$L.'+idCtrl);
     if (!ctrl) return;
     ctrl.style.display = "block";

     ctrl = document.getElementById('$M.'+idCtrl);
     if (!ctrl) return;
     ctrl.style.display = "none";
}

function enableSubInputs(toDisable,parentId) {
   var ctrls = document.forms['esp_form'].elements;
   for (var idx=0; idx<ctrls.length; idx++) {
       var c = ctrls[idx];
       if (c.id!='' && c.type == 'checkbox'){
         if ( (c.id.substr(0,3) == '$V.') && (c.id.substr(3,parentId.length+1)==(parentId+'.')) ) {
        if ( (c.checked && toDisable) || (!c.checked && !toDisable) )
          c.click(); 
         }
       }
   }        
}

function disableBoolInput(ctrl,disable) {
    var hCtrl = document.getElementById('$D.'+ctrl.id);
    if (ctrl) {
       if (!disable) {
         if(ctrl.disabled) {   
          hCtrl.disabled = false;
          ctrl.disabled = false;
         }
       }else{
         if (!ctrl.disabled) {
          hCtrl.disabled = true;
          ctrl.disabled = true;
         }
       }
    }
}

function disableIEControl(ctrl,disable){
   if (ctrl.type=='checkbox') {
      ctrl.disabled = disable;
   } else {
      if (disable) {
        ctrl.disabled = true;
        ctrl.className = 'disabled';
      } else {
        ctrl.disabled = false;
        ctrl.className = '';
      }
    } 
}

function disableInputControl(ctrl,disable) {
   if (ctrl.type=='checkbox') 
       disableBoolInput(ctrl,disable);
   else if (isIE) 
       disableIEControl(ctrl,disable);
   else 
       ctrl.disabled = disable;
}

function disableInputLabel(label,disable) {
    if (label) 
       label.style.color = disable ? 'gray': 'black';
}

function enableInput(checkbox,idCtrl) {
    var ctrl = document.getElementById(idCtrl);
    var disable = !checkbox.checked;

    // label
    var label = document.getElementById('$L.'+idCtrl);  
    disableInputLabel(label,disable);
    
    // input
    if (!ctrl) 
       enableSubInputs(disable,idCtrl);
    else 
       disableInputControl(ctrl,disable);
}

function onBoolChange(checkbox) {
    var hiddenCtrl = document.getElementById('$D.'+checkbox.id);
    if (hiddenCtrl) 
       hiddenCtrl.value = checkbox.checked ? "1" : "0";
}

function onTriButtonKeyPress(obj) 
{
    if (event.keyCode == 32) //space
    {
	onClickTriButton(obj, 1);
        return false;
    }
    return true;
}

function onClickTriButton(btn, clicks) 
{
    while (clicks--)
    {
        if (btn.value=='default')
        { 
            btn.value='true'; 
            btn.style.color='green';
        } 
        else if (btn.value=='true') 
        {
            btn.value='false'; 
            btn.style.color='red';
        } 
        else 
        {
            btn.value='default'; 
            btn.style.color='gray';
        }
    }
}
