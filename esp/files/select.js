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

function initSelection()
{
   totalItems = countItems(document.forms[0], false);
   checkedCount = countItems(document.forms[0], true);

   rowsChecked = checkedCount > 0;

   selectAllCheckboxChecked = checkedCount == totalItems;
   checkSelectAllCheckBoxes(selectAllCheckboxChecked);
}

/* this file handles check box states and a delete button on the form.
   Note that the html page must implement a callback method onRowCheck(checked)
   which gets invoked when check status across all rows changes (i.e. at least
   one row is checked or not). */

function selectAllItems(o,select,from,to)
{   
   if (o.tagName=='INPUT' && o.type=='checkbox')
   {
      if (o.checked != select && o.onclick && o.onclick.toString().indexOf('clicked(this)') != -1 && 
          (!from || !to || o.value>=from && to>=o.value || o.value>=to && from>=o.value))
      {
         o.checked=select;
         updateChecked(select);
      }
      return;
   }
   var ch=o.children;
   if (ch)
      for (var i in ch)
         selectAllItems(ch[i],select,from,to);                      
}

function selectAll(select)
{
   selectAllItems(document.forms[0], select);

   if (select != selectAllCheckboxChecked)
   {
      selectAllCheckboxChecked = select;
      checkSelectAllCheckBoxes(select);
   }

   rowsChecked = select;
   onRowCheck(select);
}


function checkSelectAllCheckBoxes(check)
{
   selectAllCell = document.forms[0].all.selectAll1;
   if (selectAllCell)
      selectAllCell.children[0].checked = check;

   selectAllCell = document.forms[0].all.selectAll2;
   if (selectAllCell)
      selectAllCell.children[0].checked = check;        
}


function clicked(o)
{
   if (lastClicked == 'Y')
      lastClicked = o.value;

   if (window.event.shiftKey)
   {
      rc = o.checked == true;
      selectAllItems(document.forms[0],true,lastClicked,o.value);
   }
   else if (window.event.altKey)
   {
      rc = o.checked == false;
      selectAllItems(document.forms[0],false,lastClicked,o.value);
   }
   else
      rc = true;

   if (rc)
   {
      updateChecked(o.checked);
      select = checkedCount == totalItems;
      if (select != selectAllCheckboxChecked)
      {
         selectAllCheckboxChecked = select;
         checkSelectAllCheckBoxes(select);
      }
   }
   else //the window has already checked/unchecked the checkbox which should not have been done since alt/shift key was pressed
      updateChecked(!o.checked); //compensation

   lastClicked=o.value;
   select = checkedCount > 0;

   if (rowsChecked != select)
      rowsChecked = select;

   onRowCheck(select);
   return rc; 
}

function countItems(o, selectedOnly)
{
   if (o.tagName=='INPUT' && o.type=='checkbox' && o.onclick && o.onclick.toString().indexOf('clicked(this)') != -1)
   {      
      if (selectedOnly)
        return o.checked ? 1 : 0;
      else
        return 1;
   }

   var nItems = 0;  
   var ch=o.children;
   if (ch)
      for (var i in ch)
         nItems += countItems(ch[i], selectedOnly);

   return nItems;
}

function updateChecked(select)
{
   if (select)
      checkedCount++;
   else
      checkedCount--;
}

function isAnyRowChecked()
{
   return rowsChecked;
}

lastClicked = 'Y';
checkedCount = 0;
rowsChecked = false;
totalItems = -1;
selectAllCheckboxChecked = false;
