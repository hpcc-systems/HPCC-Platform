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

lastClicked = -1;
lastChecked = false;
checkedCount = 0;
rowsChecked = false;
totalItems = -1;
selectAllCheckboxChecked = false;
multiSelectTable = null;
oneCheckboxPerRow = true;
onCheckHandler = null;
singleSelect = false;

function initSelection(tableId, multipleCheckboxesPerRow, onCheck)
{
   if (multipleCheckboxesPerRow)
      oneCheckboxPerRow = false;

   if (onCheck)
      onCheckHandler = onCheck;

   multiSelectTable = document.getElementById(tableId);
   countItems();//updates checkedCount and totalItems

   rowsChecked = checkedCount > 0;
   checkSelectAllCheckBoxes(checkedCount == totalItems);
}

/* this file handles check box states and a delete button on the form.
   Note that the html page must implement a callback method onRowCheck(checked, tableId)
   which gets invoked when check status across all rows changes (i.e. at least
   one row is checked or not). */

function selectAllItems(select,from,to)
{   
    for (var r=from; r<=to; r++)
    {
        var row = multiSelectTable.rows[r];
      var numCells = row.cells.length;

        for (var c=0; c<numCells; c++)
        {
            var cell1 = row.cells[c];
            if (cell1.children.length > 0)
            {
                var o = cell1.children[0];
                if (o.tagName=='INPUT' && o.type=='checkbox' && 
                    (!o.id || o.id.indexOf('selectAll') == -1) && o.checked != select)
                {
                    o.checked=select;
                    updateChecked(select);
                    
            if (onCheckHandler)
               onCheckHandler(o);
                }
            }

         if (oneCheckboxPerRow)
            break;
        }
    }
}

function selectAll(select)
{
   selectAllItems(select, 1, multiSelectTable.rows.length-1);
   checkedCount = select ? totalItems : 0;

   if (select != selectAllCheckboxChecked)
      checkSelectAllCheckBoxes(select);
   
   rowsChecked = select;
   onRowCheck(select, multiSelectTable.id);
}


function checkSelectAllCheckBoxes(check)
{
   if(document.forms[0] != null)
   {
       //var all = document.forms[0].all;
       //if (all) {
           selectAllCell = document.getElementById("selectAll1");
           if (selectAllCell && selectAllCell.children[0])
               selectAllCell.children[0].checked = check;

           selectAllCell = document.getElementById("selectAll2");
           if (selectAllCell)
               selectAllCell.children[0].checked = check;
           selectAllCheckboxChecked = check;
       //}
   }
}


function clicked(o, event) {
   if (singleSelect && o.checked)
   {
      o.checked = false;
      selectAll(false); 
      o.checked = true;     
   }
   
   var cell = o.parentNode;
   var row = cell.parentNode;
   var rowNum = row.rowIndex;

   if (!event)
        event = window.event;

   if (!singleSelect && event && event.shiftKey && lastClicked != -1 && lastClicked != rowNum)
   {
      rc = lastChecked == o.checked;
      if (lastClicked < rowNum)
        selectAllItems(lastChecked, lastClicked, rowNum);
      else
        selectAllItems(lastChecked, rowNum, lastClicked);
      lastClicked = -1;
   }
   else
   {
      lastClicked = rowNum;
      lastChecked = o.checked;
      rc = true;
   }
   
   if (rc)
   {
      updateChecked(o.checked);
      select = checkedCount == totalItems;
      if (select != selectAllCheckboxChecked)
         checkSelectAllCheckBoxes(select);
   }
   else
   {
      //the window has already checked/unchecked the checkbox which 
      //should not have been done since shift key was pressed
      updateChecked(!o.checked); //compensation
   }

   select = checkedCount > 0;

   if (rowsChecked != select)
      rowsChecked = select;

   onRowCheck(select, multiSelectTable.id);   
   return rc; 
}

function countItems()
{
    if(multiSelectTable == null)
        return;

    checkedCount = 0;
    totalItems = 0;
    
    var numRows = multiSelectTable.rows.length;

    for (var r=1; r<numRows; r++) {
      var row = multiSelectTable.rows[r];
      var numCells = row.cells.length;

      for (var c = 0; c < numCells; c++) {
          var cell1 = row.cells[c];
          if (cell1.children.length > 0) {
              var o = cell1.children[0];
              if (o.tagName == 'INPUT' && o.type == 'checkbox' && (!o.id || o.id.indexOf('selectAll') == -1)) {
                  totalItems++;
                  if (o.checked) {
                      checkedCount++;
                      lastClicked = r;
                  }
              }
          }

          if (oneCheckboxPerRow)
              break;
      }
    }
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


function processSelectedItems(callbackFunction)
{
    if(multiSelectTable == null)
        return;

    var checked = 0;
    var total   = 0;
    
    var numRows = multiSelectTable.rows.length;
    for (var r=1; r<numRows && checked < checkedCount; r++)
    {
        var row = multiSelectTable.rows(r);
        var numCells = row.cells.length;

        for (var c=0; c<numCells; c++)
        {
            var cell1 = row.cells[c];
            if (cell1.children.length > 0)
            {
                var o = cell1.children[0];
                if (o.tagName=='INPUT' && o.type=='checkbox' && (!o.id || o.id.indexOf('selectAll')== -1) && o.checked)
                {
                    checked++;
                    if (!callbackFunction(o))
                        return false;
                }
            }

            if (oneCheckboxPerRow)
                break;
        }
    }
    return true;
}

