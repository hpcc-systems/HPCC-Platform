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

var command;
var component;
var xmlDoc;
var schemaNode;
var objNode;
var bAdd;

    
function onLoad()
{
   var form  = document.forms[0];
   component = form.component.value;
   command   = form.command.value;

   schemaNode = xmlSchema.documentElement.selectSingleNode('//Arguments');
   if (schemaNode == null)
   {
      alert('No arguments node found in XML schema!');
      return;
   }
   
   var prevArgsDoc = document.all.xmlPrevArgsDoc;
   if (prevArgsDoc != undefined && prevArgsDoc != null)
   {
      xmlDoc = prevArgsDoc; 
      objNode = prevArgsDoc.documentElement;
      
      if (objNode == null)
      {
         bAdd = true;
         objNode = xmlDoc.createElement('Arguments');
         xmlDoc.documentElement.appendChild(objNode);
      }
      else
      {
         bAdd = false;
         initializeHtmlObjects(objNode, schemaNode);
      }
   }
   else
   {
      bAdd = true;
      
      //create xml fragment that would be passed to any child dialogs for them to fill in 
      //and is sent back as hidden input field 'xmlArgs'
      xmlDoc = new ActiveXObject("Microsoft.XMLDOM");
      xmlDoc.async = false;
      xmlDoc.loadXML('<Arguments/>');  
      
      objNode = xmlDoc.documentElement.selectSingleNode('//Arguments');
   }   
}

   
function onSubmit()
{
   /*
   if (bAdd)
   {
      //add immediate children to our data island. Note that nested data is updated
      //as a result of add/delete handlers
      //
      if (!addImmediateChildrenFromHtmlObjects(schemaNode, objNode, xmlDoc))
         return false;   
   }
   else
      replaceImmediateChildrenFromHtmlObjects(objNode. schemaNode);
      
    //update the value of our hidden HTML control that gets sent back with the submission
    //
   document.forms[0].xmlArgs.value = xmlDoc.xml;
   alert(xmlDoc.xml);
   */
   //alert(document.forms[0].outerHTML);
   return true;
}



function initializeHtmlObjects(objNode, schemaNode)
{
    for (var childSchemaNode = schemaNode.firstChild; 
         childSchemaNode != null; 
         childSchemaNode = childSchemaNode.nextSibling)
    {
      var name = childSchemaNode.nodeName;
      var maxOccursAttr = childSchemaNode.attributes.getNamedItem('maxOccurs');
      var maxOccurs = maxOccursAttr ? maxOccursAttr.nodeValue : null;
         
       if (maxOccurs == "unbounded")
       {
         var argsTableObj = document.all.ArgumentsTable;
         var tableObj  = argsTableObj.all[name];
            
          var targetNodes = objNode.selectNodes(name);

          //append rows in table
         var nNodes = targetNodes.length;
         var rowId = tableObj.rows.length;

         for (var i=0; i<nNodes; i++)
            insertRowInTable(tableObj, rowId++, targetNodes[i], childSchemaNode, name, true, false);
       }
       else
       {          
          var htmlObj = document.forms[0].elements[name];
          var targetNode = objNode.selectSingleNode(name);
         var dataTypeAttr = childSchemaNode.attributes.getNamedItem('dataType');
         var dataType = dataTypeAttr ? dataTypeAttr.nodeValue : null;
          
         if (dataType)
         {
            if (dataType == 'boolean')
            {
               var checked = targetNode.text == '1';
               htmlObj.value = checked ? 1 : 0;
               if (checked)
                  htmlObj.checked = 'true';
            }
         }
         else
             htmlObj.value = targetNode.text;
       }
    }   
}


function addImmediateChildrenFromHtmlObjects(schemaNode, objNode, xmlDoc)
{
   //keep xmlDoc in sync with contents of this page
   //so create elements corresponding to all immediate input fields
   
   for (var childSchemaNode = schemaNode.firstChild; 
        childSchemaNode != null; 
        childSchemaNode = childSchemaNode.nextSibling)
   {
       //only process nodes which don't have children since those would be processed
       //by add/delete/edit button handlers
       if (!childSchemaNode.hasChildNodes())
       {
         var name    = childSchemaNode.nodeName;       
         var newNode = xmlDoc.createElement(name);           
         var htmlObj = document.forms[0].elements[name];
         
         //we may have already added this node if we had errored out 
         //in last try.  So only add if it does not exist.
         //
         if (!objNode.selectSingleNode(name))
         {
            newNode.text = htmlObj.value;               
            objNode.appendChild(newNode);
          }  
      }
   }   
   return true;
}

function replaceImmediateChildrenFromHtmlObjects(objNode, schemaNode)
{
    for (var childNode = objNode.firstChild; 
         childNode != null; 
         childNode = childNode.nextSibling)
    {
      var name = childNode.nodeName;

       if (childNode.selectSingleNode('*') == null)
       {
          var htmlObj = document.forms[0].elements[name];
          var schemaChildNode = schemaNode.selectSingleNode(name);
         var dataTypeAttr = schemaChildNode.attributes.getNamedItem('dataType');
         var dataType = dataTypeAttr ? dataTypeAttr.nodeValue : null;

         if (dataType)
         {
            if (dataType == 'boolean')
               childNode.text = htmlObj.checked ? '1' : '0';
            else
               childNode.text = htmlObj.value;
         }
         else
             childNode.text = htmlObj.value;
       }
    }   
}

function isNumber(str)
{
   return parseInt(str).toString()==str;
}

function updateNestedData(operation, strXPath, index)
{
    //Get a pointer to the specific note to pass to the modal dialog.
    var targetNode;
        
    if (operation == "add")
    {
      var tokens = new Array();
      tokens = strXPath.split('.');

        //find out child node in xmlSchema (our schema) corresponding to the 
      //node for which we would add the table row:
      //
      var childSchemaNode = schemaNode;
      var targetNode = objNode;

      for (var i=0; i<tokens.length; i++)
         if (!isNumber(tokens[i]))
         {
            childSchemaNode = childSchemaNode.selectSingleNode(tokens[i]);

            var newNode = xmlDoc.createElement(tokens[i]);
              targetNode = targetNode.appendChild(newNode);
         }

      var nodeName  = targetNode.nodeName;

      var argsTableObj = document.all.ArgumentsTable;
      var tableName = strXPath;
      var tableObj  = argsTableObj.all[tableName];
      var rowId = tableObj.rows.length;//row 0 is header
        insertRowInTable(tableObj, rowId, targetNode, childSchemaNode, strXPath, true, false);
    }
    else//delete
    {
       var objNodes = objNode.selectNodes(strXPath);
        targetNode = objNodes.item(index);
        
       if (targetNode == null)
        alert('No matching data available!');

      if (confirm('Are you sure you want to delete this item?'))
      {
        deleteRowFromTable(index+1, targetNode); //row 0 is header
        targetNode.parentNode.removeChild(targetNode);
          enableDeleteButton(strXPath+".Delete", false);
      }
    }
            
   return true;
}

function createTable(domNodes, schemaNode)
{
   tableObj = document.createElement('table');
   tableObj.border = 1;
   
   //make header
   var rowId = 0;
      
   var row = tableObj.insertRow(rowId++);
   
   //insert checkbox header
    //col = row.insertCell();

    for (var childSchemaNode = schemaNode.firstChild; 
         childSchemaNode != null; 
         childSchemaNode = childSchemaNode.nextSibling)
   {
       var targetNode = domNodes[0].selectSingleNode(childSchemaNode.nodeName);    
       
       var thObj = document.createElement('th');
       thObj.innerText = targetNode.nodeName;
       
       col = row.insertCell();
       col.appendChild(thObj);
   }
   
   var nNodes = domNodes.length;
   
   for (var i=0; i<nNodes; i++)
      insertRowInTable(tableObj, rowId++, domNodes[i], schemaNode, false, false);

   return tableObj;   
}


function insertRowInTable(tableObj, rowId, domNode, schemaNode, strXPath, bDrawCheckBox, bChecked)
{   
   var nodeName = domNode.nodeName;
   var row = tableObj.insertRow(rowId);//insert after last row
   var lastRowIndex = tableObj.lastRowIndex ? parseInt(tableObj.lastRowIndex) + 1 : 0;
   tableObj.lastRowIndex = lastRowIndex.toString();
   row.id = strXPath + '.' + tableObj.lastRowIndex;

   updateItemList(tableObj, tableObj.lastRowIndex, null);

    if (bDrawCheckBox == true)
    {
      //insert checkbox
       var col = row.insertCell();
       
       //it isn't funny. better leave that trailing space if you wish to get 
       //the checkbox created:
       var cbName = strXPath + ".checkbox";
       col.innerHTML = '<input type="checkbox" id="' + cbName + '" name="' + cbName + '" value=""' + (bChecked ? ' checked="true"' : '') + ' onclick="onCheck(this)"> ';
    }
    
    for (var childSchemaNode = schemaNode.firstChild; 
         childSchemaNode != null; 
         childSchemaNode = childSchemaNode.nextSibling)
    {
       var targetNode = domNode.selectSingleNode(childSchemaNode.nodeName);
      if (targetNode == null)
      {
         targetNode = xmlDoc.createElement(childSchemaNode.nodeName);
         domNode.appendChild(targetNode);
      }
      insertCellsInRow(row, targetNode, childSchemaNode, strXPath + '.' + tableObj.lastRowIndex);
    }
   return row;
}

function updateItemList(tableObj, addItem, delItem)
{
   var id = tableObj.id + ".itemlist";
   var itemListInput = document.forms[0].all[id];
   if (itemListInput == null)
   {
      itemListInput = document.createElement("input");
      itemListInput.type = "hidden";
      itemListInput.id = id;
      itemListInput.name = id;
      itemListInput.value = "+";
      document.forms[0].appendChild( itemListInput );
   }

   if (addItem)
      itemListInput.value += addItem + '+';
   else
   {
      var list = itemListInput.value;
      var begin = list.indexOf('+' + delItem + '+');
      if (begin == -1)
         alert("Item list management error!");
      begin++;
      var end = list.indexOf('+', begin);
      if (end == -1)
         alert("Item list management error!");
      itemListInput.value = list.substring(0, begin) + list.substring(end+1);
   }
}

function insertCellsInRow(row, targetNode, schemaNode, strXPath)
{
   var maxOccursAttr = schemaNode.attributes.getNamedItem('maxOccurs');
   var maxOccurs = maxOccursAttr ? maxOccursAttr.nodeValue : null;
    
    if (maxOccurs == "unbounded")
    {   
       var targetNodes = domNode.selectNodes(childSchemaNode.nodeName);
       if (targetNodes != null && targetNodes.length > 0)
       {
          var tableObj = createTable(targetNodes, childSchemaNode);
          cell.appendChild(tableObj);
       }
    }
   else
   if (maxOccurs == "1" && schemaNode.selectNodes("*"))
   {
      var xpath = strXPath + '.' + schemaNode.nodeName;

       for (var childSchemaNode = schemaNode.firstChild; 
            childSchemaNode != null; 
            childSchemaNode = childSchemaNode.nextSibling)
       {
         var name = childSchemaNode.nodeName;
         var targetNode2 = targetNode.selectSingleNode(name);
         if (targetNode2 == null)
         {      
            targetNode2 = xmlDoc.createElement(name);
            targetNode.appendChild(targetNode2);
         }

         insertCellsInRow(row, targetNode2, childSchemaNode, xpath);
       }
   }
    else
    {
      //create an HTML object corresponding to this element
      //
       var cell = row.insertCell();
      var input = createInputControlForNode(cell, strXPath, targetNode, schemaNode);
      cell.appendChild(input);
    }       
}

function createInputControlForNode(cell, idPrefix, node, schemaNode)
{
   var id = node.nodeName;
   var dataTypeAttr = schemaNode.attributes.getNamedItem('dataType');
   var dataType = dataTypeAttr ? dataTypeAttr.nodeValue : null;

   var value = node.text;
   if (value == null)
   {
      var defaultAttr = schemaNode.attributes.getNamedItem('default');
      value = defaultAttr ? defaultAttr.nodeValue : "";
   }

   var type;
   var checked = false;
   if (dataType == "boolean")
   {
      type = "checkbox";
      //debugger;
      if (value == "1" || value == "yes" || value=="true")
      {
         checked = true;
         value = "1";
      }
      else
         value = "0";
   }
   else
   {
      type = "text";
      value = value;
   }         

   var input = document.createElement("input");
   input.name = idPrefix + '.' + id;
   input.type = type;
   input.value = value;


   if (type == "checkbox")
   {
      if (checked)
         input.checked = "true";
      input.onclick=function(){this.value=1-this.value;};
   }
   else
      input.width=cell.clientWidth ? cell.clientWidth-2 : 70;

   return input;
}

function deleteRowFromTable(rowId, domNode)
{   
   var argsTableObj = document.all.ArgumentsTable;
   
   var tableName = domNode.nodeName;
   var tableObj = argsTableObj.all[tableName];
   var id = tableObj.rows[rowId].id;
   var pos = id.lastIndexOf('.');
   if (pos == -1)
      alert('Invalid row id!');

   updateItemList(tableObj, null, id.substring(pos+1));
   tableObj.deleteRow(rowId);      
}

function onAdd(nodeSetName) 
{
    //if (!addImmediateChildrenFromHtmlObjects(schemaNode, objNode, xmlDoc))
   //   return;
         
   updateNestedData("add", nodeSetName, -1);
   //alert(document.all.ArgumentsTable.outerHTML);
}

function onDelete(nodeSetName)
{
   var nSelectedRow = getCheckedBoxWithId(nodeSetName+".checkbox");   
    updateNestedData("delete", nodeSetName, nSelectedRow);          
}
    
function onCheck(cb)
{
    //the user checked on one so we have at least one checkbox
    //However, we may have only that checkbox and length is 0 in that case
    
    checkBoxes = document.forms[0].all[ cb.id ];
        
    //if we have only one checkbox then length is undefined in that case
    if (checkBoxes.length != undefined)
    {
        //uncheck any previously selected checkbox
      var nCheckBoxes = checkBoxes.length;
        for (var i=0; i<nCheckBoxes; i++)
           if (checkBoxes[i].checked && checkBoxes[i] != cb)
           {
              checkBoxes[i].checked = false;
              break;
           }              
    }
    
    //enable delete and edit buttons
   var pos = cb.id.lastIndexOf(".checkbox");
   var delId = cb.id.substring(0, pos) + ".Delete";
    enableDeleteButton(delId, cb.checked);
}

function getCheckedBoxWithId(id)
{
    var checkBoxes = document.forms[0].all[id];
    
    //if we have only one checkbox then length is undefined in that case
    if (checkBoxes.length == undefined)
    {
        //we (un)checked the same checkbox so toggle selection
        return checkBoxes.checked ? 0 : -1;
    }
    else
    {
      var nCheckBoxes = checkBoxes.length;
        for (var i=0; i<nCheckBoxes; i++)
           if (checkBoxes[i].checked)
              return i;
        
        return -1;
    }
}

function enableDeleteButton(id, enable)
{
   var btn = document.all[id];
    btn.disabled = !enable;
}

