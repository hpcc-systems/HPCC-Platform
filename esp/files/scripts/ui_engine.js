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
var schemaRootNode;
var objRootNode;
var bAdd;
var argsNodeName;


function loadCommandInfo()
{
    setModified(false);
    document.body.onbeforeunload = onBeforeUnloadDocument;

    component = document.getElementById('component').value;
    command = document.getElementById('command').value;
    
    if (typeof xmlSchema != 'undefined' && xmlSchema.documentElement)
    {
        var xpath = '/Components/' + component + '/Commands/' + command;
        var commandSchemaNode = xmlSchema.documentElement.selectSingleNode(xpath);
        argsNodeName = getAttribute(commandSchemaNode, 'argsNode', 'Arguments');

        schemaRootNode = commandSchemaNode.selectSingleNode(argsNodeName);
        if (schemaRootNode == null)
        {
            alert('No arguments node found in XML schema!');
            return;
        }
    }
    else
    {
        schemaRootNode = null;
        argsNodeName = 'Arguments';
    }

    var prevArgsDoc = document.getElementById("xmlPrevArgsDoc");
    if (prevArgsDoc)
    {
        xmlDoc = prevArgsDoc; 
        objRootNode = prevArgsDoc.documentElement;

        if (schemaRootNode)
        {
            var captionObj = document.getElementById('pageCaption');
            if (captionObj)
                captionObj.innerText = expand_embedded_xpaths( captionObj.innerText, schemaRootNode, objRootNode, false);
        }
    }
    else
        objRootNode = null; 

    var subCaptionObj = document.getElementById('pageSubCaption');
    if (subCaptionObj)
    {
        var subcaption = subCaptionObj.innerHTML;
        if (subcaption.indexOf('Loading,')==0)
            subCaptionObj.style.display = 'none';
        else
            if (prevArgsDoc && schemaRootNode)
                subCaptionObj.innerText = expand_embedded_xpaths( subcaption, schemaRootNode, objRootNode, false);
    }
    //alert(tableObj.outerHTML);
    //alert(document.forms[0].outerHTML);

    initFixedTables();
    ms_initializeAll();
    var pageBody = document.getElementById('pageBody');
    if (pageBody)
        pageBody.focus();

    if (typeof onLoadCustom != 'undefined')
        onLoadCustom();
}

function createTable(id, className, border, width)
{
    tableObj = document.createElement('table');
    tableObj.id = id;

    if (border)
        tableObj.border = border;

    if (className)
        tableObj.className='sort-table';

    if (width)
        tableObj.width = width; 
    return tableObj;    
}

function populateTable(tableObj, targetNode, schemaNode, xpath)
{
    //make header
    createColumnHeaders(tableObj, null, targetNode, schemaNode, xpath, undefined);

    addTableRows(tableObj, targetNode, schemaNode, xpath, null);

    //create buttons
    var xpathBtns = (schemaNode == schemaRootNode && argsNodeName == '.') ? 'Buttons' : '../Buttons';
    var btnsNode = schemaNode.selectSingleNode(xpathBtns);
    if (btnsNode || schemaNode == schemaRootNode)
    {
        createButtons(tableObj, btnsNode, false);
        if (tableObj.offsetHeight > 600)
            createButtons(tableObj, btnsNode, true);
    }
}

function createColumnHeaders(tableObj, row, targetNode, schemaNode, xpath, nTableRows)
{
    //insert checkbox column header unless checkboxes are not desired
    //
    var xpath2;
    if (schemaNode == schemaRootNode) 
        xpath2 = null;
    else
        xpath2 = xpath ? xpath + '.' + schemaNode.nodeName : schemaNode.nodeName;
    
    var maxOccurs = getAttribute(schemaNode, 'maxOccurs', null);
    if (maxOccurs)//each table column is an attribute of a target node 
    {
        if (maxOccurs == 'unbounded')
        {
            var checkboxes = getAttributeBool(schemaNode, 'checkboxes', true);
            if (checkboxes)
            {
                if (!row)
                    row = tableObj.insertRow();

                col = document.createElement('th');
                col.width = '10';
                row.appendChild(col);

                //make multi selection checkbox if multiple selection is desired
                //
                var multiselect = getAttributeBool(schemaNode, 'multiselect', false);
                if (multiselect)
                {
                    var childObjNodes = targetNode.selectNodes(schemaNode.nodeName);
                    nTableRows = childObjNodes.length;
                    var multiselect = ms_create(    tableObj.id, onRowCheck )
                    if (nTableRows > 1)
                    {
                        //insert checkbox
                        var bChecked = getAttributeBool(schemaNode, 'checked', false);
                        
                        //var cbName = xpath2 + ".checkbox";
                        var s = new Array();
                        var i=0;
                        s[i++] = '<input type="checkbox" value="1"';
                        if (bChecked)
                            s[i++] = ' checked="true"';
                        s[i++] = ' title="Select or unselect all items"';
                        s[i++] = ' onclick="ms_setAll(this, \'';
                        s[i++] = multiselect.tableId;
                        s[i++] = "',";
                        s[i++] = multiselect.n_column;
                        s[i++] = ')"> </input>';
                        col.innerHTML =  s.join('');
                        col.id = multiselect.makeSetAllId(true);
                    }
                }               
            }
        }
        else
            if (xpath == null)//if this node does not recur then we don't need column headers for this node
                return;

        //descend to the children of schemaNode
        //
        var childObjNode = targetNode ? targetNode.selectSingleNode(schemaNode.nodeName) : null;
        for ( var childSchemaNode = schemaNode.firstChild; 
                childSchemaNode; 
                childSchemaNode = childSchemaNode.nextSibling)
        {
            var name = childSchemaNode.nodeName;
            if (name != "#comment" && name != 'Buttons')//ignore comments and buttons
            {
                var maxOccurs = getAttribute(childSchemaNode, 'maxOccurs', null);

                if (maxOccurs=='1')
                    createColumnHeaders(tableObj, row, childObjNode, childSchemaNode, xpath2, nTableRows);
                else
                {
                    if (maxOccurs == null)//an attribute
                    {
                        var viewType = getAttribute(childSchemaNode, 'viewType', null);
                        if (viewType != 'hidden')
                        {
                            if (!row)
                                row = tableObj.insertRow();

                            var thObj = document.createElement('th');
                            var caption = getAttribute(childSchemaNode, 'caption', name);
                            var innerText = caption;

                            //if multiple selection is desired on this column
                            //
                            if ((nTableRows == undefined || nTableRows > 1) && 
                                 getAttributeBool(childSchemaNode, 'multiselect', false))
                            {
                                //if there are multiple rows in this table
                                //create a multiple selection object to manage this column
                                //
                                var multiselect = ms_create(    tableObj.id, onColumnCheck, null, row.cells.length )
                                thObj.id = multiselect.makeSetAllId(true);

                                var dataNode = childObjNode ? childObjNode.selectNodes(childSchemaNode.nodeName)[0] : null;
                                var dataType = getAttribute(childSchemaNode, 'dataType', null);
                                var input = createInputControlForNode(xpath, dataNode, childSchemaNode, dataType, viewType, multiselect, true);
                                input.style.display = 'none';
                                multiselect.inputTag  = input.tagName;
                                multiselect.inputType = input.type;
                                
                                var s = new Array();
                                var i = 0;
                                s[i++] = "<a href='' onClick=\"toggleMultiSelect( '";
                                s[i++] = tableObj.id;
                                s[i++] = "',";
                                s[i++] = row.cells.length;
                                s[i++] = ", true); return false;\"";
                                s[i++] = "title='Set all items in this column...'>";
                                s[i++] = caption
                                s[i++] = "</a>"

                                thObj.innerHTML = s.join('');
                                thObj.appendChild(input);
                                innerText = null;
                            }

                            if (innerText)
                                thObj.innerText = innerText;

                            row.appendChild(thObj);
                        }
                    }
                }
            }
        }
    }
}           

function toggleMultiSelect( tableId, column, bTopOfTable )
{
    var ms = ms_lookupByTableAndColumn(tableId, column);
    if (!ms.b_initialized)
        ms.initialize();

    var id = ms.makeSetAllId(bTopOfTable) + '_I';
    var o = document.getElementById( id );
    if (o)
    {
        var bShow = o.style.display == 'none';
        o.style.display = bShow ? 'block' : 'none';
    }
}

function createButtons(tableObj, btnsNode, addOnTop)
{
    //create a blank line for vertical spacing
    //
    var nCols = findMaxTableColumns(tableObj);
    if (nCols < 1)
        nCols = 1;      

    //insert blank line for vertical separation
    //
    if (addOnTop)
    {
        row = tableObj.insertRow(0);
        blankRow = tableObj.insertRow(1);
    }
    else
    {
        blankRow = tableObj.insertRow(-1);
        row = tableObj.insertRow(-1);
    }

    cell = blankRow.insertCell();
    cell.height = '15px';
    cell.colSpan= nCols;

    cell = row.insertCell();
    cell.align="center";
    cell.colSpan= nCols;

    var innerHTML = '';
    //if (!btnsNode || getAttributeBool(btnsNode, 'showResetButton', true))
    //  innerHTML += '<input type="reset" value="Reset"/>&nbsp';

    if (btnsNode)
    {
        for (var btn = btnsNode.firstChild; btn; btn=btn.nextSibling)
        {
            if (innerHTML != '')
                innerHTML += '&nbsp;&nbsp;';
            innerHTML += btn.xml;
        }
    }
    else
    {
        if (innerHTML != '')
            innerHTML += '&nbsp;&nbsp;';

        var onClickOKBtn = btnsNode ? getAttribute(btnsNode, 'OnClickOKBtn', null) : null;
        var btnName = "OK" + (addOnTop ? "1" : "");
        if (onClickOKBtn)
            innerHTML += '<input type="button" name="' + btnName + '" value="OK" onclick="' + onClickOKBtn + '"/>';
        else
            innerHTML += '<input type="submit" name="' + btnName + '" value="Submit"/>';
    }
    cell.innerHTML = innerHTML;
}

function addTableRows(tableObj, objNode, schemaNode, xpath, lastRow)
{
    var name = schemaNode.nodeName;
    if (name == "#comment" || name == 'Buttons')//ignore comments
        return;

    var maxOccurs = getAttribute(schemaNode, 'maxOccurs', null);
    var xpath2;
    if (xpath)
        xpath2 = xpath + '.' + name;
    else
        xpath2 = (schemaNode == schemaRootNode && argsNodeName=='Arguments') ? null : name;

    var showHideRow = null;

    if (maxOccurs == "unbounded")
    {
        if (tableObj.id != xpath2)
        {
            //create another table that would occupy entire cell of parent table's row  
            var childTableObj = createTable(xpath2, 'sort-table', 1, '100%');
            var row = tableObj.insertRow(-1);//insert new row
            var cell = row.insertCell();
            cell.appendChild(childTableObj);

            populateTable(childTableObj, objNode, schemaNode, xpath);
        }
        else
        {
            var bRowCheckbox = getAttributeBool(schemaNode, 'checkboxes', true);
            var checked = getAttributeBool(schemaNode, 'checked', false);
            
            //append rows in table
            var targetNodes = objNode.selectNodes(name);
            var nNodes = targetNodes.length;
            for (var i=0; i<nNodes; i++)
            {
                var row = tableObj.insertRow(-1);//insert after last row
                var lastRowIndex = tableObj.lastRowIndex ? parseInt(tableObj.lastRowIndex) + 1 : 0;
                updateItemList(tableObj, lastRowIndex, null);
                var showHideRow = populateTableRow( tableObj, row, targetNodes[i], 
                                                                schemaNode, xpath2, bRowCheckbox, 
                                                                checked == '1' || checked == 'true');
                if (showHideRow)
                {
                    var newRow = tableObj.insertRow(-1);//insert after last row
                    newRow.id = row.id + '.toggle';
                    newRow.style.display = 'none';
            
                    var cell = newRow.insertCell();
                    cell.colSpan = row.cells.length - (bRowCheckbox ? 1 : 0);
                    cell.style.textAlign = 'left';
                    //cell.innerHTML = showHideRow['node'].text.replace( new RegExp('\n', 'g'), '<br/>');
                    cell.innerText = showHideRow['node'].text;
                }//if
            }//for
        }
    }
    else
    if (maxOccurs == "1" || schemaNode == schemaRootNode)
    {
        var targetNode = objNode.selectSingleNode(name);
        if (!targetNode)
        {
            targetNode = xmlDoc.createElement(name);
            objNode.appendChild(targetNode);
        }

        var caption = getAttribute(schemaNode, 'caption', null);
        var captionCell = null;
        if (caption && (argsNodeName !='.' || schemaNode != schemaRootNode))
        {
            //if this table has at least one line then leave a blank line
            if (tableObj.rows.length && tableObj.rows.length > 0)
            {
                var row = tableObj.insertRow(-1);
                var cell = document.createElement('td');
                cell.height = 20;
                row.appendChild(cell);
            }

            //create another table that would occupy entire cell of parent table's row  
            var childTableObj = createTable(xpath2, null, 0, '100%');

            //add caption for this child table
            //
            row = childTableObj.insertRow();
            captionCell = row.insertCell();
            captionCell.innerHTML = '<h3>' + caption + '</h3>';
            captionCell.align = 'left';
            captionCell.noWrap= true;

            row = tableObj.insertRow();//insert new row
            cell = row.insertCell();
            cell.noWrap = true;
            cell.appendChild(childTableObj);

            tableObj = childTableObj;
        }

        for ( var childSchemaNode = schemaNode.firstChild; 
                childSchemaNode; 
                childSchemaNode = childSchemaNode.nextSibling)
        {
            var childName = childSchemaNode.nodeName;
            if (childName != "#comment" && childName != 'Buttons')//ignore comments and buttons
            {
                var showHideRow2 = addTableRows(tableObj, targetNode, childSchemaNode, xpath2, null);//false, false
                if (showHideRow2)
                    showHideRow = showHideRow2;
            }
        }

        if (captionCell)
            captionCell.colSpan = findMaxTableColumns(childTableObj);
    }
    else//an attribute
    {   
        var caption = getAttribute(schemaNode, 'caption', name);

        var row = tableObj.insertRow();
        var cell = document.createElement('td');
        //cell.style.padding=0;
        cell.innerHTML = '<b>' + caption + '</b>';
        cell.width = '1%';
        row.appendChild(cell);

        cell = document.createElement('td');
        cell.innerHTML = '<b>:</b>';
        cell.width = '1%';
        row.appendChild(cell);

        var targetNode = objNode.selectSingleNode(name);
        if (!targetNode)
        {
            targetNode = xmlDoc.createElement(name);
            objNode.appendChild(targetNode);
        }

        showHideRow2 = insertAttribNodeInRow(tableObj, row, targetNode, schemaNode, xpath, 'left');
        var ncells= row.cells.length;
        row.cells[ncells-1].width = '98%';
        if (showHideRow2)
            showHideRow = showHideRow2;
    }
}

function populateTableRow(tableObj, row, domNode, schemaNode, strXPath, bRowCheckbox, bChecked)
{   
   var nodeName = domNode.nodeName;
   var lastRowIndex = tableObj.lastRowIndex ? parseInt(tableObj.lastRowIndex) + 1 : 0;
   tableObj.lastRowIndex = lastRowIndex.toString();
   var showHideRow = null;

   if (bRowCheckbox)
   {
      row.id = strXPath + '.' + tableObj.lastRowIndex;
      
      //insert checkbox
      var col = row.insertCell();      
      var cbName = strXPath + ".checkbox";

        //it is much faster to join a string array than assign concatenating strings
        //
        var innerHTML = new Array();
        var i = 0;
        innerHTML[i++] = '<input type="checkbox" name="';
        innerHTML[i++] = cbName;
        innerHTML[i++] = '" value="1"';
        if (bChecked)
            innerHTML[i++] = ' checked="true"';

        var multiselect = ms_lookupByTableAndColumn(tableObj.id, 0);
        if (multiselect)
        {
            innerHTML[i++] = ' onclick="ms_onChange(this, \'';
            innerHTML[i++] = multiselect.tableId;
            innerHTML[i++] = "',";
            innerHTML[i++] = multiselect.n_column;
            innerHTML[i++] = ')"';
        }
        innerHTML[i++] = '> </input>';
      col.innerHTML = innerHTML.join('');
   }
   
   for ( var childSchemaNode = schemaNode.firstChild; 
            childSchemaNode; 
            childSchemaNode = childSchemaNode.nextSibling)
   {
      var name = childSchemaNode.nodeName;
      if (name != "#comment" && name != 'Buttons')
        {      
            var showHideRow2 = insertCellInRow(tableObj, row, domNode, childSchemaNode, strXPath + '.' + tableObj.lastRowIndex);
            if (showHideRow2)
                showHideRow = showHideRow2;
        }
   }
   return showHideRow;
}



function insertCellInRow(tableObj, row, targetNode, schemaNode, strXPath)
{
    var showHideRow = null;
    
    var maxOccurs = getAttribute(schemaNode, 'maxOccurs', null);    
    if (maxOccurs == "unbounded")
    {   
        //create another table that would occupy entire cell of parent table's row  
        var xpath2 = strXPath ? strXPath + '.' + schemaNode.nodeName : schemaNode.nodeName;
        var childTableObj = createTable(xpath2, null, 0, '100%');

        populateTable(childTableObj, targetNode, schemaNode, strXPath);

        var cell = row.insertCell();
        cell.appendChild(childTableObj);
        return;
    }

    var name = schemaNode.nodeName;
    var childNode = null;
    if (maxOccurs == null)//see if this is for innerText
    {
        var dataType = getAttribute(schemaNode, 'dataType', null);
        if (dataType == 'innerText')
            childNode = targetNode;
    }

    if (childNode == null)
    {
        childNode = targetNode.selectSingleNode(name);
        if (childNode == null)
        {
            childNode = xmlDoc.createElement(name);
            targetNode.appendChild(childNode);
        }
    }

    if (maxOccurs == "1")
    {
        var xpath = strXPath ? (strXPath + '.' + name) : name;      
        for ( var childSchemaNode = schemaNode.firstChild; 
                childSchemaNode; 
                childSchemaNode = childSchemaNode.nextSibling)
        {
            var childName = childSchemaNode.nodeName;
            if (childName != '#comment' && childName != 'Buttons')//ignore comments
            {
                var showHideRow2 = insertCellInRow(tableObj, row, childNode, childSchemaNode, xpath);
                if (showHideRow2)
                    showHideRow = showHideRow2;
            }
        }
    }
    else //attribute
        showHideRow = insertAttribNodeInRow(tableObj, row, childNode, schemaNode, strXPath);

    return showHideRow;
}

function insertAttribNodeInRow(tableObj, row, targetNode, schemaNode, strXPath, alignment)
{

    var showHideRow = null;
    //create an HTML object corresponding to this element
    //
    var dataType = getAttribute(schemaNode, 'dataType', null);
    var viewType = getAttribute(schemaNode, 'viewType', null);
    var multiselect = ms_lookupByTableAndColumn( tableObj.id, row.cells.length );
    var input = createInputControlForNode(strXPath, targetNode, schemaNode, dataType, viewType, multiselect, false);
    var inputType = input.type;
    var disabled = getAttribute(schemaNode, 'disabled', null);
    
    var b_static = false;
    if (inputType == "hidden")
    {
        row.appendChild(input);
        b_static = viewType && (viewType.indexOf('static')==0 || viewType == 'tooltip');
        if (!b_static)
            return null;
    }
    
    
    if (disabled == 'true') {
        input.disabled = true;
    }
    
    if (viewType == 'showHideRowBtn')
    {
        if (targetNode.text == '')
            input.disabled = true;
        else
        {
            showHideRow = new Array();
            showHideRow['node'] = targetNode;
            showHideRow['schemaNode'] = schemaNode;
            input.onclick = new Function('onShowHideRow(this, "' + row.id + '.toggle")');
        }

        input2 = document.createElement('input');
        input2.id = strXPath ? (strXPath + '.' + targetNode.nodeName) : targetNode.nodeName;
        input2.name = input2.id;
        input2.type = 'hidden';
        input2.value = targetNode.text;
        if (input2.value=="")
            input2.value = getAttribute(schemaNode, 'default', '');
        row.appendChild(input2);
    }

    var cell = row.insertCell(row.cells.length);

    var align = getAttribute(schemaNode, 'align', null);
    if (align)
        cell.align = align;
    else
        if (alignment)
            cell.align = alignment;

    var vAlign = getAttribute(schemaNode, 'valign', null);
    if (vAlign)
        cell.vAlign = vAlign;


    if (inputType != "checkbox" && inputType != "file" && inputType != "button" && inputType != "hidden")
    {
        if (getAttribute(schemaNode, 'noWrap', null))
            cell.noWrap = true;

        var cellWidth = getAttribute(schemaNode, 'width', null);
        if (cellWidth)
            cell.width = cellWidth;
        else
        {
        /*
            if ((inputType != 'text' || !getAttribute(schemaNode, 'size', null)) && row.rowIndex > 0)
            {
                var firstRow = tableObj.rows[0].cells[;
                input.width=cell.clientWidth ? cell.clientWidth : 70;
            }
        */
        }
    }

    if (b_static)
    {
        if (viewType == 'tooltip')
        {
            var len = targetNode.text.length;
            if (len <= 7)
                cell.innerText = targetNode.text;
            else
            {
                cell.innerText = targetNode.text.substring(0,4) + '...';
                cell.onmouseover=function(){ EnterContent('ToolTip', null, targetNode.text, true); Activate();};
                cell.onmouseout=deActivate;
            }               
        }
        else if (dataType == 'boolean')
        {
            cell.innerText = targetNode.text;
        }
        else if (targetNode.text != '')
        {
            var colonPos = viewType.indexOf(':');
            if (colonPos == -1)//not found
                cell.innerText = targetNode.text;
            else
            {
                var optionsArray = viewType.substring(colonPos+1).split('|');
                cell.innerText = optionsArray[ targetNode.text ];
            }
        }
    }
     
    cell.appendChild(input);

    //hack: somehow the checkboxes get unchecked after insertion into the cell so check 'em
    if (inputType=="checkbox")
    { 
        if (input.value=="1" && !input.checked)
            input.checked = true;
        input.value = "1";
    }
    return showHideRow;
}
    

//-----------------------------------------------------------------------------
//-----------------------------------------------------------------------------

function createInputControlForNode(idPrefix, node, schemaNode, dataType, viewType, multiselect, columnHeader)
{   
    var id; 
    var value;

    if (columnHeader)
    {
        id = multiselect ? multiselect.makeSetAllId(true) + '_0': null; 
        value = null;
    }
    else
    {
        id = node.nodeName; 
        value = node.text;
        if (value=="")
            value = getAttribute(schemaNode, 'default', '');
    }
        
    var type;
    var checked = false;
    var input;
    var inputName = 'input';
    var modifiedHandlerName = null;
    
    if ( viewType && 
         (viewType == "hidden" || viewType.indexOf("static")==0 || viewType == "tooltip"))
    {
        type = "hidden";
        if (columnHeader)
        {
            alert("Multiple selection is not supported for columns with controls of view type '" + viewType + "'!");
            return null;
        }
    }
    else if (viewType == 'select')
    {
        type = "select";
        inputName = 'select';
        modifiedHandlerName = 'onchange';
    }
    else if (viewType == 'showHideRowBtn')
    {
        if (columnHeader)
        {
            alert('Multiple selection is not supported for columns with toggle buttons!');
            return null;
        }
        id += '.btn';
        type = "button";
        value = "Show";
    }
    else if (dataType == "boolean")
    {
        type = "checkbox";
        modifiedHandlerName = 'onclick';
        if (value == "1" || value == "yes" || value=="true")
            checked = true;
    }
    else    if (dataType == "file")
    {
        if (columnHeader)
        {
            alert('Multiple selection is not supported for columns with file controls!');
            return null;
        }
        type = "file";
        modifiedHandlerName = 'onclick';
    }
    else
    {
        type = "text";
        modifiedHandlerName = 'onchange';
    }
    
    input = document.createElement(inputName);
    if (inputName == 'input')
        input.type = type;
    
    if (columnHeader)
    {
        input.title = type=='checkbox' ? "Select or unselect all items" : "Update all items in this column with this value";
        input.id = id;
    }
    else
    {
        input.id = idPrefix ? (idPrefix + '.' + id) : id;
        input.name = input.id;
        input.value = value;    
    }

    if (type == "checkbox")
    {
        if (checked)
            input.checked = true;
    }
    else if (type == 'text')
    {
        sz = getAttribute(schemaNode, 'size', null);
        if (sz)
            input.size = sz;
    }
    else if (viewType == 'select')
    {
        var source = getAttribute(schemaNode, 'source', null);
        if (source)
        {
            //we only support source == 'object' so far
            //optionTag defines tag name of child that defines an option
            var optionTag = getAttribute(schemaNode, 'option', 'option');
        
            /*
                '@text', '@value' and '@selected' attributes of schemaNode 
                hold the needed tag names which are used to populate the drop down list
                these are supposed to be child nodes of option node in object hierarchy
            
                for instance, the following schema node definition creates a select object
                from the object node below and selects second item:
                <NameServices caption="Name Service" viewType="select" source="object"
                 option="NameService" text="Name" value="Value" selected="Selected"/>
                
                object node:
                <NameServices>
                    <NameService>
                        <Name>ns1</Name>
                        <Value>val1</Value>
                    </NameService>
                    <NameService>
                        <Name>ns2</Name>
                        <Value>val2</Value>
                        <Selected>true</Selected>
                    </NameService>
                </NameServices>
            */
            var textTag = getAttribute(schemaNode, 'text', null);
            var valueTag = getAttribute(schemaNode, 'value', null);
            var selectedTag = getAttribute(schemaNode, 'selected', null);
            
            if (node)
            {
                var options = node.selectNodes(optionTag);
                var n_options = options.length;
                for (var i=0; i<n_options; i++)
                {
                    var option = document.createElement("option");
                    option.text= options[i].selectSingleNode(textTag ).text;
                    option.value=options[i].selectSingleNode(valueTag).text;
                    input.add(option);
                    
                    var selected = options[i].selectSingleNode(selectedTag)
                    if (selected && (selected.text=='1' || selected.text == 'true'))
                        option.selected = true;
                }
            }
            else
            {
                var option = document.createElement("option");
                option.text= "-";
                option.value="";
                input.add(option);
                input.disabled = true;              
            }
        }//source
        else
        {
            for ( var childSchemaNode = schemaNode.firstChild; 
                    childSchemaNode;
                    childSchemaNode = childSchemaNode.nextSibling)
            {
                if (childSchemaNode.nodeName != '#comment' && 
                    childSchemaNode.nodeName != 'Buttons')//ignore comments and buttons
                {                   
                    if (childSchemaNode.nodeName == 'option')
                    {
                        var option = document.createElement("option");
                        option.text=childSchemaNode.text;
                        option.value=getAttribute(childSchemaNode, 'value', '');
                        
                        input.add(option);              
                        
                        if (value && value == option.value)
                            option.selected = true;
                    }
                }
            }
        }//else     
    }//viewType == 'select'

    if (modifiedHandlerName)
        setModifiedHandler(schemaNode, node, input, modifiedHandlerName, multiselect, columnHeader);

    if ( getAttributeBool(schemaNode, 'disabled', false) || (node && getAttributeBool(node, 'disabled', false)) )
        input.disabled = true;
        
    return input;
}

function getAttribute(obj, attrName, defaultVal)
{
    var attr = obj.attributes.getNamedItem(attrName);
    return attr ? attr.nodeValue : defaultVal;
}

function getAttributeBool(obj, attrName, defaultVal)
{
    var attr = obj.attributes.getNamedItem(attrName);
    return attr ? (attr.nodeValue=='true' || attr.nodeValue == '1') : defaultVal;
}

//handle button with a given name
//
function enableButton(btnName, enable)
{
    //note that there may be 2 buttons (at top and bottom of table) with the same name
    //
    var btns = document.getElementsByName(btnName);
    var nBtns= btns.length;

    for (var j=0; j < nBtns; j++)
        btns[j].disabled = !enable;
}

//handle button with a given prefix + enumeration (i.e. prefix1, prefix2, prefix3...)
//
function enableButtons(btnNamePrefix, enable)
{
    for (var i=1; ; i++)
    {
        //note that there may be 2 buttons (at top and bottom of table) with the same name
        //
        var btns = document.getElementsByName(btnNamePrefix + i);
        var nBtns= btns.length;

        if (nBtns > 0)
        {
            for (var j=0; j < nBtns; j++)
                btns[j].disabled = !enable;
        }
        else 
            break;
    }
}

function onAdd(nodeSetName) 
{
    //if (!addImmediateChildrenFromHtmlObjects(schemaRootNode, objRootNode, xmlDoc))
    //  return;
    updateNestedData("add", nodeSetName, -1);
    ms_reinitializeAll();
    setModified();
}

function onEdit(nodeSetName) 
{
    var index = getFirstCheckedItemIndex(nodeSetName);
    updateNestedData("edit", nodeSetName, index);
    setModified();
}

function onDelete(nodeSetName, simulate)
{
    var multiselect = ms_lookupByTableAndColumn(nodeSetName, 0);
    var index = getFirstCheckedItemIndex(nodeSetName);
    var rc = false;
    if (index == -1)
        alert("Please select one or more items to delete!");
    else
        if (confirm('Are you sure you want to ' + (simulate ? 'remove' : 'delete') + ' the selected item' + 
                            (multiselect.getSelectionCount()!=1?'s':'') + '?'))
        {
            rc = true;
            while (rc && index != -1)
            {
                if (!updateNestedData("delete", nodeSetName, index, simulate))
                    rc = false;
                else
                    index = getFirstCheckedItemIndex(nodeSetName, simulate ? index+1 : index);
            }

            if (!simulate)
                ms_reinitializeAll();
            onRowCheck(multiselect.id);
            setModified();
        }
    return rc;
}
    
function getFirstCheckedItemIndex(xpath, startIndex)
{
    var ndx = -1;
    var checkBoxes = document.getElementsByName(xpath+".checkbox");
    if (startIndex == undefined)
        startIndex = 0;
    
    //if we have only one checkbox then length is undefined in that case
    if (checkBoxes.length == undefined)
    {
        //we (un)checked the same checkbox so toggle selection
        if (checkBoxes.checked && startIndex == 0)
            ndx = 0;
    }
    else
    {
        var nCheckBoxes = checkBoxes.length;
        for (var i=startIndex; i<nCheckBoxes; i++)
            if (checkBoxes[i].checked)
            {
                ndx = i;
                break;
            }
    }

    return ndx;
}

function getEnclosingTableRow(htmlObj)
{
    var row = null; 
    for (var parent = htmlObj.parentElement; parent; parent = parent.parentElement)
        if (parent.tagName.toUpperCase() == 'TR' && parent.id != '')
        {
            row = parent;
            break;
        }
    return row;
}

function addImmediateChildrenFromHtmlObjects(doc, schemaNode, objNode, xmlDoc)
{
    //keep xmlDoc in sync with contents of this page
    //so create elements corresponding to all immediate input fields
    
    for ( var childSchemaNode = schemaNode.firstChild; 
            childSchemaNode != null; 
            childSchemaNode = childSchemaNode.nextSibling)
    {
        var name = childSchemaNode.nodeName;        
        if (name == "#comment" || name == 'Buttons')//ignore comments and buttons
            continue;
        if (childSchemaNode.hasChildNodes())
        {
            //if this node has children but only occurs once then create a node
            //corresponding to this schema node and process its children instead
            //
            var maxOccurs = getAttribute(childSchemaNode, 'maxOccurs', null);           
            if (maxOccurs == "1")
            {
                var newNode = objNode.selectSingleNode(name);
                if (!newNode)
                {
                    newNode = xmlDoc.createElement(name);
                    objNode.appendChild(newNode);
                }
                addImmediateChildrenFromHtmlObjects(doc, childSchemaNode, newNode, xmlDoc);
            }
        }
        else
        {
            //only process nodes which don't have children since those would be processed
            //by add/delete/edit button handlers
            
            //we may have already added this node if we had errored out 
            //in last try.  So only add if it does not exist.
            //
            var newNode = objNode.selectSingleNode(name);
            if (!newNode)
            {
                newNode = xmlDoc.createElement(name);               
                objNode.appendChild(newNode);
            }
            var htmlObj = doc.getElementById(name);
            if (htmlObj != undefined) //html objects may be spread across multiple pages in a wizard
            {
                if (htmlObj.type == "checkbox")
                    newNode.text = htmlObj.checked ? "1" : "0";
                else
                    newNode.text = htmlObj.value;                   
            }
        }
    }   
    return true;
}

function replaceImmediateChildrenFromHtmlObjects(objNode, schemaNode)
{
    for ( var childNode = objNode.firstChild; 
            childNode != null; 
            childNode = childNode.nextSibling)
    {
        var name = childNode.nodeName;

        if (childNode.selectSingleNode('*') == null)
        {
            var htmlObj = document.getElementById(name);
            var schemaChildNode = schemaNode.selectSingleNode(name);
            var dataType = getAttribute(schemaChildNode, 'dataType', null);

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

/*unused so far...
function createUnescapedUrl(doc, schemaNode, objNode, xmlDoc, prefix)
{
    //create a url with elements in schema and object nodes
    //
    var url;
    for ( var childSchemaNode = schemaNode.firstChild; 
            childSchemaNode != null; 
            childSchemaNode = childSchemaNode.nextSibling)
    {
        var name = childSchemaNode.nodeName;
        if (name == "#comment" || name == 'Buttons')//ignore comments and buttons
             continue;
             
        if (childSchemaNode.hasChildNodes())
        {
            //if this node has children but only occurs once then create a node
            //corresponding to this schema node and process its children instead
            //
            var maxOccurs = getAttribute(schemaChildNode, 'maxOccurs', null);
            if (maxOccurs == "1")
            {
                var childNode = objNode.selectSingleNode(name);
                if (childNode)
                    url += createUnescapedUrl(doc, childSchemaNode, childNode, xmlDoc, name);
            }
        }
        else
        {
            //only process nodes which don't have children since those would be processed
            //by add/delete/edit button handlers
            
            //we may have already added this node if we had errored out 
            //in last try.  So only add if it does not exist.
            //
            var childNode = objNode.selectSingleNode(name);
            if (childNode)
            {
                url += '&';
                if (prefix)
                    url += prefix + '.';
                url += name + '=' + childNode.text;
            }
        }
    }   
    return url;
}
*/
function isNumber(str)
{
    return parseInt(str).toString()==str;
}

function updateNestedData(operation, strXPath, index, simulateDelete)
{
    var tableObj = document.all[strXPath];
    var rc = true;

    //Get a pointer to the specific note to pass to the modal dialog.
    var targetNode;
        
    if (operation == "add" || operation == "edit")
    {
        //parse the strXPath (which is of the form A.B.2.C) to an actual xpath
        //(like A/B[2]/C) so we can use it to find the corresponding data node
        //as well as the schema node.
        //Note that the first token (i.e. A) cannot have any index since it is the
        //argument node (root) so we parse from second token.
        //
        var tokens = new Array();
        tokens = strXPath.split('.');
        var nTokens = tokens.length;

        if (index == -1)//index not specified (for appending row) so add after last row
        {
            var checkBoxes = document.getElementsByName(strXPath+".checkbox");
            index = checkBoxes.length;
            tokens[nTokens++] = index;
        }

        var childSchemaNode = schemaRootNode;
        var parentNode = objRootNode.selectSingleNode(tokens[0]);

        for (var i=1; i<nTokens; i++)
        {
            targetNode = parentNode;

            var xpath = tokens[i];
            var nextI;
            if (i+1<nTokens && isNumber(tokens[i+1]))
            {
                xpath += '[' + tokens[i+1] + ']';
                nextI = i+1;//we have already processed next token
            }
            else
                nextI = i;
            
            childSchemaNode = childSchemaNode.selectSingleNode(tokens[i]);
            targetNode = parentNode.selectSingleNode(xpath);
            i = nextI;
        }

        if (operation == 'add')
        {
            //create a new node to add
            var newNode = xmlDoc.createElement(tokens[nTokens-2]); //pick second last to get name

            //we would like to insert this new node at the specified position so if there is
            //already a sibling then specify that sibling node to insert before that node so
            //the new node pushes it down and gets inserted before it
            //
            targetNode = parentNode.insertBefore(newNode, targetNode);
        }
        else //edit
            if ( targetNode == null)
            {
                alert('No matching data to edit!');
                return false;
            }

        var nodeName  = targetNode.nodeName;
        var addMethod = getAttribute(childSchemaNode, "addMethod", null);
        if (addMethod)
        {       
            /* if addMethod is defined as customMethod(), define a Function onAddHandler as follows:
            function onAddHandler(operation, xmlDoc, targetNode, component, command, xpath, schemaNode) 
            { 
                return customMethod(operation, xmlDoc, targetNode, component, command, xpath, schemaNode); 
            }
            */
            var onAddHandler = new Function(
                "operation", "xmlDoc", "targetNode", "component", "command", "xpath", "schemaNode", 
                "return " + addMethod+"(operation, xmlDoc, targetNode, component, command, xpath, schemaNode)");

            rc = onAddHandler(operation, xmlDoc, targetNode, component, command, strXPath, schemaRootNode);
            if (!rc)
            {
                newNode = null;
                return rc;
            }
        }

        var bRowCheckbox = getAttributeBool(childSchemaNode, 'checkboxes', true);       
        var bChecked;
        var row_num;
        
        if (operation == "add")
        {
            var rows = tableObj.rows;
            row_num = rows.length;//row 0 is header

            //if this is the first row getting into the table then replace the "- none -" row
            //
            if (row_num == 2 && rows[1].cells[0].innerText == '- none -')
            {
                tableObj.deleteRow(1);
                row_num = 1;
            }
            bChecked = false;
        }
        else
        {
            //edit
            row_num = index+1;//row 0 is header
            deleteRowFromTable(tableObj, row_num, strXPath, false); //row 0 is header
            bChecked = true;
        } 
        bChecked = getAttributeBool(childSchemaNode, 'checked', bChecked);
        var row = tableObj.insertRow(row_num);//insert after row index row_num
        var lastRowIndex = tableObj.lastRowIndex ? parseInt(tableObj.lastRowIndex) + 1 : 0;
        updateItemList(tableObj, lastRowIndex, null);
        var showHideRow = populateTableRow(tableObj, row, targetNode, childSchemaNode, strXPath, bRowCheckbox, bChecked);
    }
    else//delete
    {
        if (simulateDelete)
            rc = deleteRowFromTable(tableObj, index, strXPath, true);
        else
        {
            if (objRootNode)
            {
                var objNodes = objRootNode.selectNodes(strXPath.replace('.', '/'));
                targetNode = objNodes.item(index);
                
                if (targetNode == null)
                {
                    alert('No matching data available!');
                    return false;
                }
            }
            else
                targetNode = null;

            rc = deleteRowFromTable(tableObj, index, strXPath, false);
            if (rc && targetNode)
                targetNode.parentNode.removeChild(targetNode);
        }
    }           

    return rc;
}


function deleteRowFromTable(tableObj, node_index, xpath, simulate)
{   
    var checkBoxes = document.getElementsByName(xpath+".checkbox");
    var cb = checkBoxes[node_index];
    var row = getEnclosingTableRow(cb);

    if (row == null)
    {
        alert("Failed to find a row corresponding to item '" + xpath + '[' + nodex_index + "]'");
        return false;
    }
    var rowId = row.id;
    var rowIndex = row.rowIndex;

    var pos = rowId.lastIndexOf('.');
    if (!updateItemList(tableObj, null, rowId.substring(pos+1)))
        return false;
    
    if (!simulate)
    {
        tableObj.deleteRow(rowIndex);       

        //see if there is a corresponding showHideRow (toggle row) and delete it, if present
        //
        var toggle_rows = document.getElementsByName(rowId + '.toggle');
        var n_toggle_rows = toggle_rows.length;
        for (i=0; i<n_toggle_rows; i++)
            tableObj.deleteRow(toggle_row[i].rowIndex);
    }
    return true;
}

//define a stub that may be overridden by a component's command 
//if special handling of item selection is desired
//
function onRowCheckHandler(multiSelectObj)
{
}

function onRowCheck(multiselect_id)
{
    //the user checked on select all button one so we have at least one checkbox        
    //
    //enable delete and edit buttons
    //
    var multiselect = ms_lookup(multiselect_id);
    var tableId = multiselect.o_table.id;
    var nSelected = multiselect.getSelectionCount();
    var anySelected = nSelected > 0;

    enableButton(tableId + ".Delete", anySelected); 
    enableButtons(tableId + ".Edit",  nSelected==1);
    if (!multiselect.b_singleSelect)
        enableButtons(tableId + ".MultiEdit", anySelected);

    onRowCheckHandler(multiselect);
}


function onColumnCheck(newValue, multiselect_id)
{
    //the user checked on select all button one so we have at least one checkbox        
    //
    //var multiselect = ms_lookup(multiselect_id);
    //var tableId = multiselect.o_table.id;
}

function onShowHideRow(tableId, rowId, srcObj, content)
{
    var table = document.getElementById(tableId);
    var prevTableHt = table.offsetHeight;
    var srcObjId = srcObj.id;
    var toggleRowId = srcObjId + '.toggle';
    var toggleRow = document.getElementById(toggleRowId);
    var bShow;

    var srcCell = srcObj.parentElement;
    var btnRow = document.getElementById(rowId);;

    //find out if btnRow has a checkbox in cell 0
    //
    var nRowCheckbox = 0;
    var firstBtnRowCell = btnRow.cells[0];
    var inputs = firstBtnRowCell.getElementsByTagName('INPUT');
    var nInputs = inputs.length;
    for (var i=0; i<nInputs; i++)
    {
        var input = inputs[i];
        if (input.type == 'checkbox' && input.name.lastIndexOf('.checkbox'))
        {
            nRowCheckbox = 1;
            break;
        }
    }

    var toggleRowHt;
    if (!toggleRow)
    {
        var enclosingRow = document.getElementById(rowId);

        toggleRow = table.insertRow(enclosingRow.rowIndex+1);
        toggleRow.id = toggleRowId;
        toggleRow.name = rowId + '.toggle';

        var cell = toggleRow.insertCell(-1);
        cell.colSpan = btnRow.cells.length - nRowCheckbox;
        cell.style.textAlign = 'left';

        if (!content)
        {
            var pos = srcObjId.lastIndexOf('.');
            var hiddenInputId = srcObjId.substring(0, pos);
            var hiddenInput = srcCell.all[ hiddenInputId ];
            content = '<pre>'+ hiddenInput.value + '</pre>';
        }

        cell.innerHTML = content;
        toggleRowHt = toggleRow.offsetHeight;
        bShow = true;
    }
    else
    {
        bShow = toggleRow.style.display == 'none';
        if (bShow)
        {
            toggleRow.style.display = 'block';
            toggleRowHt = toggleRow.offsetHeight;
        }
        else
        {
            toggleRowHt = toggleRow.offsetHeight;
            toggleRow.style.display = 'none';
        }
    }

    if (srcObj.type == 'button')
    {
        var pattern = bShow ? 'Show' : 'Hide';
        var replace = bShow ? 'Hide' : 'Show';
        srcObj.value = srcObj.value.replace( new RegExp(pattern), replace);
    }

    if (nRowCheckbox)
    {
        /*BUG in IE: document.getElementsByName(rowId + '.toggle') does not return any rows
          even though we may have just added one so use alternate implementation by 
          enumerating rows (slower)*/
        var toggleRowName = rowId + '.toggle';
        var rowSpan = 1;
        var toggle_rows = table.getElementsByTagName('TR');
        var n_toggle_rows = toggle_rows.length;
        for (i=0; i<n_toggle_rows; i++)
        {
            var name = toggle_rows[i].name;
            if (name==toggleRowName && toggle_rows[i].style.display != 'none')
                rowSpan++;
        }
        firstBtnRowCell.rowSpan = rowSpan;
        firstBtnRowCell.vAlign = rowSpan>1 ? 'top' : 'center';
    }

    var bDiv = document.getElementById( 'DB.' + tableId );  
    if (bDiv)
    {
        var divHt = bDiv.offsetHeight;
        bDiv.style.height = bShow ? (divHt + toggleRowHt+1) : divHt;
    }
    var resizeHandler = document.body.onresize;
    if (resizeHandler)
        resizeHandler();
}


/* this function takes a string of the form "prefix{xpath}suffix" and 
    returns prefixXYZsuffix, where XYZ is the result of execution of selectSingleNode(xpath).
    Multiple {xpath} blocks may be embedded within the string.  This is primarily intended to
    substitute parameter values before form submission with resulting action.
*/
function expand_embedded_xpaths(str, schemaNode, node, bEscape)
{
    var open_brace = str.indexOf('{');
    if (open_brace == -1)
        return str;
    
    var close_brace = str.indexOf('}', open_brace+1);
    if (close_brace == -1)
        close_brace = str.length+1;
    
    var embraced = str.substring(open_brace+1, close_brace);
    var result;
    if (embraced.length)
    {
        if (embraced == '.')
            result = node.text;
        else
        {
            var targetNode = node.selectSingleNode(embraced);
            result = targetNode ? targetNode.text : embraced;
        }
    }
    else
        result = '';

    return str.substring(0, open_brace) + 
             (bEscape != false ? escape(result) : result) + 
             expand_embedded_xpaths( str.substring(close_brace+1), schemaNode, node, bEscape );
}

function updateItemList(tableObj, addItem, delItem, newValue)
{
    var id = tableObj.id;
    id += ".itemlist";
    var itemListInput = document.getElementById(id);
    if (itemListInput == null)
    {
        /* As per DHTML references in MSDN: 
           The NAME attribute cannot be set at run time on elements dynamically 
           created with the createElement method. To create an element with a 
           name attribute, include the attribute and value when using the createElement method.
        */
        itemListInput = document.createElement("<INPUT TYPE='hidden' NAME='"+id+"'></INPUT>");
        itemListInput.id = id;
        itemListInput.value = "+";
        document.forms[0].appendChild( itemListInput );
    }

    if (addItem != null)
        itemListInput.value += addItem + '+';
    else 
        if (delItem != null)
        {
            var list = itemListInput.value;
            var pattern = '+' + delItem + '+';
            var begin = list.indexOf(pattern);
            if (begin == -1)
            {
                alert("Table row management error!");
                debugger;
                return false;
            }
            var end = begin + pattern.length -1;
            itemListInput.value = list.substring(0, begin) + list.substring(end);
        }
        else
            itemListInput.value = newValue;
    return true;
}

function setItemListFromSelectedRowsOnly(tableId)
{
    var checkBoxes = document.getElementsByName(tableId+".checkbox");
    var nCheckBoxes = checkBoxes.length;

    var itemList='+';
    for (var i=0; i<nCheckBoxes; i++)
    {
        var cb = checkBoxes[i];
        if (cb.checked)
        {
            var rowId = getEnclosingTableRow(cb).id;
            var lastDot = rowId.lastIndexOf('.');
            var index = rowId.substring(lastDot+1);
            itemList += index + '+';
        }
    }

    var tableObj = document.getElementById( tableId );
    updateItemList(tableObj, null, null, itemList);
}

function serializeTableRows(xpath, tableId, checkedRowsOnly)
{
    //xpath can be delimited by / or .
    //
    if (!tableId)
        tableId = xpath;
    tableId = tableId.replace( /\//g, '.');//replace any / in xpath by .
    var xpath_slashes = xpath.replace( /\./g, '/');//replace any . in xpath by /

    var checkBoxes = document.getElementsByName(tableId+".checkbox");
    var nCheckBoxes = checkBoxes.length;

    var xml=new Array();
    var j = 0;

    //if xpath_slashes is of the form A/B/C, add <A><B> as prefix
    //
    var s_array = xpath_slashes.split('/');
    for (var i = 0; i< s_array.length-1; i++)
    {
        xml[j++] = '<';
        xml[j++] = s_array[i];
        xml[j++] = '>';
    }

    for (var i=0; i<nCheckBoxes; i++)
    {
        var cb = checkBoxes[i];
        if (!checkedRowsOnly || cb.checked)
        {
            var objXpath = xpath_slashes + '[' + i + ']';
            var obj = objRootNode.selectSingleNode(objXpath);
            xml[j++] = obj.xml;
        }
    }
    
    //if xpath_dots is of the form A.B.C, add </B></A> as suffix
    //
    for (var i = s_array.length-2; i>=0 ; i--)
    {
        xml[j++] = '</';
        xml[j++] = s_array[i];
        xml[j++] = '>';
    }

    return xml.join('');
}

function findMaxTableColumns(tableObj)
{
    var nColsMax = 0;
    var rows = tableObj.rows;
    var nRows = rows.length;
    for (var i=0; i<nRows; i++)
    {
        var nCols = rows[i].cells.length;
        if (nCols > nColsMax)
            nColsMax = nCols;
    }
    return nColsMax;
}

// this is a place holder to be redefined by the component's command
// if special handling is needed upond modified state change
//
function onDocumentModified(modified)
{
}

function setModified(val)
{
    if (document.modified != val)
    {
        document.modified = val != undefined ? val : true;
        onDocumentModified(val);
    }
}

function setModifiedHandler(schemaNode, node, htmlObj, handlerName, multiselect, columnHeader)
{
    var handlerAttrib = getAttribute(schemaNode, handlerName, null);
    var handlerScript = new Array();
    var i = 0;
    handlerScript[i++] = 'setModified(); ';
    if (multiselect)
    {
        handlerScript[i++] = columnHeader ? 'ms_setAll' : 'ms_onChange';
        handlerScript[i++] = "(this, '";
        handlerScript[i++] = multiselect.tableId;
        handlerScript[i++] = "',";
        handlerScript[i++] = multiselect.n_column;
        handlerScript[i++] = '); ';
        
        if (multiselect.n_column > 0 && columnHeader)
            handlerScript[i++] = "this.style.display = 'none'";
    }
    if (handlerAttrib && node)
        handlerScript[i++] = expand_embedded_xpaths( handlerAttrib, schemaNode, node);

    if (handlerName == 'onclick')   
        htmlObj.onclick = new Function(handlerScript.join(''));
    else
        if (handlerName == 'onchange')  
            htmlObj.onchange = new Function(handlerScript.join(''));

}

function onBeforeUnloadDocument()
{
    //null;  //don't let IE display any dialog box...any string value returned makes IE do so
    //window.event.cancelBubble = document.modified && !confirm('Are you sure you want to lose changes to this web page?');

    if (document.modified)
        event.returnValue = 'All changes will be lost!';
}

function disableSubmitButtons(disable)
{
    if (typeof disable == 'undefined')
        disable = true;
    //enable/disable all submit buttons:
    var inputs = document.getElementsByTagName('INPUT');
    var nInputs = inputs.length;
    for (var i=0; i<nInputs; i++)
        if (inputs[i].type.toUpperCase() == 'SUBMIT')
            inputs[i].disabled = disable;
}

function onSubmit()
{
    /*
    if (bAdd)
    {
        //add immediate children to our data island. Note that nested data is updated
        //as a result of add/delete handlers
        //
        if (!addImmediateChildrenFromHtmlObjects(schemaRootNode, objRootNode, xmlDoc))
            return false;   
    }
    else
        replaceImmediateChildrenFromHtmlObjects(objRootNode. schemaRootNode);
        
    //update the value of our hidden HTML control that gets sent back with the submission
    //
    document.getElementById('xmlArgs').value = xmlDoc.xml;
    alert(xmlDoc.xml);
    */

    setModified(false); //so we don't warn about losing changes
    disableSubmitButtons(); 
    return true;
}

function submitForm(action)
{
    setModified(false); //so we don't warn about losing changes
    var form = document.forms[0];
    form.action = action;

    var bModified = document.modified;
    if (onSubmit())
    {
        try {
            form.submit();
        } catch (e) {
            alert('Form submission failed:  ' + e.description);
            if (bModified)
                setModified(true);
            disableSubmitButtons(false);    
        }
    }
    return false;
}

function submitSelectedItem(xpath, action, tableId)
{
    //xpath can be delimited by / or .
    //
    if (!tableId)
        tableId = xpath;
    tableId = tableId.replace( /\//g, '.');//replace any / in xpath by .
    var xpath_slashes = xpath.replace( /\./g, '/');//replace any . in xpath by /
    
    var n_selected_row = getFirstCheckedItemIndex(tableId);
    var objNodes = objRootNode.selectNodes(xpath_slashes);
    node = objNodes.item(n_selected_row);               
    if (node == null)
        alert('No matching data!');
    else
    {   
        var expanded = expand_embedded_xpaths(action, schemaRootNode, node);
        submitForm(expanded);
    }
    return false;
}

function submitSelectedItems(xpath, action)
{
    //xpath can be delimited by / or .
    //
    var tableId = xpath.replace( /\//g, '.');//replace any / in xpath by .  
    setItemListFromSelectedRowsOnly(tableId);
    return submitForm(action);
}

function submitSelectedItemsAsXml(xpath, action, input_name, tableId)
{
    var XmlArg = serializeTableRows(xpath, tableId, true);

    var inputs = document.getElementsByName( input_name );
    if (inputs.length==0)
    {
        input = document.createElement('input');
        input.type = 'hidden';
        input.value = XmlArg;
        input.name = input_name;
        document.forms[0].appendChild( input );
    }
    else
        inputs[0].value = XmlArg;

    return submitForm(action);
}
