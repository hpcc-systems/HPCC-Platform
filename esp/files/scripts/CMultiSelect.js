/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

/* This file defines CMultiSelect class, which is used to manage multiple selection
    across a column of a given html table.  A web page can instantiate more than one such
    objects to support multiple selection over multiple columns (individually).  This 
    class should not be instantiated directly but rather via the factory method 
    "createMultiSelect" which is defined at the bottom of the file.  By default, 
    checkboxes are managed but conceptually different html objects (for instance, 
    drop down lists, edit boxes, etc.) can be managed instead in future.

    If the callback methods onColumnSelChanged and onItemSelChanged are specified then
    they must be implemented by the html page instantiating this CMultiSelect object.

    function onColumnSelChanged(msid) {...}
        This callback gets invoked when selection status across all rows changes (i.e. at 
        least one row is selected or unselected).

    function onItemSelChanged(o, msid) {...}
        This callback is invoked for every single html object o (usually a checkbox) 
        that is being managed by the multiselect object when its selection changes.

*/

function CMultiSelect( id, 
                       tableId, 
                       onColumnSelChanged/*=null*/, 
                       onItemSelChanged/*=null*/, 
                       columnToSort/*=0*/, 
                       inputTag/*='INPUT'*/, 
                       inputType/*='checkbox'*/,
                       bRowsAreTables/*=false*/)
{
    //save parameters as instance data
    //
    this.b_initialized = false;
    this.id = id;
    this.tableId = tableId;
    this.o_table = null; //to be initialized
    this.o_htable= null; //to be initialized
    this.n_column = typeof columnToSort == 'undefined' ? 0 : columnToSort;
    this.onColumnSelChanged = onColumnSelChanged ? onColumnSelChanged : null;
    this.onItemSelChanged = onItemSelChanged ? onItemSelChanged : null;
    this.inputTag = inputTag ? inputTag : 'INPUT';
    this.inputType = inputType ? inputType : 'checkbox';
    this.b_rowsAreTables = typeof bRowsAreTables == 'undefined' ? false : bRowsAreTables;
    this.b_singleSelect = false;

    // define ids used by multiselect controls (usually checkboxes) as tableId_[x]_[y] 
    // where x is id of this multiselect and y is index of multiselect cell
    // (for instance, checkboxes at top and bottom of this table
    //
    this.selectAllIdPrefixForTable = tableId + '_ms';

    // define ids used by multiselect controls (usually checkboxes) as tableId_[x]_[y] 
    // where x is id of this multiselect and y is index of multiselect cell
    // (for instance, checkboxes at top and bottom of this table
    //
    this.selectAllIdPrefix = this.selectAllIdPrefixForTable + this.n_column + '_';

    //bind class methods to this object
    //
    this.initialize = initialize;
    this.getManagedCell = getManagedCell;
    this._determineNewSetAllValue = _determineNewSetAllValue;
    this.setAll = setAll;
    this.getSelectionCount = getSelectionCount;
    this.makeSetAllId = makeSetAllId;
    this._setRange = _setRange;
    this._setSelectAllControls = _setSelectAllControls;
    this._onChange = _onChange;
    this._getValue  = _getValue;
    this._setValue  = _setValue;
    this._updateSelectionCount = _updateSelectionCount;
}



/*------------------------------------------------------------------------------*/
/*                                 PUBLIC METHODS                               */
/*------------------------------------------------------------------------------*/
//public:
/* define the "constructor"
    function initialize()
*/
function initialize()
{
    this.b_checkbox = this.inputTag == 'INPUT' && this.inputType == 'checkbox';
    if (this.b_singleSelect && !this.b_checkbox)
    {
        alert("Single select is only supported for checkboxes!");
        debugger;
    }
    this.o_table = document.getElementById(this.tableId);
    if (!this.o_table)//it is possible that the table was nested within another table's row and got deleted
        return;

    //this table may be split with its header (separate table w/ just header) 
    //in one div and body (separate table w/ just body) in another div 
    //in order to keep its header fixed while the body is scolled up/down
    //
    this.o_htable = document.getElementById('H.' + this.tableId);

    this.lastValueSet   = undefined;
    this.n_lastRowClicked= -1;
    this.inputTag = this.inputTag.toUpperCase();

    //updates this.n_selected, this.n_totalItems and this.setAllValue
    //
    this.n_selected  = 0;
    this.n_totalItems= 0;
    this.setAllValue = undefined;
    this.setAllSet = false;
    
    var numRows = this.o_table.rows.length;
    var firstRowIndex = this.o_htable ? 0 : 1;
    for (var r=firstRowIndex; r<numRows; r++)
    {
        var row = this.o_table.rows[r];
        var numCells = row.cells.length;
        var cell = this.getManagedCell(row);
        if (cell && cell.childNodes.length > 0)
        {
            var o = cell.childNodes[0];
            if (o.tagName==this.inputTag && o.type==this.inputType && !o.disabled &&
                 (!o.id || o.id.indexOf(this.selectAllIdPrefixForTable)== -1))
            {
                this.n_totalItems++;
                var val = this._getValue(o);
                if (val != undefined)
                {
                    this.n_lastRowClicked = r;
                    this.lastValueSet = val;

                    if (this.b_checkbox)
                    {
                        if (val)
                            this.n_selected++;
                    }
                    else
                    {
                        if (r == 1) {
                            this.setAllValue = val;
                            this.setAllSet = true;
                        }
                        else
                            if (this.setAllValue != val) {                          
                                this.setAllSet = false;
                                this.setAllValue = undefined;
                            }
                                
                    }
                }
            }
        }
    }//for

    if (this.b_checkbox) {
        this.setAllValue = this.n_totalItems && (this.n_selected == this.n_totalItems);
        this.setAllSet = true;
    }

    this._setSelectAllControls(this.setAllValue);
    this.b_initialized = true;

    if (this.onColumnSelChanged)
        this.onColumnSelChanged( this.id );
}

function getManagedCell(row)
{
    if (this.b_rowsAreTables)
    {
        var cells = row.cells;
        if (cells.length > 0)
        {
            var childNodes = cells[0].childNodes;
            var table = childNodes.length > 0 && childNodes[0].tagName == 'TABLE' ? childNodes[0] : null;
            if (table)
            {
                var rows = table.rows;
                if (rows.length > 0)
                    managedRow = rows[0];
            }
        }
    }
    else
        managedRow = row;

    var found = null;
    if (managedRow)
    {
        var cells = managedRow.cells;
        if (this.n_column < cells.length)
            found = cells[ this.n_column ];
    }
    return found;
}
/*
    function _getNewValueForSetAll()
*/
function _determineNewSetAllValue(newValue)
{
    if (this.b_checkbox)
        newValue = this.n_selected == this.n_totalItems;
    else
        if (this.n_totalItems > 0)
        {
            //return value of controls in the column if all cells in that column
            //have same value, else return empty string
            var rows = this.o_table.rows;
            var nRows = rows.length;
            var firstRowIndex = this.o_htable ? 0 : 1;

            for (var i=firstRowIndex; i<nRows; i++)
            {
                var cell = this.getManagedCell(rows[i]);
                if (cell)
                {
                    var input = cell.firstChild;
                    if (input && !input.disabled)
                    {
                        var val = this._getValue(input);
                        if (i == 1 && newValue == undefined)
                            newValue = val;
                        else
                            if (newValue != val)
                            {
                                newValue = undefined;
                                break;
                            }
                    }
                }
            }
        }
    return newValue;
}


/* 
    function setAll(o)
*/
function setAll(o, notify)
{
    newValue = this._getValue(o);
    var firstRowIndex = this.o_htable ? 0 : 1;
    this._setRange(newValue, firstRowIndex, this.o_table.rows.length-1);//row 0 is heading

    if (this.b_checkbox)
        this.n_selected = newValue ? this.n_totalItems : 0;

    if (newValue != this.setAllValue)
        this._setSelectAllControls(newValue);

    this.setAllSet = true;
    
    if (this.onColumnSelChanged && (typeof notify == 'undefined' || notify)) {
        this.onColumnSelChanged( this.id );
    }
    
    this.setAllSet = false;

}

/*
    function getSelectionCount()
*/
function getSelectionCount()
{
    return this.b_checkbox ? this.n_selected : undefined;
}

/* 
    function makeSetAllId(topOfTable)
*/
function makeSetAllId(topOfTable)
{
    return this.selectAllIdPrefix + (topOfTable ? 'T':'B');
}

/*------------------------------------------------------------------------------*/
/*                                 PRIVATE METHODS                              */
/*------------------------------------------------------------------------------*/
//private:
//associate the methods with this class
//
/* 
    function _setRange(newValue, from, to)
*/
function _setRange(newValue, from, to)
{   
    var selectAllIdPrefixForAnyColumn = this.o_table.id + '_ms';
    for (var r=from; r<=to; r++)
    {
        var row = this.o_table.rows[r];
        var numCells = row.cells.length;

        var cell = this.getManagedCell(row);
        if (cell.childNodes.length > 0)
        {
            //is the first child of this cell a checkbox (not the select all checkbox)
            //which has different check state than intended?
            //
            var o = cell.childNodes[0];

            if (o.tagName==this.inputTag && o.type==this.inputType && !o.disabled && 
                (!o.id || o.id.indexOf(selectAllIdPrefixForAnyColumn) == -1) && this._getValue(o) != newValue)
            {
                this._setValue(o, newValue);
                this._updateSelectionCount(newValue);
                
                if (this.onItemSelChanged)
                    this.onItemSelChanged(o, this.id);
            }
        }
    }
}

/*
    function _setSelectAllControls(val)
*/
function _setSelectAllControls(val)
{
    for (var i=0; i<2; i++)//top and bottom
    {
        selectAllCell = document.getElementById( this.makeSetAllId(i==0) );
        if (selectAllCell)
        {
            var inputs = selectAllCell.getElementsByTagName(this.inputTag);
            if (inputs.length > 0)
            {
                var input = inputs[0];
                this._setValue(input, val);
                if (input.disabled)
                {
                    if (this.n_totalItems > 0)
                        input.disabled = false;
                }
                else
                    if (this.n_totalItems == 0)
                        input.disabled = true;
            }
        }
    }
    this.setAllValue = val;
    this.setAllSet = false;
}


/*
    function _onChange(o)
*/
function _onChange(o)
{
    if (this.b_singleSelect && this._getValue(o)) //note that this.b_checkbox is implied
    {
        this._setValue(o, false);
        this.setAll(o, false); 
        this._setValue(o, true);
    }

    var cell = o.parentNode;
    var row = cell.parentNode;
    var rowNum = row.rowIndex;

    if (!this.b_singleSelect && window.event && window.event.shiftKey && 
         this.n_lastRowClicked != -1 && this.n_lastRowClicked != rowNum)
    {
        rc = this.lastValueSet == this._getValue(o);
        if (this.n_lastRowClicked < rowNum)
            this._setRange(this.lastValueSet, this.n_lastRowClicked, rowNum);
        else
            this._setRange(this.lastValueSet, rowNum, this.n_lastRowClicked);
    }
    else
    {
        this.n_lastRowClicked = rowNum;
        this.lastValueSet = this._getValue(o);
        rc = true;
    }

    val = this._getValue(o);
    if (rc)
    {
        this._updateSelectionCount( val );
        var newSetAllValue = this._determineNewSetAllValue(val);
        if (newSetAllValue != this.setAllValue)
            this._setSelectAllControls(newSetAllValue);
    }
    else
    {
        //the window has already changed selection for the checkbox which 
        //should not have been done since shift key was pressed
        if (this.b_checkbox)
            this._updateSelectionCount(! val ); //compensation
    }

    if (this.onColumnSelChanged)
        this.onColumnSelChanged(this.id);
    return rc;
}

/*
    function _getValue(o)
*/
function _getValue(o)
{
    if (this.b_checkbox)
        return o.checked;
    else
        if (this.inputTag == 'SELECT')
            return o.selectedIndex;
        else
            return o.value;
}

/*
    function _setValue(o, val)
*/
function _setValue(o, val)
{
    if (!o.disabled)
    {
        if (this.b_checkbox)
            o.checked = val == undefined ? false : val;
        else if (this.inputTag == 'SELECT')
            o.selectedIndex = val == undefined ? -1 : val;
        else
            o.value = val == undefined ? '' : val;
    }
}

/*
    function _updateSelectionCount(select)
*/
function _updateSelectionCount(select)
{
    if (this.b_checkbox)
    {
        if (select)
            this.n_selected++;
        else
            this.n_selected--;
    }
}



/*------------------------------------------------------------------------------*/
/*                                 GLOBAL METHODS                               */
/*------------------------------------------------------------------------------*/
var a_multiselect = [];

/* instantiates a new CMultiSelect object.  The caller must initialize it using its
   initialize() method appropriately (for instance, after populating table rows).
*/
function ms_create( tableId, 
                            onColumnSelChanged/*=null*/, 
                            onItemSelChanged/*=null*/, 
                            columnToSort/*=0*/,
                            inputTag/*='INPUT'*/, 
                            inputType/*='checkbox'*/,
                            bRowsAreTables/*=false*/)
{
    //add this class instance to the array of multiselects that is used to 
    //work with multiple such objects in the same page
    //
    var id = a_multiselect.length;
    var ms = new CMultiSelect( id, tableId, onColumnSelChanged, 
                                        onItemSelChanged, columnToSort, inputTag, inputType,
                                        bRowsAreTables);
    a_multiselect[ id ] = ms;
    return ms;
}

function ms_lookup(msid/*=0*/)
{   
    if (msid == undefined)
        msid = 0;

    return (msid >= 0 && msid < a_multiselect.length) ? a_multiselect[ msid ] : null;
}

function ms_lookupByTableAndColumn(tableId, columnNum/*=0*/)
{   
    if (columnNum == undefined)
        columnNum = 0;

    var count = a_multiselect.length;
    for (var i=0; i<count; i++)
    {
        ms = a_multiselect[i];
        if (ms.tableId == tableId && ms.n_column == columnNum)
            return ms;
    }
    return null;
}

/*public*/
function ms_onChange(o, tableId, columnNum/*=0*/)
{
    o_ms = ms_lookupByTableAndColumn(tableId, columnNum);
    o_ms._onChange(o);
}

/*public*/
function ms_setAll(o, tableId, columnNum/*=0*/)
{
    o_ms = ms_lookupByTableAndColumn(tableId, columnNum);
    o_ms.setAll(o);
}

/*public*/
function ms_getSelectionCount(msid/*=0*/)
{
    o_ms = ms_lookup(msid);
    return o_ms.getSelectionCount();
}


/*public*/
function ms_initialize(startColumn/*=0*/, endColumn/*=a_multiselect.length*/)
{
    var len = a_multiselect.length;

    if (typeof startColumn == 'undefined' || startColumn < 0)
        startColumn = 0;

    if (typeof endColumn == 'undefined' || endColumn >= len)
        endColumn = a_multiselect.length-1;

    for (var i=startColumn; i<=endColumn; i++)
        a_multiselect[i].initialize();
}

/*public*/
function ms_initializeAll()
{
    var len = a_multiselect.length;
    for (var i=0; i<len; i++)
        a_multiselect[i].initialize();
}

/*public*/
function ms_reinitializeAll()
{
    var len = a_multiselect.length;
    for (var i=0; i<len; i++)
    {
        ms = a_multiselect[i];
        if (ms.b_initialized)
            ms.initialize();
    }
}
