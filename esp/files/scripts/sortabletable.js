
/*----------------------------------------------------------------------------\
|                            Sortable Table 1.1                               |
|-----------------------------------------------------------------------------|
|                         Created by Erik Arvidsson                           |
|                  (http://webfx.eae.net/contact.html#erik)                   |
|                      For WebFX (http://webfx.eae.net/)                      |
|-----------------------------------------------------------------------------|
| A DOM 1 based script that allows an ordinary HTML table to be sortable.     |
|-----------------------------------------------------------------------------|
|                  Copyright (c) 1998 - 2003 Erik Arvidsson                   |
|-----------------------------------------------------------------------------|
| This software is provided "as is", without warranty of any kind, express or |
| implied, including  but not limited  to the warranties of  merchantability, |
| fitness for a particular purpose and noninfringement. In no event shall the |
| authors or  copyright  holders be  liable for any claim,  damages or  other |
| liability, whether  in an  action of  contract, tort  or otherwise, arising |
| from,  out of  or in  connection with  the software or  the  use  or  other |
| dealings in the software.                                                   |
| - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - |
| This  software is  available under the  three different licenses  mentioned |
| below.  To use this software you must chose, and qualify, for one of those. |
| - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - |
| The WebFX Non-Commercial License          http://webfx.eae.net/license.html |
| Permits  anyone the right to use the  software in a  non-commercial context |
| free of charge.                                                             |
| - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - |
| The WebFX Commercial license           http://webfx.eae.net/commercial.html |
| Permits the  license holder the right to use  the software in a  commercial |
| context. Such license must be specifically obtained, however it's valid for |
| any number of  implementations of the licensed software.                    |
| - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - |
| GPL - The GNU General Public License    http://www.gnu.org/licenses/gpl.txt |
| Permits anyone the right to use and modify the software without limitations |
| as long as proper  credits are given  and the original  and modified source |
| code are included. Requires  that the final product, software derivate from |
| the original  source or any  software  utilizing a GPL  component, such  as |
| this, is also licensed under the GPL license.                               |
|-----------------------------------------------------------------------------|
| 2003-01-10 | First version                                                  |
| 2003-01-19 | Minor changes to the date parsing                              |
| 2003-01-28 | JScript 5.0 fixes (no support for 'in' operator)               |
| 2003-02-01 | Sloppy typo like error fixed in getInnerText                   |
| 2003-07-04 | Added workaround for IE cellIndex bug.                         |
| 2003-11-09 | The bDescending argument to sort was not correctly working     |
|            | Using onclick DOM0 event if no support for addEventListener    |
|            | or attachEvent                                                 |
| 2004-01-13 | Adding addSortType and removeSortType which makes it a lot     |
|            | easier to add new, custom sort types.                          |
| 2004-01-27 | Switch to use descending = false as the default sort order.    |
|            | Change defaultDescending to suit your needs.                   |
|-----------------------------------------------------------------------------|
| Created 2003-01-10 | All changes are in the log above. | Updated 2004-01-27 |
\----------------------------------------------------------------------------*/

var tooltipDiv = null;
var tooltipPopup = null;
var tooltipX = 0;
var tooltipY = 0;
var tooltipSrcObj = null;
var tooltipCaptionColor = "#6699FF"; //"#808080";
var tooltipBodyColor = "#FFFF99"; //"#C0C0C0";
var tooltipBodyTextColor = "black";
var tooltipMaxWidth = screen.width;
var sortImagePath = '/esp/files_/img/';

/*----------------------------------------------------------------------------\
|                       Handle Split Tables                                   |
| Table header and body are in separate tables in their own divs.             |
\----------------------------------------------------------------------------*/
var a_fixedTableNames = [];

function initFixedTables(fixedTableNames)
{
    if (typeof fixedTableNames != 'undefined')
        a_fixedTableNames = a_fixedTableNames.concat( fixedTableNames );
    var nFixedTables = a_fixedTableNames.length;
    if (nFixedTables) {
        document.body.onresize=resizeFixedTableBodyDivs;
        for (var i=0; i<nFixedTables; i++) {
            bDiv = document.getElementById('DB.' + a_fixedTableNames[i]);
            bDiv.onscroll = new Function( "scrollFixedTableHeaderDiv(this, '" + a_fixedTableNames[i] + "')" );
        }
        resizeFixedTableBodyDivs();
    }
}

function resizeFixedTableBodyDivs() {
    if (a_fixedTableNames.length == 0)
        return;

    var hfooter = document.getElementById('pageFooter');
    var bottom = hfooter ? hfooter.offsetTop : document.body.offsetHeight;

    var bDiv = document.getElementById('DB.' + a_fixedTableNames[0]);
    var bTable = document.getElementById(a_fixedTableNames[0]);
    var h = bottom - bDiv.offsetTop - 35;
    h /= a_fixedTableNames.length;
    h = Math.max(h, 50);

    if (bDiv.offsetHeight > h)
        bDiv.style.height = h;
    else
    {
        var tw = bTable.offsetWidth;
        var th = bTable.offsetHeight;
        var dw = bDiv.offsetWidth;
        var doubleBorder = 2;
        if (dw < tw+doubleBorder)
            bDiv.style.height = Math.min(h, th+20);//add height to fit horiz scrollbar to avoid vertical scrollbar
        else
            if (dw > tw+doubleBorder)
                bDiv.style.height = th+doubleBorder;
    }

    resizeFixedTableHeaderDiv(bDiv, a_fixedTableNames[0])

    for (var i=1; i<a_fixedTableNames.length; i++) {
        bDiv = document.getElementById('DB.' + a_fixedTableNames[i]);
        bDiv.style.height = h;
        resizeFixedTableHeaderDiv(bDiv, a_fixedTableNames[i]);
    }
}


function scrollFixedTableHeaderDiv(bodyDiv, tableId)
{
    var htable = document.getElementById('H.' + tableId);
    if (htable)
        htable.style.left = -bodyDiv.scrollLeft;
}

function resizeFixedTableHeaderDiv(bodyDiv, tableId)
{
    var hdiv = document.getElementById('DH.' + tableId);
    if (hdiv)
    {
        var htable = document.getElementById('H.' + tableId);
        var btable = bodyDiv.getElementsByTagName('table')[0];

        hdiv.style.width = bodyDiv.offsetWidth;
        htable.style.width = btable.offsetWidth;

        //resize individual column headers based on first row's cells in body table
        if (htable.style.tableLayout != 'fixed')
        {
            var bCells = btable.rows[0].cells;
            var hCells = htable.rows[0].cells;
            var n_bCells = bCells.length;
            var n_hCells = hCells.length;

            for (var i=0; i<n_bCells; i++)
                hCells[i].style.width = bCells[i].offsetWidth;
        }

        scrollFixedTableHeaderDiv(bodyDiv, tableId);
    }
}

/*----------------------------------------------------------------------------\
|                       Handle Column Resizing                                |
\----------------------------------------------------------------------------*/
function makeColumnsResizable(htable) {
    var rows = htable.rows;
    var nRows = rows.length;
    if (nRows) {
        var cells = rows[0].cells;
        var nCells = cells.length;
        for (var i=0; i<nCells; i++) {
            var cell = cells[i];
            //cells with no text lose border so add text
            if (cell.innerText == '')
            {
                var textNode = document.createTextNode(' ');
                cell.appendChild( textNode );
            }

            var span = document.createElement('SPAN');
            span.className = 'resizer';
            span.style.height = cell.offsetHeight;
            //span.style.border='red 1px solid';
            span.innerText = ' ';
            span.onmousedown = resizeColumn;
            
            cell.style.position = 'relative';               
            cell.style.paddingRight = 0;
            cell.appendChild(span);
        }
    }
}

function resizeColumn(resizer) {
    var resizer = event.srcElement;
    var th = resizer.parentElement;
    th._resizing = true;
    var hTable = th.parentElement;
    while (hTable.tagName != 'TABLE')
        hTable = hTable.parentElement;

    var size = th.offsetWidth;
    var firstX = event.clientX;// + resizer.offsetWidth + 10;
    var lastX = firstX;
    var prevHeaderHeight = th.offsetHeight;

    function doResize() {
        var newX = event.clientX;
        if (Math.abs(newX - lastX) > 2) {//resize every 2 pixels
            var delta = event.clientX - firstX;
            var sz = Math.max(5, size + delta);
            th.style.width = sz;
            lastX = newX;
        }
    };

    function finishResizing() {
        var newWidth = Math.max(size + event.clientX - firstX, 5);
        th.style.width = newWidth;
        if (typeof resizer.onmouseleave == "function")
            resizer.onmouseleave()          
        resizer.detachEvent("onmousemove", doResize);
        resizer.detachEvent("onmouseup", finishResizing);
        resizer.detachEvent("onlosecapture", finishResizing);
        resizer.releaseCapture();

        var hTableId = hTable.id;
        if (hTableId && hTableId.indexOf('H.') == 0)
        {
            var bTableId = hTableId.substring(2);
            var bTable = document.getElementById( bTableId );
                
            var hDiv = document.getElementById( 'DH.' + bTableId );
            var bDiv = document.getElementById( 'DB.' + bTableId );

            hDiv.style.width = bDiv.offsetWidth;
            bTable.style.width = hTable.offsetWidth;

            var hCells = hTable.rows[0].cells;
            var nCells = hCells.length;
            var nCol = th.cellIndex;
            var rows = bTable.rows;
            var nRows = rows.length;
            for (var r=0; r<nRows; r++)
                if (rows[r].id)
                    rows[r].cells[ nCol ].style.width = newWidth;
                
            var newHeaderHeight = th.offsetHeight;
            if (newHeaderHeight != prevHeaderHeight)
                for (var c=0; c<nCells; c++)
                {
                    var span = hCells[c].getElementsByTagName('SPAN')[0];
                    span.style.height = newHeaderHeight;
                }
            resizeFixedTableBodyDivs();
        }
        event.cancelBubble = true;
    };

    resizer.attachEvent("onmouseup",    finishResizing);
    resizer.attachEvent("onlosecapture", finishResizing);
    resizer.attachEvent("onmousemove", doResize);
    resizer.setCapture();
    event.cancelBubble = true;
}

/*----------------------------------------------------------------------------\
|                       SortableTable implementation                          |
\----------------------------------------------------------------------------*/
function SortableTable(hTable, oTable, oSortTypes, dataIslandParent) {
    this.element = oTable;
    this.tHead = hTable.tHead;
    if (!this.tHead)
        this.tHead = hTable.tBodies[0];
    this.tBody = oTable.tBodies[0];
    this.document = oTable.ownerDocument || oTable.document;
    this.dataIslandParent = dataIslandParent;

    this.sortColumn = null;
    this.descending = null;

    var oThis = this;
    this._headerOnclick = function (e) {
        oThis.headerOnclick(e);
    };

    // only IE needs this
    var win = this.document.defaultView || this.document.parentWindow;
    this._onunload = function () {
        oThis.destroy();
    };
    if (win && typeof win.attachEvent != "undefined") {
        win.attachEvent("onunload", this._onunload);
    }

    this.initHeader(oSortTypes || []);

   // IE does not remember input values when moving DOM elements
   if (/MSIE/.test(navigator.userAgent)) {

       // backup check box values
       this.onbeforesort = function () {
           var table = this.element;
           var inputs = table.getElementsByTagName("INPUT");
           var l = inputs.length;
           for (var i = 0; i < l; i++) {
               inputs[i].parentNode._checked = inputs[i].checked;
           }
       };

       // restore check box values
       this.onsort = function () {
           var table = this.element;
           var inputs = table.getElementsByTagName("INPUT");
           var l = inputs.length;
           for (var i = 0; i < l; i++) {
               inputs[i].checked = inputs[i].parentNode._checked;
           }
         handleOddEven(this);
       };
   }
   makeColumnsResizable(hTable);
}

SortableTable.gecko = navigator.product == "Gecko";
SortableTable.msie = /msie/i.test(navigator.userAgent);
// Mozilla is faster when doing the DOM manipulations on
// an orphaned element. MSIE is not
SortableTable.removeBeforeSort = SortableTable.gecko;

SortableTable.prototype.onsort = function () {};

// default sort order. true -> descending, false -> ascending
SortableTable.prototype.defaultDescending = false;

// shared between all instances. This is intentional to allow external files
// to modify the prototype
SortableTable.prototype._sortTypeInfo = {};

// adds arrow containers and events
// also binds sort type to the header cells so that reordering columns does
// not break the sort types
SortableTable.prototype.initHeader = function (oSortTypes) {
    var cells = this.tHead.rows[0].cells;
    var l = cells.length;
    var img, c;
    for (var i = 0; i < l; i++) {
        c = cells[i];
        if (oSortTypes[i] && oSortTypes[i] == "None")
         continue;

        img = document.createElement("IMG");
        img.src = sortImagePath + "blank.png";
        img.className = 'sort-arrow';
        c.appendChild(img);
        if (oSortTypes[i] != null && oSortTypes[i] != "None") {
            c._sortType = oSortTypes[i];
        }
        if (typeof c.addEventListener != "undefined")
            c.addEventListener("click", this._headerOnclick, false);
        else if (typeof c.attachEvent != "undefined")
            c.attachEvent("onclick", this._headerOnclick);
        else
            c.onclick = this._headerOnclick;

        c.onmouseover=function() {this.bgColor = this._resizing ? "#CCCCCC" : "#FFFFFF";};
        c.onmouseout=function()  {this.bgColor = "#CCCCCC";};
    }
    this.updateHeaderArrows();
};

// remove arrows and events
SortableTable.prototype.uninitHeader = function () {
    var cells = this.tHead.rows[0].cells;
    var l = cells.length;
    for (var i = 0; i < l; i++) {
        var c = cells[i];
        var imgs = c.getElementsByTagName('img');
        var nImages = imgs.length;
        for (var j=0; j<nImages; j++)
        {
            var img = imgs[j];
            var className = img.className;
            if (className.indexOf('sort-arrow')==0)
            {
                c.removeChild(img);
                break;
            }
        }
        if (typeof c.removeEventListener != "undefined")
            c.removeEventListener("click", this._headerOnclick, false);
        else if (typeof c.detachEvent != "undefined")
            c.detachEvent("onclick", this._headerOnclick);
    }
};

SortableTable.prototype.updateHeaderArrows = function () {
    var cells = this.tHead.rows[0].cells;
    var l = cells.length;
    for (var i = 0; i < l; i++) {
        var imgs = cells[i].getElementsByTagName('img');
        var nImages = imgs.length;
        for (var j=0; j<nImages; j++)
        {
            var img = imgs[j];
            var className = img.className;
            if (className && className.indexOf('sort-arrow')==0)
            {
                if (i == this.sortColumn)
                    img.className = "sort-arrow " + (this.descending ? "descending" : "ascending");
                else
                    img.className = "sort-arrow";
                break;
            }
        }
    }   
};

SortableTable.prototype.headerOnclick = function (e) {
    // find TD element
    var el = e.target || e.srcElement;
    while (el.tagName != "TH")
        el = el.parentNode;

    if (el._resizing)
        el._resizing = false;
    else
        this.sort(SortableTable.msie ? SortableTable.getCellIndex(el) : el.cellIndex);
};

// IE returns wrong cellIndex when columns are hidden
SortableTable.getCellIndex = function (oTd) {
    var cells = oTd.parentNode.childNodes
    var l = cells.length;
    var i;
    for (i = 0; cells[i] != oTd && i < l; i++)
        ;
    return i;
};

SortableTable.prototype.getSortType = function (nColumn) {
    var cell = this.tHead.rows[0].cells[nColumn];
    var val = cell._sortType;
    if (val != "")
        return val;
    return "String";
};

// only nColumn is required
// if bDescending is left out the old value is taken into account
// if sSortType is left out the sort type is found from the sortTypes array

SortableTable.prototype.sort = function (nColumn, bDescending, sSortType) {
    if (sSortType == null)
        sSortType = this.getSortType(nColumn);

    // exit if None
    if (sSortType == "None")
        return;

    if (bDescending == null) {
        if (this.sortColumn != nColumn)
            this.descending = this.defaultDescending;
        else
            this.descending = !this.descending;
    }
    else
        this.descending = bDescending;

    this.sortColumn = nColumn;

    if (typeof this.onbeforesort == "function")
        this.onbeforesort();

    var f = this.getSortFunction(sSortType, nColumn);
    var a = this.getCache(sSortType, nColumn);
    var tBody = this.tBody;

    a.sort(f);

    if (this.descending)
        a.reverse();

    if (SortableTable.removeBeforeSort) {
        // remove from doc
        var nextSibling = tBody.nextSibling;
        var p = tBody.parentNode;
        p.removeChild(tBody);
    }

    var l = a.length;
    if (this.dataIslandParent)
    {
        var domElements = this.dataIslandParent.childNodes;
        for (var i = 0; i < l; i++)
            this.dataIslandParent.removeChild( domElements.item(0) ); 
    }
    // insert in the new order
    for (var i = 0; i < l; i++)
    {
        tBody.appendChild(a[i].element);//automatically removes item before appending
        if (this.dataIslandParent)
            this.dataIslandParent.appendChild( a[i].domElement );
    }

    if (SortableTable.removeBeforeSort) {
        // insert into doc
        p.insertBefore(tBody, nextSibling);
    }

    this.updateHeaderArrows();

    this.destroyCache(a);

    if (typeof this.onsort == "function")
        this.onsort();
};

SortableTable.prototype.asyncSort = function (nColumn, bDescending, sSortType) {
    var oThis = this;
    this._asyncsort = function () {
        oThis.sort(nColumn, bDescending, sSortType);
    };
    window.setTimeout(this._asyncsort, 1);
};

SortableTable.prototype.getCache = function (sType, nColumn) {
    var rows = this.tBody.rows;
    var l = rows.length;
    var a = new Array(l);
    var r;
    var domElements = this.dataIslandParent ? this.dataIslandParent.childNodes : null;
    for (var i = 0; i < l; i++) {
        r = rows[i];
        a[i] = {
            value:      this.getRowValue(r, sType, nColumn),
            element:    r,
            domElement: domElements ? domElements.item(i) : null
        };
    };
    return a;
};

SortableTable.prototype.destroyCache = function (oArray) {
    var l = oArray.length;
    for (var i = 0; i < l; i++) {
        oArray[i].value = null;
        oArray[i].element = null;
        oArray[i].domElement = null;
        oArray[i] = null;
    }
};

SortableTable.prototype.getRowValue = function (oRow, sType, nColumn) {
    // if we have defined a custom getRowValue use that
    if (this._sortTypeInfo[sType] && this._sortTypeInfo[sType].getRowValue)
        return this._sortTypeInfo[sType].getRowValue(oRow, sType, nColumn);

    var s;
    var c = oRow.cells[nColumn];
   if (c==null)
      s = "";
   else
       if (typeof c.innerText != "undefined")
           s = c.innerText;
       else
           s = SortableTable.getInnerText(c);
    return this.getValueFromString(s, sType);
};

// define a custom getRowValue handler for HTML objects
//
SortableTable.prototype.getHtmlObjRowValue = function (oRow, sType, nColumn) {
    var s;
    var c = oRow.cells[nColumn];
    if (c==null)
        s = "";
    else {
        var obj = c.firstChild;
        if (obj) {
            var tagName = obj.tagName;
            if (tagName == 'INPUT' && obj.type=='checkbox')
                s = obj.checked ? '1' : '0';
            else if (tagName == 'SELECT')
                s = obj.options[obj.selectedIndex].value;
            else
                s = obj.value;
        }
        else
            s = "";
    }
    return this.getValueFromString(s, sType);
};

SortableTable.getInnerText = function (oNode) {
    var s = "";
    var cs = oNode.childNodes;
    var l = cs.length;
    for (var i = 0; i < l; i++) {
        switch (cs[i].nodeType) {
            case 1: //ELEMENT_NODE
                s += SortableTable.getInnerText(cs[i]);
                break;
            case 3: //TEXT_NODE
                s += cs[i].nodeValue;
                break;
        }
    }
    return s;
};

SortableTable.prototype.getValueFromString = function (sText, sType) {
    if (this._sortTypeInfo[sType])
        return this._sortTypeInfo[sType].getValueFromString( sText );
    return sText;
    /*
    switch (sType) {
        case "Number":
            return Number(sText);
        case "CaseInsensitiveString":
            return sText.toUpperCase();
        case "Date":
            var parts = sText.split("-");
            var d = new Date(0);
            d.setFullYear(parts[0]);
            d.setDate(parts[2]);
            d.setMonth(parts[1] - 1);
            return d.valueOf();
    }
    return sText;
    */
    };

SortableTable.prototype.getSortFunction = function (sType, nColumn) {
    if (this._sortTypeInfo[sType])
        return this._sortTypeInfo[sType].compare;
    return SortableTable.basicCompare;
};

SortableTable.prototype.destroy = function () {
    this.uninitHeader();
    var win = this.document.parentWindow;
    if (win && typeof win.detachEvent != "undefined") { // only IE needs this
        win.detachEvent("onunload", this._onunload);
    }
    this._onunload = null;
    this.element = null;
    this.tHead = null;
    this.tBody = null;
    this.document = null;
    this._headerOnclick = null;
    this.sortTypes = null;
    this._asyncsort = null;
    this.onsort = null;
};

// Adds a sort type to all instance of SortableTable
// sType : String - the identifier of the sort type
// fGetValueFromString : function ( s : string ) : T - A function that takes a
//    string and casts it to a desired format. If left out the string is just
//    returned
// fCompareFunction : function ( n1 : T, n2 : T ) : Number - A normal JS sort
//    compare function. Takes two values and compares them. If left out less than,
//    <, compare is used
// fGetRowValue : function( oRow : HTMLTRElement, nColumn : int ) : T - A function
//    that takes the row and the column index and returns the value used to compare.
//    If left out then the innerText is first taken for the cell and then the
//    fGetValueFromString is used to convert that string the desired value and type

SortableTable.prototype.addSortType = function (sType, fGetValueFromString, fCompareFunction, fGetRowValue) {
    this._sortTypeInfo[sType] = {
        type:               sType,
        getValueFromString: fGetValueFromString || SortableTable.idFunction,
        compare:            fCompareFunction || SortableTable.basicCompare,
        getRowValue:        fGetRowValue
    };
};

// this removes the sort type from all instances of SortableTable
SortableTable.prototype.removeSortType = function (sType) {
    delete this._sortTypeInfo[sType];
};

SortableTable.basicCompare = function compare(n1, n2) {
    if (n1.value < n2.value)
        return -1;
    if (n2.value < n1.value)
        return 1;
    return 0;
};

SortableTable.idFunction = function (x) {
    return x;
};

SortableTable.toUpperCase = function (s) {
    return s.toUpperCase();
};

SortableTable.toDate = function (s) {
    var parts = s.split("-");
    var d = new Date(0);
    d.setFullYear(parts[0]);
    d.setDate(parts[2]);
    d.setMonth(parts[1] - 1);
    return d.valueOf();
};

// restore the class names
function handleOddEven(st) {
    var rows = st.tBody.rows;
    var l = rows.length;
    for (var i = 0; i < l; i++)
    {
        rows[i].bgColor = i % 2 ? "#FFFFFF" : "#F0F0F0";
        rows[i].onmouseleave = i % 2 ? function() {this.bgColor = "#FFFFFF";} : function() {this.bgColor = "#F0F0F0";};
    }
};

// Thanks to Bernhard Wagner for submitting this function

function replace8a8(str) {
   //remove commas, if any
   str = removeCharacter(str, ',');
    str = str.toUpperCase();
    var splitstr = "____";
    var ar = str.replace(
        /(([0-9]*\.)?[0-9]+([eE][-+]?[0-9]+)?)(.*)/,
     "$1"+splitstr+"$4").split(splitstr);
    var num = Number(ar[0]).valueOf();
    var ml = ar[1].replace(/\s*([KMGB])\s*/, "$1");

    if (ml == "K")
        num *= 1024;
    else if(ml == "M")
        num *= 1024 * 1024;
    else if (ml == "G")
        num *= 1024 * 1024 * 1024;
    else if (ml == "T")
        num *= 1024 * 1024 * 1024 * 1024;
    // B and no prefix

    return num;
}

function removeCharacter(str, ch) {
   var temp = str.split(ch);
   str = "";
   for (i=0; i<temp.length; i++)
      str += temp[i];

   return str;
}

function numberWithCommas(str)
{
   return Number( removeCharacter(str, ',') ).valueOf();
}

function percentage(str)
{
   return Number( removeCharacter(str, '%') ).valueOf();
}

function ipAddress(str)
{
   var temp = str.split('.');
   var num = 0;
   for (i=temp.length-1; i>=0; i--)
      num += (256*i)*Number(temp[i]);
   return num.valueOf();
}

function timePeriod(s) {
   //handle format: [15 days, ]21:52:32.86
   //
   var i = s.indexOf(' days');
   var period = i > 0 ? 86400 * Number(s.substring(0, i)).valueOf() : 0;
      
   i += 7; //skip ' days, '
   s = s.substr( i, s.length - i);

    var parts = s.split(":");
   period += Number(parts[0])*3600 + Number(parts[1])*60 + Number(parts[2]);
    return period;
};


// add sort types
SortableTable.prototype.addSortType("Number", Number);
SortableTable.prototype.addSortType("CaseInsensitiveString", SortableTable.toUpperCase);
SortableTable.prototype.addSortType("Date", SortableTable.toDate);
SortableTable.prototype.addSortType("String");
SortableTable.prototype.addSortType("NumberK", replace8a8 );
SortableTable.prototype.addSortType("NumberWithCommas", numberWithCommas );
SortableTable.prototype.addSortType("IP_Address", ipAddress);
SortableTable.prototype.addSortType("TimePeriod", timePeriod);
SortableTable.prototype.addSortType("DateTime", String);
SortableTable.prototype.addSortType("Percentage", percentage);
SortableTable.prototype.addSortType("Html", String, SortableTable.basicCompare, SortableTable.prototype.getHtmlObjRowValue)
SortableTable.prototype.addSortType("iHtml", SortableTable.toUpperCase, SortableTable.basicCompare, SortableTable.prototype.getHtmlObjRowValue)
SortableTable.prototype.addSortType("nHtml", Number, SortableTable.basicCompare, SortableTable.prototype.getHtmlObjRowValue)

// None is a special case

function createTableSorter(tableId, columnSortTypes, dataIslandParent)
{
    var hTable = document.getElementById('H.' + tableId);
    var bTable = document.getElementById(tableId);
    if (!hTable)
        hTable = bTable;
    return new SortableTable(hTable, bTable, columnSortTypes, dataIslandParent);
}
