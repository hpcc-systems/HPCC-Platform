/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

var a_fixedTableNames = [];

function initFixedTables(fixedTableNames)
{
    if (typeof fixedTableNames != 'undefined')
        a_fixedTableNames = a_fixedTableNames.concat( fixedTableNames );
    var nFixedTables = a_fixedTableNames.length;
    if (nFixedTables) {
        document.body.onresize=resizeFixedTableBodyDivs;
        for (var i=0; i<a_fixedTableNames.length; i++) {
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
