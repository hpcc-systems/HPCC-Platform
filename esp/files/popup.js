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

function popup_on_click()
{
    contextMenu.hide();
    return this.doAction ? this.doAction() : null;
}

function popup_on_mouse_over()
{
    this.style.backgroundColor='highlight';
    var button=this.firstChild;
    if(button.noaction)
    {
        button.disabled=false;
        button.style.color='graytext';
    }
    else
    {
        button.style.color='highlighttext';
    }
}

function popup_on_mouse_out()
{
    this.style.backgroundColor='';
    var button=this.firstChild;

    if(button.noaction)
        button.disabled=true;

    button.style.color='menutext';
};

function popup_popup()
{
    return false;
}

function showPopup(menu,posx,posy)
{
    if(!window.createPopup) 
        return false;

    contextMenu=window.createPopup();
    var popupDoc=contextMenu.document;
    var popupBody=popupDoc.body;
    popupBody.style.backgroundColor = "menu";
    popupBody.style.border = "outset white 2px";

    var tab=popupDoc.createElement('TABLE');
    tab.style.font='menu';
    tab.id='tab';
    tab.width=10;
    tab.oncontextmenu=popup_popup;
    tab.onselectstart=popup_popup;
    tab.cellSpacing=0;

    var tbody = popupDoc.createElement('TBODY');
    var lasttd=null;

    for(var item in menu)
    {
        if(menu[item] && menu[item].length>=2)
        {
            var tr=popupDoc.createElement('TR');
            tr.doAction=menu[item][1];
            tr.onclick=popup_on_click;

            var td=popupDoc.createElement('TD');
            td.style.padding='3px 20px 3px 20px';
            td.style.whiteSpace='nowrap';
            td.onmouseover=popup_on_mouse_over;
            td.onmouseout=popup_on_mouse_out;
            if(lasttd && lasttd.groupend)
            {
                td.style.borderTop='2px outset #fff';
                td.style.paddingTop='4px';
            }

            var span=popupDoc.createElement('SPAN');
            span.name='button';
            span.disabled=span.noaction=(!menu[item][1]);
            span.style.backgroundColor='transparent';
            span.style.cursor='default';
            span.appendChild(popupDoc.createTextNode(menu[item][0]));
            td.appendChild(span);
            if(menu[item][2]=='checked')
            {
               var check=popupDoc.createElement('SPAN');
               check.innerHTML='&radic;';
               check.style.position='absolute';
               check.style.left='5px';
               check.style.fontWeight='bolder';
               td.appendChild(check);
            }

            tr.appendChild(td);
            tbody.appendChild(tr);
            lasttd=td;
        }
        else
        {   
            if(lasttd)
            {
                lasttd.style.borderBottom='1px outset #fff';
                lasttd.style.paddingBottom='5px';
                lasttd.groupend=true;
            }
        }
    }
    tab.appendChild(tbody);

    popupBody.appendChild(tab);
    contextMenu.show(posx, posy, 300, 300, null);

    var w=popupBody.all.tab.clientWidth+5,
        h=popupBody.all.tab.clientHeight+5;

    contextMenu.show(posx, posy, w, h, null);

    return true;
}


function showColumnPopup(tableId, colIndex, toggleMultiSelect)
{
    function setColumn()
    {                    
        toggleMultiSelect( tableId, colIndex, true);
    }
    function alignColumn(alignment)
    {
       var table = document.getElementById( tableId );
       if (table) {
           var rows = table.rows;
           var nrows= rows.length;
           var i = document.getElementById('H.' + tableId) ? 0 : 1;
           for (; i<nrows; i++) {
               var row = rows[i];
               if (row.id.indexOf('.toggle') == -1)
                    rows[i].cells[colIndex].style.textAlign = alignment;
            }
        }
    }
    function alignColumnLeft()
    {
        alignColumn('left');
    }
    function alignColumnCenter()
    {
        alignColumn('center');
    }
    function alignColumnRight()
    {
        alignColumn('right');
    }
    function changeColumnWidth(htable, table, points)
    {
        if (table) {
           var rows = table.rows;
           var nrows= rows.length;
           var htableRow = htable ? htable.rows[0] : null;
           var hw=0;

           for (var i=0; i<nrows; i++) {
               var row = rows[i];
               if (row.id.indexOf('.toggle') == -1)
               {
                   cell = row.cells[colIndex];
                   var w = cell.offsetWidth * (1 + points)
                   cell.style.width = w;
                   if (hw==0)
                      hw = w;
                }
            }
           if (htableRow)
               htableRow.cells[colIndex].style.width = hw;
        }
    }
    function increaseWidth()
    {
        var table = document.getElementById( tableId );
        var htable = document.getElementById('H.' + tableId);
        changeColumnWidth(htable, table, 0.2)
    }
    function decreaseWidth()
    {
        var table = document.getElementById( tableId );
        var htable = document.getElementById('H.' + tableId);
        changeColumnWidth(htable, table, -0.2)
    }
    var menu=[];
    if (toggleMultiSelect) {
        menu.push( ["Set Column...", setColumn] );
        menu.push( null );
    }
    menu.push( ["Align Left", alignColumnLeft] );
    menu.push( ["Align Center", alignColumnCenter] );
    menu.push( ["Align Right", alignColumnRight] );
    menu.push( ["Increase Width", increaseWidth]);
    menu.push( ["Decrease Width", decreaseWidth]);

    var x, y;
    if (window.event)
    {
       x = window.event.screenX;
       y = window.event.screenY;
       window.event.cancelBubble = true;
    }
    else
       x = y = 0;
    showPopup(menu, x, y);
    return false;//suppress default browser popup
}
