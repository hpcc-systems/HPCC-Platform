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

var prevFrameColumns = null;

function onLoadNav()
{
  if (app_name)
  {
    top.frames['header'].document.getElementById('AppName').innerHTML = '<p align="center"><b><font size="5">' + app_name + '</font></b></p>';
  }
}         
               
function getLowerFrameset()
{
    var frameset = null;
    if (window.top != window.self)
    {
        var frame = top.document.getElementsByName('nav')[0];
        var frameset = frame.parentElement;
        if (!frameset)
            frameset = frame.parentNode;//for Firefox
    }
    return frameset;
}

function onToggleTreeView(button) {
    var frameset = getLowerFrameset();
    if (frameset)
    {
        var content = document.getElementById('pageBody');
        var btn = document.getElementById('espnavcollapse');
        var show = prevFrameColumns != null; //content.style.display == 'none';
        if (show)
        {
            frameset.cols = prevFrameColumns;
            content.style.display = 'block';
            content.style.visibility = 'visible'
            btn.className = 'espnavcollapse';
            button.title = 'Click to hide left frame';
            prevFrameColumns = null;
        }
        else
        {
            prevFrameColumns = frameset.cols;
            btn.className = 'espnavexpand';
            frameset.cols = '15,*';
            content.style.display = 'none';
            content.style.visibility = 'hidden'
            button.title = 'Click to show left frame';  
        }
    }
}

function leftFrameResized() {
    return;
    if (prevFrameColumns)
    {
        var frameset = getLowerFrameset();

        var w = document.body.clientWidth;
        if (w > 10 && frameset)
        {
            prevFrameColumns = frameset.cols;
            var div = document.getElementById('espnavcollapse');
            onToggleTreeView(div);
        }
    }
}
