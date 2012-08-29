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
