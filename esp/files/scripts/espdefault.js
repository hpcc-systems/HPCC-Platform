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

/*-----------------------------------------------------------------------
            Default ESP Script Functions
**-----------------------------------------------------------------------*/

/*-----------------------------------------------------------------------
To be included in any ESP page that needs to prevent F5 from refreshing and 
resetting back to the home frameset.
**-----------------------------------------------------------------------*/

var isFF=/Firefox[\/\s](\d+\.\d+)/.test(navigator.userAgent)?1:0;
var isChrome=/Chrome[\/\s](\d+\.\d+)/.test(navigator.userAgent)?1:0;

function nof5() {
    if (parent.window.frames && parent.window.frames[0]) {
        //parent.window.frames[0].focus();
        for (var i_tem = 0; i_tem < parent.window.frames.length; i_tem++) {
            if (parent.window.frames[i_tem] == window) {
                if (isFF)
                    parent.window.frames[i_tem].onkeypress = new Function("e", "if(e.keyCode==116){parent.window.frames[" + i_tem + "].location.replace(parent.window.frames[" + i_tem + "].location);e.preventDefault();e.stopPropagation();}")
                else
                    parent.window.frames[i_tem].document.onkeydown = new Function("var e=parent.window.frames[" + i_tem + "].event; if(e.keyCode==116){parent.window.frames[" + i_tem + "].location.replace(parent.window.frames[" + i_tem + "].location);e.keyCode=0;return false;};");
            }
        }
    }
    
    setF5ForFrames();
}

function getAFrame(name) {
    var aFrame = null;
    if (parent.window.frames && parent.window.frames[0]) {
        for (var i_tem = 0; i_tem < parent.window.frames.length; i_tem++) {
            if (parent.window.frames[i_tem].name == name) 
                aFrame = parent.window.frames[i_tem];
        }
    }
    return aFrame;
}

function setF5ForFrames() {
    var headerFrame = getAFrame('header');       
    if (headerFrame) {
        if (isFF)
            headerFrame.onkeypress = new Function("e", "if(e.keyCode==116){getAFrame('main').location.replace(getAFrame('main').location);e.preventDefault();e.stopPropagation();}")
        else
            headerFrame.document.onkeydown = new Function("var e=getAFrame('header').event; if(e.keyCode==116){getAFrame('main').location.replace(getAFrame('main').location);e.keyCode=0;return false;};");
    }
    var navFrame = getAFrame('nav');       
    if (navFrame) {
        if (isFF)
            navFrame.onkeypress = new Function("e", "if(e.keyCode==116){getAFrame('main').location.replace(getAFrame('main').location);e.preventDefault();e.stopPropagation();}")
        else
            navFrame.document.onkeydown = new Function("var e=getAFrame('nav').event; if(e.keyCode==116){getAFrame('main').location.replace(getAFrame('main').location);e.keyCode=0;return false;};");
    }
}

function setReloadFunction(TargetFunction) {
    var LocalReloadFunction = (TargetFunction.indexOf('(') > -1 ? TargetFunction : TargetFunction + '()');
    if (parent.window.frames && parent.window.frames[0]) {
        //parent.window.frames[0].focus();
        for (var i_tem = 0; i_tem < parent.window.frames.length; i_tem++) {
            if (parent.window.frames[i_tem] == window) {
                if (isFF)
                    parent.window.frames[i_tem].onkeypress = new Function("e", "if(e.keyCode==116){" + LocalReloadFunction + ";e.preventDefault();e.stopPropagation();}")
                else
                    parent.window.frames[i_tem].document.onkeydown = new Function("var e=parent.window.frames[" + i_tem + "].event; if(e.keyCode==116){e.keyCode=0;" + LocalReloadFunction + ";return false;};");
            }
        }
    }
}

function go(url) {
    document.location.href = url;
}                     

