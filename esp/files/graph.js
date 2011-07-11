/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

var labelPopup = null;
var popupWidth = 0;
var popupHeight= 0;
var popupTimerId = null;
var prevWidth = 0;
var prevHeight= 0;
var processingPopup = false;


function resize_graph(svg)
{
    if (!svg)
        svg=document.getElementById('SVGGraph');
    if (!svg)
        return;

    var root=svg.getSVGDocument().rootElement;
    if (!root)
        return;

    var maxW = svg.offsetWidth;
    var maxH = svg.offsetHeight;
    if (Math.abs(maxW - prevWidth) < 100 && Math.abs(maxH - prevHeight) < 100)
        return;

    root.currentScale = 1;
    root.currentTranslate.x = 0;
    root.currentTranslate.y = 0;

    prevWidth = maxW;
    prevHeight = maxH;
}

function get_popup_div(popup_id)
{
    var frame=document.frames['popupFrame'];
    if(!frame) 
        return null;
        
    var div=frame.document.getElementById('popup_'+popup_id);
    if(!div) 
        return null;

    if(div.length) 
        return null; //multiple
    return div;
}

function show_popup(evt, popup_id,x,y)
{
    if (processingPopup || !evt || !evt.shiftKey || evt.ctrlKey || evt.altKey)
    {
        processingPopup = false;
        return;
    }

    processingPopup = true;
    if (labelPopup)
        hide_popup();

    labelPopup = window.createPopup();

    var svg=document.getElementById('SVGGraph');
    if(!svg) 
    {
        processingPopup = false;
        return;
    }

    if (!labelPopup.isOpen || window.top.activepopup != popup_id)
    {
        var div = get_popup_div(popup_id);
        if (!div) 
        {
            processingPopup = false;
            return;
        }

        if(!x || !y)
        {
            var o=svg.getSVGDocument().getElementById(popup_id);
            if(!o) 
            {
                processingPopup = false;
                return;
            }

            var root=svg.getSVGDocument().rootElement, 
                    scale=root.currentScale,
                    shift=root.currentTranslate;

            var rect=o.getBBox();

            x=rect.x*scale+shift.x+rect.width*scale; 
            y=rect.y*scale+shift.y+rect.height*scale;
        }
        window.top.activepopup=popup_id;
        stop_popup_timer();

        var popupBody=labelPopup.document.body;
        popupBody.style.border = "outset black 1px";
        popupBody.innerHTML=div.innerHTML;
        popupBody.style.overflow = "auto";
        popupBody.onmouseenter=stop_popup_timer;
        popupBody.onmouseleave=start_popup_timer;

        var tab = labelPopup.document.getElementById('tab');
        var link = labelPopup.document.getElementById('captionRow');
        link.style.display = 'block';
        labelPopup.show(2000,2000,640,480,null);

        popupWidth=tab.clientWidth+2,
        popupHeight=tab.clientHeight+2;
    
        if (popupWidth > screen.width)
            popupWidth = screen.width;

        if (popupHeight > screen.height)
            popupHeight = screen.height;
    }
    else
    {
        stop_popup_timer();
        start_popup_timer();
        processingPopup = false;
        return;
    }
    var xp=x+svg.offsetLeft-document.body.scrollLeft+window.screenLeft,
        yp=y+svg.offsetTop-document.body.scrollTop+window.screenTop;

    labelPopup.show(xp+5,yp+5,popupWidth,popupHeight,null);
    start_popup_timer();
    processingPopup = false;
}

function hide_popup()
{
    stop_popup_timer();
    window.top.activepopup=null;
    if(labelPopup)
    {
        if (labelPopup.isOpen)
            labelPopup.hide();
        labelPopup=null;
    }
}


function start_popup_timer()
{
    popupTimerId = setTimeout(hide_popup, 2000);//2 seconds
}


function stop_popup_timer()
{
    if (popupTimerId)
    {
        clearTimeout(popupTimerId);
        popupTimerId = null;
    }
}

function load_svg(srcNode)
{
    var svg=document.getElementById('SVGGraph');
    if(!svg)
        return null;

    var win=svg.getWindow();
    var doc = win.document;

    var srcRoot = srcNode.documentElement;
    var newsvg = win.parseXML(srcNode.text, doc);

    var root = doc.documentElement;
    
    //remove all existing children from root
    for (var child = root.firstChild; child; child = root.firstChild)
        root.removeChild(child);

    var width = srcRoot.getAttribute('width');
    if (width)
        root.setAttribute('width', width);

    var height = srcRoot.getAttribute('height')
    if (height)
        root.setAttribute('height', height);
    var viewBox = srcRoot.getAttribute('viewBox');
    if (viewBox)
        root.setAttribute('viewBox', viewBox);

    var children = newsvg.firstChild.childNodes;
    for (var i=0; i<children.length; i++)
        root.appendChild( children.item(i));
    return root;
}

function test_svg()
{
    try
    {
        if(document.getElementById('SVGGraph').getAttribute('window'))
        {
            var re=/.*;\s*(\d+.\d*)/.exec(document.getElementById('SVGGraph').getSVGViewerVersion());
            if(re && re[1]>=3.0)
                return true;
        }
    }
    catch(e)
    {
    }
    
    document.getElementById('SVGLink').innerHTML='<br />You need Adobe&reg; SVG Viewer v3.0 to view graphs.'+
                                   'It can be downloaded <a href="http://www.adobe.com/svg/viewer/install/">here</a>.<br />' + 
                                   'Adobe&reg; SVG Viewer is no longer supported by Adobe.<br />' +
                                   'If you don&prime;t already have this software installed, please use GVC Graph Viewer Option';
    return false;
}


function showNodeOrEdgeDetails(popupId, graphName, wuid, queryName)
{
    hide_popup();
    processingPopup = true;
    var win = window.open("about:blank", "_blank", 
        "toolbar=no, location=no, directories=no, status=no, menubar=no" +
        ", scrollbars=yes, resizable=yes, width=640, height=300");
    if (!win)
    {
        alert("Popup window could not be opened!  Please disable any popup killer and try again.");
         processingPopup = false;
        return; 
    }
    var doc = win.document;
    var elementType = popupId.indexOf('_')== -1 ? 'Node':'Edge';
    var s = new Array();
    var i = 0;
    s[i++] = '<h3>Graph ';
    s[i++] = elementType;
    s[i++] = ' Information</h3>';
    
    var div = get_popup_div(popupId);
    if (!div || div.innerHTML.length==0)
    {
        s[i++] = '<table id="tab" style="font:menu;">';
        s[i++] = '<colgroup><col align="left" valign="top"/></colgroup>';
        if (wuid || queryName)
        {
            s[i++] = '<tr><th>';
            s[i++] = wuid ? 'workunit' : 'query';
            s[i++] = '</th><td>';
            s[i++] = wuid ? wuid : queryName;
            s[i++] = '</td></tr>';
        }
        if (graphName)
        {
            s[i++] = '<tr><th>graph</th><td>';
            s[i++] = graphName;
            s[i++] = '</td></tr>';
        }
        s[i++] = '<tr><th>id</th><td>';
        s[i++] = popupId;
        s[i++] = '</td></tr>';
        s[i++] = '<tr><th colspan="2">No additional information is available.</th></tr>';
        s[i++] = '</table>';
        doc.write( s.join('') );
    }
    else
    {
        s[i++] = div.innerHTML;
        doc.write( s.join('') );
        
        var row = doc.getElementById('wuRow');
        if (wuid && row.cells.length==2 && row.cells[1].innerText != '')
            row.style.display = 'block';
        if (graphName)
        {
            row = doc.getElementById('graphNameRow');
            row.style.display = 'block';
        }
        row = doc.getElementById('idRow');
        if (row)
            row.style.display = 'block';
    }
    doc.title = elementType + ' ' + popupId + graphName ? (' [' + graphName + ']'): '';
    processingPopup = false;
}

var tipsWnd = null;

function showViewingTips()
{
    if (tipsWnd)
        {
           tipsWnd.close();
        }
    var win = window.open(  "about:blank", "_blank", 
                            "toolbar=0,location=0,directories=0,status=0,menubar=0," + 
                            "scrollbars=1, resizable=1, width=640, height=300");
    if (!win)
    {
        alert(  "Popup window could not be opened!  " + 
                "Please disable any popup killer and try again.");
    }
    else
    {
                tipsWnd = win;
        var s = [];
        var i = 0;
        s[i++] = '<h3>Graph Viewing Tips</h3><table align="left"><colgroup>';
        s[i++] = '<col span="*" align="left"/></colgroup><tbody><tr><th>Pan</th>';
        s[i++] = '<th>:</th><td>Press Alt key and drag graph with the hand cursor</td>';
        s[i++] = '</tr><tr><th nowrap="true">Zoom In</th><th>:</th>';
        s[i++] = '<td>Press Ctrl key and click left mouse button</td></tr>';
        s[i++] = '<tr><th nowrap="true">Zoom Out</th><th>:</th>';
        s[i++] = '<td>Press Shift-Ctrl and click left mouse button</td></tr>';
        s[i++] = '<tr><th>Tooltips</th><th>:</th><td>Press Shift key while ';
        s[i++] = 'moving cursor over nodes and edges</td></tr><tr valign="top">';
        s[i++] = '<th nowrap="true">Increase size</th><th>:</th><td>Collapse ';
        s[i++] = 'left frame by dragging or clicking on separator.<br/>Toggle ';
        s[i++] = 'full screen view by pressing F11</td></tr><tr valign="top" ';
        s[i++] = 'style="border-bottom:gray 1px solid"><th>More</th><th>:</th>';
        s[i++] = '<td>The contex menu offers options like: Find, Copy SVG, View ';
        s[i++] = 'Source, Zoom  In/Out, Save As, and Help</td></tr></tbody></table>';

        var doc = win.document.open("text/html", "replace");
        doc.write( s.join('') );
        doc.title = 'Graph Viewing Tips';
    }
}

function go(url) 
{
    document.location.href=url;
}
            
var prevFoundPolygon = null;
var prevFill = null;
function findGraphNode(id, svg)
{
    if (id.length == 0) {
        alert("Please specify a node id to find in graph!");
        return;
    }
    var root = getGraphRoot(svg);
    if (!svg)
        svg=document.getElementById('SVGGraph');
    if (!svg)
        return;

    var root=svg.getSVGDocument().rootElement;
    if (!root)
        return;

    var node = root.getElementById(id);
    var polygon = null;
    if (node)
    {
        var links = node.getElementsByTagName('a');
        if (links.length)
        {
            polygons = links.item(0).getElementsByTagName('polygon');
            if (polygons.length)
                polygon = polygons.item(0);
        }
    }
    if (polygon != prevFoundPolygon && prevFoundPolygon)
    {
        if (prevFill == '')
            prevFoundPolygon.style.removeProperty('fill');
        else
            prevFoundPolygon.style.setProperty('fill', prevFill, "");
        prevFoundPolygon = null;
    }
            
    if (!polygon)
    {
        alert('Graph node not found!');
        resetFindGraphNode();
    }
    else
        if (polygon != prevFoundPolygon)
        {
            prevFoundPolygon = polygon;

            var textNodes = node.getElementsByTagName('text');
            if (textNodes.length > 0)
            {
                var firstChild = textNodes.item(0);
                var x = firstChild.getAttribute('x');
                var y = firstChild.getAttribute('y');

                //get svg plugin dimenstions
                var width = svg.offsetWidth;
                var height = svg.offsetHeight;

                //browser crashes if we zoom in too much so avoid it
                //
                //get graph dimensions

                var maxW = root.getAttribute('width') / 10;
                var maxH = root.getAttribute('height') / 10;

                width = Math.max(width, maxW);
                height = Math.max(height, maxH);

                var viewBox = (x-width/2) + ' ' + (y-height/2) + ' ' + width + ' ' + height;
                root.currentScale = 1;
                root.currentTranslate.x = 0;
                root.currentTranslate.y = 0;
                root.setAttribute('viewBox', viewBox);

                var resetFindBtn = document.getElementById('resetFindBtn');
                if (resetFindBtn)
                    resetFindBtn.disabled = false;

                prevFill = polygon.style.getPropertyValue('fill');
                polygon.style.setProperty('fill' , 'yellow', "");
            }
        }
}
function getGraphRoot(svg)
{
    if (!svg)
        svg=document.getElementById('SVGGraph');

    return svg ? svg.getSVGDocument().rootElement : null;
}

function resetFindGraphNode(svg)
{
    var root = getGraphRoot(svg);
    if (root)
    {
        root.setAttribute('viewBox', '0 0 ' + root.getAttribute('width') + ' ' + root.getAttribute('height'));
        root.currentScale = 1;
        root.currentTranslate.x = 0;
        root.currentTranslate.y = 0;
    }
    if (prevFoundPolygon)
    {
        if (prevFill == '')
            prevFoundPolygon.style.removeProperty('fill');
        else
            prevFoundPolygon.style.setProperty('fill', prevFill, "")
        prevFoundPolygon = null;
    }
    var resetFindBtn = document.getElementById('resetFindBtn');
    if (resetFindBtn)
        resetFindBtn.disabled = true;
}

