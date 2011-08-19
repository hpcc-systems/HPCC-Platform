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

/* 
*! global vars
*/ 
var isRoxieGraph = false;
var isEclWatchGraph = false;
var isWsEclGraph = false;
var isWsRoxieQueryGraph = false;

var resultsoffset = 100;
var OffsetHeight = 90;
var gotosubgraph = '0';
var gotovertex = '0';

var currentgraphnode = '';
var currentgraph = '';

var statsWnd = null;
var timingsWnd = null;

var reloading = false;
var reloadThisGraph = true;

var gt = null;

var wuinfoRequest = null;
var graphRequest = null;
var wseclGraphXml = null;
var subgraphOnly = false;
var subgraphId = 0;
var havesubgraphtimings = '1';

var espUri = null;

function go(url)
{
   document.location.href=url;
}

function pluginLHS() {
    return document.getElementById('pluginLHS');
}

function pluginRHS() {
    return document.getElementById('pluginRHS');
}

//  Events
function addEvent(obj, name, func) {
    if (window.addEventListener) {
        obj.addEventListener(name, func, false);
    } else {
        obj.attachEvent("on" + name, func);
    }
}

function layoutFinished() {
    pluginLHS().setMessage('');
    pluginLHS().centerOnItem(0, true);
}

function layoutFinishedRHS() {
    pluginRHS().setMessage('');
    pluginRHS().centerOnItem(1, true);
}

function mouseDoubleClick(item) {
    pluginLHS().centerOnItem(item, true);
}

function centerOn(item) {
    pluginLHS().centerOnItem(item, false);
}

function selectionChanged(items) {
    DisplaySelectedProperties(items);
}

function selectionChangedRHS(items) {
    var selection = pluginRHS().getSelectionAsGlobalID();
    pluginLHS().setSelectedAsGlobalID(selection);
    DisplaySelectedProperties(pluginLHS().getSelection());
    var selectionLHS = pluginLHS().getSelection();
    if (selectionLHS.length > 0)
        pluginLHS().centerOnItem(selectionLHS[0], true);
}

function DisplaySelectedProperties(items) {
    pluginRHS().setMessage("Loading Data...");
    pluginRHS().loadXGMML(pluginLHS().getLocalisedXGMML(items));
    pluginRHS().setMessage("Performing Layout...");
    var selection = pluginLHS().getSelectionAsGlobalID();
    pluginRHS().setSelectedAsGlobalID(selection);
    pluginRHS().startLayout("dot");
    var propsText = '';
    for (var i = 0; i < items.length; ++i) {
        var props = pluginLHS().getProperties(items[i]);
        propsText += '<table border="1">';
        propsText += '<tr><td colspan="2" align="center">';
        propsText += '<input type="button" style="cursor:hand;" onclick="javascript:centerOn(' + items[i] + ')" value="' + props['id'] + '" />'
        propsText += '</td></tr>';
        for (var key in props) {
            propsText += '<tr>';
            propsText += '<td>' + key + '</td><td>' + props[key] + '</td>';
            propsText += '</tr>';
        }
        propsText += '</table>';
    }
    document.getElementById('props').innerHTML = propsText;
}

function loadXGMML() {
    pluginLHS().setMessage("Loading Data...");
    pluginLHS().loadXGMML(document.getElementById('TextXGMML').value);
    pluginLHS().setMessage("Performing Layout...");
    pluginLHS().startLayout("dot");
}

function mergeXGMML() {
    pluginLHS().setMessage("Merging Data...");
    pluginLHS().mergeXGMML(document.getElementById('TextXGMML').value);
    pluginLHS().setMessage("Performing Layout...");
    pluginLHS().startLayout("dot");
}


/******************************************************************/

function layoutFinished() {
    if (!reloading) {
        pluginLHS().setMessage('');
        pluginLHS().centerOnItem(0, true);

        if (window.graphloaded != '1') {
            if (gotosubgraph != '0') {
                selectSubGraph(gotosubgraph);
                gotosubgraph = '0';
            }
            else {
                if (gotovertex != '0') {
                    selectVertex(gotovertex);
                    gotovertex = '0';
                }
                else {
                    graphloaded = '1';
                }
            }
        }
    }
    hideElement('loadingMsg');
}

function scaled(newScale) {
    slider.setValue(newScale);
}

function scaled2(newScale) {
    slider2.setValue(newScale);
}

var oldScale;
function setScale(newScale) {
    oldScale = pluginLHS().setScale(newScale);
}

var oldScale;
function setScaleRHS(newScale) {
    oldScale = pluginRHS().setScale(newScale);
}

function onUnload()
{
    try {
        if (statsWnd)
        {
            statsWnd.close();
        }
        if (timingsWnd)
        {
            timingsWnd.close();
        }
    }
    catch (e)
    {
    }
}

var GraphCtlCreated = false;


function pause_resume()
{
    if (isrunning == '1')
    {
        if (gt == null)
        {
            gt = setTimeout("reloadGraph()", 500);
        }
        else
        {
            clearTimeout(gt) 
            gt = null;
        }
    }
}

function open_new_window(popupId)
{
    showNodeOrEdgeDetails(popupId, graphName, wuid);
}

function checkVersion() {
    var verDiv = document.getElementById('install_div');
    if (verDiv) {

       try {
          var curVersion = pluginLHS().version;
          if (curVersion == null) {
              alert("Graph Control Needs to be installed to visualize activity graphs.");
              document.location = '/WsRoxieQuery/BrowseResources';
              return false;
          }
          document.getElementById('current_version').innerHTML = curVersion;
          if (curVersion < 20110523) {
              showElement('install_div');
          }
       
       }
       catch(e) {
          showElement('no_control_msg');
          showElement('install_div');
          return false;
       }        
    }
    return true;
}

function update_details() {
    resize();
    
    if (!isWsEclGraph)
    {
      showElement('Stats');
    }
    if (isEclWatchGraph && havesubgraphtimings == '1')
    {
        showElement('Timings');
    }
    showElement('findNodeBlock');
    showElement('autoSpan');

    if (isrunning != '1' && forceFinalCountRefresh == false)
    {
        hideElement('autoSpan');
        isrunningsave = '0';
        hideElement('SelectVertex');
        return;
    }
    if (currentgraphnode != '')
    {
        var obj = document.getElementById('CurrentNode');
        if (obj) 
        {
            obj.title = 'Executing subgraph ' + currentgraphnode + ' in ' + currentgraph;
            obj.innerHTML = 'Goto executing (' + currentgraphnode + ')';
        }
        showElement('SelectVertex');
    }
    showElement('autoSpan');
}

function resize() {
    var gvcheight;
    var graphwidth;
    if (document.body) {
        gvcheight = document.body.clientHeight - OffsetHeight - 100;
        graphwidth = document.body.clientWidth - 300;
    } else {
        gvcheight = document.height - OffsetHeight - 100;
        graphwidth = document.width - 300;
    }

    document.getElementById('pluginLHS').style.height = gvcheight;
    document.getElementById('pluginRHS').style.height = "360px";

}

function test()
{
    alert('test');
}

function selectGraphSubGraph(GraphNameToSelect, VertexIdToSelect)
{
    if (graphName.substring(5) == GraphNameToSelect)
    {
        selectSubGraph(VertexIdToSelect);
    }
    else
    {
        gotosubgraph = VertexIdToSelect;
        selectGraph(GraphNameToSelect);        
    }
}

function selectSubGraph(VertexId) {
    pluginLHS().centerOnItem(pluginLHS().getItem(VertexId), true);
}

function findGraphVertex(searchstring) {
    if (searchstring == 'jo debug')
    {
      showElement('xml_xgmml');
      return;
    }
    if (searchstring.toString().length == 0)
    {
      return;
    }

    resetFind();
    document.getElementById('findgvcId').value = searchstring;

    var ivs_test = null;

    var found = pluginLHS().find(searchstring);
    pluginLHS().setSelected(found);
    DisplaySelectedProperties(found);

    document.getElementById('resetFindBtn').disabled = false;
}

function resetFind() {
    var props = document.getElementById('props');
    props.innerHTML = '';
    document.getElementById('findgvcId').value = '';
}

var Timer;


function addGraphElement(GraphNode, GraphState, GraphLabel) {
    
    var ni = document.getElementById('wugraphs');
    var graphelementname = GraphNode.substring(5);
    var thisGraphLabel = GraphLabel.length < 1 ? '' : GraphLabel;
    var opt = new Option(graphelementname + ' - ' + thisGraphLabel, graphelementname);
    ni.options[ni.options.length] = opt;
    if (graphelementname == graphName.substring(5)) {
        ni.selectedIndex = graphelementname -1;
    }

    if (!GraphState || GraphState == 0) {
        opt.style.background = 'lightgrey';
        opt.title = thisGraphLabel;
    }
    if (GraphState == 3) {
        opt.style.background = '#FF9999';
        opt.title = thisGraphLabel + ' (Failed)';
    }
    if (GraphState == 1) {
        opt.style.background = 'white';
        opt.title = thisGraphLabel + ' (Completed)';
    }
    if (GraphState == 2) {
        opt.style.background = 'lightgreen';
        opt.title = thisGraphLabel + ' (Running)';
    }

}

function setElementHtml(ElementId, ElementText)
{
    obj = document.getElementById(ElementId);
    if (obj)
    {
      obj.innerHTML = ElementText;
    }
}

function setElementText(ElementId, ElementText)
{
    obj = document.getElementById(ElementId);
    if (obj)
    {
      obj.innerHTML = ElementText;
    }
}

function hideElement(ElementId)
{
    obj = document.getElementById(ElementId);
    if (obj) 
    {
      obj.style.display = 'none';
      obj.style.visibility = 'hidden';
    }
}
function showElement(ElementId)
{
    obj = document.getElementById(ElementId);
    if (obj) 
    {
      obj.style.display = 'block';
      obj.style.visibility = 'visible';
    }
}

function removeElements(ParentId)
{
    var ctrl = document.getElementById(ParentId);
    while (ctrl.childNodes[0])  
    {    
       ctrl.removeChild(ctrl.childNodes[0]);
    }            
}

function setStateDescription()
{
    var WUStates=new Array("StateUnknown","Compiled","Running","Completed","Failed","Archived","Aborting","Aborted","Blocked","Submitted","Scheduled","Compiling","Wait","UploadingFiles");
    setElementHtml('state', '<b>Status:</b>&nbsp;' + WUStates[state]);
}

function checkFindEnter() {
    if (window.event && window.event.keyCode == 13) {
        findGraphVertex(document.getElementById('findgvcId').value);
    }
    return !(window.event && window.event.keyCode == 13);
}

function checkFindEclEnter() {
    if (window.event && window.event.keyCode == 13) {
        findEcl(document.getElementById('gvcECL').value);
    }
    return !(window.event && window.event.keyCode == 13);
}

function selectGraphVertex(GraphToSelect, VertextToSelect)
{
    if (graphName.substring(5) == GraphToSelect)
    {
        selectVertex(VertextToSelect);
    }
    else
    {
        gotovertex = VertextToSelect;
        initfind = true;
        selectGraph(GraphToSelect);        
    }

}

function selectVertex(TargetVertex) {
    var FoundVertex = String(TargetVertex);
    pluginLHS().centerOnItem(pluginLHS().getItem(FoundVertex), true);
}

var forceFinalCountRefresh = true;

        
function reloadGraph()
{
    if (isrunning == '1' || forceFinalCountRefresh)
    {
      if (document.getElementById('auto').checked)
      {
          reloading = true;
          sendWuInfoRequest();
          if (isrunning != '1') {
              forceFinalCountRefresh = false;
          }
      }
    }
}

function loadGraphs() {
    removeElements('wugraphs');
    if (graphsJson != null)
    {
        try {
            addGraphElement(graphsJson.GraphNames.Item, 1, '');
        }
        catch (e) {

            for (var n = 0; n < 400; n++) {
                if (typeof (graphsJson.GraphNames.Item[n]) != "undefined") {
                    addGraphElement(graphsJson.GraphNames.Item[n], 1, '');
                }
                else {
                    break;
                }
            }
        }
    }
}

function translateWuCompletion(WuStateId)
{
    switch(WuStateId)
    {
      case '4':
      case '7':
        return 3;
      case '3':
        return 2;
      case '2':
        return 1;
      case '8':
        return 0;
    }
    return 0;
}

function loadXGMMLGraph(xgmmlResponse) {
    var xgmmlStr = xgmmlResponse;
    var i = 0;
    var j = 0;
    if (xgmmlStr.indexOf('/Exception') > -1) {
        i = xgmmlStr.indexOf('<Message>');
        if (i > -1) {
            j = xgmmlStr.indexOf('</Message>', i);
            if (j > -1) {
                alert(xgmmlStr.substring(i + 9, j));
            }
        }
        hideElement('loadingMsg'); 
        return;
    }
    i = xgmmlStr.indexOf('<Name>' + graphName + '</Name');
    if (i > -1) {
        i = xgmmlStr.indexOf('<Graph>', i);
        j = xgmmlStr.indexOf('</Graph>', i);
        xgmmlStr = '&lt;graphxgmml&gt;' + xgmmlResponse.substring(i + 7, j - 1) + '&lt;/graphxgmml&gt;';
        document.getElementById('xml_xgmml').innerHTML = xgmmlStr;
    }

    else {
        i = xgmmlStr.indexOf('<TheGraph>');
        if (i > -1) {
            var k = xgmmlStr.indexOf('<GraphNames>');
            if (k > -1) {
                var l = xgmmlStr.indexOf('</GraphNames>', k);
                if (l > -1) {
                    var xotree = new XML.ObjTree();
                    graphsJson = xotree.parseXML(xgmmlStr.substring(k, l + 13));
                    xotree = null;

                    //graphsJson = xml2json.parser(xgmmlStr.substring(k, l+13));
                }
            }
            j = xgmmlStr.indexOf('</TheGraph>');
            xgmmlStr = '&lt;graphxgmml&gt;' + xgmmlResponse.substring(i + 10, j) + '&lt;/graphxgmml&gt;';
            document.getElementById('xml_xgmml').innerHTML = xgmmlStr;
            xgmmlStr = getInnerText(document.getElementById('xml_xgmml'));
            i = xgmmlStr.indexOf('<xgmml>');
            if (i > -1) {
                j = xgmmlStr.indexOf('</xgmml>');
                document.getElementById('xml_xgmml').innerHTML = '&lt;graphxgmml&gt;' + xgmmlStr.substring(i + 7, j) + '&lt;/graphxgmml&gt;';
            }
        }
    }
    if (!suppressGvcControlLoad) {
        if (reloading) {
            var xgmmldecoded = getInnerText(document.getElementById('xml_xgmml'));

            pluginLHS().mergeXGMML(xgmmldecoded);
            //pluginLHS().startLayout("dot");

            pluginRHS().mergeXGMML(pluginLHS().getLocalisedXGMML(pluginLHS().getSelection()));

            reloading = false;
            hideElement('loadingMsg');
        }
        else {
            var xgmmldecoded = getInnerText(document.getElementById('xml_xgmml'));
            pluginLHS().loadXGMML(xgmmldecoded);
            pluginLHS().setMessage("Performing Layout...");
            pluginLHS().startLayout("dot");

            pluginLHS().centerOnItem(0, true); // scale to fit.

        }
    }
    if (isrunning == '1' || (isrunning != '1' && forceFinalCountRefresh)) {
        if (graphloaded != '1') {
            document.getElementById('auto').checked = true;
        }
        if (gt != null) {
            clearTimeout(gt);
        }
        gt = setTimeout("reloadGraph()", 8000);
    }

    update_details();

    if (wuinfoJson == null) {
        loadGraphs();
    }
    return true;
}

function findEcl(NodeId) {
    var ecltextarea = document.getElementById('gvcECLCode');
    if (ecltextarea) {
        var xgmmldecoded = getInnerText(document.getElementById('xml_xgmml'));
        var i = xgmmldecoded.indexOf('node id="' + NodeId + '"');
        if (i > -1) {
            var Label = '';
            var a = xgmmldecoded.indexOf('label="', i);
            if (a > -1) {
                var b = xgmmldecoded.indexOf('">', a);
                if (b) {
                    Label = xgmmldecoded.substring(a + 7, b).replace(/&apos;/g, "'");  
                }
            }
            var j = xgmmldecoded.indexOf('ecl" value="', i);
            if (j > -1) {
                var k = xgmmldecoded.indexOf('/>', j);
                if (k > -1) {
                    ecltextarea.innerHTML = '<b>' + Label + '</b>: ' + xgmmldecoded.substring(j + 12, k - 1).replace(/&apos;/g, "'");
                    var eclrow = document.getElementById('eclrow');
                    eclrow.style.display = 'block';
                    eclrow.style.visibility = 'visible';
                    return;
                }
            }
        }
    }
    ecltextarea.innerHTML = 'ECL for Activity ' + NodeId + ' not found.';
}

function getInnerText(elt) {
    if (isFF) {
        return elt.textContent;
    }
    else {
        return elt.innerText;
    }
}

var wuinfoJson = null;

function clearPage() {
    if (reloading)
    {
        return;
    }
    var xgmml = document.getElementById('xml_xgmml');
    if (xgmml) {
        xgmml.innerHTML = '';
    }
    pluginLHS().clear();
    pluginRHS().clear();

}

function loadWuInfo(xmlResponse)
{
    reloadThisGraph = true;

    if (graphloaded == '1' && isrunning == '1' && currentgraph != graphName)
    {
        reloadThisGraph = false;
    }
    var k = xmlResponse.indexOf('<WUInfoResponse');
    if (k > -1)
    {
        var l = xmlResponse.indexOf('</WUInfoResponse>', k);
        if (l > -1)
        {
          var xotree = new XML.ObjTree();
          wuinfoJson = xotree.parseXML(xmlResponse.substring(k, l+17));
          xotree = null;
          //wuinfoJson = xml2json.parser(xmlResponse.substring(k, l+17));
          state = wuinfoJson.WUInfoResponse.Workunit.State;

          isrunning = '0';
          if (state == 'running')
          {
            isrunning = '1';
          }
          
          stateId = translateWuCompletion(wuinfoJson.WUInfoResponse.Workunit.StateID);
          havesubgraphtimings = wuinfoJson.WUInfoResponse.Workunit.HaveSubGraphTimings;
          
          currentgraph = '';
          currentgraphnode = '';
          removeElements('wugraphs');
          try
          {
            var label = '';
            try 
            {
                label = wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph.Label;
                if (!label)
                {
                    label = '';
                }
            }
            catch(e)
            {
            }
            try
            {
                if (wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph.Running == '1')
                {
                    currentgraph = wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph.Name;
                    currentgraphnode = wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph.RunningId;
                }
            }
            catch(e)
            {
            }
            var graphstate = 0;
            if (typeof wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph.Failed != 'undefined' && wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph.Failed == '1')
            {
                graphstate = 3;    
            }
            if (typeof wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph.Complete != 'undefined' && wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph.Complete == '1')
            {
                graphstate = 1;
            }
            addGraphElement(wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph.Name, graphstate, label);
          }
          catch(e) {
            for (var n = 0; n < 400; n++)
            {
              try
              {
                var label = '';
                try 
                {
                    label = wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph[n].Label;
                    if (!label)
                    {
                        label = '';
                    }
                }
                catch(e)
                {
                }
                try
                {
                    if (wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph[n].Running == '1')
                    {
                        currentgraph = wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph[n].Name;
                        currentgraphnode = wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph[n].RunningId;
                    }
                }
                catch(e)
                {
                }
                var graphstate = '0';
                if (typeof wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph[n].Failed != 'undefined')
                {
                    graphstate = 3;    
                }
                if (typeof wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph[n].Complete != 'undefined')
                {
                    graphstate = 1;    
                }
                addGraphElement(wuinfoJson.WUInfoResponse.Workunit.Graphs.ECLGraph[n].Name, graphstate, label);
              }
              catch(e)
              {
              
                break; 
              }
            }
          }
          
          displayWsWorkunitsDetails();  
          update_details();  
        }
    }
    return;
} 


function getEspAddressAndPort(Url)
{
    espUri = parseUri(Url);

    var s = espUri.source.substring(0, espUri.source.length - espUri.relative.length);    

    return s;
}

function parseUri (str) {
    var o   = parseUri.options,
        m   = o.parser[o.strictMode ? "strict" : "loose"].exec(str),
        uri = {},
        i   = 14;

    while (i--) uri[o.key[i]] = m[i] || "";

    uri[o.q.name] = {};
    uri[o.key[12]].replace(o.q.parser, function ($0, $1, $2) {
        if ($1) uri[o.q.name][$1] = $2;
    });

    return uri;
};

parseUri.options = {
    strictMode: false,
    key: ["source","protocol","authority","userInfo","user","password","host","port","relative","path","directory","file","query","anchor"],
    q:   {
        name:   "queryKey",
        parser: /(?:^|&)([^&=]*)=?([^&]*)/g
    },
    parser: {
        strict: /^(?:([^:\/?#]+):)?(?:\/\/((?:(([^:@]*):?([^:@]*))?@)?([^:\/?#]*)(?::(\d*))?))?((((?:[^?#\/]*\/)*)([^?#]*))(?:\?([^#]*))?(?:#(.*))?)/,
        loose:  /^(?:(?![^:@]+:[^:@\/]*@)([^:\/?#.]+):)?(?:\/\/)?((?:(([^:@]*):?([^:@]*))?@)?([^:\/?#]*)(?::(\d*))?)(((\/(?:[^?#](?![^?#\/]*\.[^?#\/.]+(?:[?#]|$)))*\/?)?([^?#\/]*))(?:\?([^#]*))?(?:#(.*))?)/
    }
};

function getUrlParam( Url, Param)
{
    Param = Param.replace(/[\[]/,"\\\[").replace(/[\]]/,"\\\]");  
    Url = Url.replace(/%3F/g, '?');
    Url = Url.replace(/%3D/g, '=');
    Url = Url.replace(/%26/g, '&');
    var regexS = "[\\?&]"+Param+"=([^&#]*)";  
    var regex = new RegExp( regexS );  
    var results = regex.exec( Url );  
    if (results == null)
    {    
        return "";  
    }
    return results[1];
}

function getWsWorkunitsDetails(Url)
{
    wuid = getUrlParam(Url, 'Wuid');
    if (wuid.length < 1)
    {
      wuid = getUrlParam(Url, 'Name');
    }
    graphName = getUrlParam(Url, 'GraphName');
    if (graphName.length < 1)
    {
        graphName = getUrlParam(Url, 'Name');
    }
    displayWsWorkunitsDetails();
}

function displayWsWorkunitsDetails()
{
    var description = '<b>Wuid:</b>&#160;<a href="/WsWorkunits/WUInfo?Wuid=' + wuid + '">' + wuid + '</a>&#160;<b>Graph:</b>&#160;' + graphName + '&#160;<b>State:</b>&#160;' + state;
    if (subgraphOnly && subgraphId != '0')
    {
        description = description + '&#160;<b>Subgraph:</b>&#160;' + subgraphId;
    }
    var queryName = document.getElementById('WuidQueryName');
    if (queryName) {
        queryName.innerHTML = description;
    }
}

function getRoxieConfigDetails(Url)
{
    wuid = getUrlParam(Url, 'QueryName');
    if (wuid.length < 1)
    {
        wuid = getUrlParam(Url, 'Name');
    }
    graphName = 'graph1';
    displayRoxieConfigDetails();
}

function getWsRoxieQueryDetails(Url) {
    wuid = getUrlParam(Url, 'QueryName');
    if (wuid.length < 1) {
        wuid = getUrlParam(Url, 'Name');
    }
    graphName = 'graph1';
    displayRoxieConfigDetails();
}



function displayRoxieConfigDetails() {
    var queryName = document.getElementById('WuidQueryName');
    if (queryName) {
        queryName.innerHTML = '<b>Query:</b>&#160;' + wuid + '&#160;<b>Graph:</b>&#160;' + graphName;
    }
}

function getGraph(Url) {
    isRoxieGraph = false;
    isEclWatchGraph = false;
    isWsRoxieQueryGraph = false;
    requestSourceUrl = '';
    requestSOAPAction = '';
    wuinfoJson = null;

    espAddressAndPort = getEspAddressAndPort(Url);
    if (Url.toLowerCase().indexOf('ws_roxieconfig') > -1)
    {
      isRoxieGraph = true;
      getRoxieConfigDetails(Url);
      requestEnvelope = getRoxieConfigRequestEnvelope(wuid, graphName);
      requestSourceUrl = espAddressAndPort + '/ws_roxieconfig/ShowGVCGraph';
            
      requestSOAPAction = 'ws_roxieconfig/ShowGVCGraph';
    }

    if (Url.toLowerCase().indexOf('wsroxiequery') > -1) {
        isWsRoxieQueryGraph = true;
        getWsRoxieQueryDetails(Url);
        requestEnvelope = getWsRoxieQueryRequestEnvelope(wuid, graphName, cluster);
        requestSourceUrl = espAddressAndPort + '/WsRoxieQuery/ShowGVCGraph';
        requestSOAPAction = 'WsRoxieQuery/ShowGVCGraph';
        
    }
    
    if (Url.toLowerCase().indexOf('wsworkunits') > -1)
    {
      requestSOAPAction = 'WsWorkunits/WUGetGraph?ver_=1.21';
      isEclWatchGraph = true;
      getWsWorkunitsDetails(Url);
      setWuInfo();
      sendWuInfoRequest();
      return;
    }
    sendGraphRequest();
}

function setWuInfo()
{
  requestWuInfoEnvelope = getWuInfoRequestEnvelope(wuid);
  requestWuInfoSourceUrl = espAddressAndPort + '/WsWorkunits/WUInfo?ver_=1.19';
}

function sendGraphRequest() 
{
    if (!reloading) {
        window.graphloaded = '0';
        subgraphOnly = false;
        subgraphId = '0';
        setScale(100);
        clearPage();
        pluginLHS().setMessage("Loading Data...");
    }
    if (graphRequest != null)
    {
        graphRequest.abort();
        graphRequest = null;
    }   
    showElement('loadingMsg');
    //window.clipboardData.setData('Text', requestEnvelope);

    var connectionCallback = {
        success: function(o) {
            var response = o.responseText;
            suppressGvcControlLoad = document.getElementById('xgmmlonly').checked;
            loadXGMMLGraph(response);

        },
        failure: function(o) {
            alert('Failure:' + o.statusText);

        }
    };

    YAHOO.util.Connect.initHeader("SOAPAction", requestSOAPAction);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;


    var getXML = YAHOO.util.Connect.asyncRequest("POST",
            "/" + requestSOAPAction,
            connectionCallback, requestEnvelope);

}

function sendWuInfoRequest() {
    showElement('loadingMsg');
    if (gt != null)
    {
       clearTimeout(gt);
       gt = null;
   }

   // abort requests if there are any.
    
    if (wuinfoRequest != null)
    {
        wuinfoRequest.abort();
        wuinfoRequest = null; 
    }

    // send request.

    requestWuInfoSOAPAction = 'WsWorkunits/WUInfo?ver_=1.19';

    var connectionCallback = {
        success: function(o) {
            var response = o.responseText;

            loadWuInfo(response);
            if (reloadThisGraph) {
                requestEnvelope = getWsWorkunitsRequestEnvelope(wuid, graphName, subgraphOnly ? subgraphId : '');
                requestSourceUrl = espAddressAndPort + '/WsWorkunits/WUGetGraph';
                if (checkVersion()) {
                    sendGraphRequest();
                }
            }
            else {
                gt = setTimeout("reloadGraph()", 8000);
                hideElement('loadingMsg');
            }
        },
        failure: function(o) {
            alert('Failure:' + o.statusText);

        }
    };

    var postBody = requestWuInfoEnvelope;
    YAHOO.util.Connect.initHeader("SOAPAction", requestWuInfoSOAPAction);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;
    var getXML = YAHOO.util.Connect.asyncRequest("POST",
            "/" + requestWuInfoSOAPAction,
            connectionCallback, postBody);

    return {};

     

    /*    
    wuinfoRequest = new Ajax.Request(requestWuInfoSourceUrl,   
        {     method: 'post',
              requestHeaders: {SOAPAction: requestWuInfoSOAPAction},
              contentType: 'text/xml; charset=UTF-8',
              postBody: requestWuInfoEnvelope, 
              onSuccess: function(transport)
              {       
                var response = transport.responseText || "no response text";       
                //alert(response); 
                //window.clipboardData.setData('Text', response);
                loadWuInfo(response);
                if (reloadThisGraph)
                {
                    requestEnvelope = getWsWorkunitsRequestEnvelope(wuid, graphName, subgraphOnly ? subgraphId : '');
                    requestSourceUrl = espAddressAndPort + '/WsWorkunits/WUGetGraph';

                    sendGraphRequest();
                }
                else
                {
                    gt = setTimeout("reloadGraph()", 8000);
                    hideElement('loadingMsg');
                }
               },     
              onFailure: function()
              { alert('Ajax Error.') }   
        }); 
    */    
}

function getRoxieConfigRequestEnvelope(QueryName, GraphName)
{
    var RoxieConfigSOAPEnvelope = '<?xml version="1.0" encoding="UTF-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body><ShowGVCGraphRequest><QueryId>%QueryName%</QueryId><GraphName>%GraphName%</GraphName></ShowGVCGraphRequest></soap:Body></soap:Envelope>';
    RoxieConfigSOAPEnvelope = RoxieConfigSOAPEnvelope.replace('%QueryName%', QueryName);
    RoxieConfigSOAPEnvelope = RoxieConfigSOAPEnvelope.replace('%GraphName%', GraphName);
    return RoxieConfigSOAPEnvelope;
}

function getWsRoxieQueryRequestEnvelope(QueryName, GraphName, Cluster) {
    var RoxieQuerySOAPEnvelope = '<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/WsRoxieQuery"><soap:Body><RoxieQueryShowGVCGraphRequest><ClusterName>%Cluster%</ClusterName><QueryName>%QueryName%</QueryName><GraphName>%GraphName%</GraphName></RoxieQueryShowGVCGraphRequest></soap:Body></soap:Envelope>';
    RoxieQuerySOAPEnvelope = RoxieQuerySOAPEnvelope.replace('%Cluster%', Cluster);
    RoxieQuerySOAPEnvelope = RoxieQuerySOAPEnvelope.replace('%QueryName%', QueryName);
    RoxieQuerySOAPEnvelope = RoxieQuerySOAPEnvelope.replace('%GraphName%', GraphName);
    return RoxieQuerySOAPEnvelope;
}

function getWsWorkunitsRequestEnvelope(wuid, GraphName, SubGraphId)
{
    var WsworkunitsSOAPEnvelope = '<?xml version="1.0" encoding="UTF-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/WsWorkunits"><soap:Body><WUGetGraphRequest><Wuid>%wuid%</Wuid><GraphName>%GraphName%</GraphName><SubGraphId>%SubGraphId%</SubGraphId></WUGetGraphRequest></soap:Body></soap:Envelope>';
    WsworkunitsSOAPEnvelope = WsworkunitsSOAPEnvelope.replace('%wuid%', wuid);
    WsworkunitsSOAPEnvelope = WsworkunitsSOAPEnvelope.replace('%GraphName%', GraphName);
    WsworkunitsSOAPEnvelope = WsworkunitsSOAPEnvelope.replace('%SubGraphId%', SubGraphId);
    return WsworkunitsSOAPEnvelope;
}

function getWuInfoRequestEnvelope(wuid)
{
    var WuInfoSOAPEnvelope = '<?xml version="1.0" encoding="UTF-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/WsWorkunits"><soap:Body><WUInfoRequest><Wuid>%wuid%</Wuid><Type></Type><IncludeExceptions>0</IncludeExceptions><IncludeGraphs>1</IncludeGraphs><IncludeSourceFiles>0</IncludeSourceFiles><IncludeResults>0</IncludeResults><IncludeVariables>0</IncludeVariables><IncludeTimers>0</IncludeTimers><IncludeDebugValues>0</IncludeDebugValues><IncludeApplicationValues>0</IncludeApplicationValues><IncludeWorkflows>0</IncludeWorkflows><SuppressResultSchemas>1</SuppressResultSchemas></WUInfoRequest></soap:Body></soap:Envelope>';
    WuInfoSOAPEnvelope = WuInfoSOAPEnvelope.replace('%wuid%', wuid);
    return WuInfoSOAPEnvelope;
}


function showGraphStats()
{
    var link = document.getElementById('StatsLink');
    link.innerHTML = 'Loading Stats...';
    var baseUrl = '/ws_roxieconfig/ProcessGraph?FileName=' + queryName + '/' + graphName;
    var url = baseUrl + '.htm&Stats=1';
    var wnd = window.open("about:blank", "_graphStats_", 
                            "toolbar=0,location=0,directories=0,status=0,menubar=0," + 
                            "scrollbars=1, resizable=1, width=640, height=480");
    link.innerHTML = 'Stats...';
    if (wnd)
    {
        wnd.location = url;
        wnd.focus();
    }
    else
    {
                alert(  "Popup window could not be opened!  " + 
                            "Please disable any popup killer and try again.");
    }
}

function selectGraph(GraphId) {
    //document.getElementById('Graph:' + graphName).style.border = '1 solid white';
    reloading = false;
    clearPage();
    if (gt != null)
    {
        clearTimeout(gt);
    } 
    if (graphRequest != null)
    {
        graphRequest.abort();
        graphRequest = null;
    }   
    resetFind();
    window.graphloaded = '0';
    graphName = 'graph' + GraphId;
    var newGraphTd = document.getElementById('Graph:' + graphName);
    if (newGraphTd != null)
    {
        newGraphTd.style.border = '1 solid black';
    }
    if (isRoxieGraph)
    {
        displayRoxieConfigDetails();
        requestEnvelope = getRoxieConfigRequestEnvelope(wuid, graphName);
        sendGraphRequest();
        return;
    }
    if (isEclWatchGraph)
    {
        displayWsWorkunitsDetails();
        sendWuInfoRequest();
    }
    if (isWsEclGraph)
    {
        loadGvcGraph(GraphId);
    }
    if (isWsRoxieQueryGraph) {
        displayRoxieConfigDetails();
        requestEnvelope = getWsRoxieQueryRequestEnvelope(wuid, graphName, cluster);
        sendGraphRequest();
    }
    
    return;
}
