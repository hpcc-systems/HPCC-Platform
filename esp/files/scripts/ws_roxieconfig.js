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


 // RoxieConfig Tab

YAHOO.namespace("esp.container");
 
var WS_ROXIECONFIG_VER = "1.01";

var tabView;

/*
window.onresize = function() {
    refreshCurrentTab(true);
    if (queryPanel) {
        if (queryPanel.cfg.getProperty("visible")) {
            tabQuery.set('activeIndex', tabQuery.get('activeIndex'));
            queryPanel.cfg.setProperty("height", (YAHOO.util.Dom.getViewportHeight() > 500 ? YAHOO.util.Dom.getViewportHeight() - 40 : 500));
            queryPanel.cfg.setProperty("width", (YAHOO.util.Dom.getViewportWidth() > 500 ? YAHOO.util.Dom.getViewportWidth() - 140 : 500));
            //        queryPanel.render();
        }
    }
}
*/

var ds_FullQueries, dt_FullQueries, ds_Queries, dt_Queries;

function loadTab(Index, Resize)
{
    switch (Index) {
        case 0:
            clearDataFiles();
            loadQueries('ListDeployedQueries', Resize);
            break;
        case 1:
            clearDataFiles();
            loadQueries('ListDeployedDataOnlyQueries', Resize);
            break;
        case 2:
            clearDataFiles();
            loadQueries('ListDeployedLibraryQueries', Resize);
            break;
        case 3:
            clearDataFiles();
            loadAliases('', "dtaliases", null, Resize);
            break;
        case 4:
            loadDataFiles('', 'dtindexes', 18, null, Resize);
            break;
        case 5:
            loadSuperFiles('', 'dtsuperfiles', 20, null, Resize);
            break;
        case 6:
            //loadComponentsXml();
            loadPendingDeployments('dt_deployments', Resize);

    }
}

function refreshCurrentTab(Resize) {
    loadTab(tabView.get('activeIndex'), Resize);
}

// Query Tab

var tabQuery;

function setQueryActionState(DisableActionButtons) {
    disableActions = DisableActionButtons;
    
    switch (tabView.get('activeIndex')) {
        case 0:
            document.getElementById('buttonDeleteQuery1').disabled = disableActions;
            document.getElementById('buttonAddAlias1').disabled = disableActions;
            document.getElementById('buttonSuspendQueries1').disabled = disableActions;
            break;
        case 1:
            document.getElementById('buttonDeleteQuery2').disabled = disableActions;
            document.getElementById('buttonAddAlias2').disabled = disableActions;
            break;
        case 2:
            document.getElementById('buttonDeleteQuery3').disabled = disableActions;
            document.getElementById('buttonAddAlias3').disabled = disableActions;
            document.getElementById('buttonSuspendQueries3').disabled = disableActions;
            break;
    }
}

function changeQueryTab(Index)
{
    switch (Index)
    {
        case 1:
          loadAliases(CurrentQueryName, "dtqueryaliases", true);
          break;
        case 2:
          loadDataFiles(CurrentQueryName, 'dtqueryfiles', 15, true);
          break;
        case 3:
          loadSuperFiles(CurrentQueryName, 'dtquerysuperfiles', 15, true);
          break;
        case 4:
          var LibraryName = CurrentQueryName.substring(0, CurrentQueryName.lastIndexOf('.'));
          loadQueriesUsingLibrary(LibraryName, 'dtlibqueries');
    }
}

// DataFiles Tab

var tabDataFiles;


var tabQueryDataFiles;

// Query Panel

var queryPanel, actionPanel, confirmationPanel;
var openPanel;
var ShowDeployTab = 0;
function init() {
    // Instantiate a Panel from markup

    /*
    var loader = new YAHOO.util.YUILoader({

        require: ["treeview", "button", "animation", "autocomplete"],
        base: '/esp/files/yui/build/',
        loadOptional: false,
        combine: true,
        filter: "MIN",
        allowRollup: true,

        //When the loading is all complete, we want to initialize   
        //our TabView process; we can set this here or pass this   
        //in as an argument to the insert() method:   
        onSuccess: function() {

            //YAHOO.util.Get.css('yui/build/treeview/assets/css/folders/tree.css');
            //YAHOO.util.Get.script('scripts/ws_roxieconfig_left.js');  
            //YAHOO.util.Get.script('/esp/files/scripts/ws_roxieconfig.js');
        }
    });
    loader.insert();
    */
    document.getElementById('bd1').style.display = "block";
    document.getElementById('tvcontainer').style.display = "block";
    
    tabView = new YAHOO.widget.TabView('tvcontainer');

    tabView.on('activeIndexChange', function(e) { loadTab(e.newValue) });

    document.getElementById('tvcontainer').style.display = 'block';
    document.getElementById('bd1').style.display = 'block';

    tabQuery = new YAHOO.widget.TabView('tvquery');
    document.getElementById('tvquery').style.display = '';

    tabDataFiles = new YAHOO.widget.TabView('tvdatafiles');
    document.getElementById('tvdatafiles').style.display = '';

    tabQueryDataFiles = new YAHOO.widget.TabView('tvquerydata');
    document.getElementById('tvquerydata').style.display = '';

    tabQuery.on('activeIndexChange', function(e) { changeQueryTab(e.newValue) });

    confirmationPanel = new YAHOO.widget.Panel("ConfirmationPanel", { width: "320px", height: "320px", modal: true, visible: false, fixedcenter: true });
    document.getElementById('ConfirmationPanel').style.display = 'block';
    confirmationPanel.render();

    actionPanel = new YAHOO.widget.Panel("ActionPanel", { width: "800px", visible: false, fixedcenter: true, constraintoviewport: true });
    document.getElementById('ActionPanel').style.display = 'block';
    actionPanel.render();

    loadTab(ShowDeployTab);
};

YAHOO.util.Event.addListener(window, "load", init);


var CurrentQueryName;
var notifyRoxie = true;

function setNotifyRoxie(Setting) {
    notifyRoxie = Setting;
    for (var i = 1; i < 6; i++) {
        var chk = document.getElementById('checkNotifyRoxie' + i);
        if (chk) {
            chk.checked = notifyRoxie;
        }
    }
}
var queryPanelWidth;

function displayQueryPanel() {
    var selectedRow = dt_Queries.getSelectedRows();
    if (selectedRow.length > 0) {
        if (queryPanel) {
            queryPanel = null;
        }
        queryPanelWidth = YAHOO.util.Dom.getViewportWidth() - 140;
        if (queryPanelWidth < 500) {
            queryPanelWidth = 500;
        }
        queryPanel = new YAHOO.widget.Panel("QueryPanel",
            { height: (YAHOO.util.Dom.getViewportHeight() > 500 ? YAHOO.util.Dom.getViewportHeight() - 40 : 500), width: queryPanelWidth, visible: false, x: 10, y: 10, constraintoviewport: true
            });

        //queryPanel.cfg.setProperty("height", (YAHOO.util.Dom.getViewportHeight() > 500 ? YAHOO.util.Dom.getViewportHeight() - 40 : 500));
        //queryPanel.cfg.setProperty("width", (YAHOO.util.Dom.getViewportWidth() > 500 ? YAHOO.util.Dom.getViewportWidth() - 140 : 500));
        
        document.getElementById('QueryPanel').style.display = 'block';
        queryPanel.render();


        var record = dt_Queries.getRecord(selectedRow[0]);

        CurrentQueryName = record.getData('QueryId');
        document.getElementById('qhd').innerHTML = CurrentQueryName;

        tabQuery.set('activeIndex', 0);
        var queryDetails = '<table class="yui-skin-sam">';
        queryDetails += '<tr><td style="width:150"><b>Name:</b></td><td>&nbsp;' + record.getData('QueryId') + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>Suspended:</b></td><td>&nbsp;' + (record.getData('Suspended') == "1" ? "Yes" : "No") + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>Aliases:</b></td><td>&nbsp;' + (record.getData('Aliased') == "1" ? "Yes" : "No") + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>Library:</b></td><td>&nbsp;' + (record.getData('IsLibrary') == "1" ? "Yes" : "No") + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>Libraries Used:</b></td><td>&nbsp;' + record.getData('LibrariesUsed') + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>Comment:</b></td><td>&nbsp;' + record.getData('Comment') + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>Label:</b></td><td>&nbsp;' + record.getData('Label') + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>Wuid:</b></td><td>&nbsp;' + record.getData('Wuid') + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>AssociatedName:</b></td><td>&nbsp;' + record.getData('AssociatedName') + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>Error:</b></td><td>&nbsp;' + record.getData('Error') + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>DeployedBy:</b></td><td>&nbsp;' + record.getData('DeployedBy') + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>PkgQueryVersion:</b></td><td>&nbsp;' + record.getData('PkgQueryVersion') + '</td></tr>';
        queryDetails += '<tr><td style="width:150"><b>SuspendedBy:</b></td><td>&nbsp;' + record.getData('SuspendedBy') + '</td></tr>';

        queryDetails += '</table>';
        document.getElementById('qdetails').innerHTML = queryDetails;

        ds_QueryDataFiles = null;
        dt_QueryDataFiles = null;
        document.getElementById('dtqueryfiles').innerHTML = '';
        queryPanel.show();
    }
}

// ListDeployedQueries

var QueryRowHandler = function(oArgs) {   
    displayQueryPanel();
};   

var queryUrlClicked = false;

var QueryRowClickedHandler = function(oArgs) {
dt_Queries.onEventSelectRow(oArgs);
  if (queryUrlClicked)
  {
    displayQueryPanel();
    queryUrlClicked = false;
  }
};

var QueryCellClickedHandler = function(oArgs) {
    var record = dt_Queries.getRecord(oArgs.target);
    var column = dt_Queries.getColumn(oArgs.target);
    if (column && column.field == 'Mark') {
        var isChecked = record.getData(column.field) == '1' ? true : false;
        record.setData(column.field, isChecked ? '0' : '1');

        var records = dt_Queries.getRecordSet().getRecords();
        var i_marked = 0;

        for (var i = 0; i < records.length; i++) {
            if (records[i].getData('Mark') == '1') {
                i_marked++;
            }
        }

        if (i_marked == records.length) { // marked all
            document.getElementById("markallqueries").checked = true;
        }
        if (i_marked == 0) { // marked none
            document.getElementById("markallqueries").checked = false;
        }
        var disableActions = i_marked > 0 ? false : true;


        setQueryActionState(disableActions);

    }
    dt_Queries.onEventShowCellEditor(oArgs);
}

function createEntry(sData, oRecord, oColumn, bMasked, DatatableName) {
    var d = '<input type=\"' + (bMasked ? 'password' : 'text') + '\" onChange=\"onEditEntryChange(\'' + oColumn.getField() + '\', \'' + oRecord.getId() + '\', this.value, \'' + DatatableName + '\')\" value=\"' + sData + '\" />';
    return d;
}

function onEditEntryChange(ColumnName, RecordId, Value, DatatableName) {

    switch (DatatableName) {
        case "dt_Action":
            {
                var record = dt_Action.getRecord(RecordId);
                record.setData(ColumnName, Value);
                break;
            }
        default:
            {
                break;
            }

    }
    
}

var formatActionInlineEntry = function(elCell, oRecord, oColumn, sData) {
    elCell.innerHTML = createEntry(sData, oRecord, oColumn, false, 'dt_Action');
};

var formatUrl = function(elCell, oRecord, oColumn, sData) {   
 elCell.innerHTML = '<a href="javascript:void(0)" onclick="queryUrlClicked=true;">' + sData + '</a>';   
};   

var formatBool = function(elCell, oRecord, oColumn, sData) {   
 elCell.innerHTML = sData == "1" ? "Yes" : "No";   
};   

var formatCheckboxDisabled = function(elCell, oRecord, oColumn, sData) {   
 elCell.innerHTML = "&nbsp;";
 if (sData == "1") {
   YAHOO.util.Dom.addClass(elCell, "yuimenuitem-checked");
 }
};

var formatUsesLibraries = function(elCell, oRecord, oColumn, sData) {
    elCell.innerHTML = "&nbsp;";
    if (sData.length > 0) {
        YAHOO.util.Dom.addClass(elCell, "yuimenuitem-checked");
    }
}

var formatQueryError = function(elCell, oRecord, oColumn, sData) {
    if (sData.length > 0) {
        elCell.innerHTML = '<a href="javascript:void(0)" class="cellstatusinformation" title="Query information" onclick="showQueryError(\'' + oRecord.getId() + '\')">&nbsp;</a>';
        return;
    }
    elCell.innerHTML = '';
};

function showQueryError(RecId) {
    var recordSet = dt_Queries.getRecordSet();
    var record = recordSet.getRecord(RecId);

    showInformation(record.getData('QueryId') + ' Status', record.getData('ErrorStatus').replace(/\n/gi, '<br />'));
}

function showInformation(HeaderText, BodyText) {

    var handleOk = function() {
        statusPanel.hide();
        statusPanel = null;
    };
    if (!statusPanel) {
        statusPanel = new YAHOO.widget.SimpleDialog("showDeploymentStatus",
        { width: "500px",
            fixedcenter: true,
            visible: true,
            close: true,
            effect: {
                effect: YAHOO.widget.ContainerEffect.FADE,
                duration: 0.25
            },
            icon: YAHOO.widget.SimpleDialog.ICON_INFO,
            constraintoviewport: true,
            buttons: [{ text: "Ok", handler: handleOk, isDefault: true}]
        });
    }

    statusPanel.setHeader(HeaderText);
    statusPanel.setBody('<div style="text-align:left; height:300px; width:auto; overflow:scroll;">' + BodyText + '</div>');
    statusPanel.render("statusdiv");
    statusPanel.show();
}

function openGraph(QueryName)
{
    window.open('/ws_roxieconfig/GVCAjaxGraph?Name=' + QueryName,'', 'menubar = no');
    return;
}

function markAllQueries() {
    var records = dt_Queries.getRecordSet().getRecords();
    var globalMark = document.getElementById('markallqueries').checked;
    var i_marked = 0;
    for (var i = 0; i < records.length; i++) {
        records[i].setData('Mark', globalMark);
        if (globalMark) {
            i_marked++;
        }
    }
    dt_Queries.render();
    setQueryActionState(i_marked > 0 ? false : true);
}

var formatGraphLink = function(elCell, oRecord, oColumn, oData) {
    elCell.innerHTML = '<a title="Graph" href="javascript:void(0);" onclick="openGraph(\'' + oRecord.getData('QueryId') + '\')" class="graphlink">&nbsp;</a>';
};

var formatInlineEditCheckbox = function(el, oRecord, oColumn, oData) {
    var bChecked = oData == '1' ? true : false;
    bChecked = (bChecked) ? " checked=\"checked\"" : "";
    el.innerHTML = "<input type=\"checkbox\"" + bChecked +
            " class=\"" + YAHOO.widget.DataTable.CLASS_CHECKBOX + "\" />";
}

YAHOO.widget.DataTable._bDynStylesFallback = true

function loadQueries(QueryType, Resize) {
    /*
    if (Resize) {
        debugger;
        dt_Queries.setAttributeConfig("height", "200px", true);
        dt_Queries.render();
        //dt_Queries.setAttributeConfig("height", (YAHOO.util.Dom.getViewportHeight() - 200) + "px", true);
        //dt_Queries.cfg.setProperty("width", (YAHOO.util.Dom.getViewportWidth() - 25) + "px");
        return;
    }
    */
    if (ds_FullQueries && dt_Queries) {
        dt_Queries.destroy();
        dt_Queries = null;
    }
    if (Resize) {
        createQueryDataTable(QueryType);
        filterDeployedQueries();
        return;
    }
    var connectionCallback = {
        success: function(o) {

            var xmlDoc = o.responseXML;
            ds_FullQueries = new YAHOO.util.DataSource(xmlDoc);
            ds_FullQueries.responseType = YAHOO.util.DataSource.TYPE_XML;

            /*
            var test_arr = [
            { QueryId: "123", QueryPriority: "1", Suspended: "1", Aliased: "1", IsLibrary: "0", DeployedBy: "Jo", Wuid: "Test", Error: "", ErrorStatus: "", Comment: "", LibrariesUsed: "", Label: "Test", AssociatedName: "Test", PkgQueryVersion: "", SuspendedBy: "" },
            { QueryId: "456", QueryPriority: "1", Suspended: "1", Aliased: "1", IsLibrary: "0", DeployedBy: "Jo", Wuid: "Test", Error: "", ErrorStatus: "", Comment: "", LibrariesUsed: "", Label: "Test", AssociatedName: "Test", PkgQueryVersion: "", SuspendedBy: "" }
            ];
            ds_FullQueries = new YAHOO.util.DataSource(test_arr);
            ds_FullQueries.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
            */
            
            ds_FullQueries.responseSchema = {
                resultNode: "QueryInfo",
                fields: ["QueryId", "QueryPriority", "Suspended", "Aliased", "IsLibrary", "DeployedBy", "Wuid", "Error", "ErrorStatus", "Comment", "LibrariesUsed", "Label", "AssociatedName", "PkgQueryVersion", "SuspendedBy"]
            };

            createQueryDataTable(QueryType);

            filterDeployedQueries();

            checkForException(xmlDoc);

            document.getElementById("markallqueries").disabled = false;
        },
        failure: function(o) {
            alert('Failure:' + o.statusText);

        }
    };

    function getCheckboxValue(ElementId) {
        if (document.getElementById(ElementId)) {
            return document.getElementById(ElementId).checked ? '1' : '';
        }
        return "";
    }
    
    function createQueryDataTable(QueryType) {
        var myColumnDefs = [
                { key: "Mark", label: "<input type=\"checkbox\" id=\"markallqueries\" onclick=\"markAllQueries();\" disabled=\"true\" />", formatter: formatInlineEditCheckbox, width: 40, resizable: false },
                { key: "QueryId", label: "Name", sortable: true, formatter: formatUrl, width: (isFF ? 320 : 380), resizeable: false },
                { key: "ErrorStatus", label: "Info", formatter: formatQueryError, width: 40, resizable: false },
                { key: "Graph", label: "<span title=\"Graph\">G</span>", formatter: formatGraphLink, width: (isFF ? 20 : 30), resizable: false },
                { key: "QueryPriority", label: "Priority", width: 60, resizable: false },
                { key: "Suspended", sortable: true, label: "<span title=\"Is Suspended\">S</span>", parser: "boolean", formatter: formatCheckboxDisabled, width: (isFF ? 20 : 30), resizable: false },
                { key: "Aliased", sortable: true, label: "<span title=\"Is Aliased\">A</span>", parser: "boolean", formatter: formatCheckboxDisabled, width: (isFF ? 20 : 30), resizable: false },
                { key: "IsLibrary", sortable: true, label: "<span title=\"Is a Library\">L</span>", parser: "boolean", formatter: formatCheckboxDisabled, width: (isFF ? 20 : 30), resizable: false },
                { key: "LibrariesUsed", label: "<span title=\"Uses Library\">UL</span>", formatter: formatUsesLibraries, width: (isFF ? 20 : 30), resizable: false },
                { key: "DeployedBy", sortable: true, label: "Deployed by", width: 120, resizable: false },
                { key: "Wuid", sortable: true, label: "Workunit", width: 200, resizable: false }
            ];

                //dt_Queries = new YAHOO.widget.DataView("dt_" + QueryType, myColumnDefs, ds_FullQueries, { height: (YAHOO.util.Dom.getViewportHeight() - 200) + "px", width: (YAHOO.util.Dom.getViewportWidth() - 25) + "px", paginator: new YAHOO.widget.Paginator({ rowsPerPage: 50 })});
                dt_Queries = new YAHOO.widget.DataView("dt_" + QueryType, myColumnDefs, ds_FullQueries, { width: "100%", paginator: new YAHOO.widget.Paginator({ rowsPerPage: 50 })});
                dt_Queries.set("selectionMode", "single");
        //dt_Queries.subscribe("rowMouseoverEvent", dt_Queries.onEventHighlightRow);
        //dt_Queries.subscribe("rowMouseoutEvent", dt_Queries.onEventUnhighlightRow);
                dt_Queries.subscribe("rowDblclickEvent", QueryRowHandler);
                dt_Queries.subscribe("rowClickEvent", QueryRowClickedHandler);
                dt_Queries.subscribe("cellClickEvent", QueryCellClickedHandler);
    }
    
    //document.getElementById('dt_' + QueryType).innerHTML = '<img src="/esp/files/img/loading.gif" style="height:18px;" />';
    if (dt_Queries) {
        dt_Queries.destroy();
        dt_Queries = null;
    }
    queryFilter.QueryName = document.getElementById(QueryType + 'Filter').value;
    queryFilter.Suspended = getCheckboxValue(QueryType + 'FilterSuspended');
    queryFilter.Aliased = getCheckboxValue(QueryType + 'FilterAliases');
    queryFilter.IsLibrary = getCheckboxValue(QueryType + 'FilterLibrary');
    queryFilter.LibrariesUsed = getCheckboxValue(QueryType + 'FilterUsesLibrary');

    ds_FullQueries = YAHOO.util.DataSource();
    ds_Queries = YAHOO.util.DataSource();
    createQueryDataTable(QueryType);
    dt_Queries.showTableMessage(dt_Queries.get("MSG_LOADING"), dt_Queries.CLASS_LOADING);
    
    var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body><' + QueryType + '><excludeAliasNames>0</excludeAliasNames><excludeQueryNames>0</excludeQueryNames><excludeLibraryNames>0</excludeLibraryNames><excludeDataOnlyNames>1</excludeDataOnlyNames></' + QueryType + '></soap:Body></soap:Envelope>';

    YAHOO.util.Connect.initHeader("SOAPAction", "ws_roxieconfig/" + QueryType + "?ver_=" + WS_ROXIECONFIG_VER);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;
    var getXML = YAHOO.util.Connect.asyncRequest("POST",
            "/ws_roxieconfig/" + QueryType + "?ver_=" + WS_ROXIECONFIG_VER,
            connectionCallback, postBody);

    return {
        oDS: ds_FullQueries,
        oDT: dt_Queries
    };
}

function checkForException(xmlDoc) {
    var exceptions = xmlDoc.getElementsByTagName('Exception');
    if (exceptions && exceptions.length > 0) {
        var message = exceptions[0].getElementsByTagName('Message')[0].childNodes[0].nodeValue;
        showInformation("Error Loading", message);
    }
}
// ListDeployedAliases

var ds_Aliases, dt_Aliases;

var AliasRowHandler = function(oArgs) {   
  dt_Aliases.onEventSelectRow(oArgs);
  //var selectedRow = dt_Aliases.getSelectedRows();
  //if (selectedRow.length > 0)
  //{
  //  var record = dt_Aliases.getRecord(selectedRow[0]);
  //} 
};

var AliasCellClickedHandler = function(oArgs) {
    var record = dt_Aliases.getRecord(oArgs.target);
    var column = dt_Aliases.getColumn(oArgs.target);
    if (column && column.field == 'Mark') {
        record.setData(column.field, record.getData(column.field) == '1' ? '0' : '1');
    }

    var records = dt_Aliases.getRecordSet().getRecords();
    var i_marked = 0;

    for (var i = 0; i < records.length; i++) {
        if (records[i].getData('Mark') == '1') {
            i_marked++;
        }
    }

    if (i_marked == records.length) { // marked all
        document.getElementById("markallaliases").checked = true;
    }
    if (i_marked == 0) { // marked none
        document.getElementById("markallaliases").checked = false;
    }
    var disableActions = i_marked > 0 ? false : true;
    if (disableActions) {
        document.getElementById('buttonDeleteAliases').disabled = true;
    } else {
        document.getElementById('buttonDeleteAliases').disabled = false;
    }

}

function markAllAliases() {
    var globalMark = document.getElementById('markallaliases').checked;
    var records = dt_Aliases.getRecordSet().getRecords();
    for (var i = 0; i < records.length; i++) {
        records[i].setData('Mark', globalMark);
    }
    dt_Aliases.render();
}

function loadAliases(QueryName, ElementId, QueryPage, Resize) {
    if (!document.getElementById('checkNotifyRoxie4')) {
        var d = document.createElement("span");
        var notifyChecked = '';
        if (notifyRoxie) {
            notifyChecked = ' checked="true"';
        }
        d.innerHTML = '<input type="checkbox" name="checkNotifyRoxie5" id="checkNotifyRoxie4" ' + notifyChecked + ' onclick="setNotifyRoxie(this.checked);">Notify Roxie?</input>';
        document.getElementById('aliasactions').appendChild(d);
    }

    if (Resize) {
        createAliasDataTable(ElementId);
        filterAliases();
        return;
    }
    var connectionCallback = {
        success: function(o) {

            var xmlDoc = o.responseXML;
            

            ds_Aliases = new YAHOO.util.DataSource(xmlDoc);
            ds_Aliases.responseType = YAHOO.util.DataSource.TYPE_XML;
            ds_Aliases.responseSchema = {
                resultNode: "AliasInfo",
                fields: ["QueryAlias","QueryId"]
            };

            createAliasDataTable(ElementId);

            dt_Aliases.subscribe("rowMouseoverEvent", dt_Aliases.onEventHighlightRow);
            dt_Aliases.subscribe("rowMouseoutEvent", dt_Aliases.onEventUnhighlightRow);

            dt_Aliases.set("selectionMode","multiple");   
            dt_Aliases.subscribe("rowClickEvent", AliasRowHandler);   
            dt_Aliases.subscribe("cellClickEvent", AliasCellClickedHandler);

            checkForException(xmlDoc);

        },
        failure: function(o) {
            alert('Failure:' + o.statusText);

        }
    };

    function createAliasDataTable(ElementId) {
        var myColumnDefs = [
                { key: "Mark", label: "<input type=\"checkbox\" id=\"markallaliases\" onclick=\"markAllAliases();\" />", formatter: formatInlineEditCheckbox, width: 40, resizable: false },
                { key: "QueryAlias", label: "Alias", sortable: true, width: 350 },
                { key: "QueryId", label: "Query", width: 350 }
            ];
                if (QueryPage) {
                    dt_Aliases = new YAHOO.widget.ScrollingDataTable(ElementId, myColumnDefs, ds_Aliases, { height: (YAHOO.util.Dom.getViewportHeight() - 155) + "px", width: (queryPanel.cfg.getProperty('width') - 65) + "px"  });
                } else {
                    dt_Aliases = new YAHOO.widget.DataView(ElementId, myColumnDefs, ds_Aliases, { height: (YAHOO.util.Dom.getViewportHeight() - 140) + "px", width: (YAHOO.util.Dom.getViewportWidth() - 25) + "px" });
                }
    }

    if (dt_Aliases) {
        dt_Aliases.destroy();
        dt_Aliases = null;
        ds_Aliases = null;
    }

    ds_Aliases = YAHOO.util.DataSource();
    createAliasDataTable(ElementId);
    dt_Aliases.showTableMessage(dt_Aliases.get("MSG_LOADING"), dt_Aliases.CLASS_LOADING);
        
    var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body><ListDeployedAliasesRequest><QueryId>' + QueryName + '</QueryId></ListDeployedAliasesRequest></soap:Body></soap:Envelope>';
    YAHOO.util.Connect.initHeader("SOAPAction", "ws_roxieconfig/ListDeployedAliases?ver_=" + WS_ROXIECONFIG_VER);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;
    var getXML = YAHOO.util.Connect.asyncRequest("POST",
            "/ws_roxieconfig/ListDeployedAliases",
            connectionCallback, postBody);
            
    return {
        oDS: ds_Aliases,
        oDT: dt_Aliases
    };
}

function filterAliases() {
    if (document.getElementById('AliasFilter').value.length > 0) {
        dt_Aliases.Filter([{ Value: document.getElementById('AliasFilter').value, ColumnKey: "Alias"}]);
    }
    else {
        dt_Aliases.ClearFilters();
    }
}

// ListDataFilesUsedByQuery

function clearDataFiles()
{
    document.getElementById('dtindexes').innerHTML = '';
    setQueryActionState(true);
}

var DataFilesRowHandler = function(oArgs) {   
  dt_IndexFiles.onEventSelectRow(oArgs);
  //var selectedRow = dt_IndexFiles.getSelectedRows();
  //if (selectedRow.length > 0)
  //{
  //  var record = dt_IndexFiles.getRecord(selectedRow[0]);
  //} 
};   

// Define a custom row formatter function   

var parseEspNumber = function(oArgs)
{
    //return oArgs.replace(',', ''));
    return YAHOO.util.DataSourceBase.parseNumber(oArgs.replace(/,/g, ''));
};

var ds_IndexFiles, dt_IndexFiles;
var ds_DataFiles, dt_DataFiles;

function loadDataFiles(QueryName, ElementId, RowsPerPage, QueryPage, Resize)
{
    if (Resize) {
        createDataFileDataTable(ElementId);
        filterDataFiles();
        filterIndexFiles();
        return;
    }
    var connectionCallback = {
        success: function(o) {

            var xmlDocMain = o.responseXML;
            var xmlIndexes = xmlDocMain.getElementsByTagName('IndexFiles')[0];
            var xmlDataFiles = xmlDocMain.getElementsByTagName('DataFiles')[0];

            var nodeTotals = xmlDocMain.getElementsByTagName('Totals');
            if (nodeTotals.length > 0) {
                var xmlTotalRecords = nodeTotals[0].getElementsByTagName('RecordCount')[0];
                if (xmlTotalRecords) {
                    document.getElementById(ElementId + "Records").innerHTML = xmlTotalRecords.childNodes[0].nodeValue;
                }
                var xmlTotalSize = nodeTotals[0].getElementsByTagName('FileSize');
                if (xmlTotalSize.length > 0) {
                    document.getElementById(ElementId + "Size").innerHTML = xmlTotalSize[0].childNodes[0].nodeValue;
                }
            }

            // Indexes
            if (xmlIndexes) {
                ds_IndexFiles = new YAHOO.util.DataSource(xmlIndexes);
                ds_IndexFiles.responseType = YAHOO.util.DataSource.TYPE_XML;
                ds_IndexFiles.responseSchema = {
                    resultNode: "FileInfo",
                    fields: [{ key: "Name" }, { key: "FileSize", parser: parseEspNumber }, { key: "RecordCount", parser: parseEspNumber }, { key: "CopiedBy"}]
                };
            }
            else {
                ds_Indexes = new YAHOO.util.DataSource();
            }
            // Data Files
            if (xmlDataFiles) {
                ds_DataFiles = new YAHOO.util.DataSource(xmlDataFiles);
                ds_DataFiles.responseType = YAHOO.util.DataSource.TYPE_XML;
                ds_DataFiles.responseSchema = {
                    resultNode: "FileInfo",
                    fields: [{ key: "Name" }, { key: "FileSize", parser: parseEspNumber }, { key: "RecordCount", parser: parseEspNumber }, { key: "CopiedBy"}]
                };
            }
            else {
                ds_DataFiles = new YAHOO.util.DataSource();
            }

            createDataFileDataTable(ElementId);

            dt_IndexFiles.set("selectionMode", "single");
            dt_IndexFiles.subscribe("rowClickEvent", DataFilesRowHandler);

            dt_DataFiles.subscribe("rowMouseoverEvent", dt_DataFiles.onEventHighlightRow);
            dt_DataFiles.subscribe("rowMouseoutEvent", dt_DataFiles.onEventUnhighlightRow);

            dt_DataFiles.set("selectionMode", "single");
            dt_DataFiles.subscribe("rowClickEvent", DataFilesRowHandler);

            checkForException(xmlDocMain);


        },
        failure: function(o) {
            alert('Failure:' + o.statusText);

        }
    };

    function createDataFileDataTable(ElementId) {
        var oConfigs = {
            height: (YAHOO.util.Dom.getViewportHeight() - 195) + "px", width: (YAHOO.util.Dom.getViewportWidth() - 35) + "px" 
            //initialRequest: "results=504"
        };

        var indexColumnDefs = [
        //{ key: "Mark", label: "<input type=\"checkbox\" id=\"markallindexes\" onclick=\"markAllIndexes();\" />", formatter: formatInlineEditCheckbox, width: 20 },
                {key: "Name", label: "Name", sortable: true, width: 600 },
                { key: "FileSize", label: "Size", sortable: true, formatter: "number", width: 80 },
                { key: "RecordCount", label: "Count", sortable: true, formatter: "number", width: 80 }
            ];

        var dataFileColumnDefs = [
        //{ key: "Mark", label: "<input type=\"checkbox\" id=\"markalldatafiles\" onclick=\"markAllDataFiles();\" />", formatter: formatInlineEditCheckbox, width: 20 },
                {key: "Name", label: "Name", sortable: true, width: 600 },
                { key: "FileSize", label: "Size", sortable: true, formatter: "number", width: 80 },
                { key: "RecordCount", label: "Count", sortable: true, formatter: "number", width: 80 }
            ];

        if (QueryPage) {
            dt_IndexFiles = new YAHOO.widget.ScrollingDataTable(ElementId, indexColumnDefs, ds_IndexFiles, { height: (YAHOO.util.Dom.getViewportHeight() - 240) + "px", width: (queryPanel.cfg.getProperty('width') - 80) + "px" });
            dt_DataFiles = new YAHOO.widget.ScrollingDataTable(ElementId + "2", dataFileColumnDefs, ds_DataFiles, { height: (YAHOO.util.Dom.getViewportHeight() - 240) + "px", width: (queryPanel.cfg.getProperty('width') - 80) + "px" });
        } else {
            dt_IndexFiles = new YAHOO.widget.DataView(ElementId, indexColumnDefs, ds_IndexFiles, oConfigs);
            dt_DataFiles = new YAHOO.widget.DataView(ElementId + "2", dataFileColumnDefs, ds_DataFiles, oConfigs);
        }

        dt_IndexFiles.subscribe("rowMouseoverEvent", dt_IndexFiles.onEventHighlightRow);
        dt_IndexFiles.subscribe("rowMouseoutEvent", dt_IndexFiles.onEventUnhighlightRow);


    }

    if (dt_DataFiles) {
        dt_DataFiles.destroy();
        dt_DataFiles = null;
        ds_DataFiles = null;
    }
    if (dt_IndexFiles) {
        dt_IndexFiles.destroy();
        ds_IndexFiles = null;
    }
    
    ds_IndexFiles = YAHOO.util.DataSource();
    ds_DataFiles = YAHOO.util.DataSource();
    
    createDataFileDataTable(ElementId);
    dt_IndexFiles.showTableMessage(dt_IndexFiles.get("MSG_LOADING"), dt_IndexFiles.CLASS_LOADING);
    dt_DataFiles.showTableMessage(dt_DataFiles.get("MSG_LOADING"), dt_DataFiles.CLASS_LOADING);
        
    var postBody = '<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body><ListFilesUsedByQueryRequest><QueryId>' + QueryName + '</QueryId><excludeSuperFileNames>1</excludeSuperFileNames><excludeDataFileNames>0</excludeDataFileNames></ListFilesUsedByQueryRequest></soap:Body></soap:Envelope>';
    YAHOO.util.Connect.initHeader("SOAPAction", "ws_roxieconfig/ListFilesUsedByQuery?ver_=" + WS_ROXIECONFIG_VER);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;
    var getXML = YAHOO.util.Connect.asyncRequest("POST",
            "/ws_roxieconfig/ListFilesUsedByQuery?ver_=" + WS_ROXIECONFIG_VER,
            connectionCallback, postBody);
            
    return {
        oDS: ds_IndexFiles,
        oDT: dt_IndexFiles
    };
}

function filterIndexFiles() {
    if (document.getElementById('IndexFilesFilter').value.length > 0) {
        dt_IndexFiles.Filter([{ Value: document.getElementById('IndexFilesFilter').value, ColumnKey: "Name"}]);
    }
    else {
        dt_IndexFiles.ClearFilters();
    }
}

function filterDataFiles() {
    if (document.getElementById('DataFilesFilter').value.length > 0) {
        dt_DataFiles.Filter([{ Value: document.getElementById('DataFilesFilter').value, ColumnKey: "Name"}]);
    }
    else {
        dt_DataFiles.ClearFilters();
    }
}

function markAllIndexes() {
    var records = dt_IndexFiles.getRecordSet().getRecords();
    var globalMark = document.getElementById('markallindexes').checked;
    for (var i = 0; i < records.length; i++) {
        records[i].setData('Mark', globalMark);
    }
    dt_IndexFiles.render();
}


// Load Super Files

var ds_SuperFiles, dt_SuperFiles;

function loadSuperFiles(QueryName, ElementId, RowsPerPage, QueryPage, Resize)
{
    if (Resize) {
        createSuperFilesDataTable();
        filterSuperFiles();
        return;
    }
    var connectionCallback = {
        success: function(o) {

            var xmlDocMain = o.responseXML;

            // SuperFiles
            ds_SuperFiles = new YAHOO.util.DataSource(xmlDocMain);
            ds_SuperFiles.responseType = YAHOO.util.DataSource.TYPE_XML;
            ds_SuperFiles.responseSchema = {
                resultNode: "SuperFileInfo",
                fields: [{ key: "Name" }, { key: "FileSize", parser: parseEspNumber }, { key: "RecordCount", parser: Number }, { key: "CopiedBy"}]
            };
            createSuperFilesDataTable();
            
            dt_SuperFiles.subscribe("rowMouseoverEvent", dt_SuperFiles.onEventHighlightRow);
            dt_SuperFiles.subscribe("rowMouseoutEvent", dt_SuperFiles.onEventUnhighlightRow);

            dt_SuperFiles.set("selectionMode", "single");
            dt_SuperFiles.subscribe("rowClickEvent", dt_SuperFiles.onEventSelectRow);

            checkForException(xmlDocMain);

        },
        failure: function(o) {
            alert('Failure:' + o.statusText);

        }
    };

    function createSuperFilesDataTable() {
        var oConfigs = {
            height: (YAHOO.util.Dom.getViewportHeight() - 140) + "px", width: (YAHOO.util.Dom.getViewportWidth() - 25) + "px" 
            //initialRequest: "results=504"
        };

        var myColumnDefs = [
        //{ key: "Mark", label: "<input type=\"checkbox\" id=\"markallsuperfiles\" onclick=\"markAllSuperFiles();\" />", formatter: formatInlineEditCheckbox, width: 20 },
                {key: "Name", label: "Name", sortable: true, width: 600 },
                { key: "Version", label: "Version", sortable: true, formatter: "number", width: 100 }
            ];

        if (QueryPage) {
            dt_SuperFiles = new YAHOO.widget.ScrollingDataTable(ElementId, myColumnDefs, ds_SuperFiles, { height: (YAHOO.util.Dom.getViewportHeight() - 150) + "px", width: (queryPanel.cfg.getProperty('width') - 70) + "px" });
        } else {
            dt_SuperFiles = new YAHOO.widget.DataView(ElementId, myColumnDefs, ds_SuperFiles, oConfigs);
        }

    }

    if (dt_SuperFiles) {
        dt_SuperFiles.destroy();
        dt_SuperFiles = null;
        ds_SuperFiles = null;
    }

    ds_SuperFiles = YAHOO.util.DataSource();

    createSuperFilesDataTable();
    dt_SuperFiles.showTableMessage(dt_SuperFiles.get("MSG_LOADING"), dt_SuperFiles.CLASS_LOADING);
        
    var postBody = '<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body><ListFilesUsedByQueryRequest><QueryId>' + QueryName + '</QueryId><excludeSuperFileNames>0</excludeSuperFileNames><excludeDataFileNames>1</excludeDataFileNames></ListFilesUsedByQueryRequest></soap:Body></soap:Envelope>';
    YAHOO.util.Connect.initHeader("SOAPAction", "ws_roxieconfig/ListFilesUsedByQuery?ver_=" + WS_ROXIECONFIG_VER);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;
    var getXML = YAHOO.util.Connect.asyncRequest("POST",
            "/ws_roxieconfig/ListFilesUsedByQuery?ver_=" + WS_ROXIECONFIG_VER,
            connectionCallback, postBody);
            
    return {
        oDS: ds_IndexFiles,
        oDT: dt_IndexFiles
    };
}

function filterSuperFiles() {
    if (document.getElementById('SuperFilesFilter').value.length > 0) {
        dt_SuperFiles.Filter([{ Value: document.getElementById('SuperFilesFilter').value, ColumnKey: "Name"}]);
    }
    else {
        dt_SuperFiles.ClearFilters();
    }
}

function markAllSuperFiles() {
    var records = dt_SuperFiles.getRecordSet().getRecords();
    var globalMark = document.getElementById('markallsuperfiles').checked;
    for (var i = 0; i < records.length; i++) {
        records[i].setData('Mark', globalMark);
    }
    dt_SuperFiles.render();
}

// 
function hideConfirmationPanel()
{
    confirmationPanel.hide();
}

function hideActionPanel()
{
    actionPanel.hide();
}

function addAlias()
{
    actionPanel.show();
}

// Query Actions

function deleteQueries()
{
    var queries = new Array();

    var formatAliasLabel = function(elCell, oRecord, oColumn, sData) {
        elCell.innerHTML = '<input type="text" value="' + sData + '" />';
    };

    var myColumnDefs = [
                { key: "QueryId", label: "Query Name", sortable: true, width: 400 },
                { key: "Response", label: "Response", width: 300 }
            ];

    var records = dt_Queries.getRecordSet().getRecords();
    var i_added = 0;
    for (var i = 0; i < records.length; i++) {
        if (records[i].getData('Mark') == '1') {
            queries[i_added] = { QueryId: records[i].getData('QueryId'), Response: '' };
            i_added++;
        }
    }

    ds_Action = new YAHOO.util.LocalDataSource(queries);

    ds_Action.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
    ds_Action.responseSchema = {
        fields: ["QueryId", "Response"]
    };
    dt_Action = new YAHOO.widget.ScrollingDataTable('dt_Action', myColumnDefs, ds_Action, { height: "400px" });

    performAction("Delete Queries", doDeleteQueries, "750px");
}


function doDeleteQueries() {
    modifyQueries('delete', 'ListDeployedQueries');
}

function suspendQueries() {
    var recordSet = dt_Queries.getRecordSet();
    confirmMarked('ConfirmationPanel', 'QueryName', recordSet, 'Suspend these Queries?', doSuspendQueries);
}

function doSuspendQueries() {
    modifyQueries('suspend', 'ListDeployedQueries');
}


function checkForEnter(e) {
    if (!e) {
        alert('no e?');
    }
    if (e && e.keyCode == 13) {
        doFilter(e);
    }
    return !(window.event && window.event.keyCode == 13); 
}

function doFilter(e) {
    var srcElement = e.srcElement;
    if (!srcElement) {
        srcElement = e.target;
        while (srcElement.nodeType != srcElement.ELEMENT_NODE) {
            srcElement = e.parentNode;
        }
    }

    switch (srcElement.id) {
        case "ListDeployedQueriesFilter":
            queryFilter.QueryName = srcElement.value;
            filterDeployedQueries();
            break;
        case "ListDeployedQueriesFilterSuspended":
        case "ListDeployedDataOnlyQueriesFilterSuspended":
        case "ListDeployedLibraryQueriesFilterSuspended":
            queryFilter.Suspended = srcElement.checked ? '1' : '';
            filterDeployedQueries();
            break;
        case "ListDeployedQueriesFilterAliases":
        case "ListDeployedDataOnlyQueriesFilterAliases":
        case "ListDeployedLibraryQueriesFilterAliases":
            queryFilter.Aliased = srcElement.checked ? '1' : '';
            filterDeployedQueries();
            break;
        case "ListDeployedQueriesFilterLibrary":
            queryFilter.IsLibrary = srcElement.checked ? '1' : '';
            filterDeployedQueries();
            break;
        case "ListDeployedQueriesFilterUsesLibrary":
        case "ListDeployedLibraryQueriesFilterUsesLibrary":
            queryFilter.LibrariesUsed = srcElement.checked ? '1' : '';
            filterDeployedQueries();
            break;
        case "ListDeployedDataOnlyQueriesFilter":
            queryFilter.QueryName = srcElement.value;
            filterDeployedQueries();
            break;
        case "ListDeployedLibraryQueriesFilter":
            queryFilter.QueryName = srcElement.value;
            filterDeployedQueries();
            break;
        case "AliasFilter":
            filterAliases();
            break;
        case "DataFilesFilter":
            filterDataFiles();
            break;
        case "IndexFilesFilter":
            filterIndexFiles();
            break;
        case "SuperFilesFilter":
            filterSuperFiles();
            break;
    }
}

var queryFilter = {
    QueryName: "",
    DeployedBy: "",
    Wuid: "",
    Suspended: "",
    Aliased: "",
    IsLibrary: "",
    LibrariesUsed: ""
}

function filterLibrariesUsed(RowValue, FilterValue) {
    // custom function to handle "Libraries used" like an exists check
    if (FilterValue == '1' && RowValue.length > 0) {
        return true;
    }
    return false;
}

function filterDeployedQueries() {
    var filterArray = new Array();
    if (queryFilter.QueryName.length > 0) {
        filterArray[filterArray.length] = { Value: queryFilter.QueryName, ColumnKey: ["QueryId", "DeployedBy", "Wuid"] };
    }
    if (queryFilter.Suspended.length > 0) {
        filterArray[filterArray.length] = { Value: queryFilter.Suspended, ColumnKey: "Suspended" };
    }
    if (queryFilter.Aliased.length > 0) {
        filterArray[filterArray.length] = { Value: queryFilter.Aliased, ColumnKey: "Aliased" };
    }
    if (queryFilter.IsLibrary.length > 0) {
        filterArray[filterArray.length] = { Value: queryFilter.IsLibrary, ColumnKey: "IsLibrary" };
    }
    if (queryFilter.LibrariesUsed.length > 0) {
        filterArray[filterArray.length] = { Value: queryFilter.LibrariesUsed, ColumnKey: "LibrariesUsed", FilterFunction: filterLibrariesUsed };
    }
    if (filterArray.length > 0) {
        dt_Queries.Filter(filterArray);
    }
    else {
        dt_Queries.ClearFilters();
    }
}

function filterDataTable(DataTableSource, FilterColumn, FilterString)
{
  var records = DataTableSource.getRecordSet().getRecords();
  for (var i = 0; i < records.length; i++)
  {
    var elRow = records[i].getId();
    if (records[i].getData(FilterColumn).toLowerCase().indexOf(FilterString.toLowerCase()) > -1) {
        document.getElementById(elRow).style.display = "";
    } else {
        document.getElementById(elRow).style.display = "none";
    }
  }   
}

function filterDataSource(OriginalDataSource, FilterColumn, FilterString)
{
    
  var records = OriginalDataSource.getRecordSet().getRecords();
  for (var i = 0; i < records.length; i++)
  {
    var elRow = records[i].getId();
    if (records[i].getData(FilterColumn).toLowerCase().indexOf(FilterString.toLowerCase()) > -1) {
        document.getElementById(elRow).style.display = "";
    } else {
        document.getElementById(elRow).style.display = "none";
    }
  }   
}

function confirmMarked(PanelId, DesciptionColumn, RecordSet, HeaderText, OkMethodToCall)
{
  document.getElementById('ConfirmationHeader').innerHTML = HeaderText;
  var records = RecordSet.getRecords();
  var innerHTML = '<table style="text-align:left;">';
  for (var i = 0; i < records.length; i++)
  {
    if (records[i].getData('Mark') == '1')
    {
      innerHTML += '<tr><td>' + records[i].getData(DesciptionColumn) + '</td></tr>';
    }
  }
  innerHTML += '</table>';
  document.getElementById('ConfirmationList').innerHTML = innerHTML;
  document.getElementById('buttonConfirmationOk').onclick = OkMethodToCall;
  confirmationPanel.show();
}

function showConfirmationMessage(PanelId, HeaderText, BodyText, OkMethodToCall) {
    document.getElementById('ConfirmationHeader').innerHTML = HeaderText;
    document.getElementById('ConfirmationList').innerHTML = BodyText;
    document.getElementById('buttonConfirmationOk').onclick = OkMethodToCall;
    confirmationPanel.show();
}

// Alias Actions

function deleteAliases() {
    var queries = new Array();

    var formatAliasLabel = function(elCell, oRecord, oColumn, sData) {
        elCell.innerHTML = '<input type="text" value="' + sData + '" />';
    };
    var myColumnDefs = [
                { key: "QueryAlias", label: "Alias", sortable: true, width: 300 },
                { key: "QueryId", label: "Query", sortable: true, width: 300 },
                { key: "Response", label: "Response", width: 100 }
            ];
    var records = dt_Aliases.getRecordSet().getRecords();
    var i_added = 0;
    for (var i = 0; i < records.length; i++) {
        if (records[i].getData('Mark') == '1') {
            queries[i_added] = { QueryAlias: records[i].getData('QueryAlias'), QueryId: records[i].getData('QueryId'), Response: '' };
            i_added++;
        }
    }

    ds_Action = new YAHOO.util.LocalDataSource(queries);

    ds_Action.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
    ds_Action.responseSchema = {
        fields: ["QueryAlias", "QueryId", "Response"]
    };
    dt_Action = new YAHOO.widget.ScrollingDataTable('dt_Action', myColumnDefs, ds_Action, { height: "300px" });

    performAction("Delete Aliases", doDeleteAliases, "900px");
}


function doDeleteAliases() {
    var connectionCallback = {
        success: function(o) {
            var xmlDoc = o.responseXML;
            var rows = xmlDoc.getElementsByTagName('StatusRow');
            // Update Response Column.

            if (rows && rows.length > 0) {
                for (var i = 0; i < rows.length; i++) {
                    var cols = rows[i].getElementsByTagName('Item');
                    if (cols && cols.length > 0) {
                        var aliasName = cols[0].childNodes[0].nodeValue;
                        var response = cols[1].childNodes[0].nodeValue;
                        var records = dt_Action.getRecordSet().getRecords();
                        for (var j = 0; j < records.length; j++) {
                            if (records[j].getData('QueryAlias') == aliasName) {
                                records[j].setData('Response', response);
                            }
                        }
                    }
                }
            }
            dt_Action.render();
            document.getElementById('buttonActionCancel').innerHTML = 'Close';
            document.getElementById('buttonActionOk').style.display = 'none';
            clearActionProgress();
            setActionCloseButton(refreshCurrentTab);
        },
        failure: function(o) {
            alert('Failure:' + o.statusText);

        }
    };


    function clearActionProgress() {
        document.getElementById('buttonActionOk').disabled = false;
        document.getElementById('ActionProgress').innerHTML = '';
    }

    document.getElementById('ActionProgress').innerHTML = '<img src="/esp/files/img/loading.gif" style="height:18px;" />';
    var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body>' + getSelectedAliases() + '</soap:Body></soap:Envelope>';
    
    YAHOO.util.Connect.initHeader("SOAPAction", "ws_roxieconfig/RemoveAliases?ver_=" + WS_ROXIECONFIG_VER);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;
    var getXML = YAHOO.util.Connect.asyncRequest("POST",
            "/ws_roxieconfig/RemoveAliases" + "?ver_=" + WS_ROXIECONFIG_VER,
            connectionCallback, postBody);

    return {
};
}

function getSelectedAliases() {
  var removeAliasesRequest = '<RemoveAliasesRequest><Aliases>';
  var records = dt_Aliases.getRecordSet().getRecords();
  for (var i = 0; i < records.length; i++) {
      if (records[i].getData('Mark') == '1') {
          removeAliasesRequest += '<AliasInfo>';
          removeAliasesRequest += '<QueryAlias>' + records[i].getData('QueryAlias') + '</QueryAlias>';
          removeAliasesRequest += '<QueryId>' + records[i].getData('QueryId') + '</QueryId>';
          removeAliasesRequest += '</AliasInfo>';
      }
  }
  removeAliasesRequest += '</Aliases></RemoveAliasesRequest>';

  return removeAliasesRequest;  

}

// Load Queries using Library

var ds_LibQueries, dt_LibQueries;

function loadQueriesUsingLibrary(LibraryName, ElementId)
{
    var connectionCallback = {
        success: function(o) {
            var xmlDoc = o.responseXML;
            var xmlQueries = xmlDoc.getElementsByTagName('Item');
            var queries = new Array();
            for(var i=0;i<xmlQueries.length;i++)
            {
                queries[i] = xmlQueries[i].childNodes[0].nodeValue;
            }
            var myColumnDefs = [
                {key:"Query", label:"Query", sortable:true, width:800}
            ];

            ds_LibQueries = new YAHOO.util.LocalDataSource(queries);  
   
            
            ds_LibQueries.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
            ds_LibQueries.responseSchema = {
                fields: ["Query"]
            };
            dt_LibQueries = new YAHOO.widget.ScrollingDataTable(ElementId, myColumnDefs, ds_LibQueries, { height: (YAHOO.util.Dom.getViewportHeight() - 155) + "px", width: (queryPanel.cfg.getProperty('width') - 65) + "px" });


        },
        failure: function(o) {
            alert('Failure:' + o.statusText);

        }
    };
    
    document.getElementById(ElementId).innerHTML = '<img src="/esp/files/img/loading.gif" style="height:18px;" />';
    var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body><ListQueriesUsingLibraryRequest><LibraryName>' + LibraryName + '</LibraryName></ListQueriesUsingLibraryRequest></soap:Body></soap:Envelope>';
    YAHOO.util.Connect.initHeader("SOAPAction", "ws_roxieconfig/ListQueriesUsingLibrary?ver_=" + WS_ROXIECONFIG_VER);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;
    var getXML = YAHOO.util.Connect.asyncRequest("POST",
            "/ws_roxieconfig/ListQueriesUsingLibrary" + "?ver_=" + WS_ROXIECONFIG_VER,
            connectionCallback, postBody);
            
    return {
    };
}

function modifyQueries(Operation, ElementId)
{
    var connectionCallback = {
        success: function(o) {
            var xmlDoc = o.responseXML;
            var rows = xmlDoc.getElementsByTagName('StatusRow');
            // Update Response Column.

            if (rows && rows.length > 0) {
                for (var i = 0; i < rows.length; i++) {
                    var cols = rows[i].getElementsByTagName('Item');
                    if (cols && cols.length > 0) {
                        var queryName = cols[0].childNodes[0].nodeValue;
                        var response = cols[1].childNodes[0].nodeValue;
                        if (response.indexOf('Error val') > -1) {
                            var ttmp = unescape(response);
                            ttmp = ttmp.replace(/\/+/gi, ' ');
                            ttmp = ttmp.replace(/</gi, ' ');
                            ttmp = ttmp.replace(/>/gi, ' ');
                            response = ttmp;
                        }
                        var records = dt_Action.getRecordSet().getRecords();
                        for (var j = 0; j < records.length; j++) {
                            if (records[j].getData('QueryId') == queryName) {
                                records[j].setData('Response', response);
                            }
                        }
                    }
                }
            }
            dt_Action.render();
            document.getElementById('buttonActionCancel').innerHTML = 'Close';
            document.getElementById('buttonActionOk').style.display = 'none';
            clearActionProgress();
            setActionCloseButton(refreshCurrentTab);
        },
        failure: function(o) {
            alert('Failure:' + o.statusText);
            debugger;

        }
    };


    function clearActionProgress() {
        document.getElementById('buttonActionOk').disabled = false;
        document.getElementById('ActionProgress').innerHTML = '';
    }

    document.getElementById('ActionProgress').innerHTML = '<img src="/esp/files/img/loading.gif" style="height:18px;" />';
    var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body>' + getSelectedQueries(Operation) + '</soap:Body></soap:Envelope>';
    YAHOO.util.Connect.initHeader("SOAPAction", "ws_roxieconfig/ModifyQueries?ver_=" + WS_ROXIECONFIG_VER);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;
    var getXML = YAHOO.util.Connect.asyncRequest("POST",
            "/ws_roxieconfig/ModifyQueries" + "?ver_=" + WS_ROXIECONFIG_VER,
            connectionCallback, postBody);
            
    return {
    };
}

function getSelectedQueries(Operation) {
    var modifyQueriesRequest = '<ModifyQueriesRequest><Operation>' + Operation + '</Operation><NotifyRoxie>' + notifyRoxie + '</NotifyRoxie><Queries>';
    var records = dt_Queries.getRecordSet().getRecords();
    for (var i = 0; i < records.length; i++) {
        if (records[i].getData('Mark') == '1') {
            modifyQueriesRequest += '<QueryInfo>';
            modifyQueriesRequest += '<QueryId>' + records[i].getData('QueryId') + '</QueryId>';
            if (Operation == 'suspend') {
                modifyQueriesRequest += '<Suspended>' + (records[i].getData('Suspended') == '1' ? '0' : '1') + '</Suspended>';
            }
            modifyQueriesRequest += '</QueryInfo>';
        }
    }
    modifyQueriesRequest += '</Queries></ModifyQueriesRequest>';

    return modifyQueriesRequest;
}

var ds_Action, dt_Action;

function activateQueries(Activate) {
    var queries = new Array();

    var formatAliasLabel = function(elCell, oRecord, oColumn, sData) {
        elCell.innerHTML = '<input type="text" value="' + sData + '" />';
    };

    var myColumnDefs = [
                { key: "QueryId", label: "Query Name", sortable: true, width: 200 },
                { key: "Alias", label: "Alias", formatter: formatActionInlineEntry, width: 200, sortable: true },
                { key: "SuspendPrevious", label: "Suspend<br />Previous", parser: "boolean", formatter: "checkbox", width: 80 },
                { key: "DeletePrevious", label: "Delete<br />Previous", parser: "boolean", formatter: "checkbox", width: 80 },
                { key: "Response", label: "Response", width: 150 }
            ];

    var records = dt_Queries.getRecordSet().getRecords();
    var i_added = 0;
    for (var i = 0; i < records.length; i++) {
        if (records[i].getData('Mark') == '1') {
            var aliasName = records[i].getData('QueryId');
            aliasName = aliasName.substring(0, aliasName.lastIndexOf('.'));
            queries[i_added] = { QueryId: records[i].getData('QueryId'), Alias: aliasName, SuspendPrevious: false, DeletePrevious: false, Response: '' };
            i_added++;
        }
    }

    ds_Action = new YAHOO.util.LocalDataSource(queries);

    ds_Action.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
    ds_Action.responseSchema = {
        fields: ["QueryId", "Alias", "SuspendPrevious", "DeletePrevious", "Response"]
    };
    dt_Action = new YAHOO.widget.ScrollingDataTable('dt_Action', myColumnDefs, ds_Action, { height: "200px" });

    var highlightEditableCell = function(oArgs) {
        var elCell = oArgs.target;
        if (YAHOO.util.Dom.hasClass(elCell, "yui-dt-editable")) {
            this.highlightCell(elCell);
        }
    };

    dt_Action.subscribe("cellMouseoverEvent", highlightEditableCell);
    dt_Action.subscribe("cellMouseoutEvent", dt_Action.onEventUnhighlightCell);
    dt_Action.subscribe("cellClickEvent", dt_Action.onEventShowCellEditor);

    performAction((Activate ? "Activate Queries" : "Add Aliases"), (Activate ? doActivateQueries : doAddAliases), "800px");
}

function doActivateQueries() {
    actionQueries(true);
}

function doAddAliases() {
    actionQueries(false);
}

function performAction(HeaderText, OkMethodToCall, Width) {
    document.getElementById('ahd').innerHTML = HeaderText;
    // dt_Action
    actionPanel.cfg.setProperty("width", Width);
    YAHOO.util.Dom.addClass('ActionProgress', 'spanleft');
    YAHOO.util.Dom.addClass('ActionButtons', 'spanright');
    var test = dt_Action.get('width');
    var t = document.getElementById('dt_Action');

    document.getElementById('buttonActionOk').disabled = false;
    document.getElementById('buttonActionOk').style.display = 'block';
    document.getElementById('buttonActionCancel').innerHTML = 'Cancel';
    document.getElementById('ActionProgress').innerHTML = '';

    document.getElementById('buttonActionOk').onclick = function() { document.getElementById('buttonActionOk').disabled = true; OkMethodToCall(); };  // disable the Ok button
    document.getElementById('buttonActionCancel').onclick = function() { hideActionPanel(); };  // disable the Ok button
    actionPanel.show();
}

function setActionCloseButton(CloseMethodToCall) {
    if (CloseMethodToCall) {
        document.getElementById('buttonActionCancel').onclick = function() { CloseMethodToCall(); hideActionPanel(); };   // disable the Ok button
    } else {
        document.getElementById('buttonActionCancel').onclick = function() { hideActionPanel(); };   // disable the Ok button
    }
}

function actionQueries(Activate) {
    var connectionCallback = {
        success: function(o) {
            var xmlDoc = o.responseXML;
            var rows = xmlDoc.getElementsByTagName('StatusRow');
            // Update Response Column.

            if (rows && rows.length > 0) {
                for (var i = 0; i < rows.length; i++) {
                    var cols = rows[i].getElementsByTagName('Item');
                    if (cols && cols.length > 0) {
                        var queryName = cols[0].childNodes[0].nodeValue;
                        var response = cols[1].childNodes[0].nodeValue;
                        var records = dt_Action.getRecordSet().getRecords();
                        for (var j = 0; j < records.length; j++) {
                            if (records[j].getData('QueryId') == queryName) {
                                records[j].setData('Response', response);
                            }
                        }
                    }
                }
            }
            dt_Action.render();
            document.getElementById('buttonActionCancel').innerHTML = 'Close';
            document.getElementById('buttonActionOk').style.display = 'none';
            clearActionProgress();
            setActionCloseButton(refreshCurrentTab);
        },
        failure: function(o) {
            alert('Failure:' + o.statusText);
            clearActionProgress();
            hideActionPanel();

        }
    };

    function clearActionProgress() {
        document.getElementById('buttonActionOk').disabled = false;
        document.getElementById('ActionProgress').innerHTML = '';
    } 
    
    document.getElementById('ActionProgress').innerHTML = '<img src="/esp/files/img/loading.gif" style="height:18px;" />';
    var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body>' + getActionQueries(Activate) + '</soap:Body></soap:Envelope>';
    YAHOO.util.Connect.initHeader("SOAPAction", "ws_roxieconfig/AddAliasesToQueries?ver_=" + WS_ROXIECONFIG_VER);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;
    
    var getXML = YAHOO.util.Connect.asyncRequest("POST",
           "/ws_roxieconfig/AddAliasesToQueries" + "?ver_=" + WS_ROXIECONFIG_VER,
            connectionCallback, postBody);
            
    

    return {
};
}

function getActionQueries(Activate) {
    var modifyQueriesRequest = '<AddAliasesRequest><Activate>' + (Activate ? '1' : '0') + '</Activate><Info>';
    var records = dt_Action.getRecordSet().getRecords();
    for (var i = 0; i < records.length; i++) {
            modifyQueriesRequest += '<RoxieQueryModificationInfo>';
            modifyQueriesRequest += '<QueryId>' + records[i].getData('QueryId') + '</QueryId>';
            modifyQueriesRequest += '<QueryAlias>' + records[i].getData('Alias') + '</QueryAlias>';
            modifyQueriesRequest += '<SuspendPrevious>' + records[i].getData('SuspendPrevious') + '</SuspendPrevious>';
            modifyQueriesRequest += '<DeletePrevious>' + records[i].getData('DeletePrevious') + '</DeletePrevious>';
            modifyQueriesRequest += '</RoxieQueryModificationInfo>';
    }
    modifyQueriesRequest += '</Info></AddAliasesRequest>';

    return modifyQueriesRequest;
}

function toggleQueries() {
    var queries = new Array();

    var formatAliasLabel = function(elCell, oRecord, oColumn, sData) {
        elCell.innerHTML = '<input type="text" value="' + sData + '" />';
    };

    var myColumnDefs = [
                { key: "QueryId", label: "Query Name", sortable: true, width: 300 },
                { key: "Suspended", label: "Suspended", formatter: formatCheckboxDisabled, width: 80, sortable: true },
                { key: "Action", label: "Action", width: 80 },
                { key: "Response", label: "Response", width: 200 }
            ];

    var records = dt_Queries.getRecordSet().getRecords();
    var i_added = 0;
    for (var i = 0; i < records.length; i++) {
        if (records[i].getData('Mark') == '1') {
            var aliasName = records[i].getData('QueryId');
            var suspended = records[i].getData('Suspended');
            var action = suspended == '1' ? 'Unsuspend' : 'Suspend';
            queries[i_added] = { QueryId: records[i].getData('QueryId'), Suspended: suspended, Action: action, Response: '' };
            i_added++;
        }
    }

    ds_Action = new YAHOO.util.LocalDataSource(queries);

    ds_Action.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
    ds_Action.responseSchema = {
        fields: ["QueryId", "Suspended", "Action", "Response"]
    };
    dt_Action = new YAHOO.widget.ScrollingDataTable('dt_Action', myColumnDefs, ds_Action, { height: "500px" });

    var highlightEditableCell = function(oArgs) {
        var elCell = oArgs.target;
        if (YAHOO.util.Dom.hasClass(elCell, "yui-dt-editable")) {
            this.highlightCell(elCell);
        }
    };

    dt_Action.subscribe("cellMouseoverEvent", highlightEditableCell);
    dt_Action.subscribe("cellMouseoutEvent", dt_Action.onEventUnhighlightCell);
    dt_Action.subscribe("cellClickEvent", dt_Action.onEventShowCellEditor);

    performAction("Change Query Status", doSuspendQueries, "750px");
}

function listQueriesUsingFile(SourceDt) {
    var connectionCallback = {
        success: function(o) {
            var xmlDoc = o.responseXML;
            var xmlQueries = xmlDoc.getElementsByTagName('Item');
            if (xmlQueries) {
                var queries = new Array();
                for (var i = 0; i < xmlQueries.length; i++) {
                    queries[i] = xmlQueries[i].childNodes[0].nodeValue;
                }

                var myColumnDefs = [{ key: "Query", label: "Query", sortable: true, width: 385}];

                ds_Action = new YAHOO.util.LocalDataSource(queries);

                ds_Action.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
                ds_Action.responseSchema = {
                    fields: ["Query"]
                };
                dt_Action = new YAHOO.widget.ScrollingDataTable('dt_Action', myColumnDefs, ds_Action, { height: "300px", width: "390px" });

                dt_Action.render();
                document.getElementById('buttonActionCancel').innerHTML = 'Close';
                document.getElementById('buttonActionOk').style.display = 'none';
                clearActionProgress();
                setActionCloseButton();

                document.getElementById('ahd').innerHTML = "Queries using index";
                // dt_Action
                actionPanel.cfg.setProperty("width", "420px");
                YAHOO.util.Dom.addClass('ActionProgress', 'spanleft');
                YAHOO.util.Dom.addClass('ActionButtons', 'spanright');
                actionPanel.show();
            }
        },
        failure: function(o) {
            alert('Failure:' + o.statusText);
        }
    };

    function clearActionProgress() {
        document.getElementById('buttonActionOk').disabled = false;
        document.getElementById('ActionProgress').innerHTML = '';
    }

    // Find selected rows...

    var rows = SourceDt.getSelectedRows();
    if (rows.length > 0) {
        var rec = SourceDt.getRecord(rows[0]);
        var FileName = rec.getData('Name');
        document.getElementById('ActionProgress').innerHTML = '<img src="/esp/files/img/loading.gif" style="height:18px;" />';
        var postBody = '<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body><ListQueriesUsingFileRequest><fileName>' + FileName + '</fileName></ListQueriesUsingFileRequest></soap:Body></soap:Envelope>';
        YAHOO.util.Connect.initHeader("SOAPAction", "ws_roxieconfig/ListQueriesUsingFile?ver_=" + WS_ROXIECONFIG_VER);
        YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
        YAHOO.util.Connect._use_default_post_header = false;

        var getXML = YAHOO.util.Connect.asyncRequest("POST",
           "/ws_roxieconfig/ListQueriesUsingFile" + "?ver_=" + WS_ROXIECONFIG_VER,
            connectionCallback, postBody);

    }
    
    return {};
}



