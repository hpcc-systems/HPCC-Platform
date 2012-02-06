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

var dtDeploy, dsDeploy;
var addEclPanel;
var uploader;

(function() {

    var btnDeploySelected = new YAHOO.widget.Button({
        label: "Deploy",
        id: "deploySelected",
        container: "btnDeployments",
        onclick: { fn: deployWorkunits }
    });

})();

function createDroplist(SelectedNameService, DroplistArray, oRecord, oColumn) {
    var d = "<select onChange=\"onEditDroplistChange('" + oColumn.getField() + "', '" + oRecord.getId() + "', this.value)\">";
    for (var i = 0; i < DroplistArray.length; i++) {
        d += '<option value="' + DroplistArray[i].value + '" ' + (DroplistArray[i].value == SelectedNameService ? 'selected = "1"' : '') + '>' + DroplistArray[i].label + '</option>';
    }
    d += '</select>';
    return d;
}

var formatInlinePassword = function(elCell, oRecord, oColumn, sData) {
    elCell.innerHTML = createEntry(sData, oRecord, oColumn, true);
};

function onEditDroplistChange(ColumnName, RecordId, Value) {
    var record = dtDeploy.getRecord(RecordId);
    record.setData(ColumnName, Value);
    dtDeploy.render();
}

var formatMaskedLabel = function(elCell, oRecord, oColumn, sData) {
    elCell.innerHTML = sData.replace(/./g, '*');
};

var formatProcessAction = function(elCell, oRecord, oColumn, sData) {
    if (sData == "1") {
        elCell.innerHTML = '<img src="/esp/files/yui/build/assets/skins/sam/loading.gif" />';
    }
    if (sData == "2") {
        elCell.innerHTML = '<img src="/esp/files/yui/build/assets/skins/sam/menuitem_checkbox.png" />';
    }
    
};

function checkNotifyRoxieCheck() {
    if (document.getElementById('deploySavePending') && !document.getElementById('checkNotifyRoxie5')) {
        var d = document.createElement("span");
        var notifyChecked = '';
        if (notifyRoxie) {
            notifyChecked = ' checked="true"';
        }
        d.innerHTML = '<input type="checkbox" name="checkNotifyRoxie5" id="checkNotifyRoxie5" ' + notifyChecked + ' onclick="setNotifyRoxie(this.checked);">Notify Roxie?</input>';
        document.getElementById('btnDeployments').appendChild(d);
    }
}

var WorkunitCellClickedHandler = function(oArgs) {
    var record = dtDeploy.getRecord(oArgs.target);
    var column = dtDeploy.getColumn(oArgs.target);
    if (column && column.field == 'Mark') {
        var isChecked = record.getData(column.field) == '1' ? true : false;
        record.setData(column.field, isChecked ? '0' : '1');
        record.setData('Activation', '1');
        dtDeploy.render();
        return;
    }
    dtDeploy.onEventShowCellEditor(oArgs);

}

var ar_Activate = new Array({ label: "Don't Activate", value: "0" }, { label: "Activate", value: "1" }, { label: "Suspend Previous", value: "2" }, { label: "Delete Previous", value: "3" });

var formatActivate = function(elCell, oRecord, oColumn, sData) {
    var iActivate = 1;
    if (sData && sData.length > 0) { // not setting
        iActivate = parseInt(sData);
    }
    if (oRecord.getData('Mark') == '1') {
        elCell.innerHTML = createDroplist(iActivate, ar_Activate, oRecord, oColumn);
    } else {
        elCell.innerHTML = '';
    }
};

function onEditDroplistChange(ColumnName, RecordId, Value) {
    var record = dtDeploy.getRecord(RecordId);
    record.setData(ColumnName, Value);
    dtDeploy.render();
}

function createDroplist(SelectedNameService, DroplistArray, oRecord, oColumn) {
    var d = "<select onChange=\"onEditDroplistChange('" + oColumn.getField() + "', '" + oRecord.getId() + "', this.value)\">";
    for (var i = 0; i < DroplistArray.length; i++) {
        d += '<option value="' + DroplistArray[i].value + '" ' + (DroplistArray[i].value == SelectedNameService ? 'selected = "1"' : '') + '>' + DroplistArray[i].label + '</option>';
    }
    d += '</select>';
    return d;
}
function loadPendingDeployments(ElementId, Resize) {
    if (Resize) {
        createDeploymentDataTable(ElementId);
        return;
    }
    var connectionCallback = {
        success: function(o) {
            var xmlDoc = o.responseXML;

            dsDeploy = new YAHOO.util.DataSource(xmlDoc);
            dsDeploy.responseType = YAHOO.util.DataSource.TYPE_XML;

            dsDeploy.responseSchema = {
                resultNode: "RoxieECLWorkunitInfo",
                fields: ["Wuid", "Owner", "Cluster", "Jobname", "StateID", "State", "Protected", "IsPausing"]
            };

            createDeploymentDataTable(ElementId);
            
            checkNotifyRoxieCheck();
        },
        failure: function(o) {
            alert('Failure:' + o.statusText);

        }
    };
    
    clearDeploymentDataTable();
    createDeploymentDataTable(ElementId);
    dtDeploy.showTableMessage(dtDeploy.get("MSG_LOADING"), dtDeploy.CLASS_LOADING);

    var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body><RoxieWUQueryRequest><Cluster></Cluster><TargetClusterType>roxie</TargetClusterType><Owner/><Jobname/></RoxieWUQueryRequest></soap:Body></soap:Envelope>';
    YAHOO.util.Connect.initHeader("SOAPAction", "/ws_roxieconfig/RoxieWUQuery?ver_=" + WS_ROXIECONFIG_VER);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;

    var getXML = YAHOO.util.Connect.asyncRequest("POST",
            "/ws_roxieconfig/RoxieWUQuery",
            connectionCallback, postBody);
            
    return {
    };
}

function clearDeploymentDataTable() {
    if (dtDeploy) {
        dtDeploy.destroy();
        dtDeploy = null;
        dsDeploy = null;
    }
    dsDeploy = new YAHOO.util.DataSource();
}

function createDeploymentDataTable(ElementId) {
    var myColumnDefs = [{ key: "Mark", label: "", formatter: formatInlineEditCheckbox, width: 40 },
                 { key: "Wuid", lable: "Workunit", sortable: true, resizeable: false, width: 300 },
                 { key: "Owner", label: "Owner", sortable: true, resizeable: false, width: 180 },
                 { key: "Jobname", label: "Jobname", editor: new YAHOO.widget.TextboxCellEditor({disableBtns:true}), sortable: true, resizeable: false, width: 120 },
                 { key: "State", label: "State", sortable: true, resizeable: false, width: 150 },
                 { key: "Activation", label: "Activation", formatter: formatActivate, sortable: true, resizeable: false, width: 150 },
                 { key: "Action", label: "Action", formatter: formatProcessAction, sortable: true, resizeable: false, width: 150 },
                 { key: "Protected", label: "Protected", formatter: formatCheckboxDisabled, sortable: true, resizeable: false, width: 100}];

                 //dtDeploy = new YAHOO.widget.ScrollingDataTable(ElementId, myColumnDefs, dsDeploy, { width: "100%", height: "100%" });
                 dtDeploy = new YAHOO.widget.ScrollingDataTable(ElementId, myColumnDefs, dsDeploy, { width: "100%" });

                 var highlightEditableCell = function(oArgs) {
                     var elCell = oArgs.target;
                     if (YAHOO.util.Dom.hasClass(elCell, "yui-dt-editable")) {
                         dtDeploy.highlightCell(elCell);
                     }
                 };
                 
                 dtDeploy.subscribe("cellMouseoverEvent", highlightEditableCell);
                 
                 dtDeploy.subscribe("cellMouseoutEvent", dtDeploy.onEventUnhighlightCell);
                 dtDeploy.subscribe("cellClickEvent", WorkunitCellClickedHandler);

}

function deployWorkunits() {
    var records = dtDeploy.getRecordSet().getRecords();
    for (var i = 0; i < records.length; i++) {
        var mark = records[i].getData('Mark');
        var wuid = records[i].getData('Wuid');
        var jobName = records[i].getData('Jobname');
        var activate = records[i].getData('Activation');
        if (mark == '1') {
            // set ui to show 'deploying' and turn off when the ajax call completes.
            records[i].setData('Action', '1');
            deployWorkunit(wuid, jobName, activate);
        }
    }
    dtDeploy.render();
    
}

function deployWorkunit(Workunit, JobName, Activate) {
    var connectionCallback = {
        success: function(o) {
            var xmlDoc = o.responseXML;
            // find workunit in dtDeploy and set to deployed Action = '2'.
            var records = dtDeploy.getRecordSet().getRecords();
            for (var i = 0; i < records.length; i++) {
                var wuid = records[i].getData('Wuid');
                if (wuid == Workunit) {
                    // set ui to show 'deploying' and turn off when the ajax call completes.
                    records[i].setData('Mark', '');
                    records[i].setData('Action', '2');
                }
            }
            dtDeploy.render();
        },
        failure: function(o) {
            alert('Failure:' + o.statusText);

        }
    };

    var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body><RoxieDeployWorkunit><Wuid>' + Workunit + '</Wuid><QueryName>' + JobName + '</QueryName><Options><Activate>' + Activate + '</Activate><NotifyRoxie>' + notifyRoxie + '</NotifyRoxie></Options></RoxieDeployWorkunit></soap:Body></soap:Envelope>';
    YAHOO.util.Connect.initHeader("SOAPAction", "/ws_roxieconfig/DeployWorkunit?ver_=" + WS_ROXIECONFIG_VER);
    YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
    YAHOO.util.Connect._use_default_post_header = false;

    var getXML = YAHOO.util.Connect.asyncRequest("POST",
            "/ws_roxieconfig/DeployWorkunit",
            connectionCallback, postBody);

    return {};

}

var statusPanel;

function showDeployStatus(RecId) {
    var recordSet = dtDeploy.getRecordSet();
    var record = recordSet.getRecord(RecId);
    if (!record) {
        recordSet = dt_Action.getRecordSet();
        record = recordSet.getRecord(RecId);
    }
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

    statusPanel.setHeader(record.getData('OriginalName') + ' Status');
    statusPanel.setBody('<div style="text-align:left; height:200px; overflow:scroll;">' + record.getData('Status').replace(/\n/gi, '<br />') + '</div>');
    statusPanel.render("statusdiv");
    statusPanel.show()
}