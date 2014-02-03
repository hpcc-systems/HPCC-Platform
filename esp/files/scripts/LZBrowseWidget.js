/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/common",
    "dojo/i18n!./nls/LZBrowseWidget",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/request/iframe",
    "dojo/date",
    "dojo/on",

    "dijit/registry",
    "dijit/Dialog",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "dgrid/OnDemandGrid",
    "dgrid/tree",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/editor",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/_TabContainerWidget",
    "hpcc/FileSpray",
    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/ESPDFUWorkunit",
    "hpcc/HexViewWidget",
    "hpcc/DFUWUDetailsWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/SelectionGridWidget",

    "dojo/text!../templates/LZBrowseWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Form",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/RadioButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/form/DropDownButton",

    "dojox/form/Uploader",
    "dojox/form/uploader/FileList",

    "hpcc/TableContainer"
], function (declare, lang, i18n, nlsCommon, nlsSpecific, arrayUtil, dom, domAttr, domClass, domForm, iframe, date, on,
                registry, Dialog, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                OnDemandGrid, tree, Keyboard, Selection, editor, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, FileSpray, ESPUtil, ESPRequest, ESPDFUWorkunit, HexViewWidget, DFUWUDetailsWidget, TargetSelectWidget, SelectionGridWidget,
                template) {
    return declare("LZBrowseWidget", [_TabContainerWidget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "LZBrowseWidget",
        i18n: lang.mixin(nlsCommon, nlsSpecific),

        sprayFixedForm: null,
        sprayVariableForm: null,
        sprayXmlForm: null,

        landingZonesTab: null,
        landingZonesGrid: null,

        tabMap: [],

        validateDialog: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.sprayFixedForm = registry.byId(this.id + "SprayFixedForm");
            this.sprayVariableForm = registry.byId(this.id + "SprayVariableForm");
            this.sprayXmlForm = registry.byId(this.id + "SprayXmlForm");
            this.sprayRECFMForm = registry.byId(this.id + "SprayRECFMForm");
            this.landingZonesTab = registry.byId(this.id + "_LandingZones");
            this.uploader = registry.byId(this.id + "Upload");
            this.uploadFileList = registry.byId(this.id + "UploadFileList");
            this.spraySourceSelect = registry.byId(this.id + "SpraySourceSelect");
            this.sprayFixedDestinationSelect = registry.byId(this.id + "SprayFixedDestination");
            this.sprayVariableDestinationSelect = registry.byId(this.id + "SprayVariableDestination");
            this.sprayXmlDestinationSelect = registry.byId(this.id + "SprayXmlDestinationSelect");
            this.sprayRECFMDestinationSelect = registry.byId(this.id + "SprayRECFMDestination");
            this.dropZoneSelect = registry.byId(this.id + "DropZoneTargetSelect");
            this.fileListDialog = registry.byId(this.id + "FileListDialog");

            var context = this;
            this.connect(this.uploader, "onComplete", function () {
                context.fileListDialog.hide();
                context.refreshGrid();
            });
            //  Workaround for HPCC-9414  --->
            this.connect(this.uploader, "onError", function (msg, e) {
                if (msg === context.i18n.Errorparsingserverresult + ":") {   
                    context.fileListDialog.hide();
                    context.refreshGrid();
                }
            });
            //  <---  Workaround for HPCC-9414
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title;
        },

        _handleResponse: function (wuidQualifier, response) {
            if (lang.exists(wuidQualifier, response)) {
                var wu = ESPDFUWorkunit.Get(lang.getObject(wuidQualifier, false, response));
                wu.startMonitor(true);
                var tab = this.ensurePane("dfu", this.id + "_" + wu.ID, wu.ID, {
                    Wuid: wu.ID
                });
                if (tab) {
                    this.selectChild(tab);
                }
            }
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _onUpload: function (event) {
            this.uploadFileList.hideProgress();
            this.fileListDialog.show();
        },

        _onDownload: function (event) {
            var context = this;
            arrayUtil.forEach(this.landingZonesGrid.getSelected(), function (item, idx) {
                var downloadIframeName = "downloadIframe_" + item.calculatedID;
                var frame = iframe.create(downloadIframeName);
                var url = ESPRequest.getBaseURL("FileSpray") + "/DownloadFile?Name=" + encodeURIComponent(item.partialPath) + "&NetAddress=" + item.DropZone.NetAddress + "&Path=" + encodeURIComponent(item.DropZone.Path) + "&OS=" + item.DropZone.OS;
                iframe.setSrc(frame, url, true);
            });
        },

        _onDelete: function (event) {
            if (confirm(this.i18n.DeleteSelectedFiles)) {
                var context = this;
                arrayUtil.forEach(this.landingZonesGrid.getSelected(), function(item, idx) {
                    FileSpray.DeleteDropZoneFile({
                        request:{
                            NetAddress:	item.DropZone.NetAddress,
                            Path: item.DropZone.Path,
                            OS: item.DropZone.OS,
                            Names: item.partialPath
                        },
                        load: function (response) {
                            context.refreshGrid();
                        }
                    });
                });
            }
        },

        _onHexPreview: function (event) {
            var selections = this.landingZonesGrid.getSelected();
            var firstTab = null;
            var context = this;
            arrayUtil.forEach(selections, function (item, idx) {
                var tab = context.ensurePane("hex", context.id + "_" + item.calculatedID, item.displayName, {
                    logicalFile: item.getLogicalFile()
                });
                if (firstTab === null) {
                    firstTab = tab;
                }
            });
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onUploadSubmit: function (event) {
            var item = this.dropZoneSelect.get("row");
            this.uploader.set("uploadUrl", "/FileSpray/UploadFile.json?upload_&rawxml_=1&NetAddress=" + item.machine.Netaddress + "&OS=" + item.machine.OS + "&Path=" + item.machine.Directory);
            this.uploader.upload();
        },

        _onUploadCancel: function (event) {
            registry.byId(this.id + "FileListDialog").hide();
        },

        _spraySelected: function (dropDownID, formID, doSpray) {
            if (registry.byId(this.id + formID).validate()) {
                var selections = this.landingZonesGrid.getSelected();
                var context = this;
                arrayUtil.forEach(selections, function (item, idx) {
                    var request = domForm.toObject(context.id + formID);
                    if (request.namePrefix && !context.endsWith(request.namePrefix, "::")) {
                        request.namePrefix += "::";
                    }
                    lang.mixin(request, {
                        sourceIP: item.DropZone.NetAddress,
                        sourcePath: item.fullPath,
                        destLogicalName: request.namePrefix + item.targetName
                    });
                    doSpray(request, item);
                });
                registry.byId(this.id + dropDownID).closeDropDown();
            }
        },

        _onSprayFixed: function (event) {
            var context = this;
            this._spraySelected("SprayFixedDropDown", "SprayFixedForm", function (request, item) {
                lang.mixin(request, {
                    sourceRecordSize: item.targetRecordLength
                });
                FileSpray.SprayFixed({
                    request: request
                }).then(function (response) {
                    context._handleResponse("SprayFixedResponse.wuid", response);
                });
            });
        },

        _onSprayVariable: function(event) {
            var context = this;
            this._spraySelected("SprayVariableDropDown", "SprayVariableForm", function (request, item) {
                FileSpray.SprayVariable({
                    request: request
                }).then(function (response) {
                    context._handleResponse("SprayResponse.wuid", response);
                });
            });
        },

        _onSprayXml: function(event) {
            var context = this;
            this._spraySelected("SprayXmlDropDown", "SprayXmlForm", function (request, item) {
                FileSpray.SprayVariable({
                    request: request
                }).then(function (response) {
                    context._handleResponse("SprayResponse.wuid", response);
                });
            });
        },

        _onSprayRECFM: function (event) {
            var context = this;
            this._spraySelected("SprayRECFMDropDown", "SprayRECFMForm", function (request, item) {
                FileSpray.SprayFixed({
                    request: request
                }).then(function (response) {
                    context._handleResponse("SprayResponse.wuid", response);
                });
            });
            if (this.sprayRECFMForm.validate()) {
                var selections = this.landingZonesGrid.getSelected();
                var context = this;
                arrayUtil.forEach(selections, function (item, idx) {
                    var request = domForm.toObject(context.id + "SprayRECFMForm");
                    if (request.namePrefix && !context.endsWith(request.namePrefix, "::")) {
                        request.namePrefix += "::";
                    }
                    lang.mixin(request, {
                        sourceIP: item.DropZone.NetAddress,
                        sourcePath: item.DropZone.fullPath,
                        destLogicalName: request.namePrefix + item.targetName
                    });
                    FileSpray.SprayFixed({
                        request: request
                    }).then(function (response) {
                        context._handleResponse("SprayFixedResponse.wuid", response);
                    });
                });
                registry.byId(this.id + "SprayRECFMDropDown").closeDropDown();
            }
        },

        _onRowContextMenu: function (item, colField, mystring) {
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.initLandingZonesGrid();
            this.selectChild(this.landingZonesTab, true);
            this.sprayFixedDestinationSelect.init({
                Groups: true
            });
            this.sprayVariableDestinationSelect.init({
                Groups: true
            });
            this.sprayXmlDestinationSelect.init({
                Groups: true
            });
            this.sprayRECFMDestinationSelect.init({
                Groups: true
            });
            this.dropZoneSelect.init({
                DropZones: true
            });
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id == this.landingZonesTab.id) {
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel.params);
                    }
                }
            }
        },

        addMenuItem: function (menu, details) {
            var menuItem = new MenuItem(details);
            menu.addChild(menuItem);
            return menuItem;
        },

        initLandingZonesGrid: function () {
            var store = new FileSpray.CreateLandingZonesStore();
            this.landingZonesGrid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox',
                        disabled: function (item) {
                            if (item.type) {
                                switch (item.type) {
                                    case "dropzone":
                                    case "folder":
                                        return true;
                                }
                            }
                            return false;
                        },
                        sortable: false
                    }),
                    displayName: tree({
                        label: this.i18n.Name,
                        collapseOnRefresh: true,
                        sortable: false,
                        formatter: function (name, row) {
                            var img = "../files/img/";
                            if (row.isDir === undefined) {
                                img += "server.png";
                            } else if (row.isDir) {
                                img += "folder.png";
                            } else {
                                img += "file.png";
                            }
                            return "<img src='" + img + "'/>&nbsp;" + name;
                        }
                    }),
                    filesize: { label: this.i18n.Size, width: 108, sortable: false },
                    modifiedtime: { label: this.i18n.Date, width: 180, sortable: false }
                },
                getSelected: function () {
                    var retVal = [];
                    var store = FileSpray.CreateFileListStore();
                    for (var key in this.selection) {
                        retVal.push(store.get(key));
                    }
                    return retVal;
                }
            }, this.id + "LandingZonesGrid");
            this.landingZonesGrid.set("noDataMessage", "<span class='dojoxGridNoData'>" + this.i18n.noDataMessage + "</span>");

            var context = this;
            this.landingZonesGrid.on(".dgrid-row:contextmenu", function (evt) {
                if (context._onRowContextMenu) {
                    var item = context.landingZonesGrid.row(evt).data;
                    var cell = context.landingZonesGrid.cell(evt);
                    var colField = cell.column.field;
                    var mystring = "item." + colField;
                    context._onRowContextMenu(item, colField, mystring);
                }
            });
            this.landingZonesGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
            });
            this.landingZonesGrid.onContentChanged(function (object, removedFrom, insertedInto) {
                context.refreshActionState();
            });
            this.landingZonesGrid.startup();

            this.sprayFixedGrid = new SelectionGridWidget({
                idProperty: "calculatedID",
                columns: {
                    targetName: editor({
                        label: this.i18n.TargetName,
                        width: 144,
                        autoSave: true,
                        editor: "text"
                    }),
                    targetRecordLength: editor({
                        label: this.i18n.RecordLength,
                        width: 72,
                        autoSave: true,
                        editor: "text"
                    })
                }
            }, this.id + "SprayFixedGrid");

            this.sprayVariableGrid = new SelectionGridWidget({
                idProperty: "calculatedID",
                columns: {
                    targetName: editor({
                        label: this.i18n.TargetName,
                        width: 144,
                        autoSave: true,
                        editor: "text"
                    })
                }
            }, this.id + "SprayVariableGrid");

            this.sprayXMLGrid = new SelectionGridWidget({
                idProperty: "calculatedID",
                columns: {
                    targetName: editor({
                        label: this.i18n.TargetName,
                        width: 144,
                        autoSave: true,
                        editor: "text"
                    })
                }
            }, this.id + "SprayXMLGrid");

            this.sprayRECFMGrid = new SelectionGridWidget({
                idProperty: "calculatedID",
                columns: {
                    targetName: editor({
                        label: this.i18n.TargetName,
                        width: 144,
                        autoSave: true,
                        editor: "text"
                    })
                }
            }, this.id + "SprayRECFMGrid");

            this.refreshActionState();
        },

        refreshGrid: function (args) {
            this.landingZonesGrid.set("query", {
                id: "*"
            });
        },

        refreshActionState: function () {
            var selection = this.landingZonesGrid.getSelected();
            var hasSelection = selection.length;
            registry.byId(this.id + "HexPreview").set("disabled", !hasSelection);
            registry.byId(this.id + "Download").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "SprayFixedDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "SprayVariableDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "SprayXmlDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "SprayRECFMDropDown").set("disabled", !hasSelection);

            if (hasSelection) {
                var context = this;
                var data = [];
                arrayUtil.forEach(selection, function (item, idx) {
                    lang.mixin(item, {
                        targetName: item.displayName,
                        targetRecordLength: 1
                    });
                    data.push(item);
                });
                this.sprayFixedGrid.setData(data);
                this.sprayVariableGrid.setData(data);
                this.sprayXMLGrid.setData(data);
                this.sprayRECFMGrid.setData(data);
            }
        },

        ensurePane: function (type, id, title, params) {
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                switch (type) {
                    case "hex":
                        retVal = new HexViewWidget({
                            id: id,
                            title: title,
                            closable: true,
                            params: params
                        });
                        break;
                    case "dfu":
                        retVal = new DFUWUDetailsWidget.fixCircularDependency({
                            id: id,
                            title: title,
                            closable: true,
                            params: params
                        });
                        break;
                }
                if (retVal) {
                    this.addChild(retVal);
                }
            }
            return retVal;
        }

    });
});
