/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-form",
    "dojo/dom-class",
    "dojo/request/iframe",
    "dojo/on",
    "dojo/topic",

    "dijit/registry",
    "dijit/Dialog",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "dgrid/tree",
    "dgrid/editor",
    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "hpcc/FileSpray",
    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/ESPDFUWorkunit",
    "hpcc/DelayLoadWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/TargetComboBoxWidget",
    "hpcc/SelectionGridWidget",
    "hpcc/FilterDropDownWidget",

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
    "dijit/Fieldset",

    "dojox/form/Uploader",
    "dojox/form/uploader/FileList",

    "hpcc/TableContainer"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domForm, domClass, iframe, on, topic,
                registry, Dialog, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                tree, editor, selector,
                _TabContainerWidget, FileSpray, ESPUtil, ESPRequest, ESPDFUWorkunit, DelayLoadWidget, TargetSelectWidget, TargetComboBoxWidget, SelectionGridWidget, FilterDropDownWidget,
                template) {
    return declare("LZBrowseWidget", [_TabContainerWidget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "LZBrowseWidget",
        i18n: nlsHPCC,

        filter: null,
        dropZoneTarget2Select: null,
        serverFilterSelect: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.sprayFixedForm = registry.byId(this.id + "SprayFixedForm");
            this.sprayFixedDestinationSelect = registry.byId(this.id + "SprayFixedDestination");
            this.sprayFixedGrid = registry.byId(this.id + "SprayFixedGrid");
            this.sprayDelimitedForm = registry.byId(this.id + "SprayDelimitedForm");
            this.sprayDelimitedDestinationSelect = registry.byId(this.id + "SprayDelimitedDestination");
            this.sprayDelimitedGrid = registry.byId(this.id + "SprayDelimitedGrid");
            this.sprayXmlForm = registry.byId(this.id + "SprayXmlForm");
            this.sprayXmlDestinationSelect = registry.byId(this.id + "SprayXmlDestinationSelect");
            this.sprayXmlGrid = registry.byId(this.id + "SprayXmlGrid");
            this.sprayJsonForm = registry.byId(this.id + "SprayJsonForm");
            this.sprayJsonDestinationSelect = registry.byId(this.id + "SprayJsonDestinationSelect");
            this.sprayJsonGrid = registry.byId(this.id + "SprayJsonGrid");
            this.sprayVariableForm = registry.byId(this.id + "SprayVariableForm");
            this.sprayVariableDestinationSelect = registry.byId(this.id + "SprayVariableDestination");
            this.sprayVariableGrid = registry.byId(this.id + "SprayVariableGrid");
            this.sprayBlobForm = registry.byId(this.id + "SprayBlobForm");
            this.sprayBlobDestinationSelect = registry.byId(this.id + "SprayBlobDestination");
            this.sprayBlobGrid = registry.byId(this.id + "SprayBlobGrid");
            this.landingZonesTab = registry.byId(this.id + "_LandingZones");
            this.uploader = registry.byId(this.id + "Upload");
            this.uploadFileList = registry.byId(this.id + "UploadFileList");
            this.dropZoneTargetSelect = registry.byId(this.id + "DropZoneTargetSelect");
            this.dropZoneMachineSelect = registry.byId(this.id + "DropZoneMachineSelect");
            this.dropZoneFolderSelect = registry.byId(this.id + "DropZoneFolderSelect");
            this.dfuSprayFixedQueues = registry.byId(this.id + "SprayFixedDFUSprayQueues");
            this.dfuSprayDelimitedQueues = registry.byId(this.id + "SprayDelimitedDFUQueues");
            this.dfuSprayXMLQueues = registry.byId(this.id + "SprayXMLDFUQueues");
            this.dfuSprayJSONQueues = registry.byId(this.id + "SprayJSONDFUQueues");
            this.dfuSprayVariableQueues = registry.byId(this.id + "SprayVariableDFUQueues");
            this.dfuSprayBLOBQueues = registry.byId(this.id + "SprayBLOBDFUQueues");
            this.fileListDialog = registry.byId(this.id + "FileListDialog");
            this.overwriteCheckbox = registry.byId(this.id + "FileOverwriteCheckbox");
            this.fixedSprayReplicateCheckbox = registry.byId(this.id + "FixedSprayReplicate");
            this.delimitedSprayReplicateCheckbox = registry.byId(this.id + "DelimitedSprayReplicate");
            this.xmlSprayReplicateCheckbox = registry.byId(this.id + "XMLSprayReplicate");
            this.sprayXMLButton = registry.byId(this.id + "SprayXMLButton");
            this.variableSprayReplicateCheckbox = registry.byId(this.id + "VariableSprayReplicate");
            this.blobSprayReplicateCheckbox = registry.byId(this.id + "BlobSprayReplicate");
            this.filter = registry.byId(this.id + "Filter");
            this.dropZoneTarget2Select = registry.byId(this.id + "DropZoneName2");
            this.serverFilterSelect = registry.byId(this.id + "ServerFilter");

            var context = this;
            this.connect(this.uploader, "onComplete", function (response) {
                if (lang.exists("Exceptions.Source", response)) {
                    topic.publish("hpcc/brToaster", {
                        Severity: "Error",
                        Source: "FileSpray.UploadFile",
                        Exceptions: response.Exceptions.Exception
                    });
                }
                context.fileListDialog.hide();
                context.refreshGrid();
            });

            this.connect(this.uploader, "onError", function (response) {
                if (response.type === 'error') {
                    topic.publish("hpcc/brToaster", {
                        Severity: "Error",
                        Source: "FileSpray.UploadFile",
                        Exceptions: [{ Message: this.i18n.ErrorUploadingFile }]
                    });
                    this.uploader.reset();
                }
            });

            this.dropZoneTarget2Select.on("change", function (evt) {
                if (evt) {
                    context.serverFilterSelect.loadDropZoneMachines(evt);
                }
            });
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_LZBrowse;
        },

        _handleResponse: function (wuidQualifier, response) {
            if (lang.exists(wuidQualifier, response)) {
                var wu = ESPDFUWorkunit.Get(lang.getObject(wuidQualifier, false, response));
                wu.startMonitor(true);
                var tab = this.ensurePane("dfu", wu.ID, wu.ID, {
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
            var context = this;
            var targetRow;
            if (!this.dropZoneTargetSelect.initalized) {
                this.dropZoneTargetSelect.init({
                    DropZones: true,
                    callback: function (value, row) {
                        if (context.dropZoneMachineSelect) {
                            context.dropZoneMachineSelect.defaultValue = context.dropZoneMachineSelect.get("value");
                            context.dropZoneMachineSelect.loadDropZoneMachines(value);
                            targetRow = row;
                        }
                    }
                });
            }

            if (!this.dropZoneMachineSelect.initalized) {
                this.dropZoneMachineSelect.init({
                    DropZoneMachines: true,
                    callback: function (value, row) {
                        targetRow.machine.Name = value
                        targetRow.machine.Netaddress = value
                        if (context.dropZoneFolderSelect) {
                            context.dropZoneFolderSelect._dropZoneTarget = targetRow;
                            context.dropZoneFolderSelect.defaultValue = "/"
                            context.dropZoneFolderSelect.loadDropZoneFolders();
                        }
                    }
                 });
            }

            var fileList = registry.byId(this.id + "Upload").getFileList();
            var totalFileSize = 0;

            this.uploadFileList.hideProgress();
            this.fileListDialog.show();

            arrayUtil.forEach(fileList, function (file,idx){
                totalFileSize += file.size;
            });

            if (totalFileSize >= 2147483648 ) {
                domClass.remove("BrowserSizeMessage", "hidden");
            } else {
                domClass.add("BrowserSizeMessage", "hidden");
            }
        },

        _onUploadBegin: function (dataArray) {
            this.fileListDialog.hide();
            this.uploadString = this.i18n.FileUploadStillInProgress + ":";
            arrayUtil.forEach(dataArray, function (item, idx) {
                this.uploadString += "\n" + item.name;
            }, this);
        },

        _onCheckUploadSubmit: function () {
            var context = this;
            var fileList = registry.byId(this.id + "Upload").getFileList();
            var list = this.arrayToList(fileList, "name");
            if (this.overwriteCheckbox.checked) {
                this._onUploadSubmit();
                this.fileListDialog.hide();
            } else {
                var target = context.dropZoneTargetSelect.get("row");
                FileSpray.FileList({
                    request: {
                        Netaddr: target.machine.Netaddress,
                        Path: context.getUploadPath()
                    }
                }).then(function (response) {
                    var fileName = "";
                    if (lang.exists("FileListResponse.files.PhysicalFileStruct", response)) {
                        arrayUtil.forEach(response.FileListResponse.files.PhysicalFileStruct, function (item, index) {
                            arrayUtil.forEach(fileList, function (file,idx){
                                if (item.name === file.name){
                                    fileName = file.name;
                                }
                            });
                        });
                    }
                    if (fileName === ""){
                        context._onUploadSubmit();
                        context.fileListDialog.hide();
                    } else {
                        alert(context.i18n.OverwriteMessage + "\n" + list );
                    }
                });
            }
        },

        _onUploadProgress: function (progress) {
            if (progress.decimal < 1) {
                this.widget.Upload.set("label", this.i18n.Upload + " " + progress.percent);
                var context = this;
                window.onbeforeunload = function (e) {
                    return context.uploadString;
                };
            } else {
                this.widget.Upload.set("label", this.i18n.Upload);
                window.onbeforeunload = null;
            }
        },

        getUploadPath: function () {
            return this.dropZoneFolderSelect.getDropZoneFolder();
        },

        _onUploadSubmit: function (event) {
            var target = this.dropZoneTargetSelect.get("row");
            this.uploader.set("uploadUrl", "/FileSpray/UploadFile.json?upload_&rawxml_=1&NetAddress=" + target.machine.Netaddress + "&OS=" + target.machine.OS + "&Path=" + this.getUploadPath());
            this.uploader.upload();
        },

        _onUploadCancel: function (event) {
            this.fileListDialog.hide();
        },

        _onDownload: function (event) {
            var context = this;
            arrayUtil.forEach(this.landingZonesGrid.getSelected(), function (item, idx) {
                var downloadIframeName = "downloadIframe_" + item.calculatedID;
                var frame = iframe.create(downloadIframeName);
                var url = ESPRequest.getBaseURL("FileSpray") + "/DownloadFile?Name=" + encodeURIComponent(item.name) + "&NetAddress=" + item.NetAddress + "&Path=" + encodeURIComponent(item.fullFolderPath) + "&OS=" + item.OS;
                iframe.setSrc(frame, url, true);
            });
        },

        _onDelete: function (event) {
            var selection = this.landingZonesGrid.getSelected();
            var list = this.arrayToList(selection, "displayName");
            if (confirm(this.i18n.DeleteSelectedFiles + "\n" + list)) {
                var context = this;
                arrayUtil.forEach(selection, function (item, idx) {
                    FileSpray.DeleteDropZoneFile({
                        request:{
                            NetAddress: item.NetAddress,
                            Path: item.fullFolderPath,
                            OS: item.OS,
                            Names: item.displayName
                        },
                        load: function (response) {
                            context.refreshGrid(true);
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
                var tab = context.ensurePane("hex", item.calculatedID, item.displayName, {
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

        _spraySelectedOneAtATime: function (dropDownID, formID, doSpray) {
            if (registry.byId(this.id + formID).validate()) {
                var selections = this.landingZonesGrid.getSelected();
                var context = this;
                arrayUtil.forEach(selections, function (item, idx) {
                    var request = domForm.toObject(context.id + formID);
                    lang.mixin(request, {
                        sourceIP: item.NetAddress,
                        sourcePath: item.fullPath,
                        destLogicalName: request.namePrefix + (request.namePrefix && !context.endsWith(request.namePrefix, "::") && item.targetName && !context.startsWith(item.targetName, "::") ? "::" : "") + item.targetName
                    });
                    doSpray(request, item);
                });
                registry.byId(this.id + dropDownID).closeDropDown();
            }
        },

        _spraySelected: function (dropDownID, formID, doSpray) {
            if (registry.byId(this.id + formID).validate()) {
                var selections = this.landingZonesGrid.getSelected();
                if (selections.length) {
                    var request = domForm.toObject(this.id + formID);
                    var item = selections[0];
                    lang.mixin(request, {
                        sourceIP: selections[0].DropZone.NetAddress,
                        nosplit: true
                    });
                    var sourcePath = "";
                    arrayUtil.forEach(selections, function (item, idx) {
                        if (sourcePath.length)
                            sourcePath += ",";
                        sourcePath += item.fullPath;
                    });
                    lang.mixin(request, {
                        sourcePath: sourcePath
                    });
                    doSpray(request, item);
                    registry.byId(this.id + dropDownID).closeDropDown();
                }
            }
        },

        _onAddFile: function (event) {
            if (registry.byId(this.id + "AddFileForm").validate()) {
                var tmpFile = domForm.toObject(this.id + "AddFileForm");
                var dropZone = lang.mixin(this.landingZoneStore.get(tmpFile.NetAddress), {
                    NetAddress: tmpFile.NetAddress
                });
                var fullPathParts = tmpFile.fullPath.split("/");
                if (fullPathParts.length === 1) {
                    fullPathParts = tmpFile.fullPath.split("\\");
                }
                var file = lang.mixin(this.landingZoneStore.get(tmpFile.NetAddress + tmpFile.fullPath), {
                    displayName: fullPathParts[fullPathParts.length - 1],
                    fullPath: tmpFile.fullPath,
                    isDir: false,
                    DropZone: dropZone
                });
                this.landingZoneStore.addUserFile(file);
                this.refreshGrid();
                registry.byId(this.id + "AddFileDropDown").closeDropDown();
            }
        },

        _onSprayFixed: function (event) {
            var context = this;
            this._spraySelectedOneAtATime("SprayFixedDropDown", "SprayFixedForm", function (request, item) {
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

        _onSprayDelimited: function(event) {
            var context = this;
            this._spraySelectedOneAtATime("SprayDelimitedDropDown", "SprayDelimitedForm", function (request, item) {
                FileSpray.SprayVariable({
                    request: request
                }).then(function (response) {
                    context._handleResponse("SprayResponse.wuid", response);
                });
            });
        },

        _onSprayXml: function(event) {
            var context = this;
                this._spraySelectedOneAtATime("SprayXmlDropDown", "SprayXmlForm", function (request, item) {
                    lang.mixin(request, {
                        sourceRowTag: item.targetRowTag
                    });
                    FileSpray.SprayVariable({
                        request: request
                    }).then(function (response) {
                        context._handleResponse("SprayResponse.wuid", response);
                    });
                });
        },

        _onSprayJson: function(event) {
            var context = this;
            this._spraySelectedOneAtATime("SprayJsonDropDown", "SprayJsonForm", function (request, item) {
                lang.mixin(request, {
                    sourceRowPath: item.targetRowPath,
                    isJSON: true
                });
                FileSpray.SprayVariable({
                    request: request
                }).then(function (response) {
                    context._handleResponse("SprayResponse.wuid", response);
                });
            });
        },

        _onSprayVariable: function (event) {
            var context = this;
            this._spraySelectedOneAtATime("SprayVariableDropDown", "SprayVariableForm", function (request, item) {
                FileSpray.SprayFixed({
                    request: request
                }).then(function (response) {
                    context._handleResponse("SprayFixedResponse.wuid", response);
                });
            });
        },

        _onSprayBlob: function (event) {
            var context = this;
            this._spraySelected("SprayBlobDropDown", "SprayBlobForm", function (request, item) {
                FileSpray.SprayFixed({
                    request: request
                }).then(function (response) {
                    context._handleResponse("SprayFixedResponse.wuid", response);
                });
            });
        },

        _onRowContextMenu: function (item, colField, mystring) {
        },

        //  Implementation  ---
        getFilter: function () {
            var retVal = this.filter.toObject();
            var dropZoneInfo = arrayUtil.filter(this.dropZoneTarget2Select.options, function (option) {
                return option.selected === true;
            });
            var dropZoneMachineInfo = arrayUtil.filter(this.serverFilterSelect.options, function (option) {
                return option.selected === true;
            });
            if (dropZoneInfo.length && dropZoneMachineInfo.length) {
                retVal.__dropZone = dropZoneInfo[0];
                retVal.__dropZoneMachine = dropZoneMachineInfo[0];
            }
            return retVal;
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;
            var context = this;

            this.initLandingZonesGrid();
            this.serverFilterSelect.init({
                DropZoneMachines: true,
                includeBlank: true
            });
            this.dropZoneTarget2Select.init({
                DropZones: true,
                includeBlank: true
            });
            this.filter.on("clear", function (evt) {
                context.refreshGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshGrid();
            });
            this.sprayFixedDestinationSelect.init({
                SprayTargets: true
            });
            this.dfuSprayFixedQueues.init({
                DFUSprayQueues: true
            });
            this.dfuSprayDelimitedQueues.init({
                DFUSprayQueues: true
            });
            this.dfuSprayXMLQueues.init({
                DFUSprayQueues: true
            });
            this.dfuSprayJSONQueues.init({
                DFUSprayQueues: true
            });
            this.dfuSprayVariableQueues.init({
                DFUSprayQueues: true
            });
            this.dfuSprayBLOBQueues.init({
                DFUSprayQueues: true
            });
            this.sprayDelimitedDestinationSelect.init({
                SprayTargets: true
            });
            this.sprayXmlDestinationSelect.init({
                SprayTargets: true
            });
            this.sprayJsonDestinationSelect.init({
                SprayTargets: true
            });
            this.sprayVariableDestinationSelect.init({
                SprayTargets: true
            });
            this.sprayBlobDestinationSelect.init({
                SprayTargets: true
            });
            var context = this;
            this.dropZoneFolderSelect.init({
                DropZoneFolders: true,
                includeBlank: true
            });

            this.sprayFixedDestinationSelect.on('change', function (value){
                context.checkReplicate(value, context.fixedSprayReplicateCheckbox);
            });

            this.sprayDelimitedDestinationSelect.on('change', function (value){
                context.checkReplicate(value, context.delimitedSprayReplicateCheckbox);
            });

            this.sprayXmlDestinationSelect.on('change', function (value){
                context.checkReplicate(value, context.xmlSprayReplicateCheckbox);
            });

            this.sprayVariableDestinationSelect.on('change', function (value){
                context.checkReplicate(value, context.variableSprayReplicateCheckbox);
            });

            this.sprayBlobDestinationSelect.on('change', function (value){
                context.checkReplicate(value, context.blobSprayReplicateCheckbox);
            });
        },

        checkReplicate: function (value, checkBoxValue) {
            FileSpray.GetSprayTargets({
                request: {}
            }).then(function (response) {
                if (lang.exists("GetSprayTargetsResponse.GroupNodes.GroupNode", response)) {
                    var arr = response.GetSprayTargetsResponse.GroupNodes.GroupNode;
                    for (var index in arr) {
                        if (arr[index].Name === value && arr[index].ReplicateOutputs === true) {
                            checkBoxValue.set("disabled", false);
                            break;
                        } else if (arr[index].Name === value) {
                            checkBoxValue.set("disabled", true);
                            break;
                        }
                    }
                }
            });
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.landingZonesTab.id) {
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
            var context = this;
            this.landingZoneStore = new FileSpray.CreateLandingZonesStore();
            this.landingZonesGrid = new declare([ESPUtil.Grid(false, true)])({
                store: this.landingZoneStore,
                query: {
                    id: "*",
                    filter: this.filter.exists() ? this.getFilter() : null
                },
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox',
                        disabled: function (item) {
                            if (item.type) {
                                switch (item.type) {
                                    case "dropzone":
                                    case "folder":
                                    case "machine":
                                        return true;
                                }
                            }
                            return false;
                        },
                        sortable: false
                    }),
                    displayName: tree({
                        label: this.i18n.Name,
                        sortable: false,
                        formatter: function (_name, row) {
                            var img = "";
                            var name = _name;
                            if (row.isDir === undefined) {
                                img = dojoConfig.getImageHTML("server.png");
                                name += " [" + row.Path + "]";
                            } else if (row.isMachine) {
                                 img = dojoConfig.getImageHTML("machine.png");
                            } else if (row.isDir) {
                                img = dojoConfig.getImageHTML("folder.png");
                            } else {
                                img = dojoConfig.getImageHTML("file.png");
                            }
                            return img + "&nbsp;" + name;
                        }
                    }),
                    filesize: { label: this.i18n.Size, width: 108, sortable: false,
                        formatter: function (fsize, row) {
                            if (!fsize || fsize === -1) {
                                return ""
                            }
                            return fsize.toLocaleString();
                        }
                    },
                    modifiedtime: { label: this.i18n.Date, width: 180, sortable: false }
                },
                getSelected: function () {
                    if (context.filter.exists()) {
                        return this.inherited(arguments, [FileSpray.CreateLandingZonesFilterStore()]);
                    }
                    return this.inherited(arguments, [FileSpray.CreateFileListStore()]);
                }
            }, this.id + "LandingZonesGrid");

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
            this.landingZonesGrid.startup();

            this.sprayFixedGrid.createGrid({
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
            });

            this.sprayDelimitedGrid.createGrid({
                idProperty: "calculatedID",
                columns: {
                    targetName: editor({
                        label: this.i18n.TargetName,
                        width: 144,
                        autoSave: true,
                        editor: "text"
                    })
                }
            });

            this.sprayXmlGrid.createGrid({
                idProperty: "calculatedID",
                columns: {
                    targetName: editor({
                        label: this.i18n.TargetName,
                        width: 120,
                        autoSave: true,
                        editor: "text"
                    }),
                    targetRowTag: editor({
                        label: this.i18n.RowTag,
                        width:100,
                        autoSave: true,
                        editor: dijit.form.ValidationTextBox,
                        editorArgs: {
                            required: true,
                            placeholder: this.i18n.RequiredForXML,
                            promptMessage: this.i18n.RequiredForXML,
                            validator: function(value, constraints) {
                                var valid = true;
                                if (value !== null && value !== undefined && value !== "") {
                                    valid = true;
                                    context.sprayXMLButton.set("disabled", false);
                                } else {
                                    context.sprayXMLButton.set("disabled", true);
                                    valid = false;
                                }
                                return valid;
                            }
                        }
                    })
                }
            });

            this.sprayJsonGrid.createGrid({
                idProperty: "calculatedID",
                columns: {
                    targetName: editor({
                        label: this.i18n.TargetName,
                        width: 144,
                        autoSave: true,
                        editor: "text"
                    }),
                    targetRowPath: editor({
                        label: this.i18n.RowPath,
                        width: 72,
                        autoSave: true,
                        editor: "text"
                    })
                }
            });

            this.sprayVariableGrid.createGrid({
                idProperty: "calculatedID",
                columns: {
                    targetName: editor({
                        label: this.i18n.TargetName,
                        width: 144,
                        autoSave: true,
                        editor: "text"
                    })
                }
            });

            this.sprayBlobGrid.createGrid({
                idProperty: "calculatedID",
                columns: {
                    fullPath: editor({
                        label: this.i18n.SourcePath,
                        width: 144,
                        autoSave: true,
                        editor: "text"
                    })
                }
            });

            this.refreshActionState();
        },

        refreshGrid: function (clearSelection) {
            this.landingZonesGrid.set("query", {
                id: "*",
                filter: this.filter.exists() ? this.getFilter() : null
            });
            if (clearSelection) {
                this.landingZonesGrid.clearSelection();
            }
        },

        refreshActionState: function () {
            var selection = this.landingZonesGrid.getSelected();
            var hasSelection = selection.length;
            registry.byId(this.id + "HexPreview").set("disabled", !hasSelection);
            registry.byId(this.id + "Download").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "SprayFixedDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "SprayDelimitedDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "SprayXmlDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "SprayJsonDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "SprayVariableDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "SprayBlobDropDown").set("disabled", !hasSelection);

            if (hasSelection) {
                var context = this;
                var data = [];
                arrayUtil.forEach(selection, function (item, idx) {
                    lang.mixin(item, lang.mixin({
                        targetName: item.displayName,
                        targetRecordLength: "",
                        targetRowTag: "Row",
                        targetRowPath: "/"
                    }, item));
                    data.push(item);
                });
                this.sprayFixedGrid.setData(data);
                this.sprayDelimitedGrid.setData(data);
                this.sprayXmlGrid.setData(data);
                this.sprayJsonGrid.setData(data);
                this.sprayVariableGrid.setData(data);
                this.sprayBlobGrid.setData(data);
            }
        },

        ensurePane: function (type, id, title, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                switch (type) {
                    case "hex":
                        retVal = new DelayLoadWidget({
                            id: id,
                            title: title,
                            closable: true,
                            delayWidget: "HexViewWidget",
                            params: params
                        });
                        break;
                    case "dfu":
                        retVal = new DelayLoadWidget({
                            id: id,
                            title: title,
                            closable: true,
                            delayWidget: "DFUWUDetailsWidget",
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