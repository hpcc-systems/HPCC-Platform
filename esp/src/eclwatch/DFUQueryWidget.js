define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/topic",

    "dijit/registry",
    "dijit/Dialog",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "dgrid/editor",
    "dgrid/selector",
    "dgrid/tree",

    "hpcc/_TabContainerWidget",
    "src/WsDfu",
    "src/FileSpray",
    "src/ESPUtil",
    "src/ESPLogicalFile",
    "src/ESPDFUWorkunit",
    "hpcc/DelayLoadWidget",
    "src/WsTopology",
    "src/Utility",

    "put-selector/put",

    "dojo/text!../templates/DFUQueryWidget.html",

    "hpcc/TargetSelectWidget",
    "hpcc/TargetComboBoxWidget",
    "hpcc/FilterDropDownWidget",
    "hpcc/SelectionGridWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Form",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/DropDownButton",
    "dijit/form/Select",
    "dijit/form/CheckBox",
    "dijit/form/NumberTextBox",
    "dijit/form/RadioButton",
    "dijit/form/ValidationTextBox",
    "dijit/Dialog",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/Fieldset",

    "hpcc/TableContainer"

], function (declare, lang, nlsHPCCMod, arrayUtil, domClass, domForm, topic,
    registry, Dialog, Menu, MenuItem, MenuSeparator, PopupMenuItem,
    editor, selector, tree,
    _TabContainerWidget, WsDfu, FileSpray, ESPUtil, ESPLogicalFile, ESPDFUWorkunit, DelayLoadWidget, WsTopology, Utility,
    put,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("DFUQueryWidget", [_TabContainerWidget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "DFUQueryWidget",
        i18n: nlsHPCC,
        pathSepCharG: "/",
        updatedFilter: null,
        username: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.filter = registry.byId(this.id + "Filter");
            this.iconFilter = registry.byId(this.id + "IconFilter");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
            this.importForm = registry.byId(this.id + "ImportForm");
            this.importTargetSelect = registry.byId(this.id + "ImportTargetSelect");
            this.copyForm = registry.byId(this.id + "CopyForm");
            this.copyTargetSelect = registry.byId(this.id + "CopyTargetSelect");
            this.copyGrid = registry.byId(this.id + "CopyGrid");
            this.renameForm = registry.byId(this.id + "RenameForm");
            this.renameGrid = registry.byId(this.id + "RenameGrid");
            this.addToSuperFileForm = registry.byId(this.id + "AddToSuperfileForm");
            this.addToSuperfileGrid = registry.byId(this.id + "AddToSuperfileGrid");
            this.desprayForm = registry.byId(this.id + "DesprayForm");
            this.desprayTargetSelect = registry.byId(this.id + "DesprayTargetSelect");
            this.desprayIPSelect = registry.byId(this.id + "DesprayTargetIPAddress");
            this.desprayTooltipDialog = registry.byId(this.id + "DesprayTooltipDialog");
            this.addToSuperfileTargetName = registry.byId(this.id + "AddToSuperfileTargetName");
            this.createNewSuperRadio = registry.byId(this.id + "CreateNewSuperRadio");
            this.addToSuperfileTargetAppendRadio = registry.byId(this.id + "AddToSuperfileTargetAppend");
            this.downloadToList = registry.byId(this.id + "DownloadToList");
            this.downloadToListDialog = registry.byId(this.id + "DownloadToListDialog");
            this.downListForm = registry.byId(this.id + "DownListForm");
            this.fileName = registry.byId(this.id + "FileName");
            this.mineControl = registry.byId(this.id + "Mine");
            var context = this;
            var origOnOpen = this.desprayTooltipDialog.onOpen;
            this.desprayTooltipDialog.onOpen = function () {
                var targetRow;
                if (!context.desprayTargetSelect.initalized) {
                    context.desprayTargetSelect.init({
                        DropZones: true,
                        callback: function (value, item) {
                            if (context.desprayIPSelect) {
                                context.desprayIPSelect.defaultValue = context.desprayIPSelect.get("value");
                                context.desprayIPSelect.loadDropZoneMachines(value);
                                targetRow = item;
                            }
                        }
                    });
                }
                origOnOpen.apply(context.desprayTooltipDialog, arguments);

                if (!context.desprayIPSelect.initalized) {
                    var pathSepChar;
                    context.desprayIPSelect.init({
                        DropZoneMachines: true,
                        callback: function (value, row) {
                            var path = targetRow.machine.Directory.indexOf("\\");
                            targetRow.machine.Name = value;
                            targetRow.machine.Netaddress = value;
                            context.desprayTargetPath.placeholder = targetRow.machine.Directory;
                            if (context.desprayTargetPath) {
                                context.desprayTargetPath._dropZoneTarget = targetRow;
                                if (path > -1) {
                                    pathSepChar = "\\";
                                    context.pathSepCharG = "\\";
                                } else {
                                    pathSepChar = "/";
                                    context.pathSepCharG = "/";
                                }
                                context.desprayTargetPath.loadDropZoneFolders(pathSepChar, targetRow.machine.Directory);
                            }
                        }
                    });
                }
            };
            this.desprayTargetPath = registry.byId(this.id + "DesprayTargetPath");
            this.desprayGrid = registry.byId(this.id + "DesprayGrid");
            this.remoteCopyReplicateCheckbox = registry.byId(this.id + "RemoteCopyReplicate");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initContextMenu();
            this.initFilter();
        },

        _onDownloadToListCancelDialog: function (event) {
            this.downloadToListDialog.hide();
        },

        _onDownloadToList: function (event) {
            this.downloadToListDialog.show();
        },

        _buildCSV: function (event) {
            var selections = this.workunitsGrid.getSelected();
            var row = [];
            var fileName = this.fileName.get("value") + ".csv";

            arrayUtil.forEach(selections, function (cell, idx) {
                var rowData = [cell.IsProtected, cell.IsCompressed, cell.IsKeyFile, cell.__hpcc_displayName, cell.Owner, cell.Description, cell.NodeGroup, cell.RecordCount, cell.IntSize, cell.Parts, cell.Modified];
                row.push(rowData);
            });

            Utility.downloadToCSV(this.workunitsGrid, row, fileName);
            this._onDownloadToListCancelDialog();
        },

        getTitle: function () {
            return this.i18n.title_DFUQuery;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _onTree: function (event) {
            this.treeMode = this.widget.Tree.get("checked");
            this.refreshGrid();
            this.refreshActionState();
        },

        _onOpen: function (event) {
            var selections = this.workunitsGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensureLFPane(selections[i].__hpcc_id, selections[i]);
                if (i === 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab, true);
            }
        },

        _onDelete: function (event) {
            var selection = this.workunitsGrid.getSelected();
            var list = this.arrayToList(selection, "Name");
            if (confirm(this.i18n.DeleteSelectedFiles + "\n" + list)) {
                var context = this;
                WsDfu.DFUArrayAction(selection, "Delete").then(function (response) {
                    context.refreshGrid(true);
                });
            }
        },

        _handleResponse: function (wuidQualifier, response) {
            if (lang.exists(wuidQualifier, response)) {
                var wu = ESPDFUWorkunit.Get(lang.getObject(wuidQualifier, false, response));
                wu.startMonitor(true);
                var tab = this.ensureDFUWUPane(wu.ID, {
                    Wuid: wu.ID
                });
                return tab;
            }
        },

        _onImportOk: function (event) {
            if (this.importForm.validate()) {
                var request = domForm.toObject(this.importForm.id);
                var context = this;
                FileSpray.Copy({
                    request: request
                }).then(function (response) {
                    context._handleResponse("CopyResponse.result", response);
                });
                topic.publish("hpcc/dfu_wu_created");
                registry.byId(this.id + "ImportDropDown").closeDropDown();
            }
        },

        _onCopyOk: function (event) {
            var copyPreserveCompressionCheckbox = registry.byId(this.id + "CopyPreserveCompression");
            var value = copyPreserveCompressionCheckbox.get("checked") ? 1 : 0;

            if (this.copyForm.validate()) {
                var context = this;
                arrayUtil.forEach(this.copyGrid.store.data, function (item, idx) {
                    var logicalFile = ESPLogicalFile.Get(item.NodeGroup, item.Name);
                    var request = domForm.toObject(context.id + "CopyForm");
                    request.RenameSourceName = item.Name;
                    request.destLogicalName = item.targetCopyName;
                    request.preserveCompression = value;
                    logicalFile.copy({
                        request: request
                    }).then(function (response) {
                        context._handleResponse("CopyResponse.result", response);
                    });
                });
                topic.publish("hpcc/dfu_wu_created");
                registry.byId(this.id + "CopyDropDown").closeDropDown();
            }
        },

        _onRenameOk: function (event) {
            if (this.renameForm.validate()) {
                var context = this;
                arrayUtil.forEach(this.renameGrid.store.data, function (item, idx) {
                    var logicalFile = ESPLogicalFile.Get(item.NodeGroup, item.Name);
                    var request = domForm.toObject(context.id + "RenameForm");
                    request.RenameSourceName = item.Name;
                    request.dstname = item.targetRenameName;
                    logicalFile.rename({
                        request: request
                    }).then(function (response) {
                        context._handleResponse("RenameResponse.wuid", response);
                    });
                });
                topic.publish("hpcc/dfu_wu_created");
                registry.byId(this.id + "RenameDropDown").closeDropDown();
            }
        },

        _onAddToSuperfileOk: function (event) {
            if (this.addToSuperFileForm.validate()) {
                var context = this;
                var formData = domForm.toObject(this.id + "AddToSuperfileForm");
                WsDfu.AddtoSuperfile(this.workunitsGrid.getSelected(), formData.Superfile, formData.ExistingFile).then(function (response) {
                    context.refreshGrid();
                });
                registry.byId(this.id + "AddtoDropDown").closeDropDown();
            }
        },

        _onDesprayOk: function (event) {
            if (this.desprayForm.validate()) {
                var context = this;
                arrayUtil.forEach(this.desprayGrid.store.data, function (item, idx) {
                    var request = domForm.toObject(context.id + "DesprayForm");
                    request.destPath = context.desprayTargetPath.getDropZoneFolder();
                    if (!context.endsWith(request.destPath, context.pathSepCharG)) {
                        request.destPath += context.pathSepCharG;
                    }
                    request.destPath += item.targetName;
                    item.despray({
                        request: request
                    }).then(function (response) {
                        context._handleResponse("DesprayResponse.wuid", response);
                    });
                });
                topic.publish("hpcc/dfu_wu_created");
                registry.byId(this.id + "DesprayDropDown").closeDropDown();
            }
        },

        _onRowDblClick: function (item) {
            var wuTab = this.ensureLFPane(item.__hpcc_id, item);
            this.selectChild(wuTab);
        },

        _onRowContextMenu: function (item, colField, mystring) {
            this.menuFilterOwner.set("disabled", false);
            this.menuFilterCluster.set("disabled", false);

            if (item) {
                this.menuFilterOwner.set("label", this.i18n.Owner + ":  " + item.Owner);
                this.menuFilterOwner.set("hpcc_value", item.Owner);
                this.menuFilterCluster.set("label", this.i18n.Cluster + ":  " + item.NodeGroup);
                this.menuFilterCluster.set("hpcc_value", item.NodeGroup);
            }
            if (item.Owner === "") {
                this.menuFilterOwner.set("disabled", true);
                this.menuFilterOwner.set("label", this.i18n.Owner + ":  " + this.i18n.NA);
            }
            if (item.NodeGroup === "") {
                this.menuFilterCluster.set("disabled", true);
                this.menuFilterCluster.set("label", this.i18n.Cluster + ":  " + this.i18n.NA);
            }
        },

        //  Implementation  ---
        getFilter: function () {
            if (this.workunitsGrid) {
                var retVal = this.filter.toObject();
                if (retVal.Sortby) {
                    switch (retVal.Sortby) {
                        case "Smallest":
                            this.workunitsGrid.updateSortArrow([{ attribute: "IntSize", "descending": false }]);
                            break;
                        case "Largest":
                            this.workunitsGrid.updateSortArrow([{ attribute: "IntSize", "descending": true }]);
                            break;
                        case "Oldest":
                            this.workunitsGrid.updateSortArrow([{ attribute: "Modified", "descending": false }]);
                            break;
                        case "Newest":
                        /* falls through */
                        default:
                            this.workunitsGrid.updateSortArrow([{ attribute: "Modified", "descending": true }]);
                            break;
                    }
                }
            }
            var retVal = this.filter.toObject();
            if (retVal.StartDate && retVal.FromTime) {
                lang.mixin(retVal, {
                    StartDate: this.getISOString("FromDate", "FromTime")
                });
            } else if (retVal.StartDate && !retVal.FromTime) {
                lang.mixin(retVal, {
                    StartDate: registry.byId(this.id + "FromDate").attr("value").toISOString().replace(/T.*Z/, "") + "T00:00:00Z"
                });
            }
            if (retVal.EndDate && retVal.ToTime) {
                lang.mixin(retVal, {
                    EndDate: this.getISOString("ToDate", "ToTime")
                });
            } else if (retVal.EndDate && !retVal.ToTime) {
                lang.mixin(retVal, {
                    EndDate: registry.byId(this.id + "ToDate").attr("value").toISOString().replace(/T.*Z/, "") + "T23:59:59Z"
                });
            }

            this.updatedFilter = JSON.parse(JSON.stringify(retVal));    // Deep copy as checkIfWarning will append _rawxml to it  ---

            return retVal;
        },

        checkIfWarning: function () {
            var context = this;

            WsDfu.DFUQuery({
                request: this.updatedFilter
            }).then(function (response) {
                if (lang.exists("DFUQueryResponse", response)) {
                    if (response.DFUQueryResponse.Warning && dojo.byId(context.id).offsetParent !== null) {
                        context.filter.iconFilter.style.color = "red";
                        context.filter.iconFilter.title = response.DFUQueryResponse.Warning;
                    } else {
                        context.filter.setFilterMessage("");
                    }
                }
            });
        },

        //  Implementation  ---
        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;

            if (this.params.searchResults) {
                this.filter.disable(true);
                this.widget.Tree.set("disabled", true);
            }

            this.clusterTargetSelect.init({
                Groups: true,
                includeBlank: true
            });
            var context = this;
            this.importTargetSelect.init({
                Groups: true
            });

            this.importTargetSelect.on("change", function (value) {
                context.checkReplicate(value, context.remoteCopyReplicateCheckbox);
            });

            this.copyTargetSelect.init({
                Groups: true
            });

            this.desprayTargetPath.init({
                DropZoneFolders: true
            });

            this.initWorkunitsGrid();

            if (!params.searchResults) {
                this.checkIfWarning();
            }

            this.filter.init({
                ws_key: "DFUQueryRecentFilter",
                widget: this.widget
            });
            this.filter.on("clear", function (evt) {
                context.refreshHRef();
                context.refreshGrid();
                context.checkIfWarning();
            });
            this.filter.on("apply", function (evt) {
                context.refreshHRef();
                context.workunitsGrid._currentPage = 0;
                context.refreshGrid();
                context.checkIfWarning();
            });
            topic.subscribe("hpcc/dfu_wu_completed", function (topic) {
                context.refreshGrid();
            });

            this.createNewSuperRadio.on("change", function (value) {
                if (value) {
                    context.addToSuperfileTargetAppendRadio.set("checked", false);
                }
            });

            this.addToSuperfileTargetAppendRadio.on("change", function (value) {
                if (value) {
                    context.createNewSuperRadio.set("checked", false);
                }
            });

            this.userName = dojoConfig.username;
            if (this.userName === null) {
                this.mineControl.set("disabled", true);
            }
        },

        _onMine: function (event) {
            if (event) {
                this.filter.setValue(this.id + "Owner", this.userName);
                this.filter._onFilterApply();
            } else {
                this.filter._onFilterClear();
                this.filter._onFilterApply();
            }
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.workunitsTab.id) {
                    this.refreshGrid();
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel._hpccParams);
                    }
                }
            }
        },

        addMenuItem: function (menu, details) {
            var menuItem = new MenuItem(details);
            menu.addChild(menuItem);
            return menuItem;
        },

        initContextMenu: function () {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "WorkunitsGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: this.i18n.Refresh,
                onClick: function (args) { context._onRefresh(); }
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: this.i18n.Open,
                onClick: function (args) { context._onOpen(); }
            }));
            pMenu.addChild(new MenuItem({
                label: this.i18n.Delete,
                onClick: function (args) { context._onDelete(); }
            }));
            pMenu.addChild(new MenuItem({
                label: this.i18n.AddToSuperfile,
                onClick: function (args) { dijit.byId(context.id + "AddtoDropDown").openDropDown(); }
            }));
            pMenu.addChild(new MenuSeparator());
            {
                var pSubMenu = new Menu();
                this.menuFilterOwner = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.filter.clear();
                        context.filter.setValue(context.id + "Owner", context.menuFilterOwner.get("hpcc_value"));
                        context.refreshGrid();
                    }
                });
                this.menuFilterCluster = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.filter.clear();
                        context.filter.setValue(context.id + "ClusterTargetSelect", context.menuFilterCluster.get("hpcc_value"));
                        context.refreshGrid();
                    }
                });
                pSubMenu.addChild(new MenuSeparator());
                this.menuFilterClearFilter = this.addMenuItem(pSubMenu, {
                    label: this.i18n.Clear,
                    onClick: function () {
                        context.filter.clear();
                        context.refreshGrid();
                    }
                });

                pMenu.addChild(new PopupMenuItem({
                    label: this.i18n.Filter,
                    popup: pSubMenu
                }));
            }
            pMenu.startup();
        },

        checkReplicate: function (value, checkBoxValue) {
            WsTopology.TpGroupQuery({
                request: {}
            }).then(function (response) {
                if (lang.exists("TpGroupQueryResponse.TpGroups.TpGroup", response)) {
                    var arr = response.TpGroupQueryResponse.TpGroups.TpGroup;
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

        initWorkunitsGrid: function () {
            var context = this;
            this.listStore = this.params.searchResults ? this.params.searchResults : new ESPLogicalFile.CreateLFQueryStore();
            this.treeStore = new ESPLogicalFile.CreateLFQueryTreeStore();
            this.workunitsGrid = new declare([ESPUtil.Grid(true, true, false, false, "DFUQueryWidget")])({
                deselectOnRefresh: true,
                store: this.listStore,
                query: this.getFilter(),
                sort: [{ attribute: "Modified", "descending": true }],
                columns: {
                    col1: selector({
                        width: 27,
                        disabled: function (item) {
                            return item ? item.__hpcc_isDir : true;
                        },
                        selectorType: "checkbox"
                    }),
                    IsProtected: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = Utility.getImageHTML("locked.png", context.i18n.Protected);
                        },
                        width: 25,
                        sortable: false,
                        formatter: function (_protected) {
                            if (_protected === true) {
                                return Utility.getImageHTML("locked.png");
                            }
                            return "";
                        }
                    },
                    IsCompressed: {
                        width: 25, sortable: false,
                        renderHeaderCell: function (node) {
                            node.innerHTML = Utility.getImageHTML("compressed.png", context.i18n.Compressed);
                        },
                        formatter: function (compressed) {
                            if (compressed === true) {
                                return Utility.getImageHTML("compressed.png");
                            }
                            return "";
                        }
                    },
                    IsKeyFile: {
                        width: 25, sortable: false,
                        renderHeaderCell: function (node) {
                            node.innerHTML = Utility.getImageHTML("index.png", context.i18n.Index);
                        },
                        formatter: function (keyfile, row) {
                            if (row.ContentType === "key") {
                                return Utility.getImageHTML("index.png");
                            }
                            return "";
                        }
                    },
                    __hpcc_displayName: tree({
                        label: this.i18n.LogicalName, width: 600,
                        formatter: function (name, row) {
                            if (row.__hpcc_isDir) {
                                return name;
                            }
                            return (row.getStateImageHTML ? row.getStateImageHTML() + "&nbsp;" : "") + "<a href='#' onClick='return false;' class='dgrid-row-url'>" + name + "</a>";
                        },
                        renderExpando: function (level, hasChildren, expanded, object) {
                            var dir = this.grid.isRTL ? "right" : "left";
                            var cls = ".dgrid-expando-icon";
                            if (hasChildren) {
                                cls += ".ui-icon.ui-icon-triangle-1-" + (expanded ? "se" : "e");
                            }
                            var node = put("div" + cls + "[style=margin-" + dir + ": " + (level * (this.indentWidth || 9)) + "px; float: " + dir + (!object.__hpcc_isDir && level === 0 ? ";display: none" : "") + "]");
                            node.innerHTML = "&nbsp;";
                            return node;
                        }
                    }),
                    Owner: { label: this.i18n.Owner, width: 75 },
                    SuperOwners: { label: this.i18n.SuperOwner, width: 150 },
                    Description: { label: this.i18n.Description, width: 150 },
                    NodeGroup: { label: this.i18n.Cluster, width: 108 },
                    RecordCount: {
                        label: this.i18n.Records, width: 85,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = Utility.valueCleanUp(value);
                        },
                    },
                    IntSize: {
                        label: this.i18n.Size, width: 100,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = Utility.convertedSize(value);
                        },
                    },
                    Parts: {
                        label: this.i18n.Parts, width: 60,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = Utility.valueCleanUp(value);
                        },
                    },
                    Modified: { label: this.i18n.ModifiedUTCGMT, width: 162 }
                }
            }, this.id + "WorkunitsGrid");
            this.workunitsGrid.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.workunitsGrid.row(evt).data;
                    context._onRowDblClick(item);
                }
            });
            this.workunitsGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.workunitsGrid.row(evt).data;
                    context._onRowDblClick(item);
                }
            });
            this.workunitsGrid.on(".dgrid-row:contextmenu", function (evt) {
                if (context._onRowContextMenu) {
                    var item = context.workunitsGrid.row(evt).data;
                    var cell = context.workunitsGrid.cell(evt);
                    var colField = cell.column.field;
                    var mystring = "item." + colField;
                    context._onRowContextMenu(item, colField, mystring);
                }
            });
            this.workunitsGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
                var selection = context.workunitsGrid.getSelected();
                if (selection.length > 0) {
                    context.downloadToList.set("disabled", false);
                } else {
                    context.downloadToList.set("disabled", true);
                }
            });
            ESPUtil.goToPageUserPreference(this.workunitsGrid, "DFUQueryWidget_GridRowsPerPage").then(function () {
                context.workunitsGrid.startup();
            });

            this.copyGrid.createGrid({
                idProperty: "Name",
                columns: {
                    targetCopyName: editor({
                        label: this.i18n.TargetName,
                        width: 144,
                        autoSave: true,
                        editor: "text"
                    })
                }
            });

            this.renameGrid.createGrid({
                idProperty: "Name",
                columns: {
                    targetRenameName: editor({
                        label: this.i18n.TargetName,
                        width: 144,
                        autoSave: true,
                        editor: "text"
                    })
                }
            });

            this.addToSuperfileGrid.createGrid({
                idProperty: "Name",
                columns: {
                    Name: {
                        label: this.i18n.LogicalName
                    }
                }
            });

            this.desprayGrid.createGrid({
                idProperty: "Name",
                columns: {
                    Name: {
                        label: this.i18n.LogicalName
                    },
                    targetName: editor({
                        label: this.i18n.TargetName,
                        width: 144,
                        autoSave: true,
                        editor: "text"
                    })
                }
            });
        },

        initFilter: function () {
            this.validateDialog = new Dialog({
                title: this.i18n.Filter,
                content: this.i18n.NoFilterCriteriaSpecified
            });
        },

        refreshGrid: function (clearSelection) {
            this.workunitsGrid.set("store", this.treeMode ? this.treeStore : this.listStore, this.getFilter());
            if (clearSelection) {
                this.workunitsGrid.clearSelection();
            }
        },

        refreshActionState: function () {
            var selection = this.workunitsGrid.getSelected();
            var hasSelection = false;
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
            }

            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "CopyDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "RenameDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "AddtoDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "AddtoDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "DesprayDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "FilterFilterDropDown").set("disabled", this.treeMode || this.params.searchResults);

            if (hasSelection) {
                var context = this;
                var data = [];
                var matchedPrefix = [];
                var filenames = {};
                arrayUtil.forEach(selection, function (item, idx) {
                    if (item.Name) {
                        var nameParts = item.Name.split("::");
                        if (nameParts.length) {
                            var filename = nameParts[nameParts.length - 1];
                            filenames[filename] = true;
                        }
                        if (idx === 0) {
                            matchedPrefix = nameParts.slice(0, nameParts.length - 1);
                        } else {
                            var i = 0;
                            for (var i = 0; i < matchedPrefix.length && i < nameParts.length - 1; ++i) {
                                if (matchedPrefix[i] !== nameParts[i]) {
                                    break;
                                }
                            }
                            matchedPrefix = matchedPrefix.slice(0, i);
                        }
                        lang.mixin(item, {
                            targetName: nameParts[nameParts.length - 1],
                            targetCopyName: item.Name + "_copy",
                            targetRenameName: item.Name + "_rename"
                        });
                        data.push(item);
                    }
                });
                var superfileName = "superfile";
                var i = 1;
                while (filenames[superfileName]) {
                    superfileName = "superfile_" + i++;
                }
                registry.byId(this.id + "AddToSuperfileTargetName").set("value", matchedPrefix.join("::") + "::" + superfileName);
                this.copyGrid.setData(data);
                this.renameGrid.setData(data);
                this.addToSuperfileGrid.setData(data);
                this.desprayGrid.setData(data);
            }
        },

        ensureDFUWUPane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new DelayLoadWidget({
                    id: id,
                    title: params.Wuid,
                    closable: true,
                    delayWidget: "DFUWUDetailsWidget",
                    _hpccParams: params
                });
                this.addChild(retVal, 1);
            }
            return retVal;
        },

        ensureLFPane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                if (params.isSuperfile) {
                    retVal = new DelayLoadWidget({
                        id: id,
                        title: params.Name,
                        closable: true,
                        delayWidget: "SFDetailsWidget",
                        _hpccParams: {
                            Name: params.Name
                        }
                    });
                } else {
                    retVal = new DelayLoadWidget({
                        id: id,
                        title: params.Name,
                        closable: true,
                        delayWidget: "LFDetailsWidget",
                        _hpccParams: {
                            NodeGroup: params.NodeGroup,
                            Name: params.Name
                        }
                    });
                }
                this.addChild(retVal, 1);
            }
            return retVal;
        }

    });
});
