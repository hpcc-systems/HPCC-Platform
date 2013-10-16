/*##############################################################################
#	HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#	Licensed under the Apache License, Version 2.0 (the "License");
#	you may not use this file except in compliance with the License.
#	You may obtain a copy of the License at
#
#	   http://www.apache.org/licenses/LICENSE-2.0
#
#	Unless required by applicable law or agreed to in writing, software
#	distributed under the License is distributed on an "AS IS" BASIS,
#	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#	See the License for the specific language governing permissions and
#	limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-construct",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/date",
    "dojo/on",

    "dijit/registry",
    "dijit/Dialog",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",
    "dijit/form/Textarea",

    "dgrid/Grid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/_TabContainerWidget",
    "hpcc/WsDfu",
    "hpcc/FileSpray",
    "hpcc/ESPUtil",
    "hpcc/ESPLogicalFile",
    "hpcc/ESPDFUWorkunit",
    "hpcc/LFDetailsWidget",
    "hpcc/SFDetailsWidget",
    "hpcc/DFUWUDetailsWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/FilterDropDownWidget",

    "dojo/text!../templates/DFUQueryWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer"

], function (declare, lang, arrayUtil, dom, domAttr, domConstruct, domClass, domForm, date, on,
                registry, Dialog, Menu, MenuItem, MenuSeparator, PopupMenuItem, Textarea,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, WsDfu, FileSpray, ESPUtil, ESPLogicalFile, ESPDFUWorkunit, LFDetailsWidget, SFDetailsWidget, DFUWUDetailsWidget, TargetSelectWidget, FilterDropDownWidget,
                template) {
    return declare("DFUQueryWidget", [_TabContainerWidget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "DFUQueryWidget",

        addToSuperFileForm: null,
        desprayDialog: null,
        sprayFixedDialog: null,
        sprayVariableDialog: null,
        sprayXmlDialog: null,

        workunitsTab: null,
        workunitsGrid: null,

        filter: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.addToSuperFileForm = registry.byId(this.id + "AddToSuperfileForm");
            this.desprayDialog = registry.byId(this.id + "DesprayDialog");
            this.sprayFixedDialog = registry.byId(this.id + "SprayFixedDialog");
            this.sprayVariableDialog = registry.byId(this.id + "SprayVariableDialog");
            this.sprayXmlDialog = registry.byId(this.id + "SprayXmlDialog");
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.filter = registry.byId(this.id + "Filter");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
            this.desprayTargetSelect = registry.byId(this.id + "DesprayTargetSelect");
            this.sprayFixedDestinationSelect = registry.byId(this.id + "SprayFixedDestination");
            this.sprayVariableDestinationSelect = registry.byId(this.id + "SprayVariableDestination");
            this.sprayXmlDestinationSelect = registry.byId(this.id + "SprayXmlDestinationSelect");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initContextMenu();
            this.initFilter();
        },

        getTitle: function () {
            return "Logical Files";
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _onOpen: function (event) {
            var selections = this.workunitsGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensureLFPane(this.id + "_" + selections[i].Name, selections[i]);
                if (i == 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab, true);
            }
        },

        _onDelete: function (event) {
            if (confirm('Delete selected files?')) {
                var context = this;
                WsDfu.DFUArrayAction(this.workunitsGrid.getSelected(), "Delete", {
                    load: function (response) {
                        context.refreshGrid(response);
                    }
                });
            }
        },

        _onAddToSuperfileOk: function (event) {
            if (this.addToSuperFileForm.validate()) {
                var context = this;
                var formData = domForm.toObject(this.id + "AddToSuperfileForm");
                WsDfu.AddtoSuperfile(this.workunitsGrid.getSelected(), formData.Superfile, formData.ExistingFile, {
                    load: function (response) {
                        context.refreshGrid(response);
                    }
                });
                var d = registry.byId(this.id + "AddtoDropDown");
                registry.byId(this.id + "AddtoDropDown").closeDropDown();
            }
        },

        _handleResponse: function (wuidQualifier, response) {
            if (lang.exists(wuidQualifier, response)) {
                var wu = ESPDFUWorkunit.Get(lang.getObject(wuidQualifier, false, response));
                wu.startMonitor(true);
                var tab = this.ensureDFUWUPane(this.id + "_" + wu.ID, {
                    Wuid: wu.ID
                });
                if (tab) {
                    this.selectChild(tab);
                }
            }
        },

        _onDesprayOk: function (event) {
            if (this.desprayDialog.validate()) {
                var context = this;
                arrayUtil.forEach(this.workunitsGrid.getSelected(), function (item, idx) {
                    item.refresh().then(function (response) {
                        var request = domForm.toObject(context.id + "DesprayDialog");
                        request.destPath += item.Filename;
                        item.despray({
                            request: request
                        }).then(function (response) {
                            context._handleResponse("DesprayResponse.wuid", response);
                        });
                    });
                });
                registry.byId(this.id + "DesprayDropDown").closeDropDown();
            }
        },

        _onSprayFixed: function (event) {
            if (this.sprayFixedDialog.validate()) {
                var formData = domForm.toObject(this.id + "SprayFixedDialog");
                var context = this;
                FileSpray.SprayFixed({
                    request: formData
                }).then(function (response) {
                    context._handleResponse("SprayFixedResponse.wuid", response);
                })
                registry.byId(this.id + "SprayFixedDropDown").closeDropDown();
            }
        },

        _onSprayVariable: function (event) {
            if (this.sprayVariableDialog.validate()) {
                var context = this;
                var formData = domForm.toObject(this.id + "SprayVariableDialog");
                FileSpray.SprayVariable({
                    request: formData
                }).then(function (response) {
                    context._handleResponse("SprayResponse.wuid", response);
                });
                registry.byId(this.id + "SprayVariableDropDown").closeDropDown();
            }
        },

        _onSprayXml: function (event) {
            if (this.sprayXmlDialog.validate()) {
                var context = this;
                var formData = domForm.toObject(this.id + "SprayXmlDialog");
                FileSpray.SprayVariable({
                    request: formData
                }).then(function (response) {
                    context._handleResponse("SprayResponse.wuid", response);
                });
                registry.byId(this.id + "SprayXmlDropDown").closeDropDown();
            }
        },

        _onRowDblClick: function (item) {
            var wuTab = this.ensureLFPane(this.id + "_" + item.Name, item);
            this.selectChild(wuTab);
        },

        _onRowContextMenu: function (item, colField, mystring) {
            this.menuFilterOwner.set("disabled", false);
            this.menuFilterCluster.set("disabled", false);

            if (item) {
                this.menuFilterOwner.set("label", "Owner:  " + item.Owner);
                this.menuFilterOwner.set("hpcc_value", item.Owner);
                this.menuFilterCluster.set("label", "Cluster:  " + item.ClusterName);
                this.menuFilterCluster.set("hpcc_value", item.ClusterName);
            }
            if (item.Owner == "") {
                this.menuFilterOwner.set("disabled", true);
                this.menuFilterOwner.set("label", "Owner:  " + "N/A");
            }
            if (item.ClusterName == "") {
                this.menuFilterCluster.set("disabled", true);
                this.menuFilterCluster.set("label", "Cluster:  " + "N/A");
            }
        },

        //  Implementation  ---
        getFilter: function () {
            var retVal = this.filter.toObject();
            lang.mixin(retVal, {
                StartDate: this.getISOString("FromDate", "FromTime"),
                EndDate: this.getISOString("ToDate", "ToTime")
            });
            if (retVal.StartDate != "" && retVal.EndDate != "") {
            } else if (retVal.FirstN) {
                var now = new Date();
                retVal.StartDate = date.add(now, "day", retVal.LastNDays * -1).toISOString();
                retVal.EndDate = now.toISOString();
            }
            return retVal;
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.clusterTargetSelect.init({
                Groups: true,
                includeBlank: true
            });
            var context = this;
            this.desprayTargetSelect.init({
                DropZones: true,
                callback: function (value, item) {
                    registry.byId(context.id + "DesprayTargetIPAddress").set("value", item.machine.Netaddress);
                    registry.byId(context.id + "DesprayTargetPath").set("value", item.machine.Directory + "/");
                }
            });
            this.sprayFixedDestinationSelect.init({
                Groups: true
            });
            this.sprayVariableDestinationSelect.init({
                Groups: true
            });
            this.sprayXmlDestinationSelect.init({
                Groups: true
            });
            this.initWorkunitsGrid();
            this.selectChild(this.workunitsTab, true);
        },

        initTab: function() {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id == this.workunitsTab.id) {
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

        initContextMenu: function() {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "WorkunitsGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: "Refresh",
                onClick: function(args){context._onRefresh();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Open",
                onClick: function(args){context._onOpen();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Delete",
                onClick: function(args){context._onDelete();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Add To Superfile",
                onClick: function(args){dijit.byId(context.id+"AddtoDropDown").openDropDown()}
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
                        context.filter.setValue(context.id + "ClusterTargetSelect", context.menuFilterOwner.get("hpcc_value"));
                        context.refreshGrid();
                    }
                });
                pSubMenu.addChild(new MenuSeparator());
                this.menuFilterClearFilter = this.addMenuItem(pSubMenu, {
                    label: "Clear",
                    onClick: function () {
                        context.filter.clear();
                        context.refreshGrid();
                    }
                });

                pMenu.addChild(new PopupMenuItem({
                    label: "Filter",
                    popup: pSubMenu
                }));
            }
            pMenu.startup();
        },

        initWorkunitsGrid: function() {
            var store = new ESPLogicalFile.CreateLFQueryStore();
            this.workunitsGrid = new declare([Grid, Pagination, Selection, ColumnResizer, Keyboard, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: store,
                rowsPerPage: 50,
                pagingLinks: 1,
                pagingTextBox: true,
                firstLastArrows: true,
                pageSizeOptions: [25, 50, 100],
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    isZipfile: {
                        label: "C", width: 16, sortable: false,
                        formatter: function (compressed) {
                            if (compressed == true) {
                                return "C";
                            }
                            return "";
                        }
                    },
                    IsKeyFile: {
                        label: "K", width: 16, sortable: false,
                        formatter: function (keyfile) {
                            if (keyfile == true) {
                                return "K";
                            }
                            return "";
                        }
                    },
                    isSuperfile: {
                        label: "S", width: 16, sortable: false,
                        formatter: function (superfile) {
                            if (superfile == true) {
                                return "S";
                            }
                            return "";
                        }
                    },
                    Name: { label: "Logical Name",
                        formatter: function (name, idx) {
                            return "<a href='#' rowIndex=" + idx + " class='" + context.id + "LogicalNameClick'>" + name + "</a>";
                        }
                    },
                    Owner: { label: "Owner", width: 72 },
                    Description: { label: "Description", width: 153 },
                    ClusterName: { label: "Cluster", width: 108 },
                    RecordCount: { label: "Records", width: 72, sortable: false },
                    Totalsize: { label: "Size", width: 72, sortable: false },
                    Parts: { label: "Parts", width: 45, sortable: false },
                    Modified: { label: "Modified (UTC/GMT)", width: 155, sortable: false }
                }
            }, this.id + "WorkunitsGrid");
            this.workunitsGrid.noDataMessage = "<span class='dojoxGridNoData'>Zero Logical Files(check filter).</span>";

            var context = this;
            on(document, "." + context.id + "LogicalNameClick:click", function (evt) {
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
            });
            this.workunitsGrid.onContentChanged(function (object, removedFrom, insertedInto) {
                context.refreshActionState();
            });
            this.workunitsGrid.startup();
        },

        initFilter: function () {
            this.validateDialog = new Dialog({
                title: "Filter",
                content: "No filter criteria specified."
            });
        },

        refreshGrid: function (args) {
            this.workunitsGrid.set("query", this.getFilter());
        },

        refreshActionState: function () {
            var selection = this.workunitsGrid.getSelected();
            var hasSelection = false;
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
            }

            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "AddtoDropDown").set("disabled", !hasSelection);
            registry.byId(this.id + "DesprayDropDown").set("disabled", !hasSelection);

            if (hasSelection) {
                var sourceDiv = dom.byId(this.id + "DesprayDialogSource");
                domConstruct.empty(sourceDiv);
                var context = this;
                arrayUtil.forEach(selection, function (item, idx) {
                    domConstruct.create("div", {
                        innerHTML: item.Name
                    }, sourceDiv);
                });
            }
        },

        ensureDFUWUPane: function (id, params) {
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new DFUWUDetailsWidget.fixCircularDependency({
                    id: id,
                    title: params.Wuid,
                    closable: true,
                    _hpccParams: params
                });
                this.addChild(retVal, 1);
            }
            return retVal;
        },

        ensureLFPane: function (id, params) {
            var obj = id.split("::");
            id = obj.join("");
            obj = id.split(".");
            id = obj.join("");
            var retVal = registry.byId(id);
            if (!retVal) {
                if (params.isSuperfile) {
                    retVal = new SFDetailsWidget.fixCircularDependency({
                        id: id,
                        title: params.Name,
                        closable: true,
                        _hpccParams: params
                    });
                } else {
                    retVal = new LFDetailsWidget.fixCircularDependency({
                        id: id,
                        title: params.Name,
                        closable: true,
                        _hpccParams: {
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
