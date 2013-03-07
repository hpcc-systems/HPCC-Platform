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
    "dojo/dom-form",
    "dojo/data/ObjectStore",
    "dojo/date",
    "dojo/on",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",
    "dijit/Dialog",

    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/Pagination",
    "dojox/grid/enhanced/plugins/IndirectSelection",

    "hpcc/_TabContainerWidget",
    "hpcc/WsDfu",
    "hpcc/ESPLogicalFile",
    "hpcc/LFDetailsWidget",
    "hpcc/SFDetailsWidget",

    "dojo/text!../templates/DFUWUQueryWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer",

    "hpcc/TargetSelectWidget"

], function (declare, lang, arrayUtil, dom, domForm, ObjectStore, date, on, Menu, MenuItem, MenuSeparator,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry, Dialog,
                EnhancedGrid, Pagination, IndirectSelection,
                _TabContainerWidget, WsDfu, ESPLogicalFile, LFDetailsWidget, SFDetailsWidget,
                template) {
    return declare("DFUWUQueryWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "DFUWUQueryWidget",
        workunitsTab: null,
        workunitsGrid: null,
        legacyPane: null,
        legacyPaneLoaded: false,

        tabMap: [],


        postCreate: function (args) {
            this.inherited(arguments);
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.workunitsGrid = registry.byId(this.id + "WorkunitsGrid");
            this.legacyPane = registry.byId(this.id + "_Legacy");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initWorkunitsGrid();
            this.initFilter();
            this.refreshActionState();
            
            
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _onOpen: function (event) {
            var selections = this.workunitsGrid.selection.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePane(this.id + "_" + selections[i].Name, selections[i]);
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
                WsDfu.DFUArrayAction(this.workunitsGrid.selection.getSelected(), "Delete", {
                    load: function (response) {
                        context.workunitsGrid.rowSelectCell.toggleAllSelection(false);
                        context.refreshGrid(response);
                    }
                });
            }
        },
        _onAddToSuperfileOk: function (event) {
            var context = this;
            var formData = domForm.toObject(this.id + "AddToSuperfileForm");
            WsDfu.AddtoSuperfile(this.workunitsGrid.selection.getSelected(), formData.Superfile, formData.ExistingFile, {
                load: function (response) {
                    context.workunitsGrid.rowSelectCell.toggleAllSelection(false);
                    context.refreshGrid(response);
                }
            });
            var d = registry.byId(this.id + "AddtoDropDown");
            registry.byId(this.id + "AddtoDropDown").closeDropDown();
        },
        _onAddToSuperfileCancel: function (event) {
            var d = registry.byId(this.id + "AddtoDropDown");
            registry.byId(this.id + "AddtoDropDown").closeDropDown();
        },
        _onClickFilterApply: function (event) {
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);
            this.refreshGrid();
        },
        _onFilterApply: function (event) {
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);
            if (this.hasFilter()) {
                registry.byId(this.id + "FilterDropDown").closeDropDown();
                this.refreshGrid();
            } else {
                this.validateDialog.show();
            }
        },
        _onFilterClear: function(event) {
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);
            var context = this;
            arrayUtil.forEach(registry.byId(this.id + "FilterForm").getDescendants(), function (item, idx) {
                if (item.id == context.id + "ClusterTargetSelect") {
                    item.setValue("");
                } else {
                    item.set('value', null);
                }
            });
            this.refreshGrid();
        },

        getISOString: function (dateField, timeField) {
            var d = registry.byId(this.id + dateField).attr("value");
            var t = registry.byId(this.id + timeField).attr("value");
            if (d) {
                if (t) {
                    d.setHours(t.getHours());
                    d.setMinutes(t.getMinutes());
                    d.setSeconds(t.getSeconds());
                }
                return d.toISOString();
            }
            return "";
        },

        hasFilter: function () {
            return dom.byId(this.id + "Name").value !== "" ||
               dom.byId(this.id + "Description").value !== "" ||
               dom.byId(this.id + "Owner").value !== "" ||
               //dom.byId(this.id + "ClusterTargetSelect").value !== "" ||
               dom.byId(this.id + "FromSize").value !== "" ||
               dom.byId(this.id + "ToSize").value !== "" ||
               dom.byId(this.id + "FromDate").value !== "" ||
               dom.byId(this.id + "FromTime").value !== "" ||
               dom.byId(this.id + "ToDate").value !== "" ||
               dom.byId(this.id + "ToTime").value !== "" ||
               dom.byId(this.id + "LastNDays").value !== "";
        },

        getFilter: function () {
            var retVal = domForm.toObject(this.id + "FilterForm");
            lang.mixin(retVal, {
                ClusterName: this.clusterTargetSelect.getValue(),
                StartDate: this.getISOString("FromDate", "FromTime"),
                EndDate: this.getISOString("ToDate", "ToTime")
            });
            if (retVal.StartDate != "" && retVal.EndDate != "") {
            } else if (retVal.LastNDays) {
                var now = new Date();
                retVal.StartDate = date.add(now, "day", dom.byId(this.id + "LastNDays").value * -1).toISOString();
                retVal.EndDate = now.toISOString();
            }
            return retVal;
        },

        getISOString: function (dateField, timeField) {
            var d = registry.byId(this.id + dateField).attr("value");
            var t = registry.byId(this.id + timeField).attr("value");
            if (d) {
                if (t) {
                    d.setHours(t.getHours());
                    d.setMinutes(t.getMinutes());
                    d.setSeconds(t.getSeconds());
                }
                return d.toISOString();
            }
            return "";
        },

        //  Implementation  ---
        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;

            this.selectChild(this.legacyPane, true);
            this.clusterTargetSelect.init({
                Groups: true,
                includeBlank: true
            });
        },

        initTab: function() {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id == this.workunitsTab.id) {
                } else if (currSel.id == this.legacyPane.id) {
                    if (!this.legacyPaneLoaded) {
                        this.legacyPaneLoaded = true;
                        this.legacyPane.set("content", dojo.create("iframe", {
                            src: "/WsDfu/DFUQuery",
                            style: "border: 0; width: 100%; height: 100%"
                        }));
                    }
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel._hpccParams);
                    }
                }
            }
        },

        initWorkunitsGrid: function() {
            var pMenu;
            var context = this;
            pMenu = new Menu({
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
            pMenu.addChild(new MenuItem({
                label: "Filter",
                onClick: function(args){dijit.byId(context.id+"FilterDropDown").openDropDown()}
            }));
            pMenu.startup();


            this.workunitsGrid.setStructure([
                {
                    name: "C",
                    field: "isZipfile",
                    width: "16px",
                    formatter: function (compressed) {
                        if (compressed == true) {
                            return "C";
                        }
                        return "";
                    }
                },
                {
                    name: "K",
                    field: "IsKeyFile",
                    width: "16px",
                    formatter: function (keyfile) {
                        if (keyfile == true) {
                            return "K";
                        }
                        return "";
                    }
                },
                {
                    name: "S",
                    field: "isSuperfile",
                    width: "16px",
                    formatter: function (superfile) {
                        if (superfile == true) {
                            return "S";
                        }
                        return "";
                    }
                },
                { name: "Logical Name", field: "Name", width: "32" },
                { name: "Owner", field: "Owner", width: "8" },
                { name: "Description", field: "Description", width: "12" },
                { name: "Cluster", field: "ClusterName", width: "12" },
                { name: "Records", field: "RecordCount", width: "8" },
                { name: "Size", field: "Totalsize", width: "8" },
                { name: "Parts", field: "Parts", width: "4" },
                { name: "Modified (UTC/GMT)", field: "Modified", width: "12" }
            ]);
            var objStore = ESPLogicalFile.CreateLFQueryObjectStore();
            this.workunitsGrid.setStore(objStore);
            this.workunitsGrid.setQuery(this.getFilter());

            var context = this;
            this.workunitsGrid.on("RowDblClick", function (evt) {
                if (context.onRowDblClick) {
                    var idx = evt.rowIndex;
                    var item = this.getItem(idx);
                    context.onRowDblClick(item);
                }
            }, true);

             this.workunitsGrid.on("RowContextMenu", function (evt){
                if (context.onRowContextMenu) {
                    var idx = evt.rowIndex;
                    var colField = evt.cell.field;
                    var item = this.getItem(idx);
                    var mystring = "item." + colField;
                    context.onRowContextMenu(idx,item,colField,mystring);
                }
            }, true);

            dojo.connect(this.workunitsGrid.selection, 'onSelected', function (idx) {
                context.refreshActionState();
            });
            dojo.connect(this.workunitsGrid.selection, 'onDeselected', function (idx) {
                context.refreshActionState();
            });

            this.workunitsGrid.startup();
        },

        initFilter: function () {
            this.validateDialog = new Dialog({
                title: "Filter",
                content: "No filter criteria specified."
            });
            dojo.connect(registry.byId(this.id + "FromDate"), 'onClick', function (evt) {
            });
            dojo.connect(registry.byId(this.id + "ToDate"), 'onClick', function (evt) {
            });
        },

        

        refreshGrid: function (args) {
            this.workunitsGrid.setQuery(this.getFilter());
            var context = this;
            setTimeout(function () {
                context.refreshActionState()
            }, 200);
        },

        refreshActionState: function () {
            var selection = this.workunitsGrid.selection.getSelected();
            var hasSelection = false;
            var hasFilter = this.hasFilter();
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
            }

            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "AddtoDropDown").set("disabled", !hasSelection);
            dom.byId(this.id + "IconFilter").src = hasFilter ? "img/filter.png" : "img/noFilter.png";
        },

        ensurePane: function (id, params) {
            var obj = id.split("::");
            id = obj.join("");
            obj = id.split(".");
            id = obj.join("");
            var retVal = this.tabMap[id];
            if (!retVal) {
                var context = this;
                if (params.isSuperfile) {
                    retVal = new SFDetailsWidget.fixCircularDependency({
                        id: id,
                        title: params.Name,
                        closable: false,
                        onClose: function () {
                            delete context.tabMap[id];
                            return true;
                        },
                        _hpccParams: params
                    });
                } else {
                    retVal = new LFDetailsWidget.fixCircularDependency({
                        id: id,
                        title: params.Name,
                        closable: false,
                        onClose: function () {
                            delete context.tabMap[id];
                            return true;
                        },
                        _hpccParams: params
                    });
                }
                this.tabMap[id] = retVal;
                this.addChild(retVal, 2);
            }
            return retVal;
        },

        onRowDblClick: function (item) {
            var wuTab = this.ensurePane(this.id + "_" + item.Name, item);
            this.selectChild(wuTab);
        },

         onRowContextMenu: function (idx,item,colField,mystring) {
            this.workunitsGrid.selection.clear(idx,true);
            this.workunitsGrid.selection.setSelected(idx,true);
        }
    });
});
