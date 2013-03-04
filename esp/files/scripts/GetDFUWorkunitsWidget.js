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
    "dojo/dom",
    "dojo/data/ObjectStore",
    "dojo/date",
    "dojo/on",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/Pagination",
    "dojox/grid/enhanced/plugins/IndirectSelection",

    "hpcc/FileSpray",
    "hpcc/DFUWUDetailsWidget",

    "dojo/text!../templates/GetDFUWorkunitsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog"
], function (declare, dom, ObjectStore, date, on, Menu, MenuItem, MenuSeparator, 
                _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                EnhancedGrid, Pagination, IndirectSelection,
                FileSpray, DFUWUDetailsWidget,
                template) {
    return declare("GetDFUWorkunitsWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "GetDFUWorkunitsWidget",
        borderContainer: null,
        tabContainer: null,
        workunitsGrid: null,
        legacyPane: null,
        legacyPaneLoaded: false,

        tabMap: [],

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.tabContainer = registry.byId(this.id + "TabContainer");
            this.workunitsGrid = registry.byId(this.id + "WorkunitsGrid");
            this.legacyPane = registry.byId(this.id + "Legacy");

            var context = this;
            this.tabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                if (nval.id == context.id + "Workunits") {
                } else if (nval.id == context.id + "Legacy") {
                    if (!context.legacyPaneLoaded) {
                        context.legacyPaneLoaded = true;
                        context.legacyPane.set("content", dojo.create("iframe", {
                            src: "/FileSpray/GetDFUWorkunits",
                            style: "border: 0; width: 100%; height: 100%"
                        }));
                    }
                } else {
                    if (!nval.initalized) {
                        nval.init(nval.params);
                    }
                }
                context.selectedTab = nval;
            });
        },

        startup: function (args) {
            this.inherited(arguments);
            this.refreshActionState();
            this.initWorkunitsGrid();
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        //  Hitched actions  ---
        _onOpen: function (event) {
            var selections = this.workunitsGrid.selection.getSelected();
            for (var i = selections.length - 1; i >= 0; --i) {
                this.ensurePane(selections[i].ID, {
                    Wuid: selections[i].ID
                });
            }
        },
        _onDelete: function (event) {
            if (confirm('Delete selected workunits?')) {
                var context = this;
                FileSpray.WUAction(this.workunitsGrid.selection.getSelected(), "Delete", {
                    load: function (response) {
                        context.workunitsGrid.rowSelectCell.toggleAllSelection(false);
                        context.refreshGrid(response);
                    }
                });
            }
        },
        _onSetToFailed: function (event) {
            var context = this;
            FileSpray.WUAction(this.workunitsGrid.selection.getSelected(), "SetToFailed", {
                load: function (response) {
                    context.refreshGrid(response);
                }
            });
        },
        _onProtect: function (event) {
            var context = this;
            FileSpray.WUAction(this.workunitsGrid.selection.getSelected(), "Protect", {
                load: function (response) {
                    context.refreshGrid(response);
                }
            });
        },
        _onUnprotect: function (event) {
            var context = this;
            FileSpray.WUAction(this.workunitsGrid.selection.getSelected(), "Unprotect", {
                load: function (response) {
                    context.refreshGrid(response);
                }
            });
        },
        _onFilterApply: function (event) {
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);
            this.refreshGrid();
        },

        _onFilterSoapTest:function (event){

        },

        _onFilterLink:function (event){

        },

        _onFilterClearAll:function (event){

        },

        _onFilterReset: function(event) {
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);
            dom.byId(this.id + "Owner").value = "";
            dom.byId(this.id + "Cluster").value = "";
            dom.byId(this.id + "StateReq").value = "";
            dom.byId(this.id + "Type").value = "";
            dom.byId(this.id + "Jobname").value = "";
            dom.byId(this.id + "PageSize").value = "";
            dom.byId(this.id + "CurrentPage").value = "";
            dom.byId(this.id + "PageStartFrom").value = "";
            dom.byId(this.id + "Sortby").value = "";
            dom.byId(this.id + "Descending").value = "";
            this.refreshGrid();
        },

        getFilter: function () {
            var retVal = {
                Owner: dom.byId(this.id + "Owner").value,
                Cluster: dom.byId(this.id + "Cluster").value,
                StateReq: dom.byId(this.id + "StateReq").value,
                Type: dom.byId(this.id + "Type").value,
                Jobname: dom.byId(this.id + "Jobname").value,
                PageSize: dom.byId(this.id + "PageSize").value,
                CurrentPage: dom.byId(this.id + "CurrentPage").value,
                PageStartFrom: dom.byId(this.id + "PageStartFrom").value,
                Sortby: dom.byId(this.id + "Sortby").value,
                //Descending: dom.byId(this.id + "Descending").value
            };
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
        },

        initWorkunitsGrid: function() {

            var pMenu;
            var context = this;
            
            pMenu = new Menu({
                targetNodeIds: [this.id + "WorkunitsGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: "Open",
                onClick: function(){context._onOpen();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Delete",
                onClick: function(){context._onDelete();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Set To Failed",
                onClick: function(){context._onRename();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Protect",
                onClick: function(){context._onOpen();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Unprotect",
                onClick: function(){context._onReplicate();}
            }));
            

            pMenu.startup();


            this.workunitsGrid.setStructure([
			    {
				    name: "P",
				    field: "isProtected",
				    width: "20px",
				    formatter: function (_protected) {
				        if (_protected == true) {
					        return "P";
					    }
					    return "";
				    }
			    },
			    { name: "ID", field: "ID", width: "12" },
			    {
			        name: "Type",
			        field: "Command",
			        width: "8",
			        formatter: function (command) {
			            switch (command) {
			                case 1: return "Copy";
			                case 2: return "Remove";
			                case 3: return "Move";
			                case 4: return "Rename";
			                case 5: return "Replicate";
			                case 6: return "Spray";
			                case 7: return "Despray";
			                case 8: return "Add";
			                case 9: return "Transfer";
			                case 10: return "Save Map";
			                case 11: return "Add Group";
			                case 12: return "Server";
			                case 13: return "Monitor";
			                case 14: return "Copy Merge";
			                case 15: return "Super Copy";
			            }
			            return "Unknown";
			        }
			    },
			    { name: "Owner", field: "Owner", width: "8" },
			    { name: "Job Name", field: "JobName", width: "16" },
			    { name: "Cluster", field: "ClusterName", width: "8" },
			    { name: "State", field: "StateMessage", width: "8" },
			    { name: "% Complete", field: "PercentDone", width: "8" }
            ]);
            var store = new FileSpray.GetDFUWorkunits();
            var objStore = new ObjectStore({ objectStore: store });
            this.workunitsGrid.setStore(objStore);
            this.workunitsGrid.setQuery(this.getFilter());

            var context = this;
            this.workunitsGrid.on("RowDblClick", function (evt) {
                if (context.onRowDblClick) {
                    var idx = evt.rowIndex;
                    var item = this.getItem(idx);
                    var id = this.store.getValue(item, "ID");
                    context.onRowDblClick(id);
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
            var hasProtected = false;
            var hasNotProtected = false;
            var hasFailed = false;
            var hasNotFailed = false;
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
                if (selection[i] && selection[i].isProtected && selection[i].isProtected != "0") {
                    hasProtected = true;
                } else {
                    hasNotProtected = true;
                }
                if (selection[i] && selection[i].State && selection[i].State == "5") {
                    hasFailed = true;
                } else {
                    hasNotFailed = true;
                }
            }

            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasNotProtected);
            registry.byId(this.id + "SetToFailed").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Protect").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Unprotect").set("disabled", !hasProtected);
        },

        ensurePane: function (id, params) {
            var retVal = this.tabMap[id];
            if (!retVal) {
                var context = this;
                retVal = new DFUWUDetailsWidget({
                    Wuid: id,
                    title: id,
                    closable: true,
                    onClose: function () {
                        delete context.tabMap[id];
                        return true;
                    },
                    params: params
                });
                this.tabMap[id] = retVal;
                this.tabContainer.addChild(retVal, 2);
            }
            return retVal;
        },

        onRowDblClick: function (id) {
            var wuTab = this.ensurePane(id, {
                Wuid: id
            });
            this.tabContainer.selectChild(wuTab);
        }
    });
});
