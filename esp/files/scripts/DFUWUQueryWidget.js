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

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/Pagination",
    "dojox/grid/enhanced/plugins/IndirectSelection",

    "hpcc/_TabContainerWidget",
    "hpcc/WsDfu",
    "hpcc/LFDetailsWidget",

    "dojo/text!../templates/DFUWUQueryWidget.html",

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
], function (declare, dom, ObjectStore, date, on,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                EnhancedGrid, Pagination, IndirectSelection,
                _TabContainerWidget, WsDfu, LFDetailsWidget,
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
        },

        startup: function (args) {
            this.inherited(arguments);
            this.refreshActionState();
            this.initWorkunitsGrid();
        },

        //  Hitched actions  ---
        _onOpen: function (event) {
            var selections = this.workunitsGrid.selection.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePane(this.id + "_" + selections[i].Name, {
                    Name: selections[i].Name
                });
                if (i == 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab, true);
            }
        },
        _onDelete: function (event) {
            if (confirm('Delete selected workunits?')) {
                var context = this;
                WsDfu.WUAction(this.workunitsGrid.selection.getSelected(), "Delete", {
                    load: function (response) {
                        context.workunitsGrid.rowSelectCell.toggleAllSelection(false);
                        context.refreshGrid(response);
                    }
                });
            }
        },
        _onAddToSuperfile: function (event) {
        },
        _onFilterApply: function (event) {
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);
            this.refreshGrid();
        },
        _onFilterClear: function(event) {
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);
            dom.byId(this.id + "Owner").value = "";
            dom.byId(this.id + "Jobname").value = "";
            dom.byId(this.id + "Cluster").value = "";
            dom.byId(this.id + "State").value = "";
            dom.byId(this.id + "ECL").value = "";
            dom.byId(this.id + "LogicalFile").value = "";
            dom.byId(this.id + "LogicalFileSearchType").value = "";
            dom.byId(this.id + "FromDate").value = "";
            dom.byId(this.id + "FromTime").value = "";
            dom.byId(this.id + "ToDate").value = "";
            dom.byId(this.id + "LastNDays").value = "";
            this.refreshGrid();
        },

        getFilter: function () {
            var retVal = {
                Owner: dom.byId(this.id + "Owner").value,
                Jobname: dom.byId(this.id + "Jobname").value,
                Cluster: dom.byId(this.id + "Cluster").value,
                State: dom.byId(this.id + "State").value,
                ECL: dom.byId(this.id + "ECL").value,
                LogicalFile: dom.byId(this.id + "LogicalFile").value,
                StartDate: this.getISOString("FromDate", "FromTime"),
                EndDate: this.getISOString("ToDate", "ToTime"),
                LastNDays: dom.byId(this.id + "LastNDays").value
            };
            if (retVal.StartDate != "" && retVal.EndDate != "") {
                retVal["DateRB"] = "0";
            } else if (retVal.LastNDays != "") {
                retVal["DateRB"] = "0";
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

            this.selectChild(this.workunitsTab, true);
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
                        currSel.init(currSel.params);
                    }
                }
            }
        },

        initWorkunitsGrid: function() {
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
			        field: "isKeyFile",
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
            var store = new WsDfu.DFUQuery();
            var objStore = new ObjectStore({ objectStore: store });
            this.workunitsGrid.setStore(objStore);
            this.workunitsGrid.setQuery(this.getFilter());

            var context = this;
            this.workunitsGrid.on("RowDblClick", function (evt) {
                if (context.onRowDblClick) {
                    var idx = evt.rowIndex;
                    var item = this.getItem(idx);
                    var Name = this.store.getValue(item, "Name");
                    context.onRowDblClick(Name);
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
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
            }

            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "AddToSuperfile").set("disabled", !hasSelection);
        },

        ensurePane: function (id, params) {
            var obj = id.split("::");
            id = obj.join("");
            obj = id.split(".");
            id = obj.join("");
            var retVal = this.tabMap[id];
            if (!retVal) {
                var context = this;
                retVal = new LFDetailsWidget({
                    id: id,
                    title: params.Name,
                    closable: true,
                    onClose: function () {
                        delete context.tabMap[id];
                        return true;
                    },
                    params: params
                });
                this.tabMap[id] = retVal;
                this.addChild(retVal, 2);
            }
            return retVal;
        },

        onRowDblClick: function (name) {
            var wuTab = this.ensurePane(this.id + "_" + name, {
                Name: name
            });
            this.selectChild(wuTab);
        }
    });
});
