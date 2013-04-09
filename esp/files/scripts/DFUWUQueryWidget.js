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

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

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

], function (declare, lang, arrayUtil, dom, domForm, ObjectStore, date, on,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                EnhancedGrid, Pagination, IndirectSelection,
                _TabContainerWidget, WsDfu, ESPLogicalFile, LFDetailsWidget, SFDetailsWidget,
                template) {
    return declare("DFUWUQueryWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "DFUWUQueryWidget",
        workunitsTab: null,
        workunitsGrid: null,

        tabMap: [],

        postCreate: function (args) {
            this.inherited(arguments);
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.workunitsGrid = registry.byId(this.id + "WorkunitsGrid");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.refreshActionState();
            this.initWorkunitsGrid();
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
        _onFilterApply: function (event) {
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);
            this.refreshGrid();
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

            this.clusterTargetSelect.init({
                Groups: true,
                includeBlank: true
            });
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
            registry.byId(this.id + "AddtoDropDown").set("disabled", !hasSelection);
        },

        ensurePane: function (id, params) {
            var obj = id.split("::");
            id = obj.join("");
            obj = id.split(".");
            id = obj.join("");
            var retVal = this.tabMap[id];
            if (!retVal) {
                retVal = registry.byId(id);
                if (!retVal) {
                    var context = this;
                    if (params.isSuperfile) {
                        retVal = new SFDetailsWidget.fixCircularDependency({
                            id: id,
                            title: params.Name,
                            closable: true,
                            onClose: function () {
                                //  Workaround for http://bugs.dojotoolkit.org/ticket/16475
                                context._tabContainer.removeChild(this);
                                delete context.tabMap[this.id];
                                return false;
                            },
                            _hpccParams: params
                        });
                    } else {
                        retVal = new LFDetailsWidget.fixCircularDependency({
                            id: id,
                            title: params.Name,
                            closable: true,
                            onClose: function () {
                                //  Workaround for http://bugs.dojotoolkit.org/ticket/16475
                                context._tabContainer.removeChild(this);
                                delete context.tabMap[this.id];
                                return false;
                            },
                            _hpccParams: params
                        });
                    }
                }
                this.tabMap[id] = retVal;
                this.addChild(retVal, 1);
            }
            return retVal;
        },

        onRowDblClick: function (item) {
            var wuTab = this.ensurePane(this.id + "_" + item.Name, item);
            this.selectChild(wuTab);
        }
    });
});
