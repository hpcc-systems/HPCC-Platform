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
    "exports",
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/query",
    "dojo/store/Memory",
    "dojo/data/ObjectStore",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/form/Form",
    "dijit/form/SimpleTextarea",
    "dijit/form/TextBox",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/TitlePane",
    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "hpcc/ResultWidget",
    "hpcc/ECLSourceWidget",
    "hpcc/FilePartsWidget",
    "hpcc/WUDetailsWidget",
    "hpcc/DFUWUDetailsWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/ESPLogicalFile",

    "dojo/text!../templates/SFDetailsWidget.html",

    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/Pagination",
    "dojox/grid/enhanced/plugins/IndirectSelection",

    "dijit/TooltipDialog"
], function (exports, declare, lang, arrayUtil, dom, domAttr, domClass, domForm, query, Memory, ObjectStore,
                _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, Toolbar, TooltipDialog, Form, SimpleTextarea, TextBox, Button, DropDownButton, TitlePane, registry,
                _TabContainerWidget, ResultWidget, EclSourceWidget, FilePartsWidget, WUDetailsWidget, DFUWUDetailsWidget, TargetSelectWidget,
                ESPLogicalFile,
                template) {
    exports.fixCircularDependency = declare("SFDetailsWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "SFDetailsWidget",
        borderContainer: null,
        tabContainer: null,
        summaryWidget: null,
        subfilesGrid: null,

        logicalFile: null,
        prevState: "",
        initalized: false,

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.subfilesGrid = registry.byId(this.id + "SubfilesGrid");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initSubfilesGrid();
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.logicalFile.refresh();
        },
        _onSave: function (event) {
            var context = this;
            this.logicalFile.save(dom.byId(context.id + "Description").value);
        },
        _onDelete: function (event) {
            if (confirm('Delete Superfile?')) {
                this.logicalFile.removeSubfiles(this.subfilesGrid.store.objectStore.data, true);
            }
        },
        _onRemove: function (event) {
            this.logicalFile.removeSubfiles(this.subfilesGrid.selection.getSelected());
        },
        _onCopyOk: function (event) {
            this.logicalFile.copy({
                request: domForm.toObject(this.id + "CopyDialog")
            });
            registry.byId(this.id + "CopyDropDown").closeDropDown();
        },
        _onCopyCancel: function (event) {
            registry.byId(this.id + "CopyDropDown").closeDropDown();
        },
        _onDesprayOk: function (event) {
            this.logicalFile.despray({
                request: domForm.toObject(this.id + "DesprayDialog")
            });
            registry.byId(this.id + "DesprayDropDown").closeDropDown();
        },
        _onDesprayCancel: function (event) {
            registry.byId(this.id + "DesprayDropDown").closeDropDown();
        },
        _onRenameOk: function (event) {
            this.logicalFile.rename({
                request: domForm.toObject(this.id + "RenameDialog")
            });
            registry.byId(this.id + "RenameDropDown").closeDropDown();
        },
        _onRenameCancel: function (event) {
            registry.byId(this.id + "RenameDropDown").closeDropDown();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;

            var context = this;
            if (params.Name) {
                //dom.byId(this.id + "Name").innerHTML = params.Name;
                //dom.byId(this.id + "LogicalFileName2").value = params.Name;
                this.logicalFile = ESPLogicalFile.Get(params.Name);
                var data = this.logicalFile.getData();
                for (key in data) {
                    this.updateInput(key, null, data[key]);
                }
                this.logicalFile.watch(function (name, oldValue, newValue) {
                    context.updateInput(name, oldValue, newValue);
                });
                this.logicalFile.refresh();
            }
            this.selectChild(this.summaryWidget, true);
            this.subfilesGrid.startup();
        },

        initSubfilesGrid: function () {
            this.subfilesGrid.setStructure([
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
            /*
            var objStore = ESPLogicalFile.CreateLFQueryObjectStore();
            this.subfilesGrid.setStore(objStore);
            this.subfilesGrid.setQuery(this.getFilter());

            var context = this;
            this.subfilesGrid.on("RowDblClick", function (evt) {
                if (context.onRowDblClick) {
                    var idx = evt.rowIndex;
                    var item = this.getItem(idx);
                    context.onRowDblClick(item);
                }
            }, true);

            dojo.connect(this.subfilesGrid.selection, 'onSelected', function (idx) {
                context.refreshActionState();
            });
            dojo.connect(this.subfilesGrid.selection, 'onDeselected', function (idx) {
                context.refreshActionState();
            });
            */
            this.subfilesGrid.startup();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
        },

        showMessage: function (msg) {
        },

        updateInput: function (name, oldValue, newValue) {
            var domElem = dom.byId(this.id + name);
            if (domElem) {
                switch(domElem.tagName) {
                    case "SPAN":
                    case "DIV":
                        domAttr.set(this.id + name, "innerHTML", newValue)
                        break;
                    case "INPUT":
                    case "TEXTAREA":
                        domAttr.set(this.id + name, "value", newValue)
                        break;
                    default:
                        alert(domElem.tagName);
                }
            }
            if (name === "subfiles") {
                var data = [];
                arrayUtil.forEach(newValue.Item, function (item, idx) {
                    data.push(ESPLogicalFile.Get(item));
                });

                this.subfilesGrid.rowSelectCell.toggleAllSelection(false);
                var dataStore = new ObjectStore({ objectStore: new Memory({ data: data }) });
                this.subfilesGrid.setStore(dataStore);
                this.subfilesGrid.setQuery({
                    Name: "*"
                });
            }
        }
    });
});
