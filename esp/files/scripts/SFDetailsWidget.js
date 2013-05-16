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
    "dojo/store/Observable",

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

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/_TabContainerWidget",
    "hpcc/ResultWidget",
    "hpcc/ECLSourceWidget",
    "hpcc/FilePartsWidget",
    "hpcc/WUDetailsWidget",
    "hpcc/DFUWUDetailsWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/ESPUtil",
    "hpcc/ESPLogicalFile",

    "dojo/text!../templates/SFDetailsWidget.html",

    "dijit/TooltipDialog"
], function (exports, declare, lang, arrayUtil, dom, domAttr, domClass, domForm, query, Memory, Observable,
                _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, Toolbar, TooltipDialog, Form, SimpleTextarea, TextBox, Button, DropDownButton, TitlePane, registry,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                _TabContainerWidget, ResultWidget, EclSourceWidget, FilePartsWidget, WUDetailsWidget, DFUWUDetailsWidget, TargetSelectWidget,
                ESPUtil, ESPLogicalFile,
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
            this.logicalFile.removeSubfiles(this.subfilesGrid.getSelected());
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
            var store = new Memory({
                idProperty: "Name",
                data: []
            });
            this.subfilesStore = Observable(store);

            this.subfilesGrid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                columns: {
                    sel: selector({
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
                    Name: { label: "Logical Name" },
                    Owner: { label: "Owner", width: 72 },
                    Description: { label: "Description", width: 153 },
                    ClusterName: { label: "Cluster", width: 108 },
                    RecordCount: { label: "Records", width: 72, sortable: false },
                    Totalsize: { label: "Size", width: 72, sortable: false },
                    Parts: { label: "Parts", width: 45, sortable: false },
                    Modified: { label: "Modified (UTC/GMT)", width: 155, sortable: false }
                },
                store: this.subfilesStore
            }, this.id + "SubfilesGrid");
            this.subfilesGrid.startup();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
        },

        showMessage: function (msg) {
        },

        updateInput: function (name, oldValue, newValue) {
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            } else {
                var domElem = dom.byId(this.id + name);
                if (domElem) {
                    switch (domElem.tagName) {
                        case "SPAN":
                        case "DIV":
                            domAttr.set(this.id + name, "innerHTML", newValue);
                            break;
                        case "INPUT":
                        case "TEXTAREA":
                            domAttr.set(this.id + name, "value", newValue);
                            break;
                        default:
                            alert(domElem.tagName);
                    }
                }
            }
            if (name === "subfiles") {
                var data = [];
                arrayUtil.forEach(newValue.Item, function (item, idx) {
                    data.push(ESPLogicalFile.Get(item));
                });
                this.subfilesStore.setData(data);
                this.subfilesGrid.refresh();
            }
        }
    });
});
