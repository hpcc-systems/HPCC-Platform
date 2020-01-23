define([
    "exports",
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/promise/all",

    "dijit/registry",

    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "src/ESPUtil",
    "src/ESPLogicalFile",
    "hpcc/DelayLoadWidget",
    "src/Utility",

    "dojo/text!../templates/SFDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/form/Form",
    "dijit/form/CheckBox",
    "dijit/form/SimpleTextarea",
    "dijit/form/TextBox",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/ToggleButton",
    "dijit/TitlePane"
], function (exports, declare, i18n, nlsHPCC, arrayUtil, dom, domAttr, domClass, domForm, Memory, Observable, all,
    registry,
    selector,
    _TabContainerWidget,
    ESPUtil, ESPLogicalFile, DelayLoadWidget, Utility,
    template) {
    exports.fixCircularDependency = declare("SFDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "SFDetailsWidget",
        i18n: nlsHPCC,

        borderContainer: null,
        tabContainer: null,
        summaryWidget: null,
        subfilesGrid: null,

        logicalFile: null,
        prevState: "",

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.deleteBtn = registry.byId(this.id + "Delete");
            this.removeBtn = registry.byId(this.id + "Remove");
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
            var protectedCheckbox = registry.byId(this.id + "isProtected");
            this.logicalFile.save({
                Description: dom.byId(context.id + "Description").value,
                isProtected: protectedCheckbox.get("checked")
            }, null);
        },
        _onDelete: function (event) {
            if (confirm(this.i18n.DeleteSuperfile)) {
                this.logicalFile.removeSubfiles(this.subfilesGrid.store.data, true);
            }
        },
        _onRemove: function (event) {
            if (confirm(this.i18n.RemoveSubfiles2)) {
                this.logicalFile.removeSubfiles(this.subfilesGrid.getSelected());
            }
        },
        _onOpen: function (event) {
            var selections = this.subfilesGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensureLFPane(selections[i].Name, selections[i]);
                if (i === 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab, true);
            }
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
            if (this.inherited(arguments))
                return;

            var context = this;
            if (params.Name) {
                this.logicalFile = ESPLogicalFile.Get("", params.Name);
                var data = this.logicalFile.getData();
                for (var key in data) {
                    this.updateInput(key, null, data[key]);
                }
                this.logicalFile.watch(function (name, oldValue, newValue) {
                    context.updateInput(name, oldValue, newValue);
                });
                this.logicalFile.refresh();
            }
            this.subfilesGrid.startup();
        },

        initSubfilesGrid: function () {
            var context = this;
            var store = new Memory({
                idProperty: "Name",
                data: []
            });
            this.subfilesStore = Observable(store);
            this.subfilesGrid = new declare([ESPUtil.Grid(false, true)])({
                columns: {
                    sel: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
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
                    isSuperfile: {
                        width: 25, sortable: false,
                        renderHeaderCell: function (node) {
                            node.innerHTML = Utility.getImageHTML("superfile.png", context.i18n.Superfile);
                        },
                        formatter: function (superfile) {
                            if (superfile === true) {
                                return Utility.getImageHTML("superfile.png");
                            }
                            return "";
                        }
                    },
                    Name: {
                        label: this.i18n.LogicalName,
                        formatter: function (name, row) {
                            return "<a href='#' class='dgrid-row-url'>" + name + "</a>";
                        }
                    },
                    Owner: { label: this.i18n.Owner, width: 72 },
                    Description: { label: this.i18n.Description, width: 153 },
                    RecordCount: {
                        label: this.i18n.Records, width: 72, sortable: false,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = Utility.valueCleanUp(value);
                        },
                    },
                    Totalsize: {
                        label: this.i18n.Size, width: 72, sortable: false,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = Utility.valueCleanUp(value);
                        },
                    },
                    Parts: {
                        label: this.i18n.Parts, width: 45, sortable: false,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = value;
                        },
                    },
                    Modified: { label: this.i18n.ModifiedUTCGMT, width: 155, sortable: false }
                },
                store: this.subfilesStore
            }, this.id + "SubfilesGrid");
            var context = this;
            this.subfilesGrid.on(".dgrid-row-url:click", function (evt) {
                var item = context.subfilesGrid.row(evt).data;
                var tab = context.ensureLFPane(item.Name, item);
                context.selectChild(tab, true);
            });
            this.subfilesGrid.on("dgrid-select", function (evt) {
                context.deleteBtn.set("disabled", true);
                context.removeBtn.set("disabled", false);
            });
            this.subfilesGrid.on("dgrid-deselect", function (evt) {
                var selections = context.subfilesGrid.getSelected();
                if (selections.length === 0) {
                    context.deleteBtn.set("disabled", false);
                    context.removeBtn.set("disabled", true);
                }
            });
            this.subfilesGrid.on(".dgrid-row:dblclick", function (evt) {
                var item = context.subfilesGrid.row(evt).data;
                var tab = context.ensureLFPane(item.Name, item);
                context.selectChild(tab, true);
            });
            this.subfilesGrid.startup();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.summaryWidget.id) {
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel._hpccParams);
                    }
                }
            }
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
                            dom.byId(this.id + name).textContent = newValue;
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
                var dataPromise = [];
                var data = [];
                arrayUtil.forEach(newValue.Item, function (item, idx) {
                    var logicalFile = ESPLogicalFile.Get("", item);
                    dataPromise.push(logicalFile.getInfo2({
                        onAfterSend: function (response) {
                        }
                    }));
                    data.push(logicalFile);
                });
                var context = this;
                all(dataPromise).then(function (logicalFiles) {
                    context.subfilesStore.setData(data);
                    context.subfilesGrid.refresh();
                })
            } else if (name === "StateID") {
                this.summaryWidget.set("iconClass", this.logicalFile.getStateIconClass());
                domClass.remove(this.id + "StateIdImage");
                domClass.add(this.id + "StateIdImage", this.logicalFile.getStateIconClass());
            } else if (name === "ProtectList") {
                dom.byId(this.id + "ProtectedImage").src = this.logicalFile.getProtectedImage();
            } else if (name === "IsProtected") {
                this.updateInput("isProtected", oldValue, newValue);
            } else if (name === "IsCompressed") {
                dom.byId(this.id + "CompressedImage").src = this.logicalFile.getCompressedImage();
            }
        },

        ensureLFPane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                retVal = new DelayLoadWidget({
                    id: id,
                    title: params.Name,
                    closable: true,
                    delayWidget: params.isSuperfile ? "SFDetailsWidget" : "LFDetailsWidget",
                    _hpccParams: {
                        NodeGroup: params.NodeGroup,
                        Name: params.Name
                    }
                });
                this.addChild(retVal, 1);
            }
            return retVal;
        }
    });
});
