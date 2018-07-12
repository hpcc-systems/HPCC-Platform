define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/on",
    "dojo/dom",
    "dojo/dom-form",
    "dojo/dom-construct",
    "dojo/dom-class",
    "dojo/promise/all",

    "dijit/registry",
    "dijit/form/ToggleButton",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",
    "dijit/form/ValidationTextBox",
    "dijit/form/Textarea",
    "dijit/form/TextBox",
    "dijit/form/CheckBox",
    "dijit/Dialog",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/WsDFUXref",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil",
    "src/WsPackageMaps",
    "src/Utility",
    "hpcc/FilterDropDownWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/ECLSourceWidget"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, dom, domForm, domConstruct, domClass, all,
    registry, ToggleButton, ToolbarSeparator, Button, ValidationTextBox, Textarea, TextBox, CheckBox, Dialog,
    selector,
    GridDetailsWidget, WsDFUXref, DelayLoadWidget, ESPUtil, WsPackageMaps, Utility, FilterDropDownWidget, TargetSelectWidget, ECLSourceWidget) {
        return declare("PackageMapPartsWidget", [GridDetailsWidget], {
            i18n: nlsHPCC,
            gridTitle: nlsHPCC.Parts,
            idProperty: "Part",
            addPartsDropDown: null,
            addPartsDropDownLoaded: null,

            init: function (params) {
                if (this.inherited(arguments))
                    return;
                this.packageMap = params.packageMap;
                this._refreshActionState();
                this.refreshGrid();
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.addPartsSelect = registry.byId(this.id + "AddPartsSelect");
            },

            _onRefresh: function (event) {
                this.refreshGrid();
            },

            createGrid: function (domID) {
                var context = this;

                this.openButton = registry.byId(this.id + "Open");
                this.addPartsDropDown = new FilterDropDownWidget({
                    id: this.id + "AddParts",
                    disabled: false,
                    label: this.i18n.AddPart
                }).placeAt(this.openButton.domNode, "after");
                this.getPartDialog = new Dialog({
                    title: this.i18n.GetPart,
                    style: "width: 600px;"
                });
                this.addPartsDropDown.on("apply", function (evt) {
                    if (context.addPartsDropDown.filterForm.validate()) {
                        var addPartInput = context.getFilter();
                        var packageMapSearch = context.params.packageMap.search("::");
                        var packageMapClean;

                        packageMapSearch > -1 ? packageMapClean = context.params.packageMap.split('::')[1] : packageMapClean = context.params.packageMap;

                        WsPackageMaps.AddPartToPackageMap({
                            request: {
                                Target: context.params.target,
                                PackageMap: packageMapClean,
                                PartName: addPartInput.PartName,
                                Content: addPartInput.Content,
                                DaliIp: addPartInput.DaliIp,
                                SourceProcess: addPartInput.SourceProcess,
                                DeletePrevious: addPartInput.DeletePrevious === "on" ? 1 : 0,
                                AllowForeignFiles: addPartInput.AllowForeignFiles === "on" ? 1 : 0,
                                PreloadAllPackages: addPartInput.PreloadAllPackages === "on" ? 1 : 0,
                                UpdateSuperFiles: addPartInput.UpdateSuperFiles === "on" ? 1 : 0,
                                UpdateCloneFrom: addPartInput.UpdateCloneFrom === "on" ? 1 : 0,
                                AppendCluster: addPartInput.AppendCluster === "on" ? 1 : 0
                            }
                        }).then(function (response) {
                            if (lang.exists("AddPartToPackageMapResponse.status.Code", response)) {
                                context.refreshGrid();
                                context.addPartsDropDown.filterDropDown.set("label", context.i18n.Add);
                            }
                        });
                    }
                });
                dojo.destroy(this.addPartsDropDown.iconFilter);
                this.addPartsDropDown.placeAt(this.openButton.domNode, "after");
                this.addPartsDropDown.filterForm.set("style", "width:600px;");
                this.addPartsDropDown.filterDropDown.set("label", context.i18n.Add);
                this.addPartsPartName = this.createLabelAndElement("PartName", this.i18n.PartName, "ValidationTextBox", this.i18n.PartName);
                this.addPartsContent = this.createLabelAndElement("Content", this.i18n.Content, "Textarea", this.i18n.Content);
                this.addPartsDaliIp = this.createLabelAndElement("DaliIp", this.i18n.DaliIP, "TextBox", this.i18n.DaliIP);
                this.addPartsSourceProcess = this.createLabelAndElement("SourceProcess", this.i18n.SourceProcess, "TextBox", this.i18n.SourceProcess);
                this.addPartsDeletePrevious = this.createLabelAndElement("DeletePrevious", this.i18n.DeletePrevious, "CheckBox", this.i18n.DeletePrevious);
                this.addPartsAllowForeign = this.createLabelAndElement("AllowForeignFiles", this.i18n.AllowForeignFiles, "CheckBox", this.i18n.AllowForeignFiles);
                this.addPartsPreloadAllPackages = this.createLabelAndElement("PreloadAllPackages", this.i18n.PreloadAllPackages, "CheckBox", this.i18n.PreloadAllPackages);
                this.addPartsUpdateSuperFiles = this.createLabelAndElement("UpdateSuperFiles", this.i18n.UpdateSuperFiles, "CheckBox", this.i18n.UpdateSuperFiles);
                this.addPartsUpdateCloneFrom = this.createLabelAndElement("UpdateCloneFrom", this.i18n.UpdateCloneFrom, "CheckBox", this.i18n.UpdateCloneFrom);
                this.addPartsAppendCluster = this.createLabelAndElement("AppendCluster", this.i18n.AppendCluster, "CheckBox", this.i18n.AppendCluster);

                this.removeParts = new Button({
                    id: this.id + "RemoveParts",
                    disabled: true,
                    onClick: function (val) {
                        context._onRemovePart();
                    },
                    label: this.i18n.RemovePart
                }).placeAt(this.id + "AddParts", "after");

                this.getParts = new Button({
                    id: this.id + "GetParts",
                    disabled: true,
                    onClick: function (val) {
                        context._onGetPart();
                    },
                    label: this.i18n.GetPart
                }).placeAt(this.id + "RemoveParts", "after");
                dojo.destroy(this.id + "Open");

                new ToolbarSeparator().placeAt(this.id + "RemoveParts", "after");
                var retVal = new declare([ESPUtil.Grid(true, true)])({
                    store: this.store,
                    columns: {
                        col1: selector({
                            width: 27,
                            selectorType: 'checkbox',
                            label: ""
                        }),
                        Part: { label: this.i18n.Parts, sortable: false }
                    }
                }, domID);

                retVal.on(".dgrid-row:dblclick", function (evt) {
                    if (context._onRowDblClick) {
                        var item = retVal.row(evt).data;
                        WsPackageMaps.GetPartFromPackageMap({
                            request: {
                                Target: context.params.target,
                                PackageMap: context.params.packageMap.split('::')[1],
                                PartName: item.Part
                            }
                        }).then(function (response) {
                            var nameTab = context.ensurePane(item.Part, {
                                Part: item.Part,
                                PartContent: response.GetPartFromPackageMapResponse.Content
                            });
                            context.selectChild(nameTab);
                        });
                    }
                });
                return retVal;
            },

            _onRemovePart: function (event) {
                var context = this;
                var selections = this.grid.getSelected();
                var list = this.arrayToList(selections, "Part");
                if (confirm(this.i18n.YouAreAboutToDeleteThisPart + "\n" + list)) {
                    var promises = [];
                    arrayUtil.forEach(selections, function (row, idx) {
                        promises.push(WsPackageMaps.RemovePartFromPackageMap({
                            request: {
                                Target: context.params.target,
                                PackageMap: context.params.packageMap.split('::')[1],
                                PartName: row.Part
                            }
                        }));
                    });
                    all(promises).then(function () {
                        context._onRefresh();
                    });
                }
            },

            _onGetPart: function (event) {
                var context = this;
                var selections = this.grid.getSelected();
                WsPackageMaps.GetPartFromPackageMap({
                    request: {
                        Target: context.params.target,
                        PackageMap: context.params.packageMap.split('::')[1],
                        PartName: selections[0].Part
                    }
                }).then(function (response) {
                    var nameTab = context.ensurePane(selections[0].Part, {
                        Part: selections[0].Part,
                        PartContent: response.GetPartFromPackageMapResponse.Content
                    });
                    context.selectChild(nameTab);
                });
            },

            getFilter: function () {
                return this.addPartsDropDown.toObject();
            },

            createLabelAndElement: function (id, label, element, placeholder, value) {
                var context = this;
                var control = null;
                switch (element) {
                    case "CheckBox":
                        control = new CheckBox({
                            id: id,
                            name: id,
                            checked: true,
                            title: label
                        });
                        break;
                    case "Textarea":
                        control = new Textarea({
                            id: id,
                            name: id,
                            title: label,
                            style: "height: 20%;width:40%;"
                        });
                        break;
                    case "ValidationTextBox":
                        control = new ValidationTextBox({
                            id: id,
                            name: id,
                            placeholder: placeholder,
                            style: "width: 40%",
                            value: value,
                            required: true
                        });
                        break;
                    case "TextBox":
                        control = new TextBox({
                            id: id,
                            name: id,
                            placeholder: placeholder,
                            style: "width: 40%",
                            value: value
                        });
                        break;
                }

                if (control) {
                    this.addPartsDropDown.tableContainer.domNode.appendChild(
                        dojo.create(label ? "div" : "span", {
                            id: this.id + id,
                            innerHTML: label ? "<label for=" + control + " style='float:left;width:40%'>" + label + ":</label>" : '',
                            style: "vertical-align:middle;padding:2px 0 2px 5px;"
                        })
                    );
                    control.placeAt(this.id + id);
                }
            },

            refreshActionState: function (event) {
                var selection = this.grid.getSelected();
                var hasSelection = selection.length;
                var hasMultipleSelection = selection.length === 1;

                registry.byId(this.id + "RemoveParts").set("disabled", !hasSelection);
                registry.byId(this.id + "GetParts").set("disabled", !hasMultipleSelection);
            },

            refreshGrid: function () {
                var context = this;

                WsPackageMaps.GetPackageMapByIdUpdated({
                    request: {
                        PackageMapId: context.packageMap
                    }
                }).then(function (response) {
                    var results = [];
                    var newRows = [];
                    if (lang.exists("GetPackageMapByIdResponse.Info", response)) {
                        var xmlConversion = Utility.parseXML(response.GetPackageMapByIdResponse.Info);
                        var items = xmlConversion.getElementsByTagName('Part');
                        var tempObj = {}
                        for (var i = 0; i < items.length; i++) {
                            newRows.push(tempObj[i] = { Part: items[i].attributes[0].nodeValue });
                        }
                    }
                    context.store.setData(newRows);
                    context.grid.set("query", {});
                });
            },

            ensurePane: function (id, params) {
                id = this.createChildTabID(id);
                var retVal = registry.byId(id);
                if (!retVal) {
                    var context = this;
                    retVal = new DelayLoadWidget({
                        id: id,
                        title: params.Part,
                        closable: true,
                        delayWidget: "ECLSourceWidget",
                        hpcc: {
                            params: {
                                sourceMode: "xml",
                                Usergenerated: params.PartContent
                            }
                        }
                    });
                    this.addChild(retVal, 1);
                }
                return retVal;
            }
        });
    });