define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/on",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-class",

    "dijit/registry",
    "dijit/form/ToggleButton",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",

    "hpcc/GridDetailsWidget",
    "src/WsDFUXref",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, dom, domConstruct, domClass,
    registry, ToggleButton, ToolbarSeparator, Button,
    GridDetailsWidget, WsDFUXref, DelayLoadWidget, ESPUtil) {
        return declare("XrefDirectoriesWidget", [GridDetailsWidget], {
            i18n: nlsHPCC,
            gridTitle: nlsHPCC.title_DirectoriesFor + ":",
            idProperty: "Name",

            init: function (params) {
                if (this.inherited(arguments))
                    return;
                this._refreshActionState();
                this.refreshGrid();

                this.gridTab.set("title", this.i18n.title_DirectoriesFor + ":" + this.params.Name);
            },

            _onRefresh: function (event) {
                this.refreshGrid();
            },

            createGrid: function (domID) {
                var context = this;

                this.openButton = registry.byId(this.id + "Open");
                this.deleteDirectories = new Button({
                    id: this.id + "Delete",
                    disabled: false,
                    onClick: function (val) {
                        if (confirm(context.i18n.DeleteDirectories)) {
                            var selections = context.grid.getSelected();

                            WsDFUXref.DFUXRefCleanDirectories({
                                request: {
                                    Cluster: context.params.Name
                                }
                            }).then(function (response) {
                                if (response) {
                                    context.refreshGrid();
                                }
                            })

                        }
                    },
                    label: this.i18n.DeleteEmptyDirectories
                }).placeAt(this.openButton.domNode, "after");
                dojo.destroy(this.id + "Open");

                var retVal = new declare([ESPUtil.Grid(true, true)])({
                    store: this.store,
                    columns: {
                        Name: { label: this.i18n.Directory, width: 100, sortable: false },
                        Num: { label: this.i18n.Files, width: 30, sortable: false },
                        Size: { label: this.i18n.TotalSize, width: 30, sortable: false },
                        MaxIP: { label: this.i18n.MaxNode, width: 30, sortable: false },
                        MaxSize: { label: this.i18n.MaxSize, width: 30, sortable: false },
                        MinIP: { label: this.i18n.MinNode, width: 30, sortable: false },
                        MinSize: { label: this.i18n.MinSize, width: 30, sortable: false },
                        PositiveSkew: { label: this.i18n.SkewPositive, width: 30, sortable: true },
                        NegativeSkew: { label: this.i18n.SkewNegative, width: 30, sortable: true }
                    }
                }, domID);

                return retVal;
            },

            refreshGrid: function () {
                var context = this;

                WsDFUXref.DFUXRefDirectories({
                    request: {
                        Cluster: this.params.Name
                    }
                }).then(function (response) {
                    var results = [];
                    var newRows = [];
                    if (lang.exists("DFUXRefDirectoriesQueryResponse.DFUXRefDirectoriesQueryResult.Directory", response)) {
                        results = response.DFUXRefDirectoriesQueryResponse.DFUXRefDirectoriesQueryResult.Directory;
                    }
                    arrayUtil.forEach(results, function (row, idx) {
                        newRows.push({
                            Name: row.Name,
                            Num: row.Num,
                            Size: row.Size,
                            MaxIP: row.MaxIP,
                            MaxSize: row.MaxSize,
                            MinIP: row.MinIP,
                            MinSize: row.MinSize,
                            PositiveSkew: row.PositiveSkew,
                            NegativeSkew: row.NegativeSkew
                        });
                    });
                    context.store.setData(newRows);
                    context.grid.set("query", {});
                });
            }
        });
    });