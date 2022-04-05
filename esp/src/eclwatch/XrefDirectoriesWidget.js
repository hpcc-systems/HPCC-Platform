define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/dom-class",

    "dijit/registry",
    "dijit/form/Button",

    "hpcc/GridDetailsWidget",
    "src/WsDFUXref",
    "src/ESPUtil",
    "src/Utility"

], function (declare, lang, nlsHPCCMod, arrayUtil, domClass,
    registry, Button,
    GridDetailsWidget, WsDFUXref, ESPUtil, Utility) {

    var nlsHPCC = nlsHPCCMod.default;
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
                        });

                    }
                },
                label: this.i18n.DeleteEmptyDirectories
            }).placeAt(this.openButton.domNode, "after");
            dojo.destroy(this.id + "Open");

            this.newPageButton = registry.byId(this.id + "NewPage");
            this.downloadCsv = new Button({
                id: this.id + "DownloadToList",
                disabled: false,
                onClick: function (val) {
                    context._onDownloadToList();
                },
                label: this.i18n.DownloadToCSV
            }).placeAt(this.newPageButton.domNode, "after");
            dojo.addClass(this.downloadCsv.domNode, "right");

            this.downloadToList = registry.byId(this.id + "DownloadToList");
            this.downloadToListDialog = registry.byId(this.id + "DownloadToListDialog");
            this.downListForm = registry.byId(this.id + "DownListForm");
            this.fileName = registry.byId(this.id + "FileName");

            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                columns: {
                    Name: { label: this.i18n.Directory, width: 100, sortable: false },
                    Num: { label: this.i18n.Files, width: 30, sortable: false, className: "justify-right" },
                    Size: { label: this.i18n.TotalSize, width: 30, sortable: false, className: "justify-right" },
                    MaxIP: { label: this.i18n.MaxNode, width: 30, sortable: false },
                    MaxSize: { label: this.i18n.MaxSize, width: 30, sortable: false, className: "justify-right" },
                    MinIP: { label: this.i18n.MinNode, width: 30, sortable: false },
                    MinSize: { label: this.i18n.MinSize, width: 30, sortable: false, className: "justify-right" },
                    PositiveSkew: {
                        label: this.i18n.SkewPositive, width: 30, sortable: true,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            if (value === undefined) {
                                return "";
                            }
                            node.innerText = value;
                        },
                    },
                    NegativeSkew: {
                        label: this.i18n.SkewNegative, width: 30, sortable: true,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            if (value === undefined) {
                                return "";
                            }
                            node.innerText = value;
                        },
                    }
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
        },

        _onDownloadToListCancelDialog: function (event) {
            this.downloadToListDialog.hide();
        },

        _onDownloadToList: function (event) {
            this.downloadToListDialog.show();
        },

        _buildCSV: async function (event) {
            let data = await this.grid.store.query({});
            let row = [];
            let fileName = this.fileName.get("value") + ".csv";

            arrayUtil.forEach(data, function (cell, idx) {
                let rowData = [cell.Name, cell.Num, cell.Size, cell.MaxIP, cell.MaxSize, cell.MinIP, cell.MinSize, cell.PositiveSkew, cell.NegativeSkew];
                row.push(rowData);
            });

            Utility.downloadToCSV(this.grid, row, fileName);
            this._onDownloadToListCancelDialog();
        }
    });
});
