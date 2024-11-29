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
    return declare("XrefErrorsWarningsWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        gridTitle: nlsHPCC.title_ErrorsWarnings,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this._refreshActionState();
            this.refreshGrid();

            this.gridTab.set("title", this.i18n.title_ErrorsWarnings + ":" + this.params.Name);
        },

        _onRefresh: function (event) {
            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;
            this.openButton = registry.byId(this.id + "Open");
            dojo.destroy(this.id + "Open");
            dojo.destroy(this.id + "RemovableSeperator2");

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
                    File: { label: this.i18n.File, width: 100, sortable: false },
                    Text: { label: this.i18n.Message, width: 100, sortable: false },
                    Status: {
                        label: this.i18n.Status, width: 10, sortable: true,
                        renderCell: function (object, value, node, options) {
                            switch (value) {
                                case "Error":
                                    domClass.add(node, "ErrorCell");
                                    break;
                                case "Warning":
                                    domClass.add(node, "WarningCell");
                                    break;
                                case "Normal":
                                    domClass.add(node, "NormalCell");
                                    break;
                            }
                            node.innerText = value;
                        }
                    }
                }
            }, domID);

            return retVal;
        },

        refreshGrid: function () {
            var context = this;

            WsDFUXref.DFUXRefMessages({
                request: {
                    Cluster: this.params.Name
                }
            }).then(function (response) {
                var results = [];
                var newRows = [];
                if (lang.exists("DFUXRefMessagesQueryResponse.DFUXRefMessagesQueryResult", response)) {
                    results = response.DFUXRefMessagesQueryResponse.DFUXRefMessagesQueryResult;
                }

                if (lang.exists("Warning.length", results)) {
                    arrayUtil.forEach(results.Warning, function (row, idx) {
                        newRows.push({
                            File: row.File,
                            Text: row.Text,
                            Status: context.i18n.Warning
                        });
                    });
                } else if (results.Warning) {
                    newRows.push({
                        File: results.Warning.File,
                        Text: results.Warning.Text,
                        Status: context.i18n.Warning
                    });
                }
                if (lang.exists("Error.length", results)) {
                    arrayUtil.forEach(results.Error, function (row, idx) {
                        newRows.push({
                            File: row.File,
                            Text: row.Text,
                            Status: context.i18n.Error
                        });
                    });
                } else if (results.Error) {
                    newRows.push({
                        File: results.Error.File,
                        Text: results.Error.Text,
                        Status: context.i18n.Error
                    });
                }
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
                let rowData = [cell.File, cell.Text, cell.Status];
                row.push(rowData);
            });

            Utility.downloadToCSV(this.grid, row, fileName);
            this._onDownloadToListCancelDialog();
        }
    });
});
