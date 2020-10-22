define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/dom-class",

    "dijit/registry",

    "hpcc/GridDetailsWidget",
    "src/WsDFUXref",
    "src/ESPUtil"

], function (declare, lang, nlsHPCCMod, arrayUtil, domClass,
    registry,
    GridDetailsWidget, WsDFUXref, ESPUtil) {

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
            this.openButton = registry.byId(this.id + "Open");
            dojo.destroy(this.id + "Open");
            dojo.destroy(this.id + "RemovableSeperator2");

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
        }
    });
});
