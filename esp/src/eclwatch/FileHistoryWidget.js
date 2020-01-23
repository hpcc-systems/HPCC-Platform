define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",

    "dijit/registry",

    "dijit/form/Button",

    "hpcc/GridDetailsWidget",
    "src/WsDfu",
    "src/ESPUtil"
], function (declare, lang, i18n, nlsHPCC, arrayUtil,
    registry, Button,
    GridDetailsWidget, WsDfu, ESPUtil) {
    return declare("FileHistoryWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        gridTitle: nlsHPCC.History,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this._refreshActionState();
            this.refreshGrid();
            this.initTab();
        },

        _onRefresh: function (event) {
            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;

            this.openButton = registry.byId(this.id + "Open");

            this.eraseHistory = new Button({
                label: context.i18n.EraseHistory,
                onClick: function () { context._onErase(); }
            }).placeAt(this.openButton, "after");

            dojo.destroy(this.id + "Open");

            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                columns: {
                    Name: { label: this.i18n.Name, width: 70, sortable: false },
                    IP: { label: this.i18n.IP, width: 30, sortable: false },
                    Operation: { label: this.i18n.Operation, width: 30, sortable: false },
                    Owner: { label: this.i18n.Owner, width: 30, sortable: false },
                    Path: { label: this.i18n.Path, width: 70, sortable: false },
                    Timestamp: { label: this.i18n.TimeStamp, width: 30, sortable: false },
                    Workunit: { label: this.i18n.Workunit, width: 30, sortable: false }
                }
            }, domID);

            return retVal;
        },

        _onErase: function (event) {
            var context = this;
            if (confirm(this.i18n.EraseHistoryQ + "\n" + this.params.Name + "?")) {
                WsDfu.EraseHistory({
                    request: {
                        Name: context.params.Name
                    }
                }).then(function (response) {
                    if (response) {
                        context.refreshGrid();
                    }
                });
            }
        },

        refreshGrid: function () {
            var context = this;

            WsDfu.ListHistory({
                request: {
                    Name: context.params.Name
                }
            }).then(function (response) {
                var results = [];
                var newRows = [];
                if (lang.exists("ListHistoryResponse.History.Origin", response)) {
                    results = response.ListHistoryResponse.History.Origin;
                }

                if (results.length) {
                    arrayUtil.forEach(results, function (row, idx) {
                        newRows.push({
                            Name: row.Name,
                            IP: row.IP,
                            Operation: row.Operation,
                            Owner: row.Owner,
                            Path: row.Path,
                            Timestamp: row.Timestamp,
                            Workunit: row.Workunit
                        });
                    });
                }

                context.store.setData(newRows);
                context.grid.set("query", {});
            });
        }
    });
});
