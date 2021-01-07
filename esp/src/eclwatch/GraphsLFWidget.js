define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",

    "dgrid/selector",

    "hpcc/GraphsWidget",
    "src/ESPLogicalFile"
], function (declare, lang, arrayUtil,
    selector,
    GraphsWidget, ESPLogicalFile) {

    return declare("GraphsLFWidget", [GraphsWidget], {

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.NodeGroup && params.LogicalName) {
                this.logicalFile = ESPLogicalFile.Get(params.NodeGroup, params.LogicalName);
                this.refreshGrid();
            }

            this._refreshActionState();
        },

        createGridColumns: function () {
            var context = this;
            return {
                col1: selector({
                    width: 27,
                    selectorType: "checkbox"
                }),
                Name: {
                    label: this.i18n.Name, sortable: true,
                    formatter: function (Name, row) {
                        return context.getStateImageHTML(row) + "&nbsp;<a href='#' onClick='return false;' class='dgrid-row-url'>" + Name + "</a>";
                    }
                }
            };
        },

        localParams: function (_id, row, params) {
            return {
                Wuid: this.logicalFile.Wuid,
                GraphName: row.Name,
                SubGraphId: (params && params.SubGraphId) ? params.SubGraphId : null,
                SafeMode: (params && params.safeMode) ? true : false
            };
        },

        refreshGrid: function (args) {
            var context = this;
            this.logicalFile.getInfo().then(function () {
                var graphs = [];
                if (lang.exists("Graphs.ECLGraph", context.logicalFile)) {
                    arrayUtil.forEach(context.logicalFile.Graphs.ECLGraph, function (item, idx) {
                        var graph = {
                            Name: item,
                            Label: "",
                            Completed: "",
                            Time: 0,
                            Type: ""
                        };
                        graphs.push(graph);
                    });
                }
                context.store.setData(graphs);
                context.grid.refresh();
            });
        }
    });
});
