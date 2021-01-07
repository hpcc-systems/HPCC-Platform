define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",

    "dgrid/selector",

    "hpcc/GraphsWidget",
    "src/ESPQuery"
], function (declare, lang, arrayUtil,
    selector,
    GraphsWidget, ESPQuery) {

    return declare("GraphsQueryWidget", [GraphsWidget], {
        query: null,

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.QuerySetId && params.Id) {
                this.query = ESPQuery.Get(params.QuerySetId, params.Id);
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
                },
                Type: { label: this.i18n.Type, width: 72, sortable: true }
            };
        },

        localParams: function (_id, row, params) {
            params.legacyMode = true;
            return {
                Target: this.query.QuerySet,
                QueryId: this.query.QueryId,
                GraphName: row.Name,
                SubGraphId: (params && params.SubGraphId) ? params.SubGraphId : null,
                SafeMode: (params && params.safeMode) ? true : false
            };
        },

        refreshGrid: function (args) {
            var context = this;
            this.query.refresh().then(function (response) {
                var graphs = [];
                if (lang.exists("WUGraphs.ECLGraph", context.query)) {
                    arrayUtil.forEach(context.query.WUGraphs.ECLGraph, function (item, idx) {
                        var graph = {
                            Name: item.Name,
                            Label: "",
                            Completed: "",
                            Time: 0,
                            Type: item.Type
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
