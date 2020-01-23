define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "hpcc/DelayLoadWidget",
    "src/ESPQuery",
    "src/ESPUtil"
], function (declare, lang, i18n, nlsHPCC, arrayUtil,
    selector,
    GridDetailsWidget, DelayLoadWidget, ESPQuery, ESPUtil) {
    return declare("QuerySetLogicalFilesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_QuerySetLogicalFiles,
        idProperty: "File",

        queryId: null,
        querySet: null,

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.query = ESPQuery.Get(params.QuerySetId, params.Id);

            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                columns: {
                    col1: selector({ width: 27, selectorType: 'checkbox' }),
                    File: { label: this.i18n.LogicalFiles }
                }
            }, domID);
            return retVal;
        },

        createDetail: function (id, row, params) {
            return new DelayLoadWidget({
                id: id,
                title: row.File,
                closable: true,
                delayWidget: "LFDetailsWidget",
                hpcc: {
                    type: "LFDetailsWidget",
                    params: {
                        Name: row.File
                    }
                }
            });
        },

        refreshGrid: function (args) {
            var context = this;
            this.query.refresh().then(function (response) {
                var logicalFiles = [];
                if (lang.exists("LogicalFiles.Item", context.query)) {
                    arrayUtil.forEach(context.query.LogicalFiles.Item, function (item, idx) {
                        var file = {
                            File: item
                        }
                        logicalFiles.push(file);
                    });
                }
                context.store.setData(logicalFiles);
                context.grid.refresh();
            });
        }
    });
});
