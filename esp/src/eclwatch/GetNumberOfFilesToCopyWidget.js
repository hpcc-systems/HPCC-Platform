define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",

    "hpcc/GridDetailsWidget",

    "src/ESPUtil",
    "src/WsWorkunits"

], function (declare, lang, i18n, nlsHPCC, arrayUtil,
    GridDetailsWidget, ESPUtil, WsWorkunits) {
    return declare("GetNumberOfFilesToCopyWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_FilesPendingCopy,
        idProperty: "__hpcc_id",

        init: function (params) {
            var context = this;
            this.cluster = params.__hpcc_treeItem.Name;
            this._refreshActionState();
            this.refreshGrid();
        },

        createGrid: function (domID) {
            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                columns: {
                    URL: { label: this.i18n.URL, width: 180, sortable: false },
                    Status: { label: this.i18n.Status, width: 380, sortable: false },
                    NumQueryFileToCopy: { label: this.i18n.FilesPending, sortable: false }
                }
            }, domID);

            return retVal;
        },

        _onRefresh: function () {
            this.refreshGrid();
        },

        refreshGrid: function () {
            var context = this;

            WsWorkunits.WUGetNumFileToCopy({
                request: {
                    ClusterName: this.cluster
                }
            }).then(function (response) {
                var results = [];
                var newRows = [];
                if (lang.exists("WUGetNumFileToCopyResponse.Endpoints.Endpoint", response)) {
                    results = response.WUGetNumFileToCopyResponse.Endpoints.Endpoint;
                    arrayUtil.forEach(results, function (row, idx) {
                        newRows.push({
                            URL: row.URL,
                            Status: row.Status,
                            NumQueryFileToCopy: row.NumQueryFileToCopy
                        });
                    });
                }
                context.store.setData(newRows);
                context.grid.set("query", {});
            });
        }
    });
});
