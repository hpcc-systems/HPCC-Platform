define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/WsTopology",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil"

], function (declare, lang, nlsHPCCMod,
    selector,
    GridDetailsWidget, WsTopology, DelayLoadWidget, ESPUtil) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("TpClusterInfoWidget", [GridDetailsWidget], {

        i18n: nlsHPCC,
        gridTitle: nlsHPCC.title_ClusterInfo,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;

            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox',
                        sortable: false
                    }),
                    Name: {
                        label: this.i18n.Name,
                        width: 180,
                        sortable: true,
                        formatter: function (cell, row) {
                            return "<a href='#' class='dgrid-row-url'>" + cell + "</a>";
                        }
                    },
                    WorkUnit: { label: this.i18n.WUID, sortable: true }
                }
            }, domID);
            retVal.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        createDetail: function (id, row, params) {
            return new DelayLoadWidget({
                id: id,
                title: row.Name,
                closable: true,
                delayWidget: "TpThorStatusWidget",
                hpcc: {
                    params: {
                        ClusterName: this.params.ClusterName,
                        Name: row.Name
                    }
                }
            });
        },

        refreshGrid: function () {
            var context = this;
            WsTopology.TpClusterInfo({
                request: {
                    Name: this.params.ClusterName
                }
            }).then(function (response) {
                var results = [];
                if (lang.exists("TpClusterInfoResponse.TpQueues.TpQueue", response)) {
                    results = response.TpClusterInfoResponse.TpQueues.TpQueue;
                }
                context.store.setData(results);
                context.grid.refresh();
            });
        }
    });
});
