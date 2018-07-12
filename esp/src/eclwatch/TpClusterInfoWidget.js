define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/WsTopology",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil"

], function (declare, lang, i18n, nlsHPCC,
    selector,
    GridDetailsWidget, WsTopology, DelayLoadWidget, ESPUtil) {
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
                        Name: { label: this.i18n.Name, width: 180, sortable: true },
                        WorkUnit: { label: this.i18n.WUID, sortable: true }
                    }
                }, domID);
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
