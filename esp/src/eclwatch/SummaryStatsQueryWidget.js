define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",

    "@hpcc-js/comms",

    "hpcc/GraphsWidget",
    "src/ESPQuery"
], function (declare, lang, arrayUtil,
    hpccComms,
    GraphsWidget, ESPQuery) {

    return declare("SummaryStatsQueryWidget", [GraphsWidget], {
        query: null,

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.QuerySetId && params.Id) {
                this.query = hpccComms.Query.attach({ baseUrl: "" }, params.QuerySetId, params.Id);
                this.refreshGrid();
            }

            this._refreshActionState();
        },

        createGridColumns: function () {
            var context = this;
            return {
                Endpoint: { label: this.i18n.EndPoint, width: 72, sortable: true },
                Status: { label: this.i18n.Status, width: 72, sortable: true },
                StartTime: { label: this.i18n.StartTime, width: 160, sortable: true },
                EndTime: { label: this.i18n.EndTime, width: 160, sortable: true },
                CountTotal: { label: this.i18n.CountTotal, width: 88, sortable: true },
                CountFailed: { label: this.i18n.CountFailed, width: 80, sortable: true },
                AverageBytesOut: { label: this.i18n.MeanBytesOut, width: 80, sortable: true },
                SizeAvgPeakMemory: { label: this.i18n.SizeMeanPeakMemory, width: 88, sortable: true },
                TimeAvgTotalExecuteMinutes: { label: this.i18n.TimeMeanTotalExecuteMinutes, width: 88, sortable: true },
                TimeMinTotalExecuteMinutes: { label: this.i18n.TimeMinTotalExecuteMinutes, width: 88, sortable: true },
                TimeMaxTotalExecuteMinutes: { label: this.i18n.TimeMaxTotalExecuteMinutes, width: 88, sortable: true },
                Percentile97: { label: this.i18n.Percentile97, width: 80, sortable: true },
                Percentile97Estimate: { label: this.i18n.Percentile97Estimate, sortable: true }
            };
        },

        localParams: function (_id, row, params) {
            return {
                Target: this.query.QuerySet,
                QueryId: this.query.QueryId
            };
        },

        refreshGrid: function (args) {
            var context = this;
            this.query.fetchSummaryStats().then(function (response) {
                if (lang.exists("StatsList.QuerySummaryStats", response)) {
                    context.store.setData(response.StatsList.QuerySummaryStats);
                    context.grid.refresh();
                }
            });
        }
    });
});
