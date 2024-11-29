define([
    "dojo/_base/declare",
    "dojo/store/Observable",

    "src/ESPRequest"
], function (declare, Observable,
    ESPRequest) {
        var GangliaMetricStore = declare([ESPRequest.Store], {
            service: "ws_rrd",
            action: "getAvailableMetrics",
            responseQualifier: "GetAvailableMetricsResponse.Metrics",
            idProperty: "Item",
        });

        var GangliaServerStore = declare([ESPRequest.Store], {
            service: "ws_rrd",
            action: "getAvailableServers",
            responseQualifier: "GetAvailableServersForMetricsResponse.Servers",
            idProperty: "Item",
        });

        return {
            GangliaClusterList: function (params) {
                return ESPRequest.send("ws_rrd", "getAvailableClusters", params);
            },

            GangliaServerList: function (params) {
                return ESPRequest.send("ws_rrd", "getAvailableServers", params);
            },

            GangliaMetricList: function (params) {
                return ESPRequest.send("ws_rrd", "getAvailableMetrics", params);
            },

            GangliaRRDGraphList: function (params) {
                return ESPRequest.send("ws_rrd", "getGraphSVG", params);
            },

            CreateGangliaServerStore: function (options) {
                var store = new GangliaServerStore(options);
                return Observable(store);
            },

            CreateGangliaMetricStore: function (options) {
                var store = new GangliaMetricStore(options);
                return Observable(store);
            }
        };
    });
