define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/_base/Deferred",
    "dojo/store/util/QueryResults",
    "dojo/store/JsonRest",
    "dojo/store/Memory",
    "dojo/store/Cache",
    "dojo/store/Observable",

    "dojox/xml/parser",

    "src/ESPRequest"
], function (declare, lang, arrayUtil, Deferred, QueryResults, JsonRest, Memory, Cache, Observable,
    parser,
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
