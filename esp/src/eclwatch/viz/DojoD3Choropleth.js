define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/_base/Deferred",

    "@hpcc-js/map",

    "./DojoD3",
    "./Mapping"

], function (declare, lang, arrayUtil, Deferred,
    hpccMap,
    DojoD3, Mapping) {

    hpccMap.topoJsonFolder("/esp/files/dist/TopoJSON");

    return declare([Mapping, DojoD3], {
        mapping: {
            choropleth: {
                display: "Choropleth Data",
                fields: {
                    label: "County ID",
                    value: "Value"
                }
            }
        },

        constructor: function (mappings, target) {
            if (mappings)
                this.setFieldMappings(mappings);

            if (target)
                this.renderTo(target);
        },

        renderTo: function (_target) {
            var deferred = new Deferred();
            var context = this;
            switch (this._chartType) {
                case "COUNTRY":
                    this.chart = new hpccMap.ChoroplethCountries()
                        .target(_target.domNodeID)
                        .autoScaleMode("data")
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "STATE":
                    this.chart = new hpccMap.ChoroplethStates()
                        .target(_target.domNodeID)
                        .projection("AlbersPR")
                        .autoScaleMode("data")
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "COUNTY":
                    this.chart = new hpccMap.ChoroplethCounties()
                        .target(_target.domNodeID)
                        .projection("AlbersPR")
                        .autoScaleMode("data")
                        ;
                    deferred.resolve(this.chart);
                    break;
                default:
                    console.log("Invalid visualization:  " + this._chartType);
                    deferred.resolve(null);
            }
            return deferred.promise;
        },

        display: function (data) {
            if (data)
                this.setData(data);

            var data = this.getMappedData();

            var chartColumns = [];
            var chartData = arrayUtil.map(data, function (d, idx) {
                var retVal = [];
                for (var key in d) {
                    if (idx === 0) {
                        chartColumns.push(key);
                    }
                    retVal.push(d[key]);
                }
                return retVal;
            });
            this.chart
                .columns(chartColumns)
                .data(chartData)
                .render()
                ;
            return;
        },

        resize: function () {
            var _debounce = function (fn, timeout) {
                var timeoutID = -1;
                return function () {
                    if (timeoutID > -1) {
                        window.clearTimeout(timeoutID);
                    }
                    timeoutID = window.setTimeout(fn, timeout);
                };
            };

            var _debounced_draw = _debounce(lang.hitch(this, function () {
                this.chart
                    .resize()
                    .render()
                    ;
            }), 125);

            _debounced_draw();
        },

        update: function (data) {
        }
    });
});
