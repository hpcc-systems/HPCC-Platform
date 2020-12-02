define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/_base/Deferred",

    "@hpcc-js/chart",

    "./DojoD3",
    "./Mapping"
], function (declare, lang, arrayUtil, Deferred,
    hpccChart,
    DojoD3, Mapping) {

    return declare([Mapping, DojoD3], {
        mapping: {
            NDChart: {
                display: "ND Chart Data",
                fields: {
                    label: "Label",
                    value: "Value",
                    value2: "Value 2",
                    value3: "Value 3",
                    value4: "Value 4",
                    value5: "Value 5"
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
            switch (this._chartType) {
                case "COLUMN":
                    this.chart = new hpccChart.Column()
                        .target(_target.domNodeID)
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "BAR":
                    this.chart = new hpccChart.Bar()
                        .target(_target.domNodeID)
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "LINE":
                    this.chart = new hpccChart.Line()
                        .target(_target.domNodeID)
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "AREA":
                    this.chart = new hpccChart.Area()
                        .target(_target.domNodeID)
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "STEP":
                    this.chart = new hpccChart.Step()
                        .target(_target.domNodeID)
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "SCATTER":
                    this.chart = new hpccChart.Scatter()
                        .target(_target.domNodeID)
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "RADAR":
                    this.chart = new hpccChart.Radar()
                        .target(_target.domNodeID)
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
                ;
            if (!this.chart.isDOMHidden()) {
                this.chart
                    .lazyRender()
                    ;
            }
            return;
        },

        resize: function () {
            if (!this.chart.isDOMHidden()) {
                this.chart
                    .resize()
                    .lazyRender()
                    ;
            }
        },

        update: function (data) {
        }
    });
});
