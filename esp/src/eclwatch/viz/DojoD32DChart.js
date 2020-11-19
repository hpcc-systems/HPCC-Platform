define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/_base/Deferred",

    "@hpcc-js/chart",
    "@hpcc-js/other",

    "./DojoD3",
    "./Mapping"
], function (declare, lang, arrayUtil, Deferred,
    hpccChart, hpccOther,
    DojoD3, Mapping) {

    return declare([Mapping, DojoD3], {
        mapping: {
            _2DChart: {
                display: "2D Chart Data",
                fields: {
                    label: "Label",
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
            switch (this._chartType) {
                case "BUBBLE":
                    this.chart = new hpccChart.Bubble()
                        .target(_target.domNodeID)
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "PIE":
                    this.chart = new hpccChart.Pie()
                        .target(_target.domNodeID)
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "RADIAL_BAR":
                    this.chart = new hpccChart.RadialBar()
                        .target(_target.domNodeID)
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "WORD_CLOUD":
                    this.chart = new hpccChart.WordCloud()
                        .target(_target.domNodeID)
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "HEX_BIN":
                    this.chart = new hpccChart.HexBin()
                        .target(_target.domNodeID)
                        ;
                    deferred.resolve(this.chart);
                    break;
                case "CONTOUR":
                    this.chart = new hpccChart.Contour()
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

            var columns = [this.getFieldMapping("label"), this.getFieldMapping("value")];
            var chartData = [];
            arrayUtil.forEach(data, function (row, idx) {
                chartData.push([row.label, row.value]);
            });
            this.chart
                .columns(columns)
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
