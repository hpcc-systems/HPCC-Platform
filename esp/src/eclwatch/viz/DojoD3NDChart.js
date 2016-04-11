define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/_base/array",
  "dojo/_base/Deferred",

  "./DojoD3",
  "./Mapping"
], function (declare, lang, arrayUtil, Deferred,
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
            var context = this;
            require(["src/chart/MultiChart"], function (MultiChart) {
                context.chart = new MultiChart()
                    .chartType(context._chartType)
                    .target(_target.domNodeID)
                ;
                deferred.resolve(context.chart);
            });
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
                }
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
