define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/_base/array",

  "./DojoD3",
  "./Mapping",

  "src/chart/MultiChart",

  "d3",

  "dojo/text!./templates/DojoD3BarChart.css"

], function (declare, lang, arrayUtil,
    DojoD3, Mapping,
    MultiChart,
    d3,
    css) {
    return declare([Mapping, DojoD3], {
        mapping: {
            barChart: {
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
            this.chart = new MultiChart()
                .chartType(this._chartType)
                .target(_target.domNodeID)
            ;
            if (this.chart.show_title) {
                this.chart.show_title(false);
            }
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
