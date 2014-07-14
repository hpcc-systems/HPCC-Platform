define([
  "dojo/_base/declare",
  "dojo/_base/lang",

  "./DojoD3PieChart"
], function (declare, lang,
    DojoD3PieChart) {
    return declare([DojoD3PieChart], {
        mapping: {
            donutChart: {
                display: "Donut Chart Data",
                fields: {
                    label: "Label",
                    value: "Value"
                }
            }
        },

        constructor: function () {
        },

        renderTo: function (_target) {
            _target = lang.mixin({
                innerRadiusPct: 75
            }, _target);
            this.inherited(arguments);
        }
    });
});
