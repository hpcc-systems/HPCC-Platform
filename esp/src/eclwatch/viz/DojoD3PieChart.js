define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/_base/array",

  "./DojoD3",
  "./Mapping",

  "d3/d3",

  "dojo/text!./templates/DojoD3PieChart.css"

], function (declare, lang, arrayUtil,
    DojoD3, Mapping,
    d3,
    css) {
    return declare([Mapping, DojoD3], {
        mapping:{
            pieChart: {
                display: "Pie Chart Data",
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
            _target = lang.mixin({
                css: css,
                innerRadiusPct: 0
            }, _target);
            this.inherited(arguments);

            this.SvgG
                .attr("transform", "translate(" + this.target.width / 2 + "," + this.target.height / 2 + ")")
            ;

            this.update([]);
        },

        display: function (data) {
            if (data)
                this.setData(data);

            var data = this.getMappedData();

            this.update(data);
        },

        update: function (data) {
            var color = d3.scale.ordinal()
                .range(["#98abc5", "#8a89a6", "#7b6888", "#6b486b", "#a05d56", "#d0743c", "#ff8c00"])
            ;

            var arcFunc = d3.svg.arc()
                .outerRadius(this.target.radius)
                .innerRadius((this.target.radius * this.target.innerRadiusPct) / 100)
            ;

            var pie = d3.layout.pie()
                .sort(null)
                .value(function(d) { return d.value; })
            ;

            var arc = this.SvgG.selectAll(".arc")
                .data(pie(data))
            ;

            var g = arc
                .enter().append("g")
                .attr("class", "arc")
            ;

            g.append("path")
                .attr("d", arcFunc)
                .style("fill", function (d) { return color(d.data.value); })
                .on("click", lang.hitch(this, function (d) {
                    var evt = {};
                    evt[this.getFieldMapping("label")] = d.label;
                    this.emit("click", evt);
                }))
            ;

            g.append("text")
                .attr("transform", function(d) { return "translate(" + arcFunc.centroid(d) + ")"; })
                .attr("dy", ".35em")
                .style("text-anchor", "middle")
                .text(function (d) { return d.data.label; })
            ;

            arc.exit().remove();
        }
    });
});
