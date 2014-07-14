define([
  "dojo/_base/declare",
  "dojo/_base/lang",

  "./DojoD3",
  "./Mapping",

  "d3/d3",

  "dojo/text!./templates/DojoD3ScatterChart.css"

], function (declare, lang,
    DojoD3, Mapping,
    d3,
    css) {
    return declare([Mapping, DojoD3], {
        mapping: {
            barChart: {
                display: "Bar Chart Data",
                fields: {
                    x: "x",
                    y: "y"
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
                margin: { top: 0, right: 0, bottom: 20, left: 50 }
            }, _target);
            this.inherited(arguments);

            this.SvgG
                .style("fill", "none")
            ;

            this.x = d3.scale.linear()
                .domain([0, 10])
                .range([0, this.target.width - (this.target.margin.left + this.target.margin.right)])
            ;

            this.y = d3.scale.linear()
                .domain([0, 10])
                .range([this.target.height - (this.target.margin.top + this.target.margin.bottom), 0])
            ;

            this.xAxis = d3.svg.axis()
                .scale(this.x)
                .orient("bottom")
            ;

            this.yAxis = d3.svg.axis()
                .scale(this.y)
                .orient("left")
            ;

            this.SvgG
                .attr("transform", "translate(" + this.target.margin.left + "," + this.target.margin.top + ")")
            ;

            this.SvgX = this.Svg.append("g")
                .attr("transform", "translate(" + this.target.margin.left + "," + (this.target.height - this.target.margin.bottom) + ")")
                .attr("class", "x axis")
                .call(this.xAxis)
            ;

            this.SvgY = this.Svg.append("g")
                .attr("transform", "translate(" + this.target.margin.left + "," + this.target.margin.top + ")")
                .attr("class", "y axis")
                .call(this.yAxis)
            ;

            this.update([]);
        },

        display: function (data) {
            if (data)
                this.setData(data);

            var data = this.getMappedData();
            this.x.domain([d3.min(data, function (d) { return d.x; }), d3.max(data, function (d) { return d.x; })]);
            this.y.domain([d3.min(data, function (d) { return d.y; }), d3.max(data, function (d) { return d.y; })]);

            this.Svg.selectAll("g.y.axis").transition()
                .call(this.yAxis)
            ;

            this.Svg.selectAll("g.x.axis").transition()
                 .call(this.xAxis)
            ;

            this.update(data);
        },

        update: function (data) {
            this.SvgG.selectAll(".dot").remove();

            var dot = this.SvgG.selectAll(".scatterDot").data(data);
            var x = dot.enter().append("g")
                .attr("class", "dot")
            ;
            x.append("line")
                .attr("x1", lang.hitch(this, function (d) { return this.x(d.x) - 4; }))
                .attr("y1", lang.hitch(this, function (d) { return this.y(d.y) - 4; }))
                .attr("x2", lang.hitch(this, function (d) { return this.x(d.x) + 4; }))
                .attr("y2", lang.hitch(this, function (d) { return this.y(d.y) + 4; }))
            ;
            x.append("line")
                .attr("x1", lang.hitch(this, function (d) { return this.x(d.x) - 4; }))
                .attr("y1", lang.hitch(this, function (d) { return this.y(d.y) + 4; }))
                .attr("x2", lang.hitch(this, function (d) { return this.x(d.x) + 4; }))
                .attr("y2", lang.hitch(this, function (d) { return this.y(d.y) - 4; }))
            ;
        }
    });
});
