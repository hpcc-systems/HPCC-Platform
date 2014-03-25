define([
  "dojo/_base/declare",
  "dojo/_base/lang",

  "./DojoD3",
  "./Mapping",

  "d3/d3",

  "dojo/text!./templates/DojoD3BarChart.css"

], function (declare, lang,
    DojoD3, Mapping,
    d3,
    css) {
    return declare([Mapping, DojoD3], {
        mapping: {
            barChart: {
                display: "Bar Chart Data",
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
                margin: { top: 0, right: 0, bottom: 20, left: 50 }
            }, _target);
            this.inherited(arguments);

            this.SvgG
                .style("fill", "none")
            ;

            this.x = d3.scale.ordinal()
                .rangeRoundBands([0, this.target.width - (this.target.margin.left + this.target.margin.right)], .1);

            this.y = d3.scale.linear()
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
            this.x.domain(data.map(function (d) { return d.label; }));
            this.y.domain([0, d3.max(data, function (d) { return d.value; })]);

            this.Svg.selectAll("g.y.axis").transition()
                .call(this.yAxis)
            ;

            this.Svg.selectAll("g.x.axis").transition()
                 .call(this.xAxis)
            ;

            this.update(data);
        },

        update: function (data) {
            // DATA JOIN
            // Join new data with old elements, if any.
            var bar = this.SvgG.selectAll(".bar").data(data);

            // UPDATE
            // Update old elements as needed.
            bar
                .attr("class", "bar")
            ;

            // ENTER
            // Create new elements as needed.
            bar.enter().append("rect")
                .attr("class", "bar")
                .on("click", lang.hitch(this, function (d) {
                    var evt = {};
                    evt[this.getFieldMapping("label")] = d.label;
                    this.emit("click", evt);
                }))
                .append("title").text(lang.hitch(this, function (d) { return d.value; }))
            ;

            // ENTER + UPDATE
            // Appending to the enter selection expands the update selection to include
            // entering elements; so, operations on the update selection after appending to
            // the enter selection will apply to both entering and updating nodes.
            bar.transition()
                .attr("x", lang.hitch(this, function (d) { return this.x(d.label); }))
                .attr("width", this.x.rangeBand())
                .attr("y", lang.hitch(this, function (d) { return this.y(d.value); }))
                .attr("height", lang.hitch(this, function (d) { return this.target.height - (this.target.margin.top + this.target.margin.bottom) - this.y(d.value); }))
            ;

            // EXIT
            // Remove old elements as needed.
            bar.exit().remove();
        }
    });
});
