define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/_base/array",

  "./DojoD3",
  "./Mapping",

  "dojo/text!./templates/DojoD3Histogram.css"
], function (declare, lang, arrayUtil,
    DojoD3, Mapping,
    css) {
    return declare([Mapping, DojoD3], {
        mapping: {
            histogram: {
                display: "Histogram Data",
                fields: {
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

            this.x = d3.scale.linear()
                .domain([this.loVal ? this.loVal : 0, this.hiVal ? this.hiVal : 1])
                .range([0, this.target.width])
            ;

            this.y = d3.scale.linear()
                .domain([0, 100])
                .range([this.target.height - (this.target.margin.top + this.target.margin.bottom), 0])
            ;

            this.xAxis = d3.svg.axis()
                .scale(this.x)
                .orient("bottom")
            ;

            this.SvgG
                .attr("transform", "translate(" + this.target.margin.left + "," + this.target.margin.top + ")")
            ;

            this.SvgX = this.Svg.append("g")
                .attr("transform", "translate(" + this.target.margin.left + "," + (this.target.height - this.target.margin.bottom) + ")")
                .attr("class", "x axis")
                .call(this.xAxis)
            ;

            this.update([]);
        },

        display: function (_data) {
            if (_data)
                this.setData(_data);

            var mappedData = this.getMappedData();
            this.loVal = null;
            this.hiVal = null;
            var rawData = [];
            arrayUtil.forEach(mappedData, lang.hitch(this, function (item, idx) {
                if (this.loVal === null || this.loVal > item.value)
                    this.loVal = item.value;
                if (this.hiVal === null || this.hiVal < item.value)
                    this.hiVal = item.value;
                rawData.push(item.value);
            }));

            this.SvgG.selectAll(".bar").remove();

            this.x.domain([this.loVal, this.hiVal]);
            var data = d3.layout.histogram()
                .bins(this.x.ticks(20))
                (rawData)
            ;
            this.y.domain([0, d3.max(data, function (d) { return d.y; })]);

            this.Svg.selectAll("g.x.axis").transition()
                 .call(this.xAxis)
            ;

            this.update(data);
        },

        update: function (data) {
            if (!data.length)
                return;

            this.SvgG.selectAll("svg").remove();

            var barX = this.SvgG.selectAll(".bar").data(data);

            var bar = barX.enter().append("g")
                .attr("class", "bar")
                .attr("transform", lang.hitch(this, function (d) { return "translate(" + this.x(d.x) + "," + this.y(d.y) + ")"; }))
            ;

            bar.append("rect")
                .attr("x", 1)
                .attr("width", this.x(this.loVal + data[0].dx) - 1)
                .attr("height", lang.hitch(this, function (d) { return this.target.height - (this.target.margin.top + this.target.margin.bottom) - this.y(d.y); }))
            ;

            var formatCount = d3.format(",.0f");
            bar.append("text")
                .attr("class", "barText")
                .attr("dy", ".75em")
                .attr("y", 6)
                .attr("x", this.x(data[0].dx) / 2)
                .attr("text-anchor", "middle")
                .text(lang.hitch(this, function (d) { return formatCount(d.y); }))
            ;
        }
    });
});
