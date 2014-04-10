define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/_base/array",

  "./DojoD3",
  "./Mapping",
  "./map/us-counties",

  "d3/d3",
  "topojson/topojson",

  "dojo/text!./templates/DojoD3Choropleth.css"

], function (declare, lang, arrayUtil,
    DojoD3, Mapping, usCounties,
    d3, topojson,
    css) {
    return declare([Mapping, DojoD3], {
        mapping: {
            choropeth: {
                display: "Choropleth Data",
                fields: {
                    county: "County ID",
                    value: "Value"
                }
            }
        },

        constructor: function (mappings, target) {
            this.mappedData = d3.map();

            if (mappings)
                this.setFieldMappings(mappings);

            if (target)
                this.renderTo(target);
        },

        resize: function (args) {
            //  No resize (yet - its too slow)
        },

        renderTo: function (_target) {
            _target = lang.mixin({
                css: css
            }, _target);
            this.inherited(arguments);

            this.SvgG
                .style("fill", "none")
            ;
            var path = d3.geo.path();
            var p = this.SvgG.selectAll("path").data(topojson.feature(usCounties.topology, usCounties.topology.objects.counties).features);
            var context = this;
            p.enter().append("path")
                .attr("class", "counties")
                .attr("d", path)
                .on("click", lang.hitch(this, function (d) {
                    var evt = {};
                    evt[this.getFieldMapping("county")] = d.id;
                    this.emit("click", evt);
                }))
                .append("title").text(lang.hitch(this, function (d) { return usCounties.countyNames[d.id]; }))
            ;
            p.exit().remove();
            this.Svg.append("path").datum(topojson.mesh(usCounties.topology, usCounties.topology.objects.states, function (a, b) { return a !== b; }))
                .attr("class", "states")
                .attr("d", path)
            ;
            this.Svg.append("path").datum(topojson.feature(usCounties.topology, usCounties.topology.objects.land))
                .attr("class", "usa")
                .attr("d", path)
            ;
        },

        display: function (data) {
            if (data)
                this.setData(data);

            var maxVal = this._prepData();
            var quantize = d3.scale.quantize()
                            .domain([0, maxVal])
                            .range(d3.range(255).map(lang.hitch(this, function (i) {
                                var negRed = 255 - i;
                                return "rgb(255, " + negRed + ", " + negRed + ")";
                            })))
            ;
            this.SvgG.selectAll("path")
                .style("fill", lang.hitch(this, function (d) { return this.mappedData.get(d.id) == null ? "lightgrey" : quantize(this.mappedData.get(d.id)); }))
                .select("title")
                .text(lang.hitch(this, function (d) {
                    return usCounties.countyNames[d.id] + " (" + this.mappedData.get(d.id) + ")";
                }))
            ;
        },

        _prepData: function () {
            this.data = this.getMappedData();
            var maxVal = 0;
            this.mappedData = d3.map();
            arrayUtil.forEach(this.data, lang.hitch(this, function (item, idx) {
                if (+item.value > maxVal) {
                    maxVal = +item.value;
                }
                this.mappedData.set(item.county, +item.value);
            }));
            return maxVal;
        }
    });
});
