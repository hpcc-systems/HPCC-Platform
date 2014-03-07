define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/_base/array",
  "dojo/json",

  "./DojoD3",
  "./Mapping",

  "dojo/text!./map/us.json",
  "dojo/text!./map/us_counties.json",
  "dojo/text!./templates/DojoD3Choropeth.css",

  "topojson/topojson"
], function (declare, lang, arrayUtil, JSON,
    DojoD3, Mapping,
    us, us_counties, css) {
    return declare([Mapping, DojoD3], {
        mapping: {
            choropeth: {
                display: "Choropeth Data",
                fields: {
                    county: "County ID",
                    value: "Value"
                }
            }
        },

        constructor: function (mappings, target) {
            this.mappedData = d3.map();
            this.us = JSON.parse(us);
            this.us_counties = JSON.parse(us_counties);

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
            var p = this.SvgG.selectAll("path").data(topojson.feature(this.us, this.us.objects.counties).features);
            var context = this;
            p.enter().append("path")
                .attr("class", "counties")
                .attr("d", path)
                .on("click", lang.hitch(this, function (d) {
                    var evt = {};
                    evt[this.getFieldMapping("county")] = d.id;
                    this.emit("click", evt);
                }))
                .append("title").text(lang.hitch(this, function (d) { return context.us_counties[d.id]; }))
            ;
            p.exit().remove();
            this.Svg.append("path").datum(topojson.mesh(this.us, this.us.objects.states, function (a, b) { return a !== b; }))
                .attr("class", "states")
                .attr("d", path)
            ;
            this.Svg.append("path").datum(topojson.feature(this.us, this.us.objects.land))
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
                    return this.us_counties[d.id] + " (" + this.mappedData.get(d.id) + ")";
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
