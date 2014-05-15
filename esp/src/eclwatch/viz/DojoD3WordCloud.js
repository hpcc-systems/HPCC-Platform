/*##############################################################################
#	HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#	Licensed under the Apache License, Version 2.0 (the "License");
#	you may not use this file except in compliance with the License.
#	You may obtain a copy of the License at
#
#	   http://www.apache.org/licenses/LICENSE-2.0
#
#	Unless required by applicable law or agreed to in writing, software
#	distributed under the License is distributed on an "AS IS" BASIS,
#	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#	See the License for the specific language governing permissions and
#	limitations under the License.
############################################################################## */
define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/_base/array",
  "dojo/dom-construct",

  "./DojoD3",
  "./Mapping",

  "d3/d3",

  "dojo/text!./d3-cloud/d3.layout.cloud.js",
  "dojo/text!./templates/DojoD3WordCloud.css"

], function (declare, lang, arrayUtil, domConstruct,
    DojoD3, Mapping,
    d3,
    cloudSrc,
    css) {

    eval(cloudSrc);
    return declare([Mapping, DojoD3], {
        mapping: {
            wordCloud: {
                display: "Word Cloud Data",
                fields: {
                    text: "Text",
                    size: "Size"
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

            this.fill = d3.scale.category20();
            this.g = this.SvgG.append("g")
                .attr("transform", "translate(" + this.target.width / 2 + "," + this.target.height / 2 + ")")
            ;

            this.update([]);
        },

        display: function (data) {
            if (data)
                this.setData(data);

            var data = this.getMappedData();

            domConstruct.empty(this.target.domNodeID);
            this.renderTo(this._target);

            this.update(data);
        },

        draw: function (words) {
            var text = this.SvgG
                .attr("width", this.target.width)
                .attr("height", this.target.height)
              .append("g")
                .attr("transform", "translate(" + this.target.width / 2 + "," + this.target.height / 2 + ")")
              .selectAll("text")
                .data(words);

            text.enter().append("text")
                .style("font-size", function (d) { return d.size + "px"; })
                .style("font-family", "Impact")
                .style("fill", lang.hitch(this, function (d, i) { return this.fill(i); }))
                .attr("text-anchor", "middle")
                .attr("transform", function (d) {
                    return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
                })
                .text(function (d) { return d.text; });

            text.exit().remove();
        },

        update: function (data) {
            d3.layout.cloud().size([this.target.width, this.target.height])
                .words(data)
                .padding(5)
                .rotate(function () { return Math.random() * 180 - 90; })
                .font("Impact")
                .fontSize(function (d) { return d.size; })
                .on("end", lang.hitch(this, this.draw))
                .start()
            ;
        }
    });
});
