define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/_base/array",

  "./DojoD3",
  "./Mapping",

  "dojo/text!./templates/DojoD3ForceDirectedGraph.css"
], function (declare, lang, arrayUtil,
    DojoD3, Mapping,
    css) {
    return declare([Mapping, DojoD3], {
        mapping:{
            vertices: {
                display: "Vertices",
                fields: {
                    id: "ID",
                    label: "Label",
                    category: "Category"
                }
            },
            edges: {
                display: "Edges",
                fields: {
                    sourceID: "Source ID",
                    targetID: "Target ID",
                    weight:  "Weight"
                }
            }
        },

        constructor: function (mappings, target) {
            if (mappings)
                this.setFieldMappings(mappings);

            if (target)
                this.renderTo(target);
        },

        resize: function (args) {
            //  No resize needed
        },

        renderTo: function (_target) {
            _target = lang.mixin({
                css: css,
                innerRadiusPct: 0
            }, _target);
            this.inherited(arguments);
        },

        display: function (vertices, edges) {
            if (vertices)
                this.setData(vertices, "vertices");

            if (edges)
                this.setData(edges, "edges");

            var vertices = this.getMappedData("vertices");
            idIndex = {};
            arrayUtil.forEach(vertices, function (item, idx) {
                idIndex[item.id] = idx;
            });
            var edges = this.getMappedData("edges");
            arrayUtil.forEach(edges, function (item, idx) {
                item.source = idIndex[item.sourceID];
                item.target = idIndex[item.targetID];
            });

            var color = d3.scale.category20();
            var force = d3.layout.force().charge(-120).linkDistance(30).size([this.target.width, this.target.height]);
            force.nodes(vertices).links(edges);

            var link = this.SvgG.selectAll(".linkFD").data(edges).enter().append("line")
                .style("stroke-width", function (e) { return Math.sqrt(e.weight) })
                .style("stroke", "#999")
                .style("stroke-opacity", ".6")
            ;
            var node = this.SvgG.selectAll(".nodeFD").data(vertices).enter().append("circle")
                .style("fill", function (e) { return color(e.category) })
                .style("stroke", "#fff")
                .style("stroke-width", "1.5px")
                .attr("r", 5)
                .call(force.drag)
            ;
            node.append("title").text(function (e) { return e.name });
            force.on("tick", function () {
                link
                    .attr("x1", function (e) { return e.source.x })
                    .attr("y1", function (e) { return e.source.y })
                    .attr("x2", function (e) { return e.target.x })
                    .attr("y2", function (e) { return e.target.y })
                ;
                node
                    .attr("cx", function (e) { return e.x })
                    .attr("cy", function (e) { return e.y })
                ;
            });

            var n = vertices.length;
            var context = this;
            arrayUtil.forEach(vertices, function (e, t) {
                e.x = e.y = context.target.width / n * t
            });

            force.start();
            for (var i = n; i > 0; --i) {
                force.tick()
            }
            force.stop();
        }
    });
});
