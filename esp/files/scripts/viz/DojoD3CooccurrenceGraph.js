define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/_base/array",

  "./DojoD3",
  "./Mapping",

  "dojo/text!./templates/DojoD3CooccurrenceGraph.css"
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

        renderTo: function (_target) {
            _target = lang.mixin({
                css: css,
                margin: { top: 80, right: 0, bottom: 20, left: 100 }
            }, _target);
            this.inherited(arguments);

            this.SvgG
                .attr("transform", "translate(" + this.target.margin.left + "," + this.target.margin.top + ")")
            ;
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

            function row(e) {
                var t = d3.select(this).selectAll(".cell").data(e.filter(function (e) {
                    return e.z
                })).enter().append("rect").attr("class", "cell").attr("x", function (e) {
                    return x(e.x)
                }).attr("width", x.rangeBand()).attr("height", x.rangeBand()).style("fill-opacity", function (e) {
                    return z(e.z)
                }).style("fill", function (e) {
                    return nodes[e.x].category == nodes[e.y].category ? c(nodes[e.x].category) : null
                }).on("mouseover", mouseover).on("mouseout", mouseout)
            }
            function mouseover(e) {
                d3.selectAll(".row text").classed("active", function (t, n) {
                    return n == e.y
                });
                d3.selectAll(".column text").classed("active", function (t, n) {
                    return n == e.x
                })
            }
            function mouseout() {
                d3.selectAll("text").classed("active", false)
            }
            function order(e) {
                x.domain(orders[e]);
                var t = this.SvgG.transition().duration(2500);
                t.selectAll(".row").delay(function (e, t) {
                    return x(t) * 4
                }).attr("transform", function (e, t) {
                    return "translate(0," + x(t) + ")"
                }).selectAll(".cell").delay(function (e) {
                    return x(e.x) * 4
                }).attr("x", function (e) {
                    return x(e.x)
                });
                t.selectAll(".column").delay(function (e, t) {
                    return x(t) * 4
                }).attr("transform", function (e, t) {
                    return "translate(" + x(t) + ")rotate(-90)"
                })
            }
            var width = Math.min(this.target.width, this.target.height) - this.target.margin.left - this.target.margin.right;
            var x = d3.scale.ordinal().rangeBands([0, width]), z = d3.scale.linear().domain([0, 4]).clamp(true), c = d3.scale.category10().domain(d3.range(10));
            var matrix = [];
            var nodes = vertices;
            var n = nodes.length;
            nodes.forEach(function (e, t) {
                e.index = t;
                e.count = 0;
                matrix[t] = d3.range(n).map(function (e) {
                    return {
                        x: e,
                        y: t,
                        z: 0
                    }
                })
            });
            edges.forEach(function (e) {
                var d = matrix[e.source];
                var d1 = matrix[e.source][e.target];
                var d2 = matrix[e.source][e.target].z;
                matrix[e.source][e.target].z += e.weight;
                matrix[e.target][e.source].z += e.weight;
                matrix[e.source][e.source].z += e.weight;
                matrix[e.target][e.target].z += e.weight;
                nodes[e.source].count += e.weight;
                nodes[e.target].count += e.weight
            });
            var orders = {
                name: d3.range(n).sort(function (e, t) {
                    return d3.ascending(nodes[e].label, nodes[t].label)
                }),
                count: d3.range(n).sort(function (e, t) {
                    return nodes[t].count - nodes[e].count
                }),
                category: d3.range(n).sort(function (e, t) {
                    return nodes[t].category - nodes[e].category
                })
            };
            x.domain(orders.category);
            this.SvgG.append("rect").attr("width", width).attr("height", width).style("fill", "#eee");
            var row = this.SvgG.selectAll(".row").data(matrix).enter().append("g").attr("class", "row").attr("transform", function (e, t) {
                return "translate(0," + x(t) + ")"
            }).each(row);
            row.append("line").attr("x2", width).style("stroke", "#fff");
            row.append("text").attr("x", -6).attr("y", x.rangeBand() / 2).attr("dy", ".32em").attr("text-anchor", "end").text(function (e, t) {
                return nodes[t].label
            });
            var column = this.SvgG.selectAll(".column").data(matrix).enter().append("g").attr("class", "column").attr("transform", function (e, t) {
                return "translate(" + x(t) + ")rotate(-90)"
            });
            column.append("line").attr("x1", -width).style("stroke", "#fff");
            column.append("text").attr("x", 6).attr("y", x.rangeBand() / 2).attr("dy", ".32em").attr("text-anchor", "start").text(function (e, t) {
                return nodes[t].label
            });
        }
    });
});
