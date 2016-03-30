/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/on",

    "dijit/registry",

    "hpcc/_Widget",
    "hpcc/WsWorkunits",

    "dojo/text!../templates/WUStatsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Button",
    "dijit/form/Select"
], function (declare, lang, i18n, nlsHPCC, on,
            registry,
            _Widget, WsWorkunits,
            template) {
    return declare("WUStatsWidget", [_Widget], {
        templateString: template,
        baseClass: "WUStatsWidget",
        i18n: nlsHPCC,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
            if (this.pieCreatorType) this.pieCreatorType.widget.resize().render();
            if (this.pieScopeType) this.pieScopeType.widget.resize().render();
            if (this.scopesSurface) this.scopesSurface.resize().render();
            if (this.bar) this.bar.resize().render();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        //  Implementation  ---
        _onRefresh: function () {
            this.doRefreshData();
        },

        _onReset: function () {
            this.doReset();
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;
            if (dojoConfig.vizDebug) {
                requireWidget();
            } else {
                require(["dist-amd/hpcc-viz"], function () {
                    require(["dist-amd/hpcc-viz-common"], function () {
                        require(["dist-amd/hpcc-viz-api"], function () {
                            require(["dist-amd/hpcc-viz-layout"], function () {
                                require(["dist-amd/hpcc-viz-chart", "dist-amd/hpcc-viz-google", "dist-amd/hpcc-viz-c3chart", "dist-amd/hpcc-viz-amchart", "dist-amd/hpcc-viz-other", "dist-amd/hpcc-viz-tree", "dist-amd/hpcc-viz-composite"], function () {
                                    requireWidget();
                                });
                            });
                        });
                    });
                });
            }
            function requireWidget() {
                require(["src/other/Comms", "src/composite/MegaChart", "src/layout/Surface", "src/tree/SunburstPartition", "src/other/Table", "crossfilter"], function (Comms, MegaChart, Surface, SunburstPartition, Table, crossfilterXXX) {
                    function CFGroup(crossfilter, dimensionID, targetID) {
                        this.targetID = targetID;
                        this.dimensionID = dimensionID;
                        this.dimension = crossfilter.dimension(function (d) { return d[dimensionID]; });
                        this.group = this.dimension.group().reduceSum(function (d) { return d.RawValue; });

                        this.widget = new MegaChart()
                            .target(targetID)
                            .title(dimensionID)
                            .titleFontFamily("Verdana")
                            .columns([dimensionID, "Total"])
                            .chartType("C3_PIE")
                        ;

                        this.filter = null;
                        var context = this;
                        this.widget.click = function (row, column) {
                            if (context.filter === row[dimensionID]) {
                                context.filter = null;
                            } else {
                                context.filter = row[dimensionID];
                            }
                            context.dimension.filter(context.filter);
                            context.click(row, column);
                            context.render();
                        };
                    }
                    CFGroup.prototype.click = function (row, column) {
                    }
                    CFGroup.prototype.resetFilter = function () {
                        this.filter = null;
                        this.dimension.filter(null);
                    }
                    CFGroup.prototype.render = function () {
                        this.widget
                            .title(this.dimensionID + (this.filter ? " (" + this.filter + ")" : ""))
                            .data(this.group.all().map(function (row) {
                                return [row.key, row.value];
                            }))
                            .render()
                        ;
                    }

                    context.stats = crossfilter([]);
                    context.summaryByKind = context.stats.dimension(function (d) { return d.Kind; });
                    context.groupByKind = context.summaryByKind.group().reduceCount();

                    context.select = registry.byId(context.id + "Kind");
                    var prevKind = "";
                    context.select.on("change", function (newValue) {
                        if (prevKind !== newValue) {
                            context.pieCreatorType.resetFilter();
                            context.pieScopeType.resetFilter();
                            context.prevScope = null;
                            context.summaryByScope.filterAll();
                            context.summaryByKind.filter(newValue);
                            context.doRender(context.select);
                            prevKind = newValue;
                        }
                    });

                    context.pieCreatorType = new CFGroup(context.stats, "CreatorType", context.id + "CreatorType");
                    context.pieCreatorType.click = function (row, column) {
                        context.doRender(context.pieCreatorType);
                    }

                    context.pieScopeType = new CFGroup(context.stats, "ScopeType", context.id + "ScopeType");
                    context.pieScopeType.click = function (row, column) {
                        context.doRender(context.pieScopeType);
                    }

                    context.summaryByScope = context.stats.dimension(function (d) { return d.Scope; });
                    context.groupByScope = context.summaryByScope.group().reduceSum(function (d) { return d.RawValue; });

                    context.scopes = new SunburstPartition();
                    context.scopesSurface = new Surface()
                        .target(context.id + "Scope")
                        .title("Scope")
                        .widget(context.scopes)
                    ;

                    context.prevScope = null;
                    context.scopes.click = SunburstPartition.prototype.debounce(function (row, column) {
                        if (row.id === "") {
                            context.prevScope = null;
                            context.summaryByScope.filter(null);
                        } else if (context.prevScope === row.id) {
                            context.prevScope = null;
                            context.summaryByScope.filter(null);
                        } else {
                            context.prevScope = row.id;
                            context.summaryByScope.filter(function (d) {
                                return d.indexOf(context.prevScope + ":") === 0;
                            });
                        }
                        context.doRender(context.scopes);
                    }, 250);

                    context.bar = new MegaChart()
                        .target(context.id + "Stats")
                        .titleFontFamily("Verdana")
                        .chartType("BAR")
                    ;

                    context.doRefreshData();
                });
            }
        },

        formatTree: function (data, label) {
            var cache = {};
            var treeDedup = {
                "": {
                    parentID: null,
                    id: "",
                    label: label,
                    children: [],
                    childrenDedup: {}
                }
            };
            data.forEach(function (row, idx) {
                var i = 1;
                var scopeParts = row.key.split(":");
                var scope = "";
                scopeParts.forEach(function (item, idx) {
                    var prevScope = scope;
                    scope += (scope.length ? ":" : "") + item;
                    if (!treeDedup[scope]) {
                        var newTreeItem = {
                            parentID: prevScope,
                            id: scope,
                            children: [],
                            childrenDedup: {}
                        }
                        treeDedup[scope] = newTreeItem;
                        treeDedup[prevScope].children.push(newTreeItem);
                        treeDedup[prevScope].childrenDedup[scope] = newTreeItem;
                    }
                    var scopeItem = treeDedup[scope];
                    if (idx === scopeParts.length - 1) {
                        scopeItem.__data = row;
                        scopeItem.label = row.key;
                        scopeItem.value = row.value;
                    };
                });
            });
            function trimTree(node) {
                var newChildren = [];
                node.children.forEach(function (childNode) {
                    trimTree(childNode);
                    if (childNode.value || childNode.children.length) {
                        newChildren.push(childNode);
                    }
                })
                node.children = newChildren;
                return node;
            }
            var retVal = trimTree(treeDedup[""]);
            return retVal;
        },

        doReset: function () {
            this.pieCreatorType.resetFilter();
            this.pieScopeType.resetFilter();
            this.prevScope = null;
            this.summaryByScope.filterAll();
            if (this.select.get("value") !== "TimeElapsed") {
                this.select.set("value", "TimeElapsed");
            } else {
                this.doRender();
            }
        },

        doRender: function (source) {
            if (source !== this.pieCreatorType) this.pieCreatorType.render();
            if (source !== this.pieScopeType) this.pieScopeType.render();

            if (source !== this.scopes) {
                var tree = this.formatTree(this.groupByScope.all(), this.params.Wuid);
                this.scopes
                    .data(tree)
                ;
                this.scopesSurface
                    .title("Scope" + (this.prevScope ? " (" + this.prevScope + ")" : ""))
                    .render()
                ;
            } else {
                this.scopesSurface
                    .title("Scope" + (this.prevScope ? " (" + this.prevScope + ")" : ""))
                    .render()
                ;
            }

            var scopeData = this.summaryByScope.top(Infinity);
            var columns = ["Creator", "CreatorType", "Scope", "ScopeType", "Description", "TimeStamp", "Measure", "Kind", "Value", "RawValue", "Count", "Max"];
            var data = scopeData.map(function (row, idx) {
                var rowData = [];
                columns.forEach(function (column) {
                    rowData.push(row[column]);
                });
                return rowData;
            });

            var statsData = [];
            if (this.select.get("value")) {
                statsData = scopeData.map(function (row) {
                    if (this.prevScope === row.Scope) {
                        return [row.Scope, row.RawValue];
                    }
                    return [(this.prevScope && row.Scope.indexOf(this.prevScope) === 0 ? row.Scope.substring(this.prevScope.length + 1) : row.Scope), row.RawValue];
                }, this);
            }
            var statsLabel = [this.select.get("value"), this.pieCreatorType.filter, this.pieScopeType.filter, this.prevScope].filter(function (item) {
                return item;
            }).join(", ") || "Unknown";
            statsLabel += (scopeData[0] ? " (" + scopeData[0].Measure + ")" : "");
            this.bar
                .title(statsLabel)
                .columns(["Stat", statsLabel])
                .data(statsData)
                .render(function (d) {
                    if (d._content && d._content._chart && d._content._chart.legendShow) {
                        d._content._chart.legendShow(false);
                    }
                })
            ;
        },

        doRefreshData: function () {
            var context = this;
            this.summaryByKind.filterAll();
            this.pieCreatorType.dimension.filterAll();
            this.pieScopeType.dimension.filterAll();
            this.summaryByScope.filterAll();
            this.stats.remove();

            WsWorkunits.WUGetStats({
                request: {
                    WUID: this.params.Wuid
                }
            }).then(function (response) {
                if (lang.exists("WUGetStatsResponse.Statistics.WUStatisticItem", response)) {
                    context.stats.add(response.WUGetStatsResponse.Statistics.WUStatisticItem.filter(function (row) {
                        return row.ScopeType !== "global" && row.Scope !== "Process";
                    }));
 
                    var kind = context.select.get("value");
                    context.select.set("options", context.groupByKind.all().map(function (row) {
                        return { label: row.key + " (" + row.value + ")", value: row.key, selected: kind === row.key };
                    }));

                    if (kind) context.summaryByKind.filter(kind);
                    if (context.pieCreatorType.filter) context.pieCreatorType.dimension.filter(context.pieCreatorType.filter);
                    if (context.pieScopeType.filter) context.pieScopeType.dimension.filter(context.pieScopeType.filter);
                    if (context.prevScope) context.summaryByScope.filter(function (d) {
                        return d.indexOf(context.prevScope + ":") === 0;
                    });
                    if (kind === "") {
                        context.select.set("value", "TimeElapsed");
                    } else {
                        context.doRender();
                    }
                }
            });
        }
    });
});
