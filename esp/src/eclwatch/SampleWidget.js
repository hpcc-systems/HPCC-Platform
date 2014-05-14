/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
    "dojo/_base/array",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-geometry",
    "dojo/store/Memory",
    "dojo/aspect",

    "dijit/registry",

    "dgrid/Grid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/_Widget",
    "hpcc/ESPBase",
    "hpcc/ESPWorkunit",
    "hpcc/ESPLogicalFile",

    "d3/d3",

    "dojo/text!../templates/SampleWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/layout/StackController",
    "dijit/layout/StackContainer",
    "dijit/form/Button",
    "dijit/form/NumberSpinner",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator"
], function (declare, lang, arrayUtil, i18n, nlsHPCC, dom, domConstruct, domGeom, Memory, aspect,
                registry,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _Widget, ESPBase, ESPWorkunit, ESPLogicalFile,
                d3,
                template) {

    var dc = null;

    return declare("SampleWidget", [_Widget], {
        templateString: template,
        baseClass: "SampleWidget",
        i18n: nlsHPCC,

        borderContainer: null,
        grid: null,

        loaded: false,

        constructor: function (mappings, target) {
        },

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.grid = registry.byId(this.id + "Grid");
            require(["dojo/text!hpcc/../crossfilter/crossfilter.js",
                "dc/dc",
                "dojo/text!hpcc/../dc/dc.css"], lang.hitch(this, function (
                crossfilterSrc, 
                _dc, dcCss) {
                    dc = _dc;
                    eval(crossfilterSrc);
                    this.injectStyleSheet(dcCss);
                }));
        },

        startup: function (args) {
            this.inherited(arguments);
            this.chartDiv = dom.byId(this.id + "HistogramCP");
            this.chartNode = dom.byId(this.id + "Histogram");
            this.widget.BorderContainer.removeChild(this.widget.HistogramCP);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        _onRefresh: function (evt) {
            this.refresh();
        },

        _onSampleCountChange: function (evt) {
        },

        _onSampleSizeChange: function (evt) {
        },

        _onSampleGroupChange: function (evt) {
            this.refreshGroupBy();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            //TODO:  Encapsulate this IF into ESPResult.js
            if (params.result && params.result.canShowResults()) {
                this.initResult(params.result);
            } else if (params.Wuid && lang.exists("Sequence", params)) {
                var wu = ESPWorkunit.Get(params.Wuid);
                var context = this;
                wu.fetchSequenceResults(function (results) {
                    context.initResult(results[params.Sequence]);
                });
            } else if (params.LogicalName) {
                var logicalFile = ESPLogicalFile.Get(params.ClusterName, params.LogicalName);
                var context = this;
                logicalFile.getInfo({
                    onAfterSend: function (response) {
                        context.initResult(logicalFile.result);
                    }
                });
            } else if (params.result && params.result.Name) {
                var logicalFile = ESPLogicalFile.Get(params.result.ClusterName, params.result.Name);
                var context = this;
                logicalFile.getInfo({
                    onAfterSend: function (response) {
                        context.initResult(logicalFile.result);
                    }
                });
            } else {
                this.initResult(null);
            }
        },

        initResult: function (result) {
            this.result = result;
            if (result) {
                this.store = new Memory();
                var context = this;
                result.fetchStructure(function (_structure) {
                    var structure = arrayUtil.map(_structure, function (item) {
                        return lang.mixin(item, {
                            sortable: true
                        });
                    });
                    context.grid = new declare([Grid, Pagination, Keyboard, ColumnResizer, DijitRegistry])({
                        columns: structure,
                        rowsPerPage: 50,
                        pagingLinks: 1,
                        pagingTextBox: true,
                        firstLastArrows: true,
                        pageSizeOptions: [25, 50, 100],
                        store: context.store
                    }, context.id + "Grid");
                    context.grid.startup();

                    var groupOptions = arrayUtil.map(structure, function (item) {
                        return {
                            label: item.label,
                            value: item.field
                        };
                    });
                    if (groupOptions.length && groupOptions[0].label === "##") {
                        groupOptions[0].label = "&nbsp;";
                        groupOptions[0].selected = true;
                        groupOptions[0].value = "";
                    }
                    context.widget.SampleGroup.set("options", groupOptions);
                    context.refresh();
                });
            }
        },

        injectStyleSheet: function(css) {
            var styleNode = dom.byId(this.id + "Style");
            if (styleNode) {
                domConstruct.destroy(this.id + "Style");
            }
            var style = domConstruct.create("style", {
                id: this.id + "Style",
                innerHTML: css
            });
            dojo.query("head").some(function(item, idx) {
                item.appendChild(style);
            });
        },

        sampleCount: -1,
        sampleSize: -1,

        refreshGroupBy: function () {
            var groupBy = this.widget.SampleGroup.get("value");
            if (!groupBy) {
                this.widget.BorderContainer.removeChild(this.widget.HistogramCP);
                this.chartNode.innerHTML = "";
            } else {
                this.widget.BorderContainer.addChild(this.widget.HistogramCP);
                if (this.ndx) {
                    // Need to apply filter to all first
                    dc.filterAll();
                    // Then call remove
                    this.ndx.remove();
                }
                var ndx = this.crossfilter(this.store.data);
                var myDimension = ndx.dimension(function (d) { return d[groupBy]; });
                var domain = myDimension.group().all();
                var mySumGroup = myDimension.group().reduceCount();

                var pos = domGeom.position(this.chartDiv);
                var pad = domGeom.getPadExtents(this.chartDiv);

                var chart = dc.barChart("#" + this.id + "Histogram");
                chart
                  .width(pos.w - pad.w)
                  .height(pos.h - pad.h)
                  .x(d3.scale.ordinal().domain(domain.map(function (d) { return d.key; })))
                  .xUnits(dc.units.ordinal)
                  .brushOn(true)
                  //.yAxisLabel("This is the Y Axis!")
                  .dimension(myDimension)
                  .group(mySumGroup)
                ;
                chart.render();
                var context = this;
                aspect.after(chart, "onClick", function (dunno, items) {
                    var sel = chart.selectAll(".selected").data().map(function (d) { return d.data.key; });
                    context.grid.set("query", function (obj) {
                        if (sel.length) {
                            return sel.indexOf(obj[groupBy]) >= 0;
                        }
                        return true;
                    });
                });
            }
        },

        refresh: function () {
            var sampleCount = this.widget.SampleCount.get("value");
            var sampleSize = this.widget.SampleSize.get("value");
            if (this.sampleCount != sampleCount || this.sampleSize != sampleSize) {
                this.sampleCount = sampleCount;
                this.sampleSize = sampleSize;

                var context = this;
                this.result.fetchSamples(sampleCount, sampleSize).then(function (data) {
                    context.store.setData(data);
                    context.grid.set("query", {});
                    context.refreshGroupBy();
                });
            } else {
                this.refreshGroupBy();
            }
        }
    });
});
