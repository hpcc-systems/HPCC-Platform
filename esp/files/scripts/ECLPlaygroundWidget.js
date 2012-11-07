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
    "dojo/_base/xhr",
    "dojo/dom",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/registry",

    "hpcc/ECLSourceWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/SampleSelectWidget",
    "hpcc/GraphWidget",
    "hpcc/ResultsWidget",
    "hpcc/ESPWorkunit",

    "dojo/text!../templates/ECLPlaygroundWidget.html"
], function (declare, xhr, dom,
                _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, registry,
                EclSourceWidget, TargetSelectWidget, SampleSelectWidget, GraphWidget, ResultsWidget, Workunit,
                template) {
    return declare("ECLPlaygroundWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "ECLPlaygroundWidget",
        wu: null,
        editorControl: null,
        graphControl: null,
        resultsWidget: null,
        targetSelectWidget: null,
        sampleSelectWidget: null,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this._initControls();
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        //  Implementation  ---
        _initControls: function () {
            var context = this;
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.targetSelectWidget = registry.byId(this.id + "TargetSelect");
            this.resultsWidget = registry.byId(this.id + "Results");
            this.resultsWidget.onErrorClick = function (line, col) {
                context.editorControl.setCursor(line, col);
            };
        },

        hideTitle: function () {
            var topPane = dom.byId(this.id + "TopPane");
            dojo.destroy(topPane);
            this.borderContainer.resize();
        },

        init: function (params) {
            if (params.Wuid) {
                this.hideTitle();
            }

            this.wuid = params.Wuid;
            this.targetSelectWidget.setValue(params.Target);

            this.initEditor();
            this.editorControl.init(params);

            var context = this;
            this.initGraph();
            if (params.Wuid) {
                this.wu = new Workunit({
                    wuid: params.Wuid
                });
                this.wu.monitor(function () {
                    context.monitorEclPlayground();
                });
            } else {
                this.initSamples();
                this.graphControl.watchSelect(this.sampleSelectWidget.selectControl);
            }
            this.graphControl.watchSplitter(this.borderContainer.getSplitter("right"));
            this.graphControl.watchSplitter(this.borderContainer.getSplitter("bottom"));
        },

        initSamples: function () {
            var context = this;
            this.sampleSelectWidget = registry.byId(this.id + "SampleSelect");
            this.sampleSelectWidget.onNewSelection = function (eclText) {
                context.resetPage();
                context.editorControl.setText(eclText);
            };
            this.sampleSelectWidget.load();
        },

        initEditor: function () {
            this.editorControl = registry.byId(this.id + "Source");
        },

        initGraph: function () {
            var context = this;
            this.graphControl = registry.byId(this.id + "Graphs");
            this.graphControl.onSelectionChanged = function (items) {
                context.editorControl.clearHighlightLines();
                for (var i = 0; i < items.length; ++i) {
                    var props = context.graphControl.plugin.getProperties(items[i]);
                    if (props.definition) {
                        var startPos = props.definition.indexOf("(");
                        var endPos = props.definition.lastIndexOf(")");
                        var pos = props.definition.slice(startPos + 1, endPos).split(",");
                        var lineNo = parseInt(pos[0], 10);
                        context.editorControl.highlightLine(lineNo);
                        context.editorControl.setCursor(lineNo, 0);
                    }
                }
            };
        },

        resetPage: function () {
            this.editorControl.clearErrors();
            this.editorControl.clearHighlightLines();
            this.graphControl.clear();
            this.resultsWidget.clear();
        },

        monitorEclPlayground: function () {
            dom.byId(this.id + "Status").innerHTML = this.wu.state;
            var context = this;
            if (this.wu.isComplete()) {
                this.wu.getInfo({
                    onGetExceptions: function (exceptions) {
                        context.displayExceptions(exceptions);
                    },
                    onGetResults: function (results) {
                        context.displayResults(results);
                    },
                    onGetGraphs: function (graphs) {
                        context.displayGraphs(graphs);
                    },
                    onGetAll: function (workunit) {
                        context.displayAll(workunit);
                    }
                });
            }
        },

        displayExceptions: function (exceptions) {
        },

        displayResults: function (results) {
        },

        displayGraphs: function (graphs) {
            for (var i = 0; i < graphs.length; ++i) {
                var context = this;
                if (i == 0) {
                    this.wu.fetchGraphXgmml(i, function (xgmml) {
                        context.graphControl.loadXGMML(xgmml);
                    });
                } else {
                    this.wu.fetchGraphXgmml(i, function (xgmml) {
                        context.graphControl.loadXGMML(xgmml, true);
                    });
                }
            }
        },

        displayAll: function (workunit) {
            if (this.wu.exceptions.length) {
                this.editorControl.setErrors(this.wu.exceptions);
            }
            this.resultsWidget.refresh(this.wu);
        },

        _onSubmit: function (evt) {
            this.resetPage();
            var context = this;
            this.wu = new Workunit({
                onCreate: function () {
                    context.wu.update({
                        QueryText: context.editorControl.getText()
                    });
                },
                onUpdate: function () {
                    context.wu.submit(context.targetSelectWidget.getValue());
                },
                onSubmit: function () {
                    context.wu.monitor(function () {
                        context.monitorEclPlayground();
                    });
                }
            });
        }
    });
});
