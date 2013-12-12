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
    "dojo/_base/lang",
    "dojo/dom",
    "dojo/query",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/registry",

    "hpcc/_Widget",
    "hpcc/ECLSourceWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/GraphWidget",
    "hpcc/ECLPlaygroundResultsWidget",
    "hpcc/ESPWorkunit",

    "dojo/text!../templates/ECLPlaygroundWidget.html"
], function (declare, xhr, lang, dom, query,
                BorderContainer, TabContainer, ContentPane, registry,
                _Widget, EclSourceWidget, TargetSelectWidget, GraphWidget, ResultsWidget, ESPWorkunit,
                template) {
    return declare("ECLPlaygroundWidget", [_Widget], {
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

            this.stackController = registry.byId(this.id + "StackController");
            this.stackContainer = registry.byId(this.id + "StackContainer");
            this.errWarnWidget = registry.byId(this.id + "_ErrWarn");
            this.resultsWidget = registry.byId(this.id + "_Results");
            this.resultsWidget.onErrorClick = function (line, col) {
                context.editorControl.setCursor(line, col);
            };
            this.visualizeWidget = registry.byId(this.id + "_Visualize");
        },

        hideTitle: function () {
            var topPane = dom.byId(this.id + "TopPane");
            dojo.destroy(topPane);
            this.borderContainer.resize();
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.Wuid) {
                this.hideTitle();
            }

            this.Wuid = params.Wuid;
            this.targetSelectWidget.init(params);

            this.initEditor();
            this.editorControl.init(params);

            var context = this;
            this.initGraph();

            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var data = this.wu.getData();
                for (var key in data) {
                    this.updateInput(key, null, data[key]);
                }
                this.watchWU();
            } else {
                this.initSamples();
                this.graphControl.watchSelect(this.sampleSelectWidget);
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
            this.sampleSelectWidget.init({
                ECLSamples: true,
                Target: "default.ecl"
            });
        },

        initEditor: function () {
            this.editorControl = registry.byId(this.id + "Source");
        },

        initGraph: function () {
            var context = this;
            this.graphControl = registry.byId(this.id + "GraphControl");
            this.graphControl.onSelectionChanged = function (items) {
                context.editorControl.clearHighlightLines();
                for (var i = 0; i < items.length; ++i) {
                    var props = context.graphControl.getProperties(items[i]);
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

        getGraph: function () {
            return registry.byId(this.id + "GraphControl");
        },

        resetPage: function () {
            this.editorControl.clearErrors();
            this.editorControl.clearHighlightLines();
            this.graphControl.clear();
            this.resultsWidget.clear();
            this.updateInput("State", null, "...");

            this.stackContainer.selectChild(this.resultsWidget);
            this.errWarnWidget.set("disabled", true);
            this.resultsWidget.set("disabled", true);
            this.visualizeWidget.set("disabled", true);
        },

        getTitle: function () {
            return "ECL Playground";
        },

        watchWU: function () {
            if (this.watching) {
                this.watching.unwatch();
            }
            var context = this;
            this.watching = this.wu.watch(function (name, oldValue, newValue) {
                context.updateInput(name, oldValue, newValue);
                if (name === "Exceptions" && newValue) {
                    context.stackContainer.selectChild(context.errWarnWidget);
                    context.errWarnWidget.set("disabled", false);
                    context.errWarnWidget.reset();
                    context.errWarnWidget.init({
                        Wuid: context.wu.Wuid
                    });
                } else if (name === "Results" && newValue) {
                    context.stackContainer.selectChild(context.resultsWidget);
                    context.resultsWidget.set("disabled", false);
                    context.visualizeWidget.set("disabled", false);
                    context.visualizeWidget.reset();
                    context.visualizeWidget.init({
                        Wuid: context.wu.Wuid
                    });
                }
            });
            this.wu.monitor();
        },

        updateInput: function (name, oldValue, newValue) {
            var input = query("input[id=" + this.id + name + "]", this.summaryForm)[0];
            if (input) {
                var dijitInput = registry.byId(this.id + name);
                if (dijitInput) {
                    dijitInput.set("value", newValue);
                } else {
                    input.value = newValue;
                }
            } else {
                var a = query("a[id=" + this.id + name + "]", this.summaryForm)[0];
                if (a) {
                    a.innerHTML = newValue;
                    if (newValue === "...") {
                        a.style.visibility = "hidden"
                    } else if (this.wu && this.wu.Wuid) {
                        a.style.visibility = "visible"
                        a.href = "/esp/files/stub.htm?Widget=WUDetailsWidget&Wuid=" + this.wu.Wuid;
                    }
                }
            }
            if (name === "hasCompleted") {
                this.checkIfComplete();
            }
        },

        checkIfComplete: function() {
            var context = this;
            if (this.wu.isComplete()) {
                this.wu.getInfo({
                    onGetWUExceptions: function (exceptions) {
                        context.displayExceptions(exceptions);
                    },
                    onGetResults: function (results) {
                        context.displayResults(results);
                    },
                    onGetGraphs: function (graphs) {
                        context.displayGraphs(graphs);
                    },
                    onAfterSend: function (workunit) {
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
                this.wu.fetchGraphXgmml(i, function (xgmml) {
                    context.graphControl.loadXGMML(xgmml, i > 0);
                });
            }
        },

        displayAll: function (workunit) {
            if (lang.exists("Exceptions.ECLException", this.wu)) {
                this.editorControl.setErrors(this.wu.Exceptions.ECLException);
            }
            this.resultsWidget.refresh(this.wu);
        },

        _onSubmit: function (evt) {
            this.resetPage();
            var context = this;
            this.wu = ESPWorkunit.Create({
                onCreate: function () {
                    context.wu.update({
                        QueryText: context.editorControl.getText()
                    });
                    context.watchWU();
                },
                onUpdate: function () {
                    context.wu.submit(context.targetSelectWidget.getValue());
                },
                onSubmit: function () {
                }
            });
        }
    });
});
