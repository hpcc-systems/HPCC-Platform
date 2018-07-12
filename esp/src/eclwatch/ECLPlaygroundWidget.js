define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/xhr",
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
    "hpcc/JSGraphWidget",
    "hpcc/ECLPlaygroundResultsWidget",
    "src/ESPWorkunit",
    "src/ESPQuery",
    "src/Utility",

    "dojo/text!../templates/ECLPlaygroundWidget.html",

    "hpcc/InfoGridWidget"

], function (declare, lang, i18n, nlsHPCC, xhr, dom, query,
    BorderContainer, TabContainer, ContentPane, registry,
    _Widget, EclSourceWidget, TargetSelectWidget, GraphWidget, JSGraphWidget, ResultsWidget, ESPWorkunit, ESPQuery, Utility,
    template) {
        return declare("ECLPlaygroundWidget", [_Widget], {
            templateString: template,
            baseClass: "ECLPlaygroundWidget",
            i18n: nlsHPCC,

            graphType: Utility.isPluginInstalled() ? "GraphWidget" : "JSGraphWidget",
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
            getTitle: function () {
                return this.i18n.title_ECLPlayground;
            },

            _initControls: function () {
                var context = this;
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.targetSelectWidget = registry.byId(this.id + "TargetSelect");

                this.stackController = registry.byId(this.id + "StackController");
                this.stackContainer = registry.byId(this.id + "StackContainer");
                this.errWarnWidget = registry.byId(this.id + "_ErrWarn");
                this.errWarnWidget.onErrorClick = function (line, col) {
                    context.editorControl.setCursor(line, col);
                };
                this.resultsWidget = registry.byId(this.id + "_Results");
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

                this.graphControl.onDoubleClick = function (globalID, keyState) {
                    if (keyState && context.main.KeyState_Shift) {
                        context.graphControl._onSyncSelection();
                    } else {
                        context.graphControl.centerOn(globalID);
                    }
                };
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
                this.graphControl.showNextPrevious(false);
                this.graphControl.showDistance(false);
                this.graphControl.showSyncSelection(false);
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
                            Wuid: context.wu.Wuid,
                            Sequence: 0
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
                        a.textContent = newValue;
                        if (newValue === "...") {
                            a.style.visibility = "hidden"
                        } else if (this.wu && this.wu.Wuid) {
                            a.style.visibility = "visible"
                            a.href = dojoConfig.urlInfo.pathname + "?Widget=WUDetailsWidget&Wuid=" + this.wu.Wuid;
                        }
                    }
                }
                if (name === "hasCompleted") {
                    this.checkIfComplete();
                }
            },

            checkIfComplete: function () {
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
                var fetchedCount = 0;
                for (var i = 0; i < graphs.length; ++i) {
                    var context = this;
                    this.wu.fetchGraphXgmml(i, null, function (xgmml) {
                        ++fetchedCount;
                        context.graphControl.loadXGMML(xgmml, fetchedCount > 1);
                        if (fetchedCount === graphs.length) {
                            context.graphControl.startLayout("dot");
                        }
                    });
                }
            },

            displayAll: function (workunit) {
                if (lang.exists("Exceptions.ECLException", this.wu)) {
                    this.editorControl.setErrors(this.wu.Exceptions.ECLException);
                }
                this.resultsWidget.refresh({
                    Wuid: this.wu.Wuid
                });
            },

            _onSubmit: function (evt) {
                this.resetPage();

                var text = this.editorControl.getText();
                var espQuery = ESPQuery.GetFromRequestXML(this.targetSelectWidget.get("value"), text);

                if (espQuery) {
                    this.stackContainer.selectChild(this.resultsWidget);
                    this.resultsWidget.set("disabled", false);
                    this.resultsWidget.refresh({
                        QuerySetId: espQuery.QuerySetId,
                        Id: espQuery.Id,
                        RequestXml: text
                    });
                } else {
                    var context = this;
                    this.wu = ESPWorkunit.Create({
                        onCreate: function () {
                            context.wu.update({
                                QueryText: text
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
            }
        });
    });
