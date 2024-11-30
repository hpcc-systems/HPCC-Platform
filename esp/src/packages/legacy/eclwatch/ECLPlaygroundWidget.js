define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/dom",
    "dojo/query",
    "dojo/dom-style",

    "dijit/registry",

    "@hpcc-js/comms",

    "hpcc/_Widget",
    "src/ESPWorkunit",
    "src/ESPQuery",

    "dojo/text!../templates/ECLPlaygroundWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "hpcc/ECLSourceWidget",
    "hpcc/TargetSelectWidget",
    "src/Graph7Widget",
    "hpcc/ECLPlaygroundResultsWidget",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "hpcc/InfoGridWidget",
    "hpcc/VizWidget"

], function (declare, lang, nlsHPCCMod, dom, query, domStyle,
    registry,
    hpccComms,
    _Widget, ESPWorkunit, ESPQuery,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("ECLPlaygroundWidget", [_Widget], {
        templateString: template,
        baseClass: "ECLPlaygroundWidget",
        i18n: nlsHPCC,

        graphType: "Graph7Widget",
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
            this.publishForm = registry.byId(this.id + "PublishForm");
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

            var context = this;
            this.Wuid = params.Wuid;
            this.targetSelectWidget.init(params);
            this.targetSelectWidget.on("change", function () {
                var logicalCluster = context.targetSelectWidget.selectedTarget();
                var submitBtn = registry.byId(context.id + "SubmitBtn");
                var publishBtn = registry.byId(context.id + "PublishBtn");
                if (logicalCluster.QueriesOnly) {
                    domStyle.set(submitBtn.domNode, "display", "none");
                    domStyle.set(publishBtn.domNode, "display", null);
                } else {
                    domStyle.set(submitBtn.domNode, "display", null);
                    domStyle.set(publishBtn.domNode, "display", "none");
                }
            });

            this.initEditor();
            this.editorControl.init(params);

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
            }

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
            this.graphControl.init({});
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
                if (name === "Exceptions" && newValue && newValue.ECLException && newValue.ECLException.length) {
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
                        a.style.visibility = "hidden";
                    } else if (this.wu && this.wu.Wuid) {
                        a.style.visibility = "visible";
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
            this.graphControl.params = {
                Wuid: this.wu.Wuid
            };
            this.graphControl.doInit(this.wu.Wuid);
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
        },

        _onPublish: function (evt) {
            this.resetPage();
            if (this.publishForm.validate()) {
                registry.byId(this.id + "PublishBtn").closeDropDown();
                var selectedTarget = this.targetSelectWidget.selectedTarget();
                var ecl = this.editorControl.getText();
                var jobname = dom.byId(this.id + "Jobname2").value;
                var context = this;
                hpccComms.Workunit.compile({ baseUrl: "" }, selectedTarget.Name, ecl).then(function (wu) {
                    context.wu = wu;
                    context.wu.update({ Jobname: jobname });
                    return context.wu.watchUntilComplete(function () {
                        context.updateInput("State", "", context.wu.State);
                    });
                }).then(function () {
                    context.updateInput("State", "", "Publishing");
                    return context.wu.publish(jobname);
                }).then(function (response) {
                    var a = dojo.byId(context.id + "State");
                    a.textContent = "Published";
                    a.style.visibility = "visible";
                    a.href = dojoConfig.urlInfo.pathname + "?QuerySetId=" + response.QuerySet + "&Id=" + response.QueryId + "&Widget=QuerySetDetailsWidget";
                });
            }
        }
    });
});