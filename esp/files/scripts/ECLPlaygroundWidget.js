/*##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
require([
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

	"hpcc/EclEditorControl",
	"hpcc/TargetSelectWidget",
	"hpcc/SampleSelectWidget",
	"hpcc/GraphWidget",
	"hpcc/ResultsWidget",
	"hpcc/ESPWorkunit",

	"dojo/text!./templates/ECLPlaygroundWidget.html"
], function (declare, xhr, dom,
				_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, registry,
				EclEditor, TargetSelectWidget, SampleSelectWidget, GraphWidget, ResultsWidget, Workunit,
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

        init: function (wuid, target) {
            this.wuid = wuid;
            this.targetSelectWidget.setValue(target);

            this.initEditor();
            //this.initresultsWidget();

            //  ActiveX will flicker if created before initial layout
            var context = this;
            this.initGraph();
            if (wuid) {
                this.wu = new Workunit({
                    wuid: wuid
                });
                this.wu.fetchText(function (text) {
                    context.editorControl.setText(text);
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
            this.editorControl = new EclEditor({
                domId: this.id + "EclCode"
            });
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
                    context.wu.update(context.editorControl.getText());
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
