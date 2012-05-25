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
define([
	"dojo/_base/fx",
	"dojo/_base/window",
	"dojo/dom",
	"dojo/dom-style",
	"dojo/dom-geometry",
	"dojo/on",
	"dojo/ready",
	"dijit/registry",
	"hpcc/EclEditorControl",
	"hpcc/GraphControl",
	"hpcc/ResultsControl",
	"hpcc/SampleSelectControl",
	"hpcc/ESPWorkunit"
], function (fx, baseWindow, dom, domStyle, domGeometry, on, ready, registry, EclEditor, GraphControl, ResultsControl, Select, Workunit) {
	var wu = null,
			editorControl = null,
			graphControl = null,
			resultsControl = null,
			sampleSelectControl = null,

			initUi = function () {
				var wuid = dojo.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) == "?" ? 1 : 0)))["Wuid"];
				on(dom.byId("submitBtn"), "click", doSubmit);

				if (wuid) {
					dojo.destroy("topPane");
					registry.byId("appLayout").resize();
				} else {
					initSamples();
				}
				initEditor();
				initResultsControl();

				//  ActiveX will flicker if created before initial layout
				setTimeout(function () {
					initGraph();
					if (wuid) {
						wu = new Workunit({
							wuid: wuid
						});
						wu.fetchText(function (text) {
							editorControl.setText(text);
						});
						wu.monitor(monitorEclPlayground);
					}
				}, 1);
			},

			initUiResults = function () {
				var wuid = dojo.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) == "?" ? 1 : 0)))["Wuid"];
				var sequence = dojo.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) == "?" ? 1 : 0)))["Sequence"];
				initResultsControl(sequence);
				if (wuid) {
					wu = new Workunit({
						wuid: wuid
					});
					var monitorCount = 4;
					wu.monitor(function () {
						dom.byId("loadingMessage").innerHTML = wu.state;
						if (wu.isComplete() || ++monitorCount % 5 == 0) {
							wu.getInfo({
								onGetResults: displayResults
							});
						}
					});
				}
			},

			initSamples = function () {
				sampleSelectControl = new Select({
					id: "sampleSelect",
					samplesURL: "ecl/ECLPlaygroundSamples.json",
					onNewSelection: function (eclText) {
						resetPage();
						editorControl.setText(eclText);
					}
				});
			},

			initEditor = function () {
				editorControl = new EclEditor({
					domId: "eclCode"
				});
			},

			initGraph = function () {
				graphControl = new GraphControl({
					id: "gvc",
					width: "100%",
					height: "100%",
					onInitialize: function () {
					},

					onLayoutFinished: function () {
						graphControl.obj.setMessage('');
						graphControl.obj.centerOnItem(0, true);
					},

					onMouseDoubleClick: function (item) {
						graphControl.obj.centerOnItem(item, true);
					},

					onSelectionChanged: function (items) {
						editorControl.clearHighlightLines();
						for (var i = 0; i < items.length; ++i) {
							var props = graphControl.obj.getProperties(items[i]);
							if (props.definition) {
								var startPos = props.definition.indexOf("(");
								var endPos = props.definition.lastIndexOf(")");
								var pos = props.definition.slice(startPos + 1, endPos).split(",");
								var lineNo = parseInt(pos[0], 10);
								editorControl.highlightLine(lineNo);
								editorControl.setCursor(lineNo, 0);
							}
						}

					}
				}, dom.byId("graphs"));
			},

			initResultsControl = function (sequence) {
				resultsControl = new ResultsControl({
					resultsSheetID: "resultsPane",
					sequence: sequence,
					onErrorClick: function (line, col) {
						editorControl.setCursor(line, col);
					}
				});
			},

			resetPage = function () {
				editorControl.clearErrors();
				editorControl.clearHighlightLines();
				graphControl.clear();
				resultsControl.clear();
			},

			monitorEclPlayground = function () {
				dom.byId("status").innerHTML = wu.state;
				if (wu.isComplete()) {
					wu.getInfo({
						onGetExceptions: displayExceptions,
						onGetResults: displayResults,
						onGetGraphs: displayGraphs
					});
				}
			},

			displayExceptions = function (exceptions) {
				if (exceptions.length) {
					editorControl.setErrors(wu.exceptions);
					resultsControl.addExceptionTab(wu.exceptions);
				}
			},

			displayResults = function (results) {
				resultsControl.refreshResults(wu);
			},

			displayGraphs = function (graphs) {
				for (var i = 0; i < graphs.length; ++i) {
					wu.fetchGraphXgmml(i, function (xgmml) {
						graphControl.loadXGMML(xgmml, true);
					});
				}
			},

			doSubmit = function (evt) {
				resetPage();
				wu = new Workunit({
					onCreate: function () {
						wu.update(editorControl.getText());
					},
					onUpdate: function () {
						wu.submit("hthor");
					},
					onSubmit: function () {
						wu.monitor(monitorEclPlayground);
					}
				});
			},

			startLoading = function (targetNode) {
				var overlayNode = dom.byId("loadingOverlay");
				if ("none" == domStyle.get(overlayNode, "display")) {
					var coords = domGeometry.getMarginBox(targetNode || baseWindow.body());
					domGeometry.setMarginBox(overlayNode, coords);
					domStyle.set(dom.byId("loadingOverlay"), {
						display: "block",
						opacity: 1
					});
				}
			},

			endLoading = function () {
				fx.fadeOut({
					node: dom.byId("loadingOverlay"),
					duration: 175,
					onEnd: function (node) {
						domStyle.set(node, "display", "none");
					}
				}).play();
			}

	return {
		init: function () {
			startLoading();
			ready(function () {
				initUi();
				endLoading();
			});
		},
		initResults: function () {
			startLoading();
			ready(function () {
				initUiResults();
				endLoading();
			});
		}
	};
});