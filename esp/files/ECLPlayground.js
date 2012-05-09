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
	"dojo/_base/config",
	"dojo/_base/fx",
	"dojo/_base/window",
	"dojo/dom",
	"dojo/dom-style",
	"dojo/dom-geometry",
	"dojo/on",
	"hpcc/EclEditorControl",
	"hpcc/GraphControl",
	"hpcc/ResultsControl",
	"hpcc/SampleSelectControl",
	"hpcc/ESPWorkunit"
], function (baseConfig, baseFx, baseWindow, dom, domStyle, domGeometry, on, EclEditor, GraphControl, ResultsControl, Select, Workunit) {
	var editorControl = null,
			graphControl = null,
			resultsControl = null,
			sampleSelectControl = null,

			initUi = function () {
				on(dom.byId("submitBtn"), "click", doSubmit);
				initSamples();
				initEditor();
				initResultsControl();
				//  ActiveX will flicker if created before initial layout
				setTimeout(function () {
					initGraph();
					var wuid = dojo.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search[0] === "?" ? 1 : 0)))["wuid"];
					if (wuid) {
						doLoad(wuid);
					}
				}, 1);
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
								editorControl.highlightLine(parseInt(pos[0], 10));
							}
						}
					}
				}, dom.byId("graphs"));
			},

			initResultsControl = function () {
				var _resultsControl = resultsControl = new ResultsControl({
					resultsSheetID: "bottomPane",
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

			doLoad = function (wuid) {
				wu = new Workunit({
					wuid: wuid,

					onMonitor: function () {
						dom.byId("status").innerHTML = wu.state;
						if (wu.isComplete())
							wu.getInfo();
					},
					onGetText: function () {
						editorControl.setText(wu.getText());
					},
					onGetInfo: function () {
						if (wu.errors.length) {
							editorControl.setErrors(wu.errors);
							resultsControl.addExceptionTab(wu.errors);
						}
						wu.getResults();
						wu.getGraphs();
					},
					onGetGraph: function (idx) {
						graphControl.loadXGMML(wu.graphs[idx].xgmml, true);
					},
					onGetResult: function (idx) {
						resultsControl.addDatasetTab(wu.results[idx].dataset);
					}
				});
				wu.getText();
				wu.monitor();
			},

			doSubmit = function (evt) {
				resetPage();
				wu = new Workunit({
					wuid: "",

					onCreate: function () {
						wu.update(editorControl.getText());
					},
					onUpdate: function () {
						wu.submit("hthor");
					},
					onSubmit: function () {
					},
					onMonitor: function () {
						dom.byId("status").innerHTML = wu.state;
						if (wu.isComplete())
							wu.getInfo();
					},
					onGetInfo: function () {
						if (wu.errors.length) {
							editorControl.setErrors(wu.errors);
							resultsControl.addExceptionTab(wu.errors);
						}
						wu.getResults();
						wu.getGraphs();
					},
					onGetGraph: function (idx) {
						graphControl.loadXGMML(wu.graphs[idx].xgmml, true);
					},
					onGetResult: function (idx) {
						resultsControl.addDatasetTab(wu.results[idx].dataset);
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
				baseFx.fadeOut({
					node: dom.byId("loadingOverlay"),
					onEnd: function (node) {
						domStyle.set(node, "display", "none");
					}
				}).play();
			}

	return {
		init: function () {
			startLoading();
			initUi();
			endLoading();
		}
	};
});