﻿/*##############################################################################
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
	"dojo/_base/declare",
	"dojo/_base/lang",
	"dojo/_base/xhr",
	"hpcc/ESPResult",
	"hpcc/ESPBase"
], function (declare, lang, xhr, ESPResult, ESPBase) {
	return declare(ESPBase, {
		wuid: "",

		stateID: 0,
		state: "",

		text: "",

		resultCount: 0,
		results: [],

		graphs: [],

		exceptions: [],
		timers: [],

		onCreate: function () {
		},
		onUpdate: function () {
		},
		onSubmit: function () {
		},
		constructor: function (args) {
			declare.safeMixin(this, args);

			if (!this.wuid) {
				this.create();
			}
		},
		isComplete: function () {
			switch (this.stateID) {
				case 3:	//WUStateCompleted:
				case 4:	//WUStateFailed:
				case 5:	//WUStateArchived:
				case 7:	//WUStateAborted:
					return true;
			}
			return false;
		},
		monitor: function (callback, monitorDuration) {
			if (!monitorDuration)
				monitorDuration = 0;
			var request = {};
			request['Wuid'] = this.wuid;
			request['rawxml_'] = "1";

			var context = this;
			xhr.post({
				url: this.getBaseURL() + "/WUQuery.json",
				handleAs: "json",
				content: request,
				load: function (response) {
					var workunit = response.WUQueryResponse.Workunits.ECLWorkunit[0];
					context.stateID = workunit.StateID;
					context.state = workunit.State;
					if (callback) {
						callback(context);
					}

					if (!context.isComplete()) {
						var timeout = 30;	// Seconds

						if (monitorDuration < 5) {
							timeout = 1;
						} else if (monitorDuration < 10) {
							timeout = 2;
						} else if (monitorDuration < 30) {
							timeout = 5;
						} else if (monitorDuration < 60) {
							timeout = 10;
						} else if (monitorDuration < 120) {
							timeout = 20;
						}
						setTimeout(function () {
							context.monitor(callback, monitorDuration + timeout);
						}, timeout * 1000);
					}
				},
				error: function () {
					done = true;
				}
			});
		},
		create: function (ecl) {
			var request = {};
			request['rawxml_'] = "1";

			var context = this;
			xhr.post({
				url: this.getBaseURL() + "/WUCreate.json",
				handleAs: "json",
				content: request,
				load: function (response) {
					context.wuid = response.WUCreateResponse.Workunit.Wuid;
					context.onCreate();
				},
				error: function () {
				}
			});
		},
		update: function (ecl, graphName, svg) {
			var request = {};
			request['Wuid'] = this.wuid;
			if (ecl) {
				request['QueryText'] = ecl;
			}
			if (graphName && svg) {
				/*
				request['ApplicationValues'] = {
					ApplicationValue: {
						itemcount: 1,
						Application: "ESPWorkunit.js",
						Name: graphName + "_SVG",
						Value: svg
					}
				}
				*/
				request['ApplicationValues.ApplicationValue.itemcount'] = 1;
				request['ApplicationValues.ApplicationValue.0.Application'] = "ESPWorkunit.js";
				request['ApplicationValues.ApplicationValue.0.Name'] = graphName + "_SVG";
				request['ApplicationValues.ApplicationValue.0.Value'] = svg;
			}
			request['rawxml_'] = "1";

			var context = this;
			xhr.post({
				url: this.getBaseURL() + "/WUUpdate.json",
				handleAs: "json",
				content: request,
				load: function (response) {
					context.onUpdate();
				},
				error: function (error) {
				}
			});
		},
		submit: function (target) {
			var request = {
				Wuid: this.wuid,
				Cluster: target
			};
			request['rawxml_'] = "1";

			var context = this;
			xhr.post({
				url: this.getBaseURL() + "/WUSubmit.json",
				handleAs: "json",
				content: request,
				load: function (response) {
					context.onSubmit();
				},
				error: function (error) {
				}
			});
		},
		getInfo: function (args) {
			var request = {
				Wuid: this.wuid,
				TruncateEclTo64k: args.onGetText ? false : true,
				IncludeExceptions: args.onGetExceptions ? true : false,
				IncludeGraphs: args.onGetGraphs ? true : false,
				IncludeSourceFiles: false,
				IncludeResults: args.onGetResults ? true : false,
				IncludeResultsViewNames: false,
				IncludeVariables: false,
				IncludeTimers: args.onGetTimers ? true : false,
				IncludeDebugValues: false,
				IncludeApplicationValues: args.onGetApplicationValues ? true : false,
				IncludeWorkflows: false,
				SuppressResultSchemas: args.onGetResults ? false : true,
			};
			request['rawxml_'] = "1";

			var context = this;
			xhr.post({
				url: this.getBaseURL() + "/WUInfo.json",
				handleAs: "json",
				content: request,
				load: function (response) {
					//var workunit = context.getValue(xmlDom, "Workunit", ["ECLException", "ECLResult", "ECLGraph", "ECLTimer", "ECLSchemaItem", "ApplicationValue"]);
					var workunit = response.WUInfoResponse.Workunit;
					if (args.onGetText && workunit.Query.Text) {
						context.text = workunit.Query.Text;
						args.onGetText(context.text);
					}
					if (args.onGetExceptions && workunit.Exceptions && workunit.Exceptions.ECLException) {
						context.exceptions = [];
						for (var i = 0; i < workunit.Exceptions.ECLException.length; ++i) {
							if (workunit.Exceptions.ECLException[i].Severity == "Error" || 
								workunit.Exceptions.ECLException[i].Severity == "Warning")
							context.exceptions.push(workunit.Exceptions.ECLException[i]);						
						}
						args.onGetExceptions(context.exceptions);
					}
					if (args.onGetApplicationValues && workunit.ApplicationValues && workunit.ApplicationValues.ApplicationValue) {
						context.applicationValues = workunit.ApplicationValues.ApplicationValue;
						args.onGetApplicationValues(context.applicationValues)
					}
					if (args.onGetResults && workunit.Results && workunit.Results.ECLResult) {
						context.results = [];
						var results = workunit.Results.ECLResult;
						for (var i = 0; i < results.length; ++i) {
							context.results.push(new ESPResult(lang.mixin({ wuid: context.wuid }, results[i])));
						}
						args.onGetResults(context.results);
					}
					if (args.onGetTimers && workunit.Timers && workunit.Timers.ECLTimer) {
						context.timers = workunit.Timers.ECLTimer;
						args.onGetTimers(context.timers);
					}
					if (args.onGetGraphs && workunit.Graphs && workunit.Graphs.ECLGraph) {
						context.graphs = workunit.Graphs.ECLGraph;
						if (context.timers || context.applicationValues) {
							for (var i = 0; i < context.graphs.length; ++i) {
								if (context.timers) {
									context.graphs[i].Time = 0;
									for (var j = 0; j < context.timers.length; ++j) {
										if (context.timers[j].GraphName == context.graphs[i].Name) {
											context.graphs[i].Time += parseFloat(context.timers[j].Value);
										}
										context.graphs[i].Time = Math.round(context.graphs[i].Time * 1000) / 1000;
									}
								}
								if (context.applicationValues) {
									var idx = context.getApplicationValueIndex("ESPWorkunit.js", context.graphs[i].Name + "_SVG");
									if (idx >= 0) {
										context.graphs[i].svg = context.applicationValues[idx].Value;
									}
								}
							}
						}
						args.onGetGraphs(context.graphs)
					}
					if (args.onGetAll) {
						args.onGetAll(workunit);
					}
				},
				error: function (e) {
				}
			});
		},
		getGraphIndex: function (name) {
			for (var i = 0; i < this.graphs.length; ++i) {
				if (this.graphs[i].Name == name) {
					return i;
				}
			}
			return -1;
		},
		getApplicationValueIndex: function (application, name) {
			for (var i = 0; i < this.applicationValues.length; ++i) {
				if (this.applicationValues[i].Application == application && this.applicationValues[i].Name == name) {
					return i;
				}
			}
			return -1;
		},
		fetchText: function (onFetchText) {
			if (this.text) {
				onFetchText(this.text);
				return;
			}

			this.getInfo({
				onGetText: onFetchText
			});
		},
		fetchResults: function (onFetchResults) {
			if (this.results && this.results.length) {
				onFetchResults(this.results);
				return;
			}

			this.getInfo({
				onGetResults: onFetchResults
			});
		},
		fetchGraphs: function (onFetchGraphs) {
			if (this.graphs && this.graphs.length) {
				onFetchGraphs(this.graphs);
				return;
			}

			this.getInfo({
				onGetGraphs: onFetchGraphs
			});
		},
		fetchGraphXgmmlByName: function (name, onFetchGraphXgmml) {
			var idx = this.getGraphIndex(name);
			if (idx >= 0) {
				this.fetchGraphXgmml(idx, onFetchGraphXgmml);
			}
		},
		fetchGraphXgmml: function (idx, onFetchGraphXgmml) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['GraphName'] = this.graphs[idx].Name;
			request['rawxml_'] = "1";

			var context = this;
			xhr.post({
				url: this.getBaseURL() + "/WUGetGraph.json",
				handleAs: "json",
				content: request,
				load: function (response) {
					context.graphs[idx].xgmml = response.WUGetGraphResponse.Graphs.ECLGraphEx[0].Graph;
					onFetchGraphXgmml(context.graphs[idx].xgmml, context.graphs[idx].svg);
				},
				error: function () {
				}
			});
		},
		setGraphSvg: function (graphName, svg) {
			var idx = this.getGraphIndex(graphName);
			if (idx >= 0) {
				this.graphs[idx].svg = svg;
				this.update(null, graphName, svg);
			}
		}
	});
});
