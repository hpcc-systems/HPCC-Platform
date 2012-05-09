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
				case '3':
					//WUStateCompleted:
				case '4':
					//WUStateFailed:
				case '5':
					//WUStateArchived:
				case '7':
					//WUStateAborted:
					return true;
			}
			return false;
		},
		monitor: function (callback) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['rawxml_'] = "1";

			var context = this;
			xhr.post({
				url: this.getBaseURL() + "/WUQuery",
				handleAs: "xml",
				content: request,
				load: function (xmlDom) {
					var workunit = context.getValue(xmlDom, "ECLWorkunit");
					context.stateID = workunit.StateID;
					context.state = workunit.State;
					if (callback) {
						callback(context);
					}

					if (!context.isComplete()) {
						setTimeout(function () {
							context.monitor(callback);
						}, 200);
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
				url: this.getBaseURL() + "/WUCreate",
				handleAs: "xml",
				content: request,
				load: function (xmlDom) {
					context.wuid = context.getValue(xmlDom, "Wuid");
					context.onCreate();
				},
				error: function () {
				}
			});
		},
		update: function (ecl) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['QueryText'] = ecl;
			request['rawxml_'] = "1";

			var context = this;
			xhr.post({
				url: this.getBaseURL() + "/WUUpdate",
				handleAs: "xml",
				content: request,
				load: function (xmlDom) {
					context.onUpdate();
				},
				error: function () {
				}
			});
		},
		submit: function (target) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['Cluster'] = target;
			request['rawxml_'] = "1";

			var context = this;
			xhr.post({
				url: this.getBaseURL() + "/WUSubmit",
				handleAs: "xml",
				content: request,
				load: function (xmlDom) {
					context.onSubmit();
				},
				error: function () {
				}
			});
		},
		getInfo: function (args) {
			var request = {
				Wuid: this.wuid,
				IncludeExceptions: args.onGetExceptions ? true : false,
				IncludeGraphs: args.onGetGraphs ? true : false,
				IncludeSourceFiles: false,
				IncludeResults: args.onGetResults ? true : false,
				IncludeResultsViewNames: false,
				IncludeVariables: false,
				IncludeTimers: args.onGetTimers ? true : false,
				IncludeDebugValues: false,
				IncludeApplicationValues: false,
				IncludeWorkflows: false,
				SuppressResultSchemas: args.onGetResults ? false : true,
				rawxml_: true
			};

			var context = this;
			xhr.post({
				url: this.getBaseURL() + "/WUInfo",
				handleAs: "xml",
				content: request,
				load: function (xmlDom) {
					var workunit = context.getValue(xmlDom, "Workunit", ["ECLException", "ECLResult", "ECLGraph", "ECLTimer", "ECLSchemaItem"]);
					if (workunit.Query.Text && args.onGetText) {
						context.text = workunit.Query.Text;
						args.onGetText(context.text);
					}
					if (workunit.Exceptions && args.onGetExceptions) {
						context.exceptions = workunit.Exceptions;
						args.onGetExceptions(context.exceptions);
					}
					if (workunit.Results && args.onGetResults) {
						context.results = [];
						var results = workunit.Results;
						for (var i = 0; i < results.length; ++i) {
							context.results.push(new ESPResult(lang.mixin({ wuid: context.wuid }, results[i])));
						}
						args.onGetResults(context.results);
					}
					if (workunit.Timers && args.onGetTimers) {
						context.timers = workunit.Timers;
						args.onGetTimers(context.timers);
					}
					if (workunit.Graphs && args.onGetGraphs) {
						context.graphs = workunit.Graphs;
						args.onGetGraphs(context.graphs)
					}
					if (args.onGetAll) {
						args.onGetAll(workunit);
					}
				},
				error: function () {
				}
			});
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
		fetchGraphXgmml: function (idx, onFetchGraphXgmml) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['GraphName'] = this.graphs[idx].Name;
			request['rawxml_'] = "1";

			var context = this;
			xhr.post({
				url: this.getBaseURL() + "/WUGetGraph",
				handleAs: "xml",
				content: request,
				load: function (xmlDom) {
					context.graphs[idx].xgmml = context.getValue(xmlDom, "Graph");
					onFetchGraphXgmml(context.graphs[idx].xgmml);
				},
				error: function () {
				}
			});
		}
	});
});
