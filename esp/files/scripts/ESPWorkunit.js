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
	"dojo/_base/declare", 
	"dojo/_base/xhr", 
	"hpcc/ESPBase"], function(baseConfig, declare, baseXhr, ESPBase) {
	return declare(ESPBase, {
		wuid : "",

		stateID : 0,
		state : "",

		text: "",

		resultCount : 0,
		results : [],

		graphNameIndex : [],
		graphs : [],

		exceptions : [],
		errors : [],
		timers : [],

		onCreate : function() {
		},
		onUpdate : function() {
		},
		onSubmit : function() {
		},
		onMonitor : function() {
		},
		onComplete : function() {
		},
		onGetText: function () {
		},
		onGetInfo : function() {
		},
		onGetGraph : function(name) {
		},
		constructor : function(args) {
			declare.safeMixin(this, args);

			if(!this.wuid) {
				this.create();
			}
		},
		isComplete : function() {
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
		monitor : function() {
			var request = {};
			request['Wuid'] = this.wuid;
			request['rawxml_'] = "1";

			var context = this;
			baseXhr.post({
				url : this.getBaseURL() + "/WUQuery",
				handleAs : "xml",
				content : request,
				load : function(xmlDom) {
					context.stateID = context.parseKeyValue(xmlDom, "StateID");
					context.state = context.parseKeyValue(xmlDom, "State");
					context.onMonitor();
					if(!context.isComplete()) {
						setTimeout(function() {
							context.monitor();
						}, 200);
					}
				},
				error : function() {
					done = true;
				}
			});
		},
		create : function(ecl, _sync) {
			var request = {};
			request['rawxml_'] = "1";

			var context = this;
			baseXhr.post({
				url : this.getBaseURL() + "/WUCreate",
				handleAs : "xml",
				content : request,
				load : function(xmlDom) {
					context.wuid = context.parseKeyValue(xmlDom, "Wuid");
					context.onCreate();
				},
				error : function() {
				}
			});
		},
		update : function(ecl, _sync) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['QueryText'] = ecl;
			request['rawxml_'] = "1";

			var context = this;
			baseXhr.post({
				url : this.getBaseURL() + "/WUUpdate",
				handleAs : "xml",
				content : request,
				sync : _sync,
				load : function(xmlDom) {
					context.onUpdate();
				},
				error : function() {
				}
			});
		},
		submit : function(target, _sync) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['Cluster'] = target;
			request['rawxml_'] = "1";

			var context = this;
			baseXhr.post({
				url : this.getBaseURL() + "/WUSubmit",
				handleAs : "xml",
				content : request,
				sync : _sync,
				load : function(xmlDom) {
					context.onSubmit();
					context.monitor();
				},
				error : function() {
				}
			});
		},
		getInfoEx: function (_sync, func, IncludeExceptions, IncludeGraphs, IncludeSourceFiles, IncludeResults, IncludeResultsViewNames, IncludeVariables, IncludeTimers, IncludeDebugValues, IncludeApplicationValues, IncludeWorkflows, IncludeResultSchemas) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['IncludeExceptions'] = IncludeExceptions;
			request['IncludeGraphs'] = IncludeGraphs;
			request['IncludeSourceFiles'] = IncludeSourceFiles;
			request['IncludeResults'] = IncludeResults;
			request['IncludeResultsViewNames'] = IncludeResultsViewNames;
			request['IncludeVariables'] = IncludeVariables;
			request['IncludeTimers'] = IncludeTimers;
			request['IncludeDebugValues'] = IncludeDebugValues;
			request['IncludeApplicationValues'] = IncludeApplicationValues;
			request['IncludeWorkflows'] = IncludeWorkflows;
			request['SuppressResultSchemas'] = !IncludeResultSchemas;
			request['rawxml_'] = "1";

			baseXhr.post({
				url: this.getBaseURL() + "/WUInfo",
				handleAs: "xml",
				content: request,
				sync: _sync,
				load: func,
				error: function () {
				}
			});
		},
		getText: function () {
			var context = this;
			this.getInfoEx(false, function (xmlDom) {
				context.text = context.parseKeyValue(xmlDom, "Text");
				context.onGetText();
			});
			return wu.text;
		},
		getInfo: function (_sync) {
			var context = this;
			this.getInfoEx(_sync, function (xmlDom) {
				context.exceptions = context.parseRows(xmlDom, "Exception");
				context.errors = context.parseRows(xmlDom, "ECLException");
				context.timers = context.parseRows(xmlDom, "ECLTimer");
				context.graphs = context.parseRows(xmlDom, "ECLGraph");
				for (var i = 0; i < context.graphs.length; ++i) {
					context.graphNameIndex[context.graphs[i].Name] = i;
				}
				context.results = context.parseRows(xmlDom, "ECLResult");
				context.onGetInfo();
			}, true, true, false, true, false, false, true, false, false, false, false);
		},
		getGraphs : function() {
			for(var i = 0; i < this.graphs.length; ++i) {
				this.getGraph(i);
			}
		},
		getGraph : function(idx, _sync) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['GraphName'] = this.graphs[idx].Name;
			request['rawxml_'] = "1";

			var context = this;
			baseXhr.post({
				url : this.getBaseURL() + "/WUGetGraph",
				handleAs : "xml",
				content : request,
				sync : _sync,
				load : function(xmlDom) {
					context.graphs[idx].xgmml = context.parseKeyValue(xmlDom, "Graph");
					context.onGetGraph(idx);
				},
				error : function() {
				}
			});
		},
		getResults : function() {
			for(var i = 0; i < this.results.length; ++i) {
				this.getResult(i);
			}
		},
		getResult : function(idx, _sync) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['Sequence'] = this.results[idx].Sequence;
			request['Start'] = 0;
			request['Count'] = 999;
			request['rawxml_'] = "1";

			var context = this;
			baseXhr.post({
				url : this.getBaseURL() + "/WUResult",
				handleAs : "xml",
				content : request,
				sync : _sync,
				load : function(xmlDom) {
					var name = context.parseKeyValue(xmlDom, "Name");
					var resultDom = xmlDom.getElementsByTagName("Result");
					if(resultDom.length) {
						context.results[idx].dataset = context.parseDataset(resultDom[0], name, "Row");
					}
					context.onGetResult(idx);
				},
				error : function() {
				}
			});
		},
		getInfoFast : function(_sync) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['rawxml_'] = "1";

			var context = this;
			baseXhr.post({
				url : this.getBaseURL() + "/WUQuery",
				handleAs : "xml",
				content : request,
				sync : _sync,
				load : function(xmlDom) {
					context.stateID = context.parseKeyValue(xmlDom, "StateID");
					context.state = context.parseKeyValue(xmlDom, "State");
				},
				error : function() {
					done = true;
				}
			});
		}
	});
});
