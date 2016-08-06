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
	"dojox/xml/parser", 
	"dojox/xml/DomParser", 
	"hpcc/EspHelper"], function(baseConfig, declare, baseXhr, xmlParser, xmlDomParser, EspHelper) {
	return declare(null, {
		wuid : "",
		stateID : 0,
		state : "",

		resultCount : 0,
		results : [],

		graphNameIndex : [],
		graphs : [],

		exceptions : [],
		errors : [],
		timers : [],

		espHelper : null,

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
		onGetInfo : function() {
		},
		onGetGraph : function(name) {
		},
		constructor : function(args) {
			declare.safeMixin(this, args);

			this.espHelper = new EspHelper();
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
				url : this.espHelper.getBaseURL() + "/WUQuery",
				handleAs : "text",
				content : request,
				load : function(response) {
					var xmlDom = xmlDomParser.parse(response);
					context.stateID = context.espHelper.parseKeyValue(xmlDom, "StateID");
					context.state = context.espHelper.parseKeyValue(xmlDom, "State");
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
				url : this.espHelper.getBaseURL() + "/WUCreate",
				handleAs : "text",
				content : request,
				load : function(response) {
					var xmlDom = xmlDomParser.parse(response);
					context.wuid = context.espHelper.parseKeyValue(xmlDom, "Wuid");
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
				url : this.espHelper.getBaseURL() + "/WUUpdate",
				handleAs : "text",
				content : request,
				sync : _sync,
				load : function(response) {
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
				url : this.espHelper.getBaseURL() + "/WUSubmit",
				handleAs : "text",
				content : request,
				sync : _sync,
				load : function(response) {
					context.onSubmit();
					context.monitor();
				},
				error : function() {
				}
			});
		},
		getInfo : function(_sync) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['IncludeExceptions'] = true;
			request['IncludeGraphs'] = true;
			request['IncludeSourceFiles'] = false;
			request['IncludeResults'] = true;
			request['IncludeResultsViewNames'] = false;
			request['IncludeVariables'] = false;
			request['IncludeTimers'] = true;
			request['IncludeDebugValues'] = false;
			request['IncludeApplicationValues'] = false;
			request['IncludeWorkflows'] = false;
			request['SuppressResultSchemas'] = true;
			request['rawxml_'] = "1";

			var context = this;
			baseXhr.post({
				url : this.espHelper.getBaseURL() + "/WUInfo",
				handleAs : "text",
				content : request,
				sync : _sync,
				load : function(response) {
					var xmlDom = xmlDomParser.parse(response);
					context.exceptions = context.espHelper.parseRows(xmlDom, "Exception");
					context.errors = context.espHelper.parseRows(xmlDom, "ECLException");
					context.timers = context.espHelper.parseRows(xmlDom, "ECLTimer");
					context.graphs = context.espHelper.parseRows(xmlDom, "ECLGraph");
					for(var i = 0; i < context.graphs.length; ++i) {
						context.graphNameIndex[context.graphs[i].Name] = i;						
					}
					context.results = context.espHelper.parseRows(xmlDom, "ECLResult");
					context.onGetInfo();
				},
				error : function() {
				}
			});
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
				url : this.espHelper.getBaseURL() + "/WUGetGraph",
				handleAs : "text",
				content : request,
				sync : _sync,
				load : function(response) {
					var xmlDom = xmlDomParser.parse(response);
					context.graphs[idx].xgmml = context.espHelper.parseKeyValue(xmlDom, "Graph");
					context.onGetGraph(idx);
				},
				error : function() {
				}
			});
		},
		getResults : function() {
			for(var i = 0; i < this.graphs.length; ++i) {
				this.getResult(i);
			}
		},
		getResult : function(idx, _sync) {
			var request = {};
			request['Wuid'] = this.wuid;
			request['Sequence'] = this.graphs[idx].Sequence;
			request['Start'] = 0;
			request['Count'] = 999;
			request['rawxml_'] = "1";

			var context = this;
			baseXhr.post({
				url : this.espHelper.getBaseURL() + "/WUResult",
				handleAs : "text",
				content : request,
				sync : _sync,
				load : function(response) {
					var xmlDom = xmlDomParser.parse(response);
					var name = context.espHelper.parseKeyValue(xmlDom, "Name");
					var resultDom = xmlDom.getElementsByTagName("Result");
					if(resultDom.length) {
						context.results[idx].dataset = context.espHelper.parseDataset(resultDom[0], name, "Row");
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
				url : this.espHelper.getBaseURL() + "/WUQuery",
				handleAs : "text",
				content : request,
				sync : _sync,
				load : function(response) {
					var xmlDom = xmlDomParser.parse(response);
					context.stateID = context.espHelper.parseKeyValue(xmlDom, "StateID");
					context.state = context.espHelper.parseKeyValue(xmlDom, "State");
				},
				error : function() {
					done = true;
				}
			});
		}
	});
});
