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
	"dojo/_base/sniff",
	"dojo/dom"
], function(declare, has, dom) {
	return declare(null, {
		id: "gvc",
		width: "",
		height: "",
		installed:  false,
		markup: "",
		obj: {},
		eventsRegistered: false,

		onInitialize: function(){
			//  Creater can override.
		},
		
		onLayoutFinished: function() {
			//  Creater can override.
		},

		onMouseDoubleClick: function(item) {
			//  Creater can override.
		},

		onSelectionChanged: function(items) {
			//  Creater can override.
		},
	
		// The constructor    
		constructor: function(args, parentNode){
			declare.safeMixin(this, args);
			
			if(has("ie")){
				this.installed = (function(){
					try{
						var o = new ActiveXObject("HPCCSystems.HPCCSystemsGraphViewControl.1");
						return true;
					} catch(e){ }
					return false;
				})();
		
				if(!this.installed){ 
					this.markup = this.getInstallMarkup(); 
				} else {
					this.markup = '<object type="application/x-hpccsystemsgraphviewcontrol" '
						+ 'id="' + this.id + '" '
						+ 'width="' + this.width + '" '
						+ 'height="' + this.height + '"></object>';
				}
			} else {
				this.installed = (function(){
					for(var i=0, p=navigator.plugins, l=p.length; i<l; i++){
						if(p[i].name.indexOf("HPCCSystemsGraphViewControl")>-1){
							return true;
						}
					}
					return false;
				})();
			
				if(!this.installed){ 
					this.markup = this.getInstallMarkup(); 
				} else {
					this.markup = '<embed type="application/x-hpccsystemsgraphviewcontrol" '
						+ 'id="' + this.id + '" '
						+ 'name="' + this.id + '" '
						+ 'width="' + this.width + '" '
						+ 'height="' + this.height + '"></embed>';
				}
			}
			parentNode.innerHTML = this.markup;
			this.obj = dom.byId(this.id);
			var context = this;
			setTimeout(function(){
				context.onInitialize();
			}, 20);
		},
		
		getInstallMarkup: function() {
			return "<h4>Graph View</h4>" +
			"<p>To enable graph views, please install the Graph View Control plugin:</p>" + 
			"<a href=\"http://graphcontrol.hpccsystems.com/stable/SetupGraphControl.msi\">Internet Explorer + Firefox (32bit)</a><br>" + 
			"<a href=\"http://graphcontrol.hpccsystems.com/stable/SetupGraphControl64.msi\">Internet Explorer + Firefox (64bit)</a><br>" + 
			"<a href=\"https://github.com/hpcc-systems/GraphControl\">Linux/Other (sources)</a>";
		},

		clear: function() {
			if (this.obj) {
				this.obj.clear();
			}
		},
		
		loadXGMML: function(xgmml) {
			if (this.obj) {
				this.registerEvents();
				this.obj.setMessage("Loading Data...");
				this.obj.loadXGMML(xgmml);
				this.obj.setMessage("Performing Layout...");
				this.obj.startLayout("dot");
			}
		},
		
		loadDOT: function(dot) {
			this.load(dot, "dot");
		},
		
		load: function(dot, layout) {
			if (this.obj) {
				this.registerEvents();
				this.obj.setMessage("Loading Data...");
				this.obj.loadDOT(dot);
				this.obj.setMessage("Performing Layout...");
				this.obj.startLayout(layout);
			}
		},
		
		setLayout: function(layout) {
			if (this.obj) {
				this.registerEvents();
				this.obj.setMessage("Performing Layout...");
				this.obj.startLayout(layout);
			}
		},
		
		centerOn : function(globalID) {
			var item = this.obj.getItem(globalID);
			this.obj.centerOnItem(item, true);
			var items = [item];
			this.obj.setSelected(items, true);
		},
		
		registerEvents: function() {
			if (!this.eventsRegistered) {
				this.eventsRegistered = true;
				this.registerEvent("LayoutFinished", this.onLayoutFinished);
				this.registerEvent("MouseDoubleClick", this.onMouseDoubleClick);
				this.registerEvent("SelectionChanged", this.onSelectionChanged);
			}
		},		
		
		registerEvent: function(evt, func) {
			if (this.obj) {
				if(has("ie")){
					this.obj.attachEvent("on" + evt, func);
				} else {
					this.obj.addEventListener(evt, func, false);
				}
			}
		}
	});
});