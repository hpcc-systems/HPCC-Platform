/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
	"dojo/_base/declare",
	"dojo/_base/sniff",
	"dojo/aspect",
	"dojo/dom"
], function (declare, sniff, aspect, dom) {
	return declare(null, {
		id: "gvc",
		width: "",
		height: "",
		installed: false,
		markup: "",
		obj: {},
		eventsRegistered: false,

		onInitialize: function () {
			//  Creater can override.
		},

		onLayoutFinished: function () {
			//  Creater can override.
		},

		onMouseDoubleClick: function (item) {
			//  Creater can override.
		},

		onSelectionChanged: function (items) {
			//  Creater can override.
		},

		// The constructor    
		constructor: function (args, parentNode) {
			declare.safeMixin(this, args);

			if (sniff("ie")) {
				this.installed = (function () {
					try {
						var o = new ActiveXObject("HPCCSystems.HPCCSystemsGraphViewControl.1");
						return true;
					} catch (e) { }
					return false;
				})();

				if (!this.installed) {
					this.markup = this.getInstallMarkup();
				} else {
					this.markup = '<object type="application/x-hpccsystemsgraphviewcontrol" '
						+ 'id="' + this.id + '" '
						+ 'width="' + this.width + '" '
						+ 'height="' + this.height + '"></object>';
				}
			} else {
				this.installed = (function () {
					for (var i = 0, p = navigator.plugins, l = p.length; i < l; i++) {
						if (p[i].name.indexOf("HPCCSystemsGraphViewControl") > -1) {
							return true;
						}
					}
					return false;
				})();

				if (!this.installed) {
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
			setTimeout(function () {
				context.onInitialize();
			}, 20);
		},

		getInstallMarkup: function () {
			return "<h4>Graph View</h4>" +
			"<p>To enable graph views, please install the Graph View Control plugin:</p>" +
			"<a href=\"http://graphcontrol.hpccsystems.com/stable/SetupGraphControl.msi\">Internet Explorer + Firefox (32bit)</a><br>" +
			"<a href=\"http://graphcontrol.hpccsystems.com/stable/SetupGraphControl64.msi\">Internet Explorer + Firefox (64bit)</a><br>" +
			"<a href=\"https://github.com/hpcc-systems/GraphControl\">Linux/Other (sources)</a>";
		},

		clear: function () {
			if (this.obj) {
				this.obj.clear();
			}
		},

		loadXGMML: function (xgmml, merge) {
			if (this.obj) {
				this.registerEvents();
				this.obj.setMessage("Loading Data...");
				if (merge)
					this.obj.mergeXGMML(xgmml);
				else
					this.obj.loadXGMML(xgmml);
				this.obj.setMessage("Performing Layout...");
				this.obj.startLayout("dot");
			}
		},

		loadDOT: function (dot) {
			this.load(dot, "dot");
		},

		load: function (dot, layout) {
			if (this.obj) {
				this.registerEvents();
				this.obj.setMessage("Loading Data...");
				this.obj.loadDOT(dot);
				this.obj.setMessage("Performing Layout...");
				this.obj.startLayout(layout);
			}
		},

		setLayout: function (layout) {
			if (this.obj) {
				this.registerEvents();
				this.obj.setMessage("Performing Layout...");
				this.obj.startLayout(layout);
			}
		},

		centerOn: function (globalID) {
			if (this.obj) {
				var item = this.obj.getItem(globalID);
				this.obj.centerOnItem(item, true);
				var items = [item];
				this.obj.setSelected(items, true);
			}
		},

		watchSelect: function (select) {
			if (this.obj) {
				if (sniff("chrome") && select) {
					var context = this;

					aspect.before(select, "openDropDown", function () {
						dojo.style(context.obj, "height", "0px");
					});

					aspect.after(select, "closeDropDown", function (focus) {
						dojo.style(context.obj, "height", "100%");
					});
				}
			}
		},

		registerEvents: function () {
			if (!this.eventsRegistered) {
				this.eventsRegistered = true;
				this.registerEvent("LayoutFinished", this.onLayoutFinished);
				this.registerEvent("MouseDoubleClick", this.onMouseDoubleClick);
				this.registerEvent("SelectionChanged", this.onSelectionChanged);
			}
		},

		registerEvent: function (evt, func) {
			if (this.obj) {
				if (sniff("ie")) {
					this.obj.attachEvent("on" + evt, func);
				} else {
					this.obj.addEventListener(evt, func, false);
				}
			}
		}
	});
});