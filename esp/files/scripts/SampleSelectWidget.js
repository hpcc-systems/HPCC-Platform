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
	"dijit/form/Select",
	"dijit/registry",

	"hpcc/ESPBase",
	"dojo/text!./templates/SampleSelectWidget.html"
], function (declare, xhr, dom,
					_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, Select, registry,
					ESPBase, template) {
    return declare("SampleSelectWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "SampleSelectWidget",

        selectControl: null,
        _value: "",

        postCreate: function (args) {
            this.inherited(arguments);
            this._initControls();
        },

        resize: function (args) {
            this.inherited(arguments);
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        //  Implementation  ---
        _initControls: function () {
            this.selectControl = registry.byId(this.id + "SampleSelect");
            this.selectControl.maxHeight = 480;
        },

        load: function () {
            var sampleStore = new dojo.data.ItemFileReadStore({
                url: "ecl/ECLPlaygroundSamples.json"
            });
            this.selectControl.setStore(sampleStore);
            var context = this;
            this.selectControl.onChange = function () {
                var filename = this.getValue();
                xhr.get({
                    url: "ecl/" + filename,
                    handleAs: "text",
                    load: function (eclText) {
                        context.onNewSelection(eclText);
                    },
                    error: function () {
                    }
                });
            };
            this.selectControl.set("value", "default.ecl");
        },

        onNewSelection: function (eclText) {
        }
    });
});
