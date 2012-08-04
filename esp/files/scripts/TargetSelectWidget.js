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
	"dojo/text!./templates/TargetSelectWidget.html"
], function (declare, xhr, dom,
					_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, Select, registry,
					ESPBase, template) {
    return declare("TargetSelectWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "TargetSelectWidget",

        targetSelectControl: null,
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
            var context = this;
            this.targetSelectControl = registry.byId(this.id + "TargetSelect");
            this.targetSelectControl.onChange = function () {
                context.onChange(this.get("value"));
            };
            this.loadTargets();
        },

        onChange: function (target) {
            this._value = target;
        },

        setValue: function (target) {
            if (target && this._value != target) {
                this._value = target;
                this.targetSelectControl.set("value", target);
            }
        },

        getValue: function () {
            return this._value;
        },

        loadTargets: function () {
            var base = new ESPBase({
            });
            var request = {
                rawxml_: true
            };
            var context = this;
            xhr.post({
                url: base.getBaseURL("WsTopology") + "/TpTargetClusterQuery",
                handleAs: "xml",
                content: request,
                load: function (xmlDom) {
                    var targetData = base.getValues(xmlDom, "TpTargetCluster");

                    context.targetSelectControl.options = [];
                    var has_hthor = false;
                    for (var i = 0; i < targetData.length; ++i) {
                        context.targetSelectControl.options.push({
                            label: targetData[i].Name,
                            value: targetData[i].Name
                        });
                        if (targetData[i].Name == "hthor") {
                            has_hthor = true;
                        }
                    }

                    if (context._value == "") {
                        if (has_hthor) {
                            context.setValue("hthor");
                        } else {
                            context._value = context.targetSelectControl.options[0].value;
                        }
                    } else {
                        context.targetSelectControl.set("value", context._value);
                    }
                },
                error: function () {
                }
            });
        }
    });
});
