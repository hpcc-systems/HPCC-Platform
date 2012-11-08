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
