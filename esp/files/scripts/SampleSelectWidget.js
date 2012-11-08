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
