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

    "dijit/registry",
    "dijit/layout/BorderContainer",

    "hpcc/_Widget",
    "hpcc/TimingGridWidget",

    "dojo/text!../templates/TimingPageWidget.html"
],
    function (declare,
            registry, BorderContainer,
            _Widget, TimingGridWidget,
            template) {
        return declare("TimingPageWidget", [_Widget], {
            templateString: template,
            baseClass: "TimingPageWidget",
            borderContainer: null,
            timingGrid: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.timingGrid = registry.byId(this.id + "Grid");
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            resize: function (args) {
                this.inherited(arguments);
                this.borderContainer.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            //  Plugin wrapper  ---
            init: function (params) {
                if (this.inherited(arguments))
                    return;

                var context = this;
                this.timingGrid.init(params);
            }
        });
    });
