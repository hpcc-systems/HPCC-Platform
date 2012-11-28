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
    "dojo/_base/lang",
    "dojo/dom",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "hpcc/ESPWorkunit",
    "hpcc/GraphPageWidget",

    "dojo/text!../templates/GraphsWidget.html",

    "dijit/layout/TabContainer"
], function (declare, lang, dom, 
                _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                ESPWorkunit, GraphPageWidget,
                template) {
    return declare("GraphsWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "GraphsWidget",

        //borderContainer: null,
        tabContainer: null,
        tabMap: [],
        selectedTab: null,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this._initControls();
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.tabContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        //  Implementation  ---
        onErrorClick: function (line, col) {
        },

        _initControls: function () {
            var context = this;
            this.tabContainer = registry.byId(this.id + "TabContainer");

            var context = this;
            this.tabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                if (!nval.initalized) {
                    nval.init(nval.params);
                }
                context.selectedTab = nval;
            });
        },

        ensurePane: function (id, params) {
            var retVal = this.tabMap[id];
            if (!retVal) {
                if (lang.exists("graph", params)) {
                    retVal = new GraphPageWidget({
                        id: id,
                        Wuid: this.wu.Wuid,
                        GraphName: params.graph.Name,
                        title: params.graph.Name
                    });
                }
                this.tabMap[id] = retVal;
                this.tabContainer.addChild(retVal);
            }
        },

        init: function (params) {
            if (params.Wuid) {
                this.wu = new ESPWorkunit({
                    Wuid: params.Wuid
                });
                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 == 0) {
                        context.wu.getInfo({
                            onGetGraphs: function (graphs) {
                                for (var i = 0; i < graphs.length; ++i) {
                                    context.ensurePane(context.id + "graph_" + i, {
                                        graph: graphs[i]
                                    });
                                }
                            }
                        });
                    }
                });
            }
        }
    });
});
