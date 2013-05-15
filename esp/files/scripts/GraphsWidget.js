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

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPWorkunit",
    "hpcc/GraphPageWidget",

    "dojo/text!../templates/GraphsWidget.html",

    "dijit/layout/TabContainer"
], function (declare, lang, dom, 
                _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                _TabContainerWidget, ESPWorkunit, GraphPageWidget,
                template) {
    return declare("GraphsWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "GraphsWidget",

        TabPosition: "bottom",

        onErrorClick: function (line, col) {
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                currSel.init(currSel.params);
            }
        },

        ensurePane: function (id, params) {
            var retVal = registry.byId(id);
            if (!retVal) {
                if (lang.exists("graph", params)) {
                    retVal = new GraphPageWidget({
                        id: id,
                        Wuid: this.wu.Wuid,
                        GraphName: params.graph.Name,
                        title: params.graph.Name
                    });
                }
                this.addChild(retVal);
            }
            return retVal;
        },

        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;

            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 == 0) {
                        context.wu.getInfo({
                            onGetGraphs: function (graphs) {
                                for (var i = 0; i < graphs.length; ++i) {
                                    context.ensurePane(context.id + "_graph" + i, {
                                        graph: graphs[i]
                                    });
                                    if (i == 0) {
                                        context.initTab();
                                    }
                                }
                            }
                        });
                    }
                });
            }
        }
    });
});
