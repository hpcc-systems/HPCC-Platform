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

    "dojo/text!../templates/ResultsWidget.html",

    "dijit/layout/TabContainer"
], function (declare, lang, dom, 
                _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                ESPWorkunit, 
                template) {
    return declare("ResultsWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "ResultsWidget",

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
            //this.borderContainer.resize();
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
            //this.borderContainer = registry.byId(this.id + "BorderContainer");
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
                if (lang.exists("Name", params) && lang.exists("Cluster", params)) {
                    retVal = new LFDetailsWidget({
                        id: id,
                        title: params.Name,
                        params: params
                    });
                } else if (lang.exists("Wuid", params) && lang.exists("exceptions", params)) {
                    retVal = new InfoGridWidget({
                        id: id,
                        title: "Errors/Warnings",
                        params: params
                    });
                } else if (lang.exists("result", params)) {
                    retVal = new ResultWidget({
                        id: id,
                        title: params.result.Name,
                        params: params
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
                            onGetWUExceptions: function (exceptions) {
                                if (params.ShowErrors && exceptions.length) {
                                    context.ensurePane(context.id + "exceptions", {
                                        Wuid: params.Wuid,
                                        onErrorClick: context.onErrorClick,
                                        exceptions: exceptions
                                    });
                                }
                            },
                            onGetSourceFiles: function (sourceFiles) {
                                if (params.SourceFiles) {
                                    for (var i = 0; i < sourceFiles.length; ++i) {
                                        context.ensurePane(context.id + "logicalFile_" + i, {
                                            Name: sourceFiles[i].Name,
                                            Cluster: sourceFiles[i].FileCluster
                                        });
                                    }
                                }
                            },
                            onGetResults: function (results) {
                                if (!params.SourceFiles) {
                                    for (var i = 0; i < results.length; ++i) {
                                        context.ensurePane(context.id + "result_" + i, {
                                            result: results[i]
                                        });
                                    }
                                }
                            }
                        });
                        if (context.selectedTab) {
                            context.selectedTab.refresh();
                        }
                    }
                });
            }
        },

        clear: function () {
            var tabs = this.tabContainer.getChildren();
            for (var i = 0; i < tabs.length; ++i) {
                this.tabContainer.removeChild(tabs[i]);
                tabs[i].destroyRecursive();
            }
            this.tabMap = [];
            this.selectedTab = null;
        },

        refresh: function (wu) {
            if (this.workunit != wu) {
                this.clear();
                this.workunit = wu;
                this.init({
                    Wuid: wu.Wuid,
                    ShowErrors: true
                });
            }
        }
    });
});
