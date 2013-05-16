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
    "hpcc/ResultWidget",
    "hpcc/LFDetailsWidget",

    "dojo/text!../templates/ResultsWidget.html",

    "dijit/layout/TabContainer"
], function (declare, lang, dom, 
                _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                _TabContainerWidget, ESPWorkunit, ResultWidget, LFDetailsWidget,
                template) {
    return declare("ResultsWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "ResultsWidget",

        selectedTab: null,
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
                if (lang.exists("Name", params) && lang.exists("Cluster", params)) {
                    retVal = new LFDetailsWidget.fixCircularDependency({
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
                            onGetWUExceptions: function (exceptions) {
                                if (params.ShowErrors && exceptions.length) {
                                    context.ensurePane(context.id + "_exceptions", {
                                        Wuid: params.Wuid,
                                        onErrorClick: context.onErrorClick,
                                        exceptions: exceptions
                                    });
                                    context.initTab();
                                }
                            },
                            onGetSourceFiles: function (sourceFiles) {
                                if (params.SourceFiles) {
                                    for (var i = 0; i < sourceFiles.length; ++i) {
                                        var tab = context.ensurePane(context.id + "_logicalFile" + i, {
                                            Name: sourceFiles[i].Name,
                                            Cluster: sourceFiles[i].FileCluster
                                        });
                                        if (i == 0) {
                                            context.initTab();
                                        }
                                    }
                                }
                            },
                            onGetResults: function (results) {
                                if (!params.SourceFiles) {
                                    for (var i = 0; i < results.length; ++i) {
                                        var tab = context.ensurePane(context.id + "_result" + i, {
                                            result: results[i]
                                        });
                                        if (i == 0) {
                                            context.initTab();
                                        }
                                    }
                                }
                            }
                        });
                        var currSel = context.getSelectedChild();
                        if (currSel && currSel.refresh) {
                            currSel.refresh();
                        }
                    }
                });
            }
        },

        clear: function () {
            this.removeAllChildren();
            this.selectedTab = null;
            this.initalized = false;
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
