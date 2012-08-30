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
	"dijit/layout/TabContainer",
	"dijit/registry",

	"hpcc/ESPBase",
	"hpcc/ESPWorkunit",
	"hpcc/ResultsControl",
	"dojo/text!./templates/ResultsWidget.html"
], function (declare, xhr, dom,
				_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, TabContainer, registry,
				ESPBase, ESPWorkunit, ResultsControl,
				template) {
    return declare("ResultsWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "ResultsWidget",

        resultsPane: null,
        resultsControl: null,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this._initControls();
            this.resultsPane.resize();
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.resultsPane.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        //  Implementation  ---
        onErrorClick: function (line, col) {
        },

        _initControls: function () {
            var context = this;
            this.resultsPane = registry.byId(this.id + "ResultsPane");

            var context = this;
            this.resultsControl = new ResultsControl({
                resultsSheetID: this.id + "ResultsPane",
                sequence: 0,
                onErrorClick: function (line, col) {
                    context.onErrorClick(line, col);
                }
            });
        },

        init: function (wuid, sequence, showSourceFiles) {
            if (wuid) {
                this.wu = new ESPWorkunit({
                    wuid: wuid
                });
                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 == 0) {
                        if (showSourceFiles) {
                            context.wu.getInfo({
                                onGetSourceFiles: function (sourceFiles) {
                                    context.refreshSourceFiles(context.wu);
                                }
                            });
                        } else {
                            context.wu.getInfo({
                                onGetResults: function (results) {
                                    context.refresh(context.wu);
                                }
                            });
                        }
                    }
                });
            }
        },

        clear: function () {
            this.resultsControl.clear();
        },

        refresh: function (wu) {
            this.resultsControl.refresh(wu);
        },

        refreshSourceFiles: function (wu) {
            this.resultsControl.refreshSourceFiles(wu);
        }
    });
});
