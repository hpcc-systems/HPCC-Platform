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
