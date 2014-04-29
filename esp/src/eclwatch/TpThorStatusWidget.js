/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/on",

    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPRequest",
    "hpcc/ESPTopology",
    "hpcc/DelayLoadWidget",
    "hpcc/WsTopology",

    "dojo/text!../templates/TpThorStatusWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Form",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator"
], function (declare, i18n, nlsHPCC, dom, domConstruct, on,
                registry,
                _TabContainerWidget, ESPRequest, ESPTopology, DelayLoadWidget, WsTopology,
                template) {
    return declare("TpThorStatusWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "TpThorStatusWidget",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_TpThorStatus;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refresh();
        },

        _onRefreshCharts: function (event) {
        },

        _onNewPageCharts: function (event) {
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.Name) {
                this.thor = ESPTopology.GetThor(params.Name);
                var data = this.thor.getData();
                for (var key in data) {
                    this.updateInput(key, null, data[key]);
                }
                var context = this;
                this.thor.watch(function (name, oldValue, newValue) {
                    context.updateInput(name, oldValue, newValue);
                });
                this.thor.refresh();
            }
        },

        initTab: function () {
            if (!this.thor) {
                return;
            }
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.widget._Summary.id) {
                } else if (currSel.id === this.widget._Charts.id) {
                    currSel.set("content", domConstruct.create("iframe", {
                        src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src=" + encodeURIComponent(ESPRequest.getBaseURL("WsWorkunits") + "/WUJobList?form_&Cluster=" + this.params.ClusterName + "&Process=" + this.thor.Name + "&Range=30"),
                        style: "border: 0; width: 100%; height: 100%"
                    }));
                } else if (currSel.id === this.widget._Log.id) {
                    currSel.set("content", domConstruct.create("iframe", {
                        src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src=" + encodeURIComponent(ESPRequest.getBaseURL("WsTopology") + "/TpLogFile/ " + this.thor.Name + "?Name=" + this.thor.Name + "&Type=thormaster_log"),
                        style: "border: 0; width: 100%; height: 100%"
                    }));
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel.params);
                    }
                }
                currSel.initalized = true;
            }
        },

        updateInput: function (name, oldValue, newValue) {
            if (name === "Wuid" && newValue) {
                this.inherited(arguments, [name, oldValue, "<a id='" + this.id + "WuidHRef' href='#'>" + newValue + "</a>"]);
                var context = this;
                on(dom.byId(this.id + "WuidHRef"), "click", function (evt) {
                    var tab = context.ensurePane(newValue, {
                        Wuid: newValue
                    });
                    context.selectChild(tab);
                });
            } else if (name === "GraphSummary" && newValue) {
                this.inherited(arguments, [name, oldValue, "<a id='" + this.id + "GraphHRef' href='#'>" + newValue + "</a>"]);
                var context = this;
                var Wuid = this.thor.Wuid;
                var GraphName = this.thor.Graph;
                var SubGraphId = this.thor.SubGraph;
                on(dom.byId(this.id + "GraphHRef"), "click", function (evt) {
                    var tab = context.ensurePane(newValue, {
                        Wuid: Wuid,
                        GraphName: GraphName,
                        SubGraphId: SubGraphId
                    });
                    context.selectChild(tab);
                });
            } else {
                this.inherited(arguments);
            }
        },

        ensurePane: function (_id, params) {
            var id = this.createChildTabID(_id);
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                if (params.GraphName) {
                    retVal = new DelayLoadWidget({
                        id: id,
                        title: _id,
                        closable: true,
                        delayWidget: "GraphPageWidget",
                        params: params
                    });
                } else {
                    retVal = new DelayLoadWidget({
                        id: id,
                        title: _id,
                        closable: true,
                        delayWidget: "WUDetailsWidget",
                        params: params
                    });
                }
                this.addChild(retVal, 3);
            } 
            return retVal;
        },

        refresh: function () {
            this.thor.refresh();
        },

        refreshActionState: function () {
        }
    });
});