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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/on",

    "dijit/registry",
    "dijit/layout/ContentPane",
    "dijit/form/Button",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPRequest",
    "hpcc/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "hpcc/WsTopology",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on,
                registry, ContentPane, Button,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, ESPRequest, ESPWorkunit, DelayLoadWidget, WsTopology, ESPUtil) {
    return declare("ResourcesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.Resources,
        idProperty: "__hpcc_id",

        wu: null,
        query: null,

        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;
            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 == 0) {
                        context.refreshGrid();
                    }
                });
            }
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var context = this;
            this.openButton = registry.byId(this.id + "Open");
            this.clusterPauseButton = new Button({
                id: this.id + "Content",
                label: this.i18n.Content,
                onClick: function (event) {
                    context._onOpen(event, {
                        showSource: true
                    });
                }
            }).placeAt(this.openButton.domNode, "after");

            var retVal = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    DisplayPath: {
                        label: this.i18n.Name, sortable: true,
                        formatter: function (url, row) {
                            return "<a href='#' class='" + context.id + "URLClick'>" + url + "</a>";
                        }
                    }
                }
            }, domID);

            on(document, "." + this.id + "URLClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        setContent: function (target, type, postfix) {
            var context = this;
            WsTopology.GetWsEclIFrameURL(type).then(function (response) {
                var targetSrc = context.params.QuerySet + "/" + context.params.QueryId + (postfix ? postfix : "");
                var src = response + encodeURIComponent(targetSrc);
                target.set("content", dojo.create("iframe", {
                    src: src,
                    style: "border: 0; width: 100%; height: 100%"
                }));
            });
        },

        createDetail: function (id, row, params) {
            if (params && params.showSource) {
                return new DelayLoadWidget({
                    id: id,
                    title: row.DisplayPath,
                    closable: true,
                    delayWidget: "ECLSourceWidget",
                    hpcc: {
                        params: {
                            sourceMode: "text",
                            sourceURL: ESPRequest.getBaseURL("WsWorkunits") + "/" + row.URL
                        }
                    }
                });
            }
            var retVal = new ContentPane({
                id: id,
                title: row.DisplayPath,
                closable: true,
                style: "padding: 0px; border:0px; border-color:none; overflow: hidden"
            });
            if (this.params.QuerySet && this.params.QueryId) {
                this.setContent(retVal, "res", "/" + row.DisplayPath);
            } else {
                retVal.set("content", dojo.create("iframe", {
                    src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src=" + encodeURIComponent("/WsWorkunits/" + row.URL),
                    style: "border: 0; width: 100%; height: 100%"
                }));
            }
            return retVal;
        },

        refreshGrid: function (args) {
            if (this.wu) {
                var context = this;
                this.wu.getInfo({
                    onGetResourceURLs: function (resourceURLs) {
                        arrayUtil.some(resourceURLs, function (item, idx) {
                            if (!context.firstLoad && (item.DisplayName === "index.htm" || item.DisplayName === "index.html")) {
                                context.firstLoad = true;
                                context._onRowDblClick(item);
                                return false;
                            }
                        });
                        context.store.setData(resourceURLs);
                        context.grid.refresh();
                    }
                });
            }
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);
            registry.byId(this.id + "Content").set("disabled", !selection.length);
        }
    });
});
