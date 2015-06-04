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
    "dojo/_base/fx",
    "dojo/dom",
    "dojo/dom-style",
    "dojo/io-query",
    "dojo/ready",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/topic",

    "dojox/html/entities",
    "dojox/widget/Toaster"
], function (fx, dom, domStyle, ioQuery, ready, lang, arrayUtil, topic,
            entities, Toaster) {

    var initUi = function () {
        var params = ioQuery.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) == "?" ? 1 : 0)));
        var hpccWidget = params.Widget ? params.Widget : "HPCCPlatformWidget";

        require([
                "hpcc/" + hpccWidget
        ], function (WidgetClass) {
                var webParams = {
                    id: "stub",
                    "class": "hpccApp"
                };
                if (params.TabPosition) {
                    lang.mixin(webParams, {
                        TabPosition: params.TabPosition
                    });
                }
                if (params.ReadOnly) {
                    lang.mixin(webParams, {
                        readOnly: params.ReadOnly
                    });
                }
                var widget = WidgetClass.fixCircularDependency ? new WidgetClass.fixCircularDependency(webParams) : new WidgetClass(webParams);

                var myToaster = new Toaster({
                    id: 'hpcc_toaster',
                    positionDirection: 'br-left'
                });
                topic.subscribe("hpcc/brToaster", function (topic) {
                    if (lang.exists("Exceptions", topic)) {
                        var context = this;
                        arrayUtil.forEach(topic.Exceptions, function (_item, idx) {
                            var item = lang.mixin({
                                Severity: topic.Severity,
                                Source: topic.Source
                            }, _item);

                            var clipped = false;
                            if (item.Message) {
                                var MAX_LINES = 10;
                                if (item.Message.length > MAX_LINES * 80) { 
                                    item.Message = item.Message.substr(0, length = MAX_LINES * 80);
                                    item.Message += "...";
                                    clipped = true;
                                }
                            }

                            if (topic.Severity !== "Info") {
                                var message = "<h4>" + entities.encode(item.Source) + "</h4><p>" + entities.encode(item.Message) + (clipped ? "<br>...  ...  ..." : "") + "</p>";
                                myToaster.setContent(message, item.Severity, item.Severity === "Error" ? -1 : null);
                                myToaster.show();
                            }
                        });
                    }
                });

                if (widget) {
                    widget.placeAt(dojo.body(), "last");
                    widget.startup();
                    widget.init(params);
                    if (widget.restoreFromHash) {
                        widget.restoreFromHash(dojoConfig.urlInfo.hash);
                    }
                }

                document.title = widget.getTitle ? widget.getTitle() : params.Widget;
                stopLoading();
            }
        );
    },

    startLoading = function (targetNode) {
        domStyle.set(dom.byId("loadingOverlay"), "display", "block");
        domStyle.set(dom.byId("loadingOverlay"), "opacity", "255");
    },

    stopLoading = function () {
        fx.fadeOut({
            node: dom.byId("loadingOverlay"),
            onEnd: function (node) {
                domStyle.set(node, "display", "none");
            }
        }).play();
    };

    return {
        init: function () {
            ready(function () {
                initUi();
            });
        }
    };
});
