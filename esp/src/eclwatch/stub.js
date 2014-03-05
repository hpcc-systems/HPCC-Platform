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
    "dojo/ready"
], function (fx, dom, domStyle, ioQuery, ready) {

    var initUi = function () {
        var params = ioQuery.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) == "?" ? 1 : 0)));
        var hpccWidget = params.Widget ? params.Widget : "HPCCPlatformWidget";

        require([
                "dojo/_base/lang",
                "dojo/_base/array",
                "dojo/topic",
                "dojox/html/entities",
                "dojox/widget/Toaster",
                "dojox/widget/Standby",
                "hpcc/" + hpccWidget
        ], function (lang, arrayUtil, topic,
            entities, Toaster, Standby,
            WidgetClass) {
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

                var standbyBackground = new Standby({
                    color: "#FAFAFA",
                    text: "",
                    centerIndicator: "text",
                    target: "stub"
                });
                dojo.body().appendChild(standbyBackground.domNode);
                standbyBackground.startup();
                standbyBackground.hpccShowCount = 0;

                topic.subscribe("hpcc/standbyBackgroundShow", function () {
                    if (standbyBackground.hpccShowCount++ == 0) {
                        standbyBackground.show();
                    }
                });

                topic.subscribe("hpcc/standbyBackgroundHide", function () {
                    if (--standbyBackground.hpccShowCount <= 0) {
                        standbyBackground.hpccShowCount = 0;
                        standbyBackground.hide();
                    }
                });

                var standbyForeground = new Standby({
                    zIndex: 1000,
                    target: "stub"
                });
                dojo.body().appendChild(standbyForeground.domNode);
                standbyForeground.startup();
                standbyForeground.hpccShowCount = 0;

                topic.subscribe("hpcc/standbyForegroundShow", function () {
                    standbyForeground.show();
                    ++standbyForeground.hpccShowCount;
                });

                topic.subscribe("hpcc/standbyForegroundHide", function () {
                    if (--standbyForeground.hpccShowCount <= 0) {
                        standbyForeground.hpccShowCount = 0;
                        standbyForeground.hide();
                    }
                });
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

                            if ((item.Source === "WsWorkunits.WUInfo" && item.Code === 20080) ||
                                (item.Source === "WsWorkunits.WUQuery" && item.Code === 20081)) {
                            } else {
                                var message = "<h4>" + entities.encode(item.Source) + "</h4><p>" + entities.encode(item.Message) + "</p>";
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
