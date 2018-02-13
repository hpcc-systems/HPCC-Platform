/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/request/xhr",
    "dojo/cookie",

    "dijit/Dialog",
    "dijit/form/Button",

    "src/ESPUtil",
    "src/Utility",
    "hpcc/LockDialogWidget",

    "dojox/html/entities",
    "dojox/widget/Toaster",

    "css!hpcc/css/ecl.css",
    "css!dojo-themes/flat/flat.css",
    "css!hpcc/css/hpcc.css"

], function (fx, dom, domStyle, ioQuery, ready, lang, arrayUtil, topic, xhr, cookie,
            Dialog, Button,
            ESPUtil, Utility, LockDialogWidget,
            entities, Toaster) {

    var IDLE_TIMEOUT = cookie("ESPSessionTimeoutSeconds") * 1000;
    var COUNTDOWN = 3 * 60;
    var SESSION_RESET_FREQ = 30 * 1000;
    var idleWatcher;
    var monitorLockClick;
    var _prevReset = Date.now();

    function _resetESPTime(evt) {
        if (Date.now() - _prevReset > SESSION_RESET_FREQ) {
            _prevReset = Date.now();
            xhr("esp/reset_session_timeout", {
                method: "post"
            }).then(function (data) {
            });
        }
    }

    function _onLogout(evt) {
        xhr("esp/logout", {
            method: "post"
        }).then(function (data) {
            if (data) {
                document.cookie = "ESPSessionID" + location.port + " = '' "; "expires=Thu, 01 Jan 1970 00:00:00 GMT"; // or -1
                window.location.reload();
            }
        });
    }

    function showLockDialog() {
        dom.byId("Lock").click();
    }

    function startLoading(targetNode) {
        domStyle.set(dom.byId("loadingOverlay"), "display", "block");
        domStyle.set(dom.byId("loadingOverlay"), "opacity", "255");
    }

    function stopLoading() {
        fx.fadeOut({
            node: dom.byId("loadingOverlay"),
            onEnd: function (node) {
                domStyle.set(node, "display", "none");
            }
        }).play();
    }

    function initUI() {
        var params = ioQuery.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) === "?" ? 1 : 0)));
        var hpccWidget = params.Widget ? params.Widget : "HPCCPlatformWidget";

            Utility.resolve(hpccWidget, function (WidgetClass) {
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
                var widget = new WidgetClass(webParams);

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
                                    item.Message = item.Message.substr(0, MAX_LINES * 80);
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

                if (cookie("ESPSessionTimeoutSeconds")) {
                    var LockDialog = new LockDialogWidget({
                        id: 'LockDialogWidget',
                    });

                    idleWatcher = new ESPUtil.IdleWatcher(IDLE_TIMEOUT);
                    monitorLockClick = new ESPUtil.MonitorLockClick();
                    monitorLockClick.on("unlocked", function (){
                        idleWatcher.start();
                    });
                    idleWatcher.on("active", function () {
                        _resetESPTime();
                    });
                    idleWatcher.on("idle", function () {
                        idleWatcher.stop();
                        LockDialog._onLock();
                    });
                    idleWatcher.start();
                    monitorLockClick.unlocked();
                }
                stopLoading();
            }
        );
    }

    function parseUrl() {
        var baseHost = (typeof debugConfig !== "undefined") ? "http://" + debugConfig.IP + ":" + debugConfig.Port : "";
        var hashNodes = location.hash.split("#");
        var searchNodes = location.search.split("?");

        dojoConfig.urlInfo = {
            baseHost: baseHost,
            pathname: location.pathname,
            hash: hashNodes.length >= 2 ? hashNodes[1] : "",
            resourcePath: baseHost + "/esp/files/eclwatch",
            basePath: baseHost + "/esp/files"
        };
    }

    ready(function () {
        parseUrl();
        initUI();
    });
});
