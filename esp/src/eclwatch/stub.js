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

    "dijit/Dialog",
    "dijit/form/Button",

    "hpcc/ESPUtil",
    "hpcc/Utility",

    "dojox/html/entities",
    "dojox/widget/Toaster"
], function (fx, dom, domStyle, ioQuery, ready, lang, arrayUtil, topic, xhr,
            Dialog, Button,
            ESPUtil, Utility,
            entities, Toaster) {

    var IDLE_TIMEOUT = 30 * 60 * 1000;  // 30 Mins;
    var COUNTDOWN = 3 * 60;  // 3 Mins;
    var SESSION_RESET_FREQ = 30 * 1000; //  30 Seconds
    var idleWatcher;
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

    function showTimeoutDialog() {
        var dialogTimeout;
        var countdown = COUNTDOWN;
        var confirmLogoutDialog = new Dialog({
            title: "You are about to be logged out",
            content: "Due to inactivity, you will be logged out of your ECL Watch session in 3 minutes. This will close any sessions open in other tabs for this envrionment. Click on \'Continue Working\' to extend your session or click on \'Log Out\' to exit.",
            style: "width: 350px;padding:10px;",
            closable: false,
            draggable: false
        });

        var actionBar = dojo.create("div", {
            class: "dijitDialogPaneActionBar",
            style: "margin-top:10px;"
        }, confirmLogoutDialog.containerNode);

        var continueBtn = new Button({
            label: "Continue Working",
            style: "float:left;",
            onClick: function () {
                idleWatcher.start();
                confirmLogoutDialog.hide();
            }
        }).placeAt(actionBar);

        var logoutBtn = new Button({
            label: "Log Out - " + countdown,
            style: "font-weight:bold;",
            onClick: function () {
                _onLogout();
            }
        }).placeAt(actionBar);

        confirmLogoutDialog.show();
        dialogTimeout = setInterval(function () {
            if (--countdown < 0) {
                clearInterval(dialogTimeout);
                _onLogout();
            }
            logoutBtn.set("label", "Log Out - " + countdown);
        }, 1000);
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

    function initUi() {
        var params = ioQuery.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) === "?" ? 1 : 0)));
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

                idleWatcher = new ESPUtil.IdleWatcher(IDLE_TIMEOUT);
                idleWatcher.on("active", function () {
                    _resetESPTime();
                });
                idleWatcher.on("idle", function () {
                    idleWatcher.stop();
                    showTimeoutDialog();
                });
                idleWatcher.start();
                stopLoading();
            }
        );
    }

    return {
        init: function () {
            ready(function () {
                initUi();
            });
        }
    };
});
