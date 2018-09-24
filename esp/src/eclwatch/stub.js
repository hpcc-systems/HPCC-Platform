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
    "dojo/on",

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

], function (fx, dom, domStyle, ioQuery, ready, lang, arrayUtil, topic, xhr, cookie, on,
    Dialog, Button,
    ESPUtil, Utility, LockDialogWidget,
    entities, Toaster) {

        var IDLE_TIMEOUT = cookie("ESPSessionTimeoutSeconds") * 1000;
        var SESSION_RESET_FREQ = 30 * 1000;
        var idleWatcher;
        var monitorLockClick;
        var _prevReset = Date.now();
        var sessionIsActive = cookie("ESPSessionTimeoutSeconds");

        function _resetESPTime(evt) {
            if (Date.now() - _prevReset > SESSION_RESET_FREQ) {
                _prevReset = Date.now();
                xhr("esp/reset_session_timeout", {
                    method: "post"
                }).then(function (data) {
                });
            }
        }

        if (sessionIsActive > -1) {
            cookie("Status", "Unlocked");
            cookie("ECLWatchUser", "true");

            idleWatcher = new ESPUtil.IdleWatcher(IDLE_TIMEOUT);
            monitorLockClick = new ESPUtil.MonitorLockClick();

            monitorLockClick.on("unlocked", function () {
                idleWatcher.start();
            });
            monitorLockClick.on("locked", function () {
                idleWatcher.stop();
            });
            idleWatcher.on("active", function () {
                _resetESPTime();
            });
            idleWatcher.on("idle", function () {
                idleWatcher.stop();
                var LockDialog = new LockDialogWidget({});
                LockDialog._onLock();
            });
            idleWatcher.start();
            monitorLockClick.unlocked();
        } else if (cookie("ECLWatchUser")) {
            window.location.replace(dojoConfig.urlInfo.basePath + "/Login.html");
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

            topic.subscribe("hpcc/session_management_status", function (publishedMessage) {
                if (publishedMessage.status === "Unlocked") {
                    monitorLockClick.unlocked();
                } else  if (publishedMessage.status === "Locked") {
                    monitorLockClick.locked();
                }
            })

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