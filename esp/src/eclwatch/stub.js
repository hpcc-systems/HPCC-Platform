define([
    "dojo/_base/fx",
    "dojo/dom",
    "dojo/dom-style",
    "dojo/io-query",
    "dojo/ready",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/topic",

    "src/Utility",
    "src/Session",
    "src/KeyValStore",
    "src/BuildInfo",
    "hpcc/LockDialogWidget",

    "dojox/html/entities",
    "dojox/widget/Toaster",

    "css!hpcc/css/ecl.css",
    "css!dijit-themes/flat/flat.css",
    "css!hpcc/css/hpcc.css"

], function (fx, dom, domStyle, ioQuery, ready, lang, arrayUtil, topic,
    Utility, Session, KeyValStore, BuildInfo, LockDialogWidget,
    entities, Toaster) {

    Session.initSession();

    const params = ioQuery.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) === "?" ? 1 : 0)));
    const hpccWidget = params.Widget ? params.Widget : "HPCCPlatformWidget";

    const store = KeyValStore.userKeyValStore();
    store.getEx(BuildInfo.ModernMode, { defaultValue: String(true) }).then(modernMode => {
        if (modernMode === String(true) && hpccWidget !== "IFrameWidget") {
            switch (hpccWidget) {
                case "WUDetailsWidget":
                    window.location.replace(`/esp/files/index.html#/workunits/${params.Wuid}`);
                    break;
                case "GraphsWUWidget":
                    window.location.replace(`/esp/files/index.html#/workunits/${params.Wuid}/metrics`);
                    break;
                default:
                    window.location.replace("/esp/files/index.html");
            }
        } else {
            ready(function () {
                parseUrl();
                initUI();
            });
        }
    });

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
        topic.subscribe("hpcc/session_management_status", function (publishedMessage) {
            if (publishedMessage.status === "Unlocked") {
                Session.unlock();
            } else if (publishedMessage.status === "Locked") {
                Session.lock();
            } else if (publishedMessage.status === "DoIdle") {
                Session.fireIdle();
            } else if (publishedMessage.status === "Idle") {
                var LockDialog = new LockDialogWidget({});
                LockDialog._onLock(publishedMessage.idleCreator);
            }
        });

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
                id: "hpcc_toaster",
                positionDirection: "br-left"
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
            dojoConfig.pageTitle = widget.getTitle ? widget.getTitle() : params.Widget;
            document.title = dojoConfig.pageTitle;
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
            basePath: baseHost + "/esp/files",
            fullPath: location.origin + "/esp/files"
        };
    }
});
