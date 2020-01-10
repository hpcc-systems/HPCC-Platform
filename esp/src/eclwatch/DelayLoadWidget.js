define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/dom",
    "dojo/dom-style",

    "dijit/layout/ContentPane",

    "src/Utility"
], function (declare, lang, dom, domStyle,
    ContentPane,
    Utility) {
    return declare("DelayLoadWidget", [ContentPane], {
        __ensurePromise: undefined,
        __initPromise: undefined,
        refresh: null,

        style: {
            margin: "0px",
            padding: "0px"
        },

        startLoading: function (targetNode) {
            var loadingOverlay = dom.byId("loadingOverlay");
            if (loadingOverlay) {
                domStyle.set(loadingOverlay, "display", "block");
                domStyle.set(loadingOverlay, "opacity", "255");
            }
        },

        stopLoading: function () {
            var loadingOverlay = dom.byId("loadingOverlay");
            if (loadingOverlay) {
                domStyle.set(loadingOverlay, "display", "none");
                domStyle.set(loadingOverlay, "opacity", "0");
            }
        },

        ensureWidget: function () {
            if (this.__ensurePromise) return this.__ensurePromise;
            var context = this;
            this.__ensurePromise = new Promise(function (resolve, reject) {
                context.startLoading();
                Utility.resolve(context.delayWidget, function (Widget) {
                    var widgetInstance = new Widget(lang.mixin({
                        id: context.childWidgetID,
                        style: {
                            margin: "0px",
                            padding: "0px",
                            width: "100%",
                            height: "100%"
                        }
                    }, context.delayProps ? context.delayProps : {}));
                    context.widget = {};
                    context.widget[widgetInstance.id] = widgetInstance;
                    context.containerNode.appendChild(widgetInstance.domNode);
                    widgetInstance.startup();
                    widgetInstance.resize();
                    if (widgetInstance.refresh) {
                        context.refresh = function (params) {
                            widgetInstance.refresh(params);
                        }
                    }
                    context.stopLoading();
                    resolve(widgetInstance);
                });
            });
            return this.__ensurePromise;
        },

        //  Implementation  ---
        reset: function () {
            for (var key in this.widget) {
                this.widget[key].destroyRecursive();
                delete this.widget[key];
            }
            delete this.widget;
            delete this.deferred;
            delete this.__hpcc_initalized;
            delete this.childWidgetID;
            this.containerNode.innerHTML = "";
            delete this.__initPromise;
            delete this.__ensurePromise;
        },

        init: function (params) {
            if (this.__initPromise) return this.__initPromise;
            this.childWidgetID = this.id + "-DL";
            var context = this;
            this.__initPromise = new Promise(function (resolve, reject) {
                context.ensureWidget().then(function (widget) {
                    widget.init(params);
                    if (context.__hpcc_hash) {
                        context.doRestoreFromHash(context.__hpcc_hash);
                        context.__hpcc_hash = null;
                    }
                    //  Let page finish initial render ---
                    setTimeout(function () {
                        resolve(widget);
                    }, 20);
                });
            });
            return this.__initPromise;
        },

        restoreFromHash: function (hash) {
            if (this.widget && this.widget[this.childWidgetID]) {
                this.doRestoreFromHash(hash);
            } else {
                this.__hpcc_hash = hash;
            }
        },
        doRestoreFromHash: function (hash) {
            if (this.widget && this.widget[this.childWidgetID] && this.widget[this.childWidgetID].restoreFromHash) {
                this.widget[this.childWidgetID].restoreFromHash(hash);
            }
        }
    });
});
