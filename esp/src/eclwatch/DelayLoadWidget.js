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
    "dojo/_base/Deferred",
    "dojo/_base/lang",
    "dojo/dom",
    "dojo/dom-style",

    "dijit/layout/ContentPane"

], function (declare, Deferred, lang, dom, domStyle,
    ContentPane) {
    return declare("DelayLoadWidget", [ContentPane], {
        __hpcc_initalized: false,
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
            if (this.deferred) {
                return this.deferred.promise;
            } 

            this.deferred = new Deferred();
            this.startLoading();
            var context = this;
            require([(this.delayFolder ? "plugins/" + this.delayFolder + "/" : "hpcc/") + this.delayWidget], function (widget) {
                if (widget.fixCircularDependency) {
                    widget = widget.fixCircularDependency;
                }
                var widgetInstance = new widget(lang.mixin({
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
                context.deferred.resolve(widgetInstance);
            });
            return this.deferred.promise;
        },

        //  Implementation  ---
        init: function (params) {
            if (this.__hpcc_initalized)
                return false;

            this.childWidgetID = this.id + "-DL";
            this.__hpcc_initalized = true;

            var context = this;
            this.ensureWidget().then(function (widget) {
                widget.init(params);
                if (context.__hpcc_hash) {
                    context.doRestoreFromHash(context.__hpcc_hash);
                    context.__hpcc_hash = null;
                }
            });
            return true;
        },
        restoreFromHash: function (hash) {
            if (this.widget && this.widget[this.childWidgetID]) {
                this.doRestoreFromHash(hash);
            } else {
                this.__hpcc_hash = hash;
            }
        },    
        doRestoreFromHash: function (hash) {
            if (this.widget[this.childWidgetID].restoreFromHash) {
                this.widget[this.childWidgetID].restoreFromHash(hash);
            }
        }
    });
});
