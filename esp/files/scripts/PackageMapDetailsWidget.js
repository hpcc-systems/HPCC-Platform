/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.
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
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/topic",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "hpcc/WsPackageMaps",
    "hpcc/PackageSourceWidget",

    "dojo/text!../templates/PackageMapDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Button",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/TitlePane"
], function (declare, dom, domAttr, domClass, topic,
    _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry,
    WsPackageMaps, PackageSourceWidget, template) {
    return declare("PackageMapDetailsWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "PackageMapDetailsWidget",
        borderContainer: null,
        tabContainer: null,
        validateWidget: null,
        validateWidgetLoaded: false,
        xmlWidget: null,
        xmlWidgetLoaded: false,

        initalized: false,
        tabId: "",
        packageMap: "",
        target: "",
        process: "",
        active: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.tabContainer = registry.byId(this.id + "TabContainer");
            this.validateWidget = registry.byId(this.id + "Validate");
            this.xmlWidget = registry.byId(this.id + "XML");

            var context = this;
            this.tabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                if (nval.id == context.id + "Validate" && !context.validateWidgetLoaded) {
                    context.validateWidgetLoaded = true;
                    context.validateWidget.init({
                        target: context.target,
                        process: context.process,
                        packageMap: context.packageMap
                    });
                } else if (nval.id == context.id + "XML" && !context.xmlWidgetLoaded) {
                    context.xmlWidgetLoaded = true;
                    context.xmlWidget.init({
                        target: context.target,
                        process: context.process,
                        packageMap: context.packageMap
                    });
                }
            });
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;
            this.tabId = params.tabId;
            this.packageMap = params.packageMap;
            this.target = params.target;
            this.process = params.process;
            this.active = params.active;
            if (params.packageMap) {
                registry.byId(this.id + "Summary").set("title", params.packageMap);
                domAttr.set(this.id + "PMID", "innerHTML", params.packageMap);
                domAttr.set(this.id + "Target", "value", params.target);
                domAttr.set(this.id + "Process", "value", params.process);
                if (params.active == true)
                    domClass.add(this.id + "StateIdImage", "iconRunning");
                else
                    domClass.add(this.id + "StateIdImage", "iconArchived");
            }
            this.refreshActionState();
        },

        refreshActionState: function () {
            registry.byId(this.id + "Activate").set("disabled", this.active);
            registry.byId(this.id + "Deactivate").set("disabled", !this.active);
            domAttr.set(this.id + "StateIdImage", "title", this.active? "Active":"Not active");
        },

        showErrorMessage: function (message) {
            dojo.publish("hpcc/brToaster", {
                message: message,
                type: "error",
                duration: -1
            });
        },

        showErrors: function (errMsg, errStack) {
            var message = "Unknown Error";
            if (errMsg != '')
                message = "<h3>" + errMsg + "</h3>";
            if (errStack != '')
                message += "<p>" + errStack + "</p>";
            this.showErrorMessage(message);
        },

        _onActivate: function (event) {
            var context = this;
            var packageMaps = [];
            packageMaps[0] = {Target:this.target,
                Process:this.process,Id:this.packageMap};

            WsPackageMaps.activatePackageMap(packageMaps, {
                load: function (response) {
                    domClass.replace(context.id + "StateIdImage", "iconRunning");
                    context.active = true;
                    context.refreshActionState();
                },
                error: function (errMsg, errStack) {
                    context.showErrors(errMsg, errStack);
                }
            });
        },
        _onDeactivate: function (event) {
            var context = this;
            var packageMaps = [];
            packageMaps[0] = {Target:this.target,
                Process:this.process,Id:this.packageMap};

            WsPackageMaps.deactivatePackageMap(packageMaps, {
                load: function (response) {
                    domClass.replace(context.id + "StateIdImage", "iconArchived");
                    context.active = false;
                    context.refreshActionState();
                },
                error: function (errMsg, errStack) {
                    context.showErrors(errMsg, errStack);
                }
            });
        },
        _onDelete: function (event) {
            if (confirm('Delete selected package?')) {
                var context = this;
                var packageMaps = [];
                packageMaps[0] = {Target:this.target,
                    Process:this.process,Id:this.packageMap};

                WsPackageMaps.deletePackageMap(packageMaps, {
                    load: function (response) {
                        topic.publish("packageMapDeleted", context.tabId);
                    },
                    error: function (errMsg, errStack) {
                        context.showErrors(errMsg, errStack);
                    }
                });
            }
        }
    });
});
