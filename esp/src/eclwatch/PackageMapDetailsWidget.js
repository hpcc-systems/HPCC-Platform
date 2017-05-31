/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.
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
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/topic",
    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "hpcc/DelayLoadWidget",
    "hpcc/PackageSourceWidget",
    "hpcc/PackageMapPartsWidget",
    "hpcc/WsPackageMaps",

    "dojo/text!../templates/PackageMapDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/TextBox",
    "dijit/form/Button",
    "dijit/Toolbar"
], function (declare, lang, i18n, nlsHPCC, dom, domAttr, domClass, topic, registry,
    _TabContainerWidget, DelayLoadWidget, PackageSourceWidget, PackageMapPartsWidget, WsPackageMaps, template) {
    return declare("PackageMapDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "PackageMapDetailsWidget",
        i18n: nlsHPCC,
        borderContainer: null,
        tabContainer: null,
        validateWidget: null,
        validateWidgetLoaded: false,
        xmlWidget: null,
        xmlWidgetLoaded: false,
        partsWidget: null,
        partsWidgetLoaded: false,

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
            this.partsWidget = registry.byId(this.id + "Parts");

            var context = this;
            this.tabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                if (nval.id === context.id + "Validate" && !context.validateWidgetLoaded) {
                    context.validateWidgetLoaded = true;
                    context.validateWidget.init({
                        target: context.target,
                        process: context.process,
                        packageMap: context.packageMap
                    });
                } else if (nval.id === context.id + "XML" && !context.xmlWidgetLoaded) {
                    context.xmlWidgetLoaded = true;
                    context.xmlWidget.init({
                        target: context.target,
                        process: context.process,
                        packageMap: context.packageMap
                    });
                } else if (nval.id === context.id + "Parts" && !context.partsWidgetLoaded) {
                    context.partsWidgetLoaded = true;
                    context.partsWidget.init({
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
	    if (this.inherited(arguments))
                return;

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
                if (params.active === true)
                    domClass.add(this.id + "StateIdImage", "iconRunning");
                else
                    domClass.add(this.id + "StateIdImage", "iconArchived");
            }
            this.refreshActionState();
        },

        refreshActionState: function () {
            registry.byId(this.id + "Activate").set("disabled", this.active);
            registry.byId(this.id + "Deactivate").set("disabled", !this.active);
            domAttr.set(this.id + "StateIdImage", "title", this.active? this.i18n.Active:this.i18n.NotActive);
        },

        showErrors: function (err) {
            topic.publish("hpcc/brToaster", {
                Severity: "Error",
                Source: err.message,
                Exceptions: [{ Message: err.stack }]
            });
        },

        _onActivate: function (event) {
            var context = this;
            var packageMaps = [];
            packageMaps[0] = {Target:this.target,
                Process:this.process,Id:this.packageMap};

            WsPackageMaps.activatePackageMap(packageMaps).then(function (response) {
                domClass.replace(context.id + "StateIdImage", "iconRunning");
                context.active = true;
                context.refreshActionState();
                return response;
            }, function (err) {
                context.showErrors(err);
                return err;
            });
        },
        _onDeactivate: function (event) {
            var context = this;
            var packageMaps = [];
            packageMaps[0] = {Target:this.target,
                Process:this.process,Id:this.packageMap};

            WsPackageMaps.deactivatePackageMap(packageMaps).then(function (response) {
                domClass.replace(context.id + "StateIdImage", "iconArchived");
                context.active = false;
                context.refreshActionState();
                return response;
            }, function (err) {
                context.showErrors(err);
                return err;
            });
        },
        _onDelete: function (event) {
            if (confirm(this.i18n.DeleteThisPackage)) {
                var context = this;
                var packageMaps = [];
                packageMaps[0] = {Target:this.target,
                    Process:this.process,Id:this.packageMap};

                WsPackageMaps.deletePackageMap(packageMaps).then(function (response) {
                    topic.publish("packageMapDeleted", context.tabId);
                    return response;
                }, function (err) {
                    context.showErrors(err);
                    return err;
                });
            }
        }
    });
});
