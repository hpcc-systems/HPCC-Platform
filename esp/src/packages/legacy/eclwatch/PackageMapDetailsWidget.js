define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/topic",
    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "src/Clippy",
    "src/WsPackageMaps",

    "dojo/text!../templates/PackageMapDetailsWidget.html",

    "hpcc/PackageSourceWidget",
    "hpcc/PackageMapPartsWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/TextBox",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/Toolbar"
], function (declare, nlsHPCCMod, domAttr, domClass, topic, registry,
    _TabContainerWidget, Clippy, WsPackageMaps, template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("PackageMapDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "PackageMapDetailsWidget",
        i18n: nlsHPCC,

        borderContainer: null,
        tabContainer: null,
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
            this.xmlWidget = registry.byId(this.id + "XML");
            this.partsWidget = registry.byId(this.id + "Parts");

            var context = this;
            this.tabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                if (nval.id === context.id + "XML" && !context.xmlWidgetLoaded) {
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
            Clippy.attach(this.id + "ClippyButton");
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
                registry.byId(this.id + "_Summary").set("title", params.packageMap);
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
            domAttr.set(this.id + "StateIdImage", "title", this.active ? this.i18n.Active : this.i18n.NotActive);
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
            packageMaps[0] = {
                Target: this.target,
                Process: this.process, Id: this.packageMap
            };

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
            packageMaps[0] = {
                Target: this.target,
                Process: this.process, Id: this.packageMap
            };

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
                packageMaps[0] = {
                    Target: this.target,
                    Process: this.process, Id: this.packageMap
                };

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
