define([
    "dojo/_base/declare", // declare
    "dojo/io-query",
    "dojo/dom",
    "dojo/dom-attr",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry"

], function (declare, ioQuery, dom, domAttr,
    _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry) {

    return declare("_Widget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        baseClass: "_Widget",
        
        initalized: false,

        constructor: function (args) {
            this.widget = {};
        },

        startup: function (args) {
            this.inherited(arguments);
            this.registerChildWidgets(this.domNode);
        },

        _onNewPage: function (event) {
            var baseUrl = document.URL.split("#")[0];
            baseUrl = baseUrl.split("?")[0];
            var paramsString = ioQuery.objectToQuery(this.params);
            var win = window.open(baseUrl + "?" + paramsString, "_blank");
            win.focus();
        },

        init: function (params) {
            if (this.initalized)
                return true;
            this.initalized = true;
            this.params = params;
            if (!this.params.Widget) {
                this.params.Widget = this.declaredClass;
            }
            
            return false;
        },

        //  Dijit functions  ---
        registerChildWidgets: function (domNode) {
            var childWidgets = registry.findWidgets(domNode);
            for (var i = 0; i < childWidgets.length; ++i) {
                var childWidget = childWidgets[i];
                if (!childWidget.registerChildWidgets) {
                    this.registerChildWidgets(childWidget.domNode);
                }
                if (childWidget.params && childWidget.params.id) {
                    if (childWidget.params.id.indexOf(this.id) === 0) {
                        this.widget[childWidget.params.id.substring(this.id.length)] = childWidget;
                    }
                }
            }
        },

        //  DOM functions  ---
        setDisabled: function (id, disabled, icon, disabledIcon) {
            var target = registry.byId(id);
            if (target) {
                target.set("disabled", disabled);
                target.set("iconClass", disabled ? disabledIcon : icon);
            }
        },

        updateInput: function (name, oldValue, newValue) {
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            } else {
                var domElem = dom.byId(this.id + name);
                if (domElem) {
                    switch (domElem.tagName) {
                        case "SPAN":
                        case "DIV":
                            domAttr.set(this.id + name, "innerHTML", newValue);
                            break;
                        case "INPUT":
                        case "TEXTAREA":
                            domAttr.set(this.id + name, "value", newValue);
                            break;
                        default:
                            throw new Error("Unknown DOM element:  " + domElem.tagName);
                    }
                }
            }
        },


        //  String functions  ---
        endsWith: function (str, suffix) {
            return str.indexOf(suffix, str.length - suffix.length) !== -1;
        }
    });
});
