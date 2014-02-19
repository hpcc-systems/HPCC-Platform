define([
    "dojo/_base/declare", // declare
    "dojo/io-query",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry"

], function (declare, ioQuery,
    _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry) {

    return declare("_Widget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        baseClass: "_Widget",
        
        initalized: false,

        _onNewPage: function (event) {
            var baseUrl = document.URL.split("?")[0];
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

        //  Usefull functions  ---
        setDisabled: function (id, disabled, icon, disabledIcon) {
            var target = registry.byId(id);
            if (target) {
                target.set("disabled", disabled);
                target.set("iconClass", disabled ? disabledIcon : icon);
            }
        },

        endsWith: function (str, suffix) {
            return str.indexOf(suffix, str.length - suffix.length) !== -1;
        }
    });
});
