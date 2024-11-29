define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/ganglia",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-form",

    "dijit/registry",
    "dijit/form/Select",

    "hpcc/_Widget",

    "dojo/text!./templates/GangliaFilterDropDownWidget.html",

    "dijit/form/DropDownButton",
    "dijit/TooltipDialog",
    "dijit/form/Form",
    "dijit/form/Button",

    "hpcc/TableContainer"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domForm,
    registry, Select,
    _Widget,
    template) {
        return declare("GangliaFilterDropDownWidget", [_Widget], {
            templateString: template,
            baseClass: "GangliaFilterDropDownWidget",
            i18n: nlsHPCC,

            _width: "660px",
            iconFilter: null,
            filterDropDown: null,
            filterForm: null,

            postCreate: function (args) {
                this.inherited(arguments);
                this.filterDropDown = registry.byId(this.id + "FilterDropDown");
                this.filterForm = registry.byId(this.id + "FilterForm");
            },

            startup: function (args) {
                this.inherited(arguments);
                this.iconFilter = dom.byId(this.id + "IconFilter");
            },

            //  Hitched actions  ---
            _onFilterClear: function (event) {
                this.clear();
            },

            _onFilterApply: function (event) {
                this.filterDropDown.closeDropDown();
                this.emit("apply");
            },

            //  Implementation  ---
            clear: function () {
                arrayUtil.forEach(this.filterForm.getDescendants(), function (item, idx) {
                    if (item instanceof Select) {
                        item.set("value", "");
                    } else {
                        item.set("value", null);
                    }
                });
            },

            setValue: function (id, value) {
                registry.byId(id).set("value", value);
                this.refreshState();
            },

            exists: function () {
                var filter = this.toObject();
                for (var key in filter) {
                    if (filter[key] != "") {
                        return true;
                    }
                }
                return false;
            },

            toObject: function () {
                if (this.filterDropDown.get("disabled")) {
                    return {};
                }
                return domForm.toObject(this.filterForm.id);
            },

            init: function (params) {
                if (this.inherited(arguments))
                    return;
            },

            open: function (event) {
                this.filterDropDown.openDropDown();
            },

            close: function (event) {
                this.filterDropDown.closeDropDown();
            },

            disable: function (disable) {
                this.filterDropDown.set("disabled", disable);
            }
        });
    });
