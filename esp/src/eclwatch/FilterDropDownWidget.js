define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-style",

    "dijit/registry",
    "dijit/form/Select",

    "hpcc/_Widget",
    "src/Utility",

    "dojo/text!../templates/FilterDropDownWidget.html",

    "dijit/form/DropDownButton",
    "dijit/TooltipDialog",
    "dijit/form/CheckBox",
    "dijit/form/Form",
    "dijit/form/Button",

    "hpcc/TableContainer"

], function (declare, i18n, nlsHPCC, arrayUtil, dom, domStyle,
    registry, Select,
    _Widget, Utility,
    template) {
    return declare("FilterDropDownWidget", [_Widget], {
        templateString: template,
        baseClass: "FilterDropDownWidget",
        i18n: nlsHPCC,

        _width: "100%",
        iconFilter: null,
        filterDropDown: null,
        filterForm: null,
        filterLabel: null,
        filterMessage: null,
        tableContainer: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.filterDropDown = registry.byId(this.id + "FilterDropDown");
            this.filterForm = registry.byId(this.id + "FilterForm");
            this.filterLabel = registry.byId(this.id + "FilterLabel");
            this.tableContainer = registry.byId(this.id + "TableContainer");
            this.filterApply = registry.byId(this.id + "FilterApply");
            this.filterClear = registry.byId(this.id + "FilterClear");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.iconFilter = dom.byId(this.id + "IconFilter");
        },

        //  Hitched actions  ---
        _onFilterClear: function (event) {
            this.emit("clear");
            this.clear();
        },

        _onFilterApply: function (event) {
            this.filterDropDown.closeDropDown();
            this.emit("apply");
            this.refreshState();
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

        setFilterMessage: function (value) {
            dom.byId("FilterMessage").textContent = value;
            this.refreshState();
        },

        exists: function () {
            var filter = this.toObject();
            for (var key in filter) {
                if (filter[key] !== "") {
                    return true;
                }
            }
            return false;
        },

        toObject: function () {
            if (this.filterDropDown.get("disabled")) {
                return {};
            }
            var retVal = {};
            arrayUtil.forEach(this.filterForm.getDescendants(), function (item, idx) {
                var name = item.get("name");
                if (name) {
                    var value = item.get("value");
                    if (value) {
                        retVal[name] = value;
                    }
                }
            });
            return retVal;
        },

        fromObject: function (obj) {
            arrayUtil.forEach(this.filterForm.getDescendants(), function (item, idx) {
                var value = obj[item.get("name")];
                if (value) {
                    item.set("value", value);
                    if (item.defaultValue !== undefined) {
                        item.defaultValue = value;
                    }
                }
            });
            this.refreshState();
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;
        },

        open: function (event) {
            this.filterDropDown.focus();
            this.filterDropDown.openDropDown();
        },

        close: function (event) {
            this.filterDropDown.closeDropDown();
        },

        disable: function (disable) {
            this.filterDropDown.set("disabled", disable);
        },

        reset: function (disable) {
            this.filterForm.reset();
        },

        refreshState: function () {
            if (this.exists()) {
                this.iconFilter.src = Utility.getImageURL("filter1.png");
                dom.byId(this.id + "FilterDropDown_label").innerHTML = this.params.ownLabel !== undefined && this.params.ownLabel !== null ? this.params.ownLabel : this.i18n.FilterSet;
                domStyle.set(this.id + "FilterDropDown_label", {
                    "font-weight": "bold"
                });
            } else {
                this.iconFilter.src = Utility.getImageURL("noFilter1.png");
                dom.byId(this.id + "FilterDropDown_label").innerHTML = this.i18n.Filter;
                domStyle.set(this.id + "FilterDropDown_label", {
                    "font-weight": "normal"
                });
            }
        }
    });
});
