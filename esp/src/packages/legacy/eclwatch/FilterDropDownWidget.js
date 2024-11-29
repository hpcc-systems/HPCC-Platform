define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-style",

    "dijit/registry",
    "dijit/form/Select",

    "hpcc/_Widget",
    "src/Utility",
    "src/react/index",
    "src/UserPreferences/Recent",

    "dojo/text!../templates/FilterDropDownWidget.html",

    "dijit/form/DropDownButton",
    "dijit/TooltipDialog",
    "dijit/form/CheckBox",
    "dijit/form/Form",
    "dijit/form/Button",

    "hpcc/TableContainer"

], function (declare, nlsHPCCMod, arrayUtil, dom, domStyle,
    registry, Select,
    _Widget, Utility, srcReact, Recent,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
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
        username: null,

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

        destroy: function (args) {
            srcReact.unrender(this.recentFilterNode);
            this.inherited(arguments);
        },

        //  Hitched actions  ---
        _onFilterClear: function (event) {
            this.emit("clear");
            this.clear();
        },

        _onFilterApply: function (event) {
            if (event) {
                var formData = this.toObject(event.currentTarget.form);
                this.initRecentFilter(formData);
            }
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

        toObject: function (filter) {
            var context = this;
            if (this.filterDropDown.get("disabled")) {
                return {};
            }
            var retVal = {};
            arrayUtil.forEach(this.filterForm.getDescendants() || filter, function (item, idx) {
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

            this.userName = dojoConfig.username;
            var recentFilterLoaded = false;
            if (this.userName !== null) {
                this.recentFilterNode = dom.byId(this.id + "RecentFilters");

                if (params.widget && !recentFilterLoaded) {
                    srcReact.render(srcReact.RecentFilters, { ws_key: params.ws_key, widget: params.widget, filter: {} }, this.recentFilterNode);
                    recentFilterLoaded = true;
                }
            }
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

        initRecentFilter: function (retVal) {
            var context = this;
            if (this.userName !== null) {
                if (!Utility.isObjectEmpty(retVal)) {
                    Recent.addToStack(this.params.ws_key, retVal, 5, true).then(function (val) {
                        context.loadRecentFilters(retVal);
                    });
                }
            }
        },

        loadRecentFilters: function (retVal) {
            this.recentFilterNode = dom.byId(this.id + "RecentFilters");

            if (this.params.widget) {
                srcReact.render(srcReact.RecentFilters, { ws_key: this.params.ws_key, widget: this.params.widget, filter: retVal }, this.recentFilterNode);
            }
        },

        refreshState: function () {
            if (this.exists()) {
                this.iconFilter.style.color = "#1A9BD7";
                this.iconFilter.title = this.i18n.Filter;
                dom.byId(this.id + "FilterDropDown_label").innerHTML = this.params.ownLabel !== undefined && this.params.ownLabel !== null ? this.params.ownLabel : this.i18n.FilterSet;
                domStyle.set(this.id + "FilterDropDown_label", {
                    "font-weight": "bold"
                });
            } else {
                this.iconFilter.style.color = "lightgrey";
                this.iconFilter.title = this.i18n.Filter;
                dom.byId(this.id + "FilterDropDown_label").innerHTML = this.i18n.Filter;
                domStyle.set(this.id + "FilterDropDown_label", {
                    "font-weight": "normal"
                });
            }
        }
    });
});
