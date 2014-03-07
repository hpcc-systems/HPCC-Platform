/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-form",
    "dojo/on",

    "dijit/registry",
    "dijit/form/Select",

    "hpcc/_Widget",

    "dojo/text!../templates/FilterDropDownWidget.html",

    "dijit/form/DropDownButton",
    "dijit/TooltipDialog",
    "dijit/form/Form",
    "dijit/form/Button",

    "hpcc/TableContainer"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domForm, on,
                registry, Select,
                _Widget,
                template) {
    return declare("FilterDropDownWidget", [_Widget], {
        templateString: template,
        baseClass: "FilterDropDownWidget",
        i18n: nlsHPCC,

        _width:"460px",
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
            this.filterDropDown.closeDropDown();
            this.emit("clear");
            this.refreshState();
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
            var retVal = domForm.toObject(this.filterForm.id);
            return retVal;
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;
        },

        refreshState: function () {
            this.iconFilter.src = this.exists() ? dojoConfig.getImageURL("filter.png") : dojoConfig.getImageURL("noFilter.png");
        }
    });
});
