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
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/on",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/WsDfu",
    "hpcc/ESPUtil",
    "hpcc/FilterDropDownWidget",

    "dojo/text!../templates/DiskUsageWidget.html"
], function (declare, lang, i18n, nlsHPCC, on,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                WsDfu, ESPUtil, FilterDropDownWidget,
                template) {
    return declare("DiskUsageWidget", [_Widget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "DiskUsageWidget",
        i18n: nlsHPCC,

        resize: function (args) {
            this.inherited(arguments);
            this.widget.BorderContainer.resize();
        },

        getTitle: function () {
            return this.i18n.title_DiskUsage;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.initDiskUsageGrid();

            this.widget.Filter.refreshState();
            var context = this;
            this.widget.Filter.on("clear", function (evt) {
                context.refreshGrid();
            });
            this.widget.Filter.on("apply", function (evt) {
                context.refreshGrid();
            });
        },

        initDiskUsageGrid: function () {
            var store = new WsDfu.CreateDiskUsageStore();
            this.diskUsageGrid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: store,
                query: this.getFilter(),
                columns: {
                    Name: { label: this.i18n.Grouping, width: 90, sortable: true },
                    NumOfFiles: { label: this.i18n.FileCounts, width: 90, sortable: true },
                    TotalSize: { label: this.i18n.TotalSize, width: 90, sortable: true },
                    LargestFile: { label: this.i18n.LargestFile, sortable: true },
                    LargestSize: { label: this.i18n.LargestSize, width: 90, sortable: true },
                    SmallestFile: { label: this.i18n.SmallestFile, sortable: true },
                    SmallestSize: { label: this.i18n.SmallestSize, width: 90, sortable: true },
                    NumOfFilesUnknown: { label: this.i18n.FilesWithUnknownSize, width: 90, sortable: true }
                }
            }, this.id + "DiskUsageGrid");
        },

        getFilter: function () {
            var retVal = this.widget.Filter.toObject();
            lang.mixin(retVal, {
                StartDate: this.getISOString("FromDate", "FromTime"),
                EndDate: this.getISOString("ToDate", "ToTime")
            });
            return retVal;
        },

        refreshGrid: function (clearSelection) {
            this.diskUsageGrid.set("query", this.getFilter());
            if (clearSelection) {
                this.diskUsageGrid.clearSelection();
            }
        }
    });
});
