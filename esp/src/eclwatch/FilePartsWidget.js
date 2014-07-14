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
    "dojo/_base/array",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/_Widget",

    "dojo/text!../templates/FilePartsWidget.html"
],
    function (declare, lang, i18n, nlsHPCC, array, Memory, Observable,
            registry,
            OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
            _Widget,
            template) {
        return declare("FilePartsWidget", [_Widget], {
            templateString: template,
            baseClass: "FilePartsWidget",
            i18n: nlsHPCC,
            filePartsGrid: null,

            dataStore: null,

            lastSelection: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
            },

            startup: function (args) {
                this.inherited(arguments);
                var store = new Memory({
                    idProperty: "__hpcc_id",
                    data: []
                });
                this.filePartsStore = Observable(store);

                this.filePartsGrid = new declare([OnDemandGrid, Keyboard, ColumnResizer, DijitRegistry])({
                    allowSelectAll: true,
                    columns: {
                        Id: { label: this.i18n.Part, width: 40 },
                        Copy: { label: this.i18n.Copy, width: 40 },
                        Ip: { label: this.i18n.IP },
                        Cluster: { label: this.i18n.Cluster, width: 108 },
                        Partsize: { label: this.i18n.Size, width: 120 },
                        ActualSize: { label: this.i18n.ActualSize, width: 120 }
                    },
                    store: this.filePartsStore
                }, this.id + "FilePartsGrid");
                this.filePartsGrid.startup();
            },

            resize: function (args) {
                this.inherited(arguments);
                this.filePartsGrid.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            //  Plugin wrapper  ---
            init: function (params) {
                if (this.inherited(arguments))
                    return;

                this.filePartsStore.setData(params.fileParts);
                this.filePartsGrid.set("query", {});
            }
        });
    });
