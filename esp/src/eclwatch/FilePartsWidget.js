define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "src/Memory",
    "dojo/store/Observable",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/_Widget",

    "@hpcc-js/common",

    "dojo/text!../templates/FilePartsWidget.html",

    "dijit/layout/ContentPane"
],
    function (declare, nlsHPCCMod, MemoryMod, Observable,
        OnDemandGrid, Keyboard, ColumnResizer, DijitRegistry,
        _Widget,
        hpccCommon,
        template) {

        var formatNum = hpccCommon.format(",");

        var nlsHPCC = nlsHPCCMod.default;

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
                var store = new MemoryMod.Memory("__hpcc_id");
                this.filePartsStore = new Observable(store);

                this.filePartsGrid = new declare([OnDemandGrid, Keyboard, ColumnResizer, DijitRegistry])({
                    allowSelectAll: true,
                    columns: {
                        Id: { label: this.i18n.Part, width: 40 },
                        Copy: { label: this.i18n.Copy, width: 40 },
                        Ip: { label: this.i18n.IP },
                        Cluster: { label: this.i18n.Cluster, width: 108 },
                        PartSizeInt64: {
                            label: this.i18n.Size, width: 120,
                            formatter: function (intsize) {
                                return formatNum(intsize);
                            }
                        },
                        CompressedSize: {
                            label: this.i18n.CompressedSize, width: 120,
                            formatter: function (intsize) {
                                return formatNum(intsize);
                            }
                        }
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
