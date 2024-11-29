define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",

    "dijit/registry",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/WsDfu",
    "src/ESPUtil"
], function (declare, lang, nlsHPCCMod, arrayUtil,
    registry, selector,
    GridDetailsWidget, WsDfu, ESPUtil) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("FileProtectListWidget", [GridDetailsWidget, ESPUtil.FormHelper], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_ProtectBy,
        idProperty: "__hpcc_id",

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this._refreshActionState();
            this.refreshGrid();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.id + "_Grid") {
                    this.refreshGrid();
                } else if (currSel.id === this.fileProtectListWidget.id && !this.fileProtectListWidget.initalized) {
                    this.fileProtectListWidget.init({
                        firstLoad: true
                    });
                } else {
                    currSel.init(currSel.params);
                }
            }
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.openButton = registry.byId(this.id + "Open");
            this.refreshButton = registry.byId(this.id + "Refresh");
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                columns: {
                    col1: selector({
                        width: 10,
                        selectorType: "checkbox",
                        sortable: false,
                        unhidable: true
                    }),
                    Owner: {
                        width: 200,
                        label: nlsHPCC.Owner,
                        sortable: false
                    },
                    Modified: { label: nlsHPCC.Modified, width: 200, sortable: false },
                }
            }, domID);

            retVal.on(".dgrid-row:dblclick", function (evt) {
                evt.preventDefault();
                context.grid.refresh();
            });

            return retVal;
        },

        _onRefresh: function () {
            this.refreshGrid();
        },

        refreshGrid: function () {
            var context = this;
            WsDfu.DFUInfo({
                request: {
                    Name: context.params.Name
                }
            }).then(function (response) {
                var results = [];

                if (lang.exists("DFUInfoResponse.FileDetail.ProtectList.DFUFileProtect", response)) {
                    arrayUtil.forEach(response.DFUInfoResponse.FileDetail.ProtectList.DFUFileProtect, function (row, idx) {
                        lang.mixin(row, {
                            Owner: row.Owner,
                            Modified: row.Modified,
                        });
                        results.push(row);
                    });
                }

                context.store.setData(results);
                context.grid.set("query", {});
            });
        }
    });
});
