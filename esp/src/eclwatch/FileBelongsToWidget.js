define([
    "dojo/_base/declare",
    "src/nlsHPCC",

    "dgrid/selector",

    "hpcc/DelayLoadWidget",
    "hpcc/GridDetailsWidget",
    "src/ESPLogicalFile",
    "src/ESPUtil",

], function (declare, nlsHPCCMod,
    selector,
    DelayLoadWidget, GridDetailsWidget, ESPLogicalFile, ESPUtil) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("FileBelongsToWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        logicalFile: null,

        gridTitle: nlsHPCC.SuperFilesBelongsTo,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this.logicalFile = ESPLogicalFile.Get(params.NodeGroup, params.Name);
            this.refreshGrid();
        },

        createGrid: function (domID) {
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    sel: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    Name: { label: this.i18n.Name }
                }
            }, domID);
            return retVal;
        },

        createDetail: function (id, row, params) {
            return new DelayLoadWidget({
                id: id,
                title: row.Name,
                closable: true,
                delayWidget: "SFDetailsWidget",
                hpcc: {
                    type: "SFDetailsWidget",
                    params: {
                        Name: row.Name
                    }
                }
            });
        },

        refreshGrid: function (args) {
            var context = this;
            if (this.logicalFile.Superfiles.DFULogicalFile) {
                context.store.setData(this.logicalFile.Superfiles.DFULogicalFile);
                context.grid.refresh();
            }
        }
    });
});
