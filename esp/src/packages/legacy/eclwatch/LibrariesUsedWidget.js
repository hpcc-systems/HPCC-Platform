define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",

    "hpcc/GridDetailsWidget",
    "src/ESPQuery",
    "src/ESPUtil"
], function (declare, lang, nlsHPCCMod, arrayUtil,
    GridDetailsWidget, ESPQuery, ESPUtil) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("LibrariesUsedWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_LibrariesUsed,
        idProperty: "__hpcc_id",

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.query = ESPQuery.Get(params.QuerySetId, params.Id);

            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                sort: [{ attribute: "__hpcc_id" }],
                columns: {
                    Name: { label: this.i18n.LibrariesUsed }
                }
            }, domID);
            return retVal;
        },

        refreshGrid: function (args) {
            var context = this;
            this.query.refresh().then(function (response) {
                var librariesUsed = [];
                if (lang.exists("LibrariesUsed.Item", context.query)) {
                    arrayUtil.forEach(context.query.LibrariesUsed.Item, function (item, idx) {
                        var file = {
                            __hpcc_id: idx,
                            Name: item
                        };
                        librariesUsed.push(file);
                    });
                }
                context.store.setData(librariesUsed);
                context.grid.refresh();
            });
        }
    });
});
