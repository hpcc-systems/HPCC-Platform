define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",

    "hpcc/GridDetailsWidget",
    "src/ESPWorkunit",
    "src/ESPUtil"

], function (declare, lang, i18n, nlsHPCC, arrayUtil,
    GridDetailsWidget, ESPWorkunit, ESPUtil) {
    return declare("VariablesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.Variables,
        idProperty: "__hpcc_id",

        wu: null,

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 === 0) {
                        context.refreshGrid();
                    }
                });
            }
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var retVal = new declare([ESPUtil.Grid(true, false)])({
                store: this.store,
                columns: {
                    Type: { label: this.i18n.Type, width: 180 },
                    Name: { label: this.i18n.Name, width: 360 },
                    Value: { label: this.i18n.Value }
                }
            }, domID);
            return retVal;
        },

        refreshGrid: function (args) {
            var context = this;
            var variables = [];
            this.wu.getInfo({
                onGetVariables: function (vars) {
                    arrayUtil.forEach(vars, function (item, idx) {
                        variables.push(lang.mixin({
                            Type: context.i18n.ECL
                        }, item));
                    })
                },
                onGetApplicationValues: function (values) {
                    arrayUtil.forEach(values, function (item, idx) {
                        variables.push(lang.mixin({
                            Type: item.Application
                        }, item));
                    })
                },
                onGetDebugValues: function (values) {
                    arrayUtil.forEach(values, function (item, idx) {
                        variables.push(lang.mixin({
                            Type: context.i18n.Debug
                        }, item));
                    })
                },
                onAfterSend: function (wu) {
                    context.store.setData(variables);
                    context.grid.refresh();
                }
            });
        }
    });
});
