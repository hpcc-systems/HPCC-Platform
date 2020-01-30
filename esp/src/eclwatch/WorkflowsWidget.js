define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",

    "hpcc/GridDetailsWidget",
    "src/ESPWorkunit",
    "src/ESPUtil"

], function (declare, i18n, nlsHPCC,
    GridDetailsWidget, ESPWorkunit, ESPUtil) {
    return declare("WorkflowsWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.Workflows,
        idProperty: "WFID",

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
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    EventName: { label: this.i18n.Name, width: 180 },
                    EventText: { label: this.i18n.Subtype },
                    Count: {
                        label: this.i18n.Count, width: 180,
                        formatter: function (count) {
                            if (count === -1) {
                                return 0;
                            }
                            return count;
                        }
                    },
                    CountRemaining: {
                        label: this.i18n.Remaining, width: 180,
                        formatter: function (countRemaining) {
                            if (countRemaining === -1) {
                                return 0;
                            }
                            return countRemaining;
                        }
                    }
                }
            }, domID);
            return retVal;
        },

        refreshGrid: function (args) {
            var context = this;
            this.wu.getInfo({
                onGetWorkflows: function (workflows) {
                    context.store.setData(workflows);
                    context.grid.refresh();
                }
            });
        }
    });
});
